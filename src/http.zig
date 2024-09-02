const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const posix = std.posix;
const socket_t = std.posix.socket_t;
const fd_t = std.posix.fd_t;

const protocol = @import("protocol.zig");
const Options = protocol.Options;
const Io = @import("io.zig").Io;
const Op = @import("io.zig").Op;
const Error = @import("io.zig").Error;
const Timer = @import("io.zig").Timer;
const Server = @import("tcp.zig").Server;

const log = std.log.scoped(.http);

pub const Listener = struct {
    allocator: mem.Allocator,
    server: *Server,
    options: Options,
    io: *Io,
    op: Op = undefined,
    accepted: usize = 0,
    completed: usize = 0,

    pub fn init(allocator: mem.Allocator, io: *Io, server: *Server, options: Options) !Listener {
        return .{
            .allocator = allocator,
            .server = server,
            .options = options,
            .io = io,
        };
    }

    pub fn deinit(self: *Listener) void {
        _ = self;
    }

    pub fn accept(self: *Listener, socket: socket_t) !void {
        try self.io.accept(&self.op, socket, self, accepted, failed);
    }

    fn accepted(self: *Listener, socket: socket_t) Error!void {
        var conn = try self.allocator.create(Conn);
        conn.* = Conn{
            .allocator = self.allocator,
            .socket = socket,
            .listener = self,
            .io = self.io,
        };
        try conn.init();
        self.accepted +%= 1;
    }

    fn failed(_: *Listener, err: anyerror) Error!void {
        log.err("accept failed {}", .{err});
        // TODO: handle this
    }

    fn release(self: *Listener, conn: *Conn) void {
        self.allocator.destroy(conn);
        self.completed +%= 1;
    }
};

pub const Conn = struct {
    allocator: mem.Allocator,
    io: *Io,
    socket: socket_t = 0,
    listener: *Listener,

    recv_op: ?*Op = null,
    send_op: ?*Op = null,

    send_vec: [2]posix.iovec_const = undefined, // header and body
    send_msghdr: posix.msghdr_const = .{
        .iov = undefined,
        .iovlen = undefined,
        .name = null,
        .namelen = 0,
        .control = null,
        .controllen = 0,
        .flags = 0,
    },

    fn init(self: *Conn) !void {
        self.recv_op = try self.io.recv(self.socket, self, received, recvFailed);
    }

    fn received(self: *Conn, bytes: []const u8) Error!void {
        if (self.send_op != null)
            return try self.close();

        const body = self.handle(bytes) catch |err| {
            const rsp = switch (err) {
                error.NotFound => not_found,
                else => bad_request,
            };
            self.send_vec[0] = .{ .base = rsp.ptr, .len = rsp.len };
            self.send_vec[1].len = 0;
            return self.send(1);
        };

        const header = try std.fmt.allocPrint(
            self.allocator,
            header_template,
            .{ body.len, "text/plain" },
        );
        self.send_vec[0] = .{ .base = header.ptr, .len = header.len };
        self.send_vec[1] = .{ .base = body.ptr, .len = body.len };
        try self.send(2);
    }

    fn handle(self: *Conn, bytes: []const u8) ![]const u8 {
        var hp: std.http.HeadParser = .{};
        const head_end = hp.feed(bytes);
        if (hp.state != .finished) return error.BadRequest;
        const head = try std.http.Server.Request.Head.parse(bytes[0..head_end]);
        const content_length = if (head.content_length) |l| l else 0;
        if (content_length != bytes[head_end..].len) {
            log.err("{} receive buffer overflow method: {}, target: {s}, content length: {}", .{ self.socket, head.method, head.target, content_length });
            return error.BadRequest;
        }

        if (std.mem.startsWith(u8, head.target, "/stat"))
            return try self.statResponse();

        log.debug("{} method: {}, target: {s}, content length: {}", .{ self.socket, head.method, head.target, content_length });
        return error.NotFound;
    }

    fn send(self: *Conn, vec_len: usize) !void {
        assert(self.send_op == null);
        self.send_msghdr.iov = &self.send_vec;
        self.send_msghdr.iovlen = @intCast(vec_len);
        self.send_op = try self.io.sendv(self.socket, &self.send_msghdr, self, sent, sendFailed);
    }

    fn sent(self: *Conn, _: usize) Error!void {
        self.send_op = null;
        if (self.send_vec[1].len > 0) {
            var buf: []const u8 = undefined;
            buf.ptr = self.send_vec[0].base;
            buf.len = self.send_vec[0].len;
            self.allocator.free(buf);
            buf.ptr = self.send_vec[1].base;
            buf.len = self.send_vec[1].len;
            self.allocator.free(buf);
        }
    }

    fn sendFailed(self: *Conn, err: anyerror) Error!void {
        self.send_op = null;
        switch (err) {
            error.BrokenPipe, error.ConnectionResetByPeer => {},
            else => log.err("{} send failed {}", .{ self.socket, err }),
        }
        try self.close();
    }

    fn recvFailed(self: *Conn, err: anyerror) Error!void {
        self.recv_op = null;
        switch (err) {
            error.EndOfFile => {},
            error.ConnectionResetByPeer => {},
            else => log.err("{} recv failed {}", .{ self.socket, err }),
        }
        try self.close();
    }

    fn close(self: *Conn) !void {
        log.debug("{} close", .{self.socket});
        if (self.recv_op) |op| {
            try op.cancel();
            op.unsubscribe(self);
        }
        if (self.send_op) |op| op.unsubscribe(self);
        try self.io.close(self.socket);
        self.listener.release(self);
    }

    fn statResponse(self: *Conn) ![]u8 {
        var al = std.ArrayList(u8).init(self.allocator);
        try dumpStat(al.writer().any(), self.listener.server);
        return al.toOwnedSlice();
        //return try self.allocator.dupe(u8, "Hello world!");
    }
};

const header_template =
    "HTTP/1.1 200 OK\r\n" ++
    "Content-Length: {d}\r\n" ++
    "Content-Type: {s}\r\n\r\n";

const not_found =
    "HTTP/1.1 404 Not Found\r\n" ++
    "content-length: 9\r\n" ++
    "content-type: text/plain\r\n\r\n" ++
    "Not Found";

const bad_request =
    "HTTP/1.1 404 Bad Request\r\n" ++
    "content-length: 11\r\n" ++
    "content-type: text/plain\r\n\r\n" ++
    "Bad Request";

fn dumpStat(w: anytype, server: *Server) !void {
    //const print = writer.print;
    // const print = std.debug.print;
    // print("listener connections:\n", .{});
    // print("  active {}, accepted: {}, completed: {}\n", .{ listener.accepted - listener.completed, listener.accepted, listener.completed });
    // print("io operations: loops: {}, cqes: {}, cqes/loop {}\n", .{
    //     io.stat.loops,
    //     io.stat.cqes,
    //     if (io.stat.loops > 0) io.stat.cqes / io.stat.loops else 0,
    // });
    // print("  all    {}\n", .{io.stat.all});
    // print("  recv   {}\n", .{io.stat.recv});
    // print("  sendv  {}\n", .{io.stat.sendv});
    // print("  ticker {}\n", .{io.stat.ticker});
    // print("  close  {}\n", .{io.stat.close});
    // print("  accept {}\n", .{io.stat.accept});
    // print(
    //     "  receive buffers group:\n    success: {}, no-buffs: {} {d:5.2}%\n",
    //     .{ io.recv_buf_grp_stat.success, io.recv_buf_grp_stat.no_bufs, io.recv_buf_grp_stat.noBufs() },
    // );

    try w.print("server topics: {}\n", .{server.topics.count()});
    var ti = server.topics.iterator();
    while (ti.next()) |te| {
        const topic_name = te.key_ptr.*;
        const topic = te.value_ptr.*;
        const size = topic.messages.size();
        try w.print("  {s} messages: {d} bytes: {} {}Mb {}Gb, sequence: {}\n", .{
            topic_name,
            topic.messages.count(),
            size,
            size / 1024 / 1024,
            size / 1024 / 1024 / 1024,
            topic.sequence,
        });

        var ci = topic.channels.iterator();
        while (ci.next()) |ce| {
            const channel_name = ce.key_ptr.*;
            const channel = ce.value_ptr.*;
            try w.print("  --{s} consumers: {},  in flight messages: {}, deferred: {}, offset: {}\n", .{
                channel_name,
                channel.consumers.items.len,
                channel.in_flight.count(),
                channel.deferred.count(),
                channel.offset,
            });
            try w.print("    pull: {}, send: {}, finish: {}, timeout: {}, requeue: {}\n", .{
                channel.stat.pull,
                channel.stat.send,
                channel.stat.finish,
                channel.stat.timeout,
                channel.stat.requeue,
            });
        }
    }
}
