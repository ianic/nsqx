const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const log = std.log;
const posix = std.posix;
const linux = std.os.linux;
const IoUring = linux.IoUring;
const socket_t = std.posix.socket_t;
const fd_t = std.posix.fd_t;
const Atomic = std.atomic.Value;

const protocol = @import("protocol.zig");
const Io = @import("io.zig").Io;
const Op = @import("io.zig").Op;
const Error = @import("io.zig").Error;
const Message = @import("server.zig").Message;
const Server = @import("server.zig").ServerType(*Conn);
const Channel = @import("server.zig").ServerType(*Conn).Channel;
const ChannelMsg = @import("server.zig").ChannelMsg;
const max_msgs_send_batch_size = @import("server.zig").max_msgs_send_batch_size;

const recv_buffers = 256;
const recv_buffer_len = 64 * 1024;
const port = 4150;
const ring_entries: u16 = 16 * 1024;

var server: Server = undefined;

pub const std_options = std.Options{
    .log_level = .info,
};

pub fn main() !void {
    // var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    // defer _ = gpa.deinit();
    // const allocator = gpa.allocator();
    const allocator = std.heap.c_allocator;

    const addr = std.net.Address.initIp4([4]u8{ 127, 0, 0, 1 }, port);
    const socket = (try addr.listen(.{ .reuse_address = true })).stream.handle;

    var ring = try IoUring.init(
        ring_entries,
        linux.IORING_SETUP_SQPOLL | linux.IORING_SETUP_SINGLE_ISSUER,
        // linux.IORING_SETUP_SINGLE_ISSUER | linux.IORING_SETUP_COOP_TASKRUN | linux.IORING_SETUP_DEFER_TASKRUN,
    );
    defer ring.deinit();

    var io = Io{ .allocator = allocator };
    try io.init(ring_entries, recv_buffers, recv_buffer_len);
    defer io.deinit();

    server = Server.init(allocator);
    defer server.deinit();

    var listener = try Listener.init(allocator, &io);
    defer listener.deinit();
    try listener.accept(socket);

    catchSignals();
    while (true) {
        try io.loop(run_loop);
        const sig = signal.load(.monotonic);
        signal.store(0, .release);
        run_loop.store(true, .release);
        switch (sig) {
            posix.SIG.USR1 => {
                const print = std.debug.print;
                print("listener connections:\n", .{});
                print("  active {}, accepted: {}, completed: {}\n", .{ listener.accepted - listener.completed, listener.accepted, listener.completed });

                print("io operations:", .{});
                print("\n  all    ", .{});
                io.stat.all.print();
                print("\n  recv   ", .{});
                io.stat.recv.print();
                print("\n  sendv  ", .{});
                io.stat.sendv.print();
                print("\n  ticker ", .{});
                io.stat.ticker.print();
                print("\n  close  ", .{});
                io.stat.close.print();
                print("\n  accept ", .{});
                io.stat.accept.print();
                print("\n", .{});

                print(
                    "  receive buffers group:\n    success: {}, no-buffs: {} {d:5.2}%\n",
                    .{ io.recv_buf_grp_stat.success, io.recv_buf_grp_stat.no_bufs, io.recv_buf_grp_stat.noBufs() },
                );

                print("server topics: {}\n", .{server.topics.count()});
                var ti = server.topics.iterator();
                while (ti.next()) |te| {
                    const topic_name = te.key_ptr.*;
                    const topic = te.value_ptr.*;
                    const size = topic.messages.size();
                    print("\t{s} messages: {d} bytes: {} {}Mb {}Gb, sequence: {}\n", .{
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
                        print("\t --{s} consumers: {} in flight messages: {} offset: {}\n", .{
                            channel_name,
                            channel.consumers.items.len,
                            channel.in_flight.count(),
                            channel.sequence,
                        });
                    }
                }
            },
            posix.SIG.USR2 => {
                const c = @cImport(@cInclude("malloc.h"));

                c.malloc_stats();
                const ret = c.malloc_trim(0);
                log.info("malloc_trim: {}", .{ret});
                c.malloc_stats();
            },
            posix.SIG.TERM, posix.SIG.INT => break,
            else => {},
        }
    }

    log.debug("done", .{});
}

var run_loop = Atomic(bool).init(true);
var signal = Atomic(c_int).init(0);

fn catchSignals() void {
    var act = posix.Sigaction{
        .handler = .{
            .handler = struct {
                fn wrapper(sig: c_int) callconv(.C) void {
                    signal.store(sig, .release);
                    run_loop.store(false, .release);
                }
            }.wrapper,
        },
        .mask = posix.empty_sigset,
        .flags = 0,
    };
    posix.sigaction(posix.SIG.TERM, &act, null);
    posix.sigaction(posix.SIG.INT, &act, null);
    posix.sigaction(posix.SIG.USR1, &act, null);
    posix.sigaction(posix.SIG.USR2, &act, null);
    posix.sigaction(posix.SIG.PIPE, &act, null);
}

const Listener = struct {
    allocator: mem.Allocator,
    io: *Io,
    accepted: usize = 0,
    completed: usize = 0,

    fn init(allocator: mem.Allocator, io: *Io) !Listener {
        return .{
            .allocator = allocator,
            .io = io,
        };
    }

    fn deinit(self: *Listener) void {
        _ = self;
    }

    fn accept(self: *Listener, socket: socket_t) !void {
        _ = try self.io.accept(socket, self, accepted, failed);
    }

    fn accepted(self: *Listener, socket: socket_t) Error!void {
        var conn = try self.allocator.create(Conn);
        conn.* = Conn{ .allocator = self.allocator, .socket = socket, .listener = self, .io = self.io };
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

const Conn = struct {
    allocator: mem.Allocator,
    io: *Io,
    socket: socket_t = 0,
    listener: *Listener,

    recv_op: ?*Op = null,
    send_op: ?*Op = null,
    ticker_op: ?*Op = null, // heartbeat ticker
    // if currently sending message store here response to send later
    pending_response: ?Response = null,
    ready_count: u32 = 0,
    in_flight: u32 = 0,
    unanswered_heartbeats: u8 = 0,

    recv_buf: []u8 = &.{}, // holds unprocessed bytes from previous receive
    send_header_buf: [34]u8 = undefined, // message header
    send_vec: [max_msgs_send_batch_size * 2]posix.iovec_const = undefined, // header and body for each message
    send_msghdr: posix.msghdr_const = .{
        .iov = undefined,
        .iovlen = undefined,
        .name = null,
        .namelen = 0,
        .control = null,
        .controllen = 0,
        .flags = 0,
    },
    channel: ?*Channel = null,

    const Response = enum {
        ok,
        cls,
        heartbeat,
    };

    fn init(self: *Conn) !void {
        self.recv_op = try self.io.recv(self.socket, self, received, recvFailed);
        self.ticker_op = try self.io.ticker(30, self, tick, tickerFailed);
    }

    pub fn ready(self: Conn) u32 {
        if (self.send_op != null) return 0;
        if (self.in_flight > self.ready_count) return 0;
        return @min(
            self.ready_count - self.in_flight,
            max_msgs_send_batch_size,
        );
    }

    fn tick(self: *Conn) Error!void {
        if (self.unanswered_heartbeats > 2) {
            return try self.close();
        }
        if (self.unanswered_heartbeats > 0)
            try self.respond(.heartbeat);
        self.unanswered_heartbeats += 1;
    }

    fn tickerFailed(self: *Conn, err: anyerror) Error!void {
        self.ticker_op = null;
        switch (err) {
            error.Canceled => {},
            else => log.err("{} ticker failed {}", .{ self.socket, err }),
        }
    }

    fn received(self: *Conn, bytes: []const u8) Error!void {
        var parser = protocol.Parser{ .buf = try self.appendRecvBuf(bytes) };
        while (parser.next() catch |err| {
            log.err("{} protocol parser failed {}, un-parsed: {d}", .{ self.socket, err, parser.unparsed()[0..@min(128, parser.unparsed().len)] });
            return try self.close();
        }) |msg| {
            self.unanswered_heartbeats = 0;
            switch (msg) {
                .identify => |data| {
                    log.debug("{} identify: {s}", .{ self.socket, data });
                    try self.respond(.ok);
                },
                .sub => |sub| {
                    log.debug("{} subscribe: {s} {s}", .{ self.socket, sub.topic, sub.channel });
                    self.channel = try server.sub(self, sub.topic, sub.channel);
                    try self.respond(.ok);
                },
                .spub => |spub| {
                    log.debug("{} publish: {s}", .{ self.socket, spub.topic });
                    try server.publish(spub.topic, spub.data);
                    try self.respond(.ok);
                },
                .mpub => |mpub| {
                    log.debug("{} multi publish: {s} messages: {}", .{ self.socket, mpub.topic, mpub.msgs });
                    try server.mpub(mpub.topic, mpub.msgs, mpub.data);
                    try self.respond(.ok);
                },
                .rdy => |count| {
                    log.debug("{} ready: {}", .{ self.socket, count });
                    self.ready_count = count;
                    if (self.channel) |channel|
                        if (self.ready() > 0) try channel.ready(self);
                },
                .fin => |msg_id| {
                    if (self.channel) |channel| {
                        self.in_flight -|= 1;
                        const res = try channel.fin(msg_id);
                        if (self.ready() > 0) try channel.ready(self);
                        log.debug("{} fin {} {}", .{ self.socket, ChannelMsg.seqFromId(msg_id), res });
                    } else {
                        try self.close();
                    }
                },
                .cls => {
                    self.ready_count = 0;
                    log.debug("{} cls", .{self.socket});
                    try self.respond(.cls);
                },
                .nop => {
                    log.debug("{} nop", .{self.socket});
                },
                else => {
                    std.debug.print("{}\n", .{msg});
                    unreachable;
                },
            }
        }

        const unparsed = parser.unparsed();
        if (unparsed.len > 0)
            try self.setRecvBuf(unparsed)
        else
            self.deinitRecvBuf();
    }

    fn appendRecvBuf(self: *Conn, bytes: []const u8) ![]const u8 {
        if (self.recv_buf.len == 0) return bytes;
        const old_len = self.recv_buf.len;
        self.recv_buf = try self.allocator.realloc(self.recv_buf, old_len + bytes.len);
        @memcpy(self.recv_buf[old_len..], bytes);
        return self.recv_buf;
    }

    fn setRecvBuf(self: *Conn, bytes: []const u8) !void {
        if (self.recv_buf.len == bytes.len) return;
        const new_buf = try self.allocator.dupe(u8, bytes);
        self.deinitRecvBuf();
        self.recv_buf = new_buf;
    }

    fn deinitRecvBuf(self: *Conn) void {
        self.allocator.free(self.recv_buf);
        self.recv_buf = &.{};
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

    fn sendResponse(self: *Conn, data: []const u8) !void {
        var hdr = &self.send_header_buf;
        assert(data.len <= hdr.len - 8);
        mem.writeInt(u32, hdr[0..4], @intCast(4 + data.len), .big);
        mem.writeInt(u32, hdr[4..8], @intFromEnum(FrameType.response), .big);
        @memcpy(hdr[8..][0..data.len], data);
        self.send_vec[0] = .{ .base = &self.send_header_buf, .len = data.len + 8 };
        try self.send(1);
    }

    pub fn sendMsgs(self: *Conn, msgs: []*ChannelMsg) !void {
        assert(msgs.len <= max_msgs_send_batch_size);
        var n: usize = 0;
        for (msgs) |msg| {
            self.send_vec[n] = .{ .base = &msg.header, .len = msg.header.len };
            self.send_vec[n + 1] = .{ .base = msg.body.ptr, .len = msg.body.len };
            n += 2;
        }
        try self.send(n);
        self.in_flight += @intCast(msgs.len);
    }

    pub fn sendMsg(self: *Conn, msg: *ChannelMsg) !void {
        self.send_vec[0] = .{ .base = &msg.header, .len = msg.header.len };
        self.send_vec[1] = .{ .base = msg.body.ptr, .len = msg.body.len };
        try self.send(2);
        self.in_flight += 1;
    }

    fn send(self: *Conn, vec_len: usize) !void {
        assert(self.send_op == null);
        self.send_msghdr.iov = &self.send_vec;
        self.send_msghdr.iovlen = @intCast(vec_len);
        self.send_op = try self.io.sendv(self.socket, &self.send_msghdr, self, sent, sendFailed);
        //self.send_op = try self.io.writev(self.socket, self.send_vec[0..vec_len], self, sent, sendFailed);
    }

    fn respond(self: *Conn, rsp: Response) !void {
        if (self.send_op != null) {
            self.pending_response = rsp;
            return;
        }
        switch (rsp) {
            .ok => try self.sendResponse("OK"),
            .heartbeat => try self.sendResponse("_heartbeat_"),
            .cls => try self.sendResponse("CLOSE_WAIT"),
        }
    }

    fn sent(self: *Conn, _: usize) Error!void {
        self.send_op = null;
        if (self.pending_response) |r| {
            try self.respond(r);
            self.pending_response = null;
        }
        if (self.ready() > 0)
            if (self.channel) |channel| try channel.ready(self);
    }

    fn sendFailed(self: *Conn, err: anyerror) Error!void {
        self.send_op = null;
        switch (err) {
            error.BrokenPipe, error.ConnectionResetByPeer => {},
            else => log.err("{} send failed {}", .{ self.socket, err }),
        }
        try self.close();
    }

    fn close(self: *Conn) !void {
        log.debug("{} close", .{self.socket});
        if (self.channel) |channel| try channel.unsub(self);
        if (self.ticker_op) |op| {
            try op.cancel();
            op.unsubscribe(self);
        }
        if (self.recv_op) |op| {
            try op.cancel();
            op.unsubscribe(self);
        }
        if (self.send_op) |op| op.unsubscribe(self);
        try self.io.close(self.socket);

        self.deinitRecvBuf();
        self.listener.release(self);
    }
};

const FrameType = enum(u32) {
    response = 0,
    err = 1,
    message = 2,
};

test {
    _ = @import("server.zig");
    _ = @import("protocol.zig");
}
