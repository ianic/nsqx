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
const Server = @import("server.zig").Server(*Conn);
const Channel = @import("server.zig").Channel(*Conn);

const recv_buffers = 4096;
const recv_buffer_len = 4096;
const port = 4150;
const ring_entries: u16 = 16;

var server: Server = undefined;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const addr = std.net.Address.initIp4([4]u8{ 127, 0, 0, 1 }, port);
    const socket = (try addr.listen(.{ .reuse_address = true })).stream.handle;

    var ring = try IoUring.init(ring_entries, linux.IORING_SETUP_SQPOLL & linux.IORING_SETUP_SINGLE_ISSUER);
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
    try io.loop(run_loop);
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
                    switch (sig) {
                        posix.SIG.TERM, posix.SIG.INT => {
                            run_loop.store(false, .release);
                        },
                        else => {}, // ignore USR1, USR2, PIPE
                    }
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
    conns_pool: std.heap.MemoryPool(Conn),

    fn init(allocator: mem.Allocator, io: *Io) !Listener {
        return .{
            .allocator = allocator,
            .io = io,
            .conns_pool = std.heap.MemoryPool(Conn).init(allocator),
        };
    }

    fn deinit(self: *Listener) void {
        self.conns_pool.deinit();
    }

    fn accept(self: *Listener, socket: socket_t) !void {
        _ = try self.io.accept(socket, self, accepted, failed);
    }

    fn accepted(self: *Listener, socket: socket_t) Error!void {
        var conn = try self.conns_pool.create();
        conn.* = Conn{ .allocator = self.allocator, .socket = socket, .listener = self, .io = self.io };
        try conn.init();
    }

    fn failed(_: *Listener, err: anyerror) Error!void {
        log.err("accept failed {}", .{err});
        // TODO: handle this
    }

    fn release(self: *Listener, conn: *Conn) void {
        self.conns_pool.destroy(conn);
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
    unanswered_heartbeats: u8 = 0,

    recv_buf: ?[]u8 = null, // holds unprocessed bytes from previous receive
    send_header_buf: [34]u8 = undefined, // message header
    send_vec: [2]posix.iovec_const = undefined, // header and body

    channel: ?*Channel = null,

    //msg_id: usize = 0,

    const Response = enum {
        ok,
        cls,
        heartbeat,
    };

    fn init(self: *Conn) !void {
        self.recv_op = try self.io.recv(self.socket, self, received, recvFailed);
        self.ticker_op = try self.io.ticker(5, self, tick, tickerFailed);
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

    // fn testSendMsg(self: *Conn) !void {
    //     var id: [16]u8 = .{0} ** 16;
    //     mem.writeInt(usize, id[8..], self.msg_id, .big);
    //     self.msg_id += 1;
    //     const m = Message{ .body = "Hello world!", .id = id };
    //     try self.sendMsg(m);
    // }

    fn received(self: *Conn, bytes: []const u8) Error!void {
        var parser = protocol.Parser{ .buf = try self.appendRecvBuf(bytes) };
        while (parser.next() catch |err| {
            log.err("{} protocol parser failed {}", .{ self.socket, err });
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
                .rdy => |count| {
                    log.debug("{} ready: {}", .{ self.socket, count });
                    self.ready_count = count;
                },
                .fin => |msg_id| {
                    _ = msg_id;
                    std.debug.print(".", .{});
                    //log.debug("{} fin {x}", .{ self.socket, msg_id });
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
        if (self.recv_buf) |old_buf| {
            const new_buf = try self.allocator.realloc(old_buf, old_buf.len + bytes.len);
            @memcpy(new_buf[old_buf.len..], bytes);
            self.recv_buf = new_buf;
            return new_buf;
        }
        return bytes;
    }

    fn setRecvBuf(self: *Conn, bytes: []const u8) !void {
        const new_buf = try self.allocator.dupe(u8, bytes);
        self.deinitRecvBuf();
        self.recv_buf = new_buf;
    }

    fn deinitRecvBuf(self: *Conn) void {
        if (self.recv_buf) |recv_buf| {
            self.allocator.free(recv_buf);
            self.recv_buf = null;
        }
    }

    fn recvFailed(self: *Conn, err: anyerror) Error!void {
        self.recv_op = null;
        switch (err) {
            error.EndOfFile => {},
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
        self.send_vec[1].len = 0;
        try self.send();
    }

    fn sendMsg(self: *Conn, msg: Message) !void {
        var hdr = &self.send_header_buf;
        mem.writeInt(u32, hdr[0..4], @intCast(msg.body.len + 30), .big);
        mem.writeInt(u32, hdr[4..8], @intFromEnum(FrameType.message), .big);
        mem.writeInt(u64, hdr[8..16], msg.timestamp, .big);
        mem.writeInt(u16, hdr[16..18], msg.attempts, .big);
        hdr[18..34].* = msg.id;
        self.send_vec[0] = .{ .base = &self.send_header_buf, .len = hdr.len };
        self.send_vec[1] = .{ .base = msg.body.ptr, .len = msg.body.len };
        try self.send();
    }

    fn send(self: *Conn) !void {
        assert(self.send_op == null);
        self.send_op = try self.io.writev(self.socket, &self.send_vec, self, sent, sendFailed);
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
        if (self.send_op == null and self.ready_count > 0) {
            if (self.channel) |channel| try channel.ready(self);
            // TODO javi se u channel da si spreman
            //try self.testSendMsg();
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

    fn close(self: *Conn) !void {
        log.debug("{} close", .{self.socket});
        if (self.channel) |channel| channel.unsub(self);
        try self.io.close(self.socket);
        if (self.ticker_op) |op| {
            try op.cancel();
            op.unsubscribe(self);
        }
        if (self.recv_op) |op| op.unsubscribe(self);
        if (self.send_op) |op| op.unsubscribe(self);

        self.deinitRecvBuf();
        self.listener.release(self);
    }
};

const FrameType = enum(u32) {
    response = 0,
    err = 1,
    message = 2,
};
