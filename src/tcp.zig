const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const log = std.log;
const posix = std.posix;
const socket_t = std.posix.socket_t;
const fd_t = std.posix.fd_t;

const protocol = @import("protocol.zig");
const Options = protocol.Options;
const Io = @import("io.zig").Io;
const Op = @import("io.zig").Op;
const Error = @import("io.zig").Error;
const Timer = @import("io.zig").Timer;
pub const Server = @import("server.zig").ServerType(*Conn, *Timer);
const Channel = Server.Channel;
const Msg = @import("server.zig").ChannelMsg;
const max_msgs_send_batch_size = @import("server.zig").max_msgs_send_batch_size;

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
    ticker_op: ?*Op = null, // heartbeat ticker
    // if currently sending message store here response to send later
    pending_response: ?Response = null,
    ready_count: u32 = 0,
    in_flight: u32 = 0,
    outstanding_heartbeats: u8 = 0,

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
    identify: protocol.Identify = .{},

    const Response = enum {
        ok,
        cls,
        heartbeat,
    };

    fn init(self: *Conn) !void {
        self.recv_op = try self.io.recv(self.socket, self, received, recvFailed);
        try self.initTicker(self.listener.options.heartbeat_interval);
    }

    fn initTicker(self: *Conn, heartbeat_interval: i64) !void {
        log.debug("{} heartbeat interval: {}", .{ self.socket, heartbeat_interval });
        if (self.ticker_op) |op| {
            try op.cancel();
            op.unsubscribe(self);
        }
        if (heartbeat_interval == 0) return;
        const msec: i64 = @divTrunc(heartbeat_interval, 2);
        self.ticker_op = try self.io.ticker(msec, self, tick, tickerFailed);
    }

    pub fn msgTimeout(self: *Conn) u32 {
        return self.identify.msg_timeout;
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
        if (self.outstanding_heartbeats > 4) {
            log.debug("{} no heartbeat, closing", .{self.socket});
            return try self.close();
        }
        if (self.outstanding_heartbeats > 0) {
            log.debug("{} send heartbeat", .{self.socket});
            try self.respond(.heartbeat);
        }
        self.outstanding_heartbeats += 1;
    }

    fn tickerFailed(self: *Conn, err: anyerror) Error!void {
        self.ticker_op = null;
        switch (err) {
            error.Canceled => {},
            else => log.err("{} ticker failed {}", .{ self.socket, err }),
        }
    }

    fn received(self: *Conn, bytes: []const u8) Error!void {
        const server = self.listener.server;
        const options = self.listener.options;
        var ready_changed: bool = false;

        var parser = protocol.Parser{ .buf = try self.appendRecvBuf(bytes) };
        while (parser.next() catch |err| {
            log.err("{} protocol parser failed {}, un-parsed: {d}", .{ self.socket, err, parser.unparsed()[0..@min(128, parser.unparsed().len)] });
            return try self.close();
        }) |msg| {
            self.outstanding_heartbeats = 0;
            switch (msg) {
                .identify => {
                    self.identify = msg.parseIdentify(self.allocator, options) catch |err| {
                        log.err("{} failed to parse identify {}", .{ self.socket, err });
                        return try self.close();
                    };
                    if (self.identify.heartbeat_interval != options.heartbeat_interval)
                        try self.initTicker(self.identify.heartbeat_interval);
                    try self.respond(.ok);
                    log.debug("{} identify {}", .{ self.socket, self.identify });
                },
                .sub => |sub| {
                    self.channel = try server.sub(self, sub.topic, sub.channel);
                    try self.respond(.ok);
                    log.debug("{} subscribe: {s} {s}", .{ self.socket, sub.topic, sub.channel });
                },
                .spub => |spub| {
                    try server.publish(spub.topic, spub.data);
                    try self.respond(.ok);
                    log.debug("{} publish: {s}", .{ self.socket, spub.topic });
                },
                .mpub => |mpub| {
                    try server.multiPublish(mpub.topic, mpub.msgs, mpub.data);
                    try self.respond(.ok);
                    log.debug("{} multi publish: {s} messages: {}", .{ self.socket, mpub.topic, mpub.msgs });
                },
                .dpub => |dpub| {
                    try server.deferredPublish(dpub.topic, dpub.data, dpub.delay);
                    try self.respond(.ok);
                    log.debug("{} deferred publish: {s} delay: {}", .{ self.socket, dpub.topic, dpub.delay });
                },
                .rdy => |count| {
                    self.ready_count = count;
                    ready_changed = true;
                    log.debug("{} ready: {}", .{ self.socket, count });
                },
                .fin => |msg_id| {
                    if (self.channel) |channel| {
                        self.in_flight -|= 1;
                        const res = try channel.fin(msg_id);
                        log.debug("{} fin {} {}", .{ self.socket, Msg.seqFromId(msg_id), res });
                        ready_changed = true;
                    } else {
                        return try self.close();
                    }
                },
                .req => |req| {
                    if (self.channel) |channel| {
                        self.in_flight -|= 1;
                        const res = try channel.req(req.msg_id, req.delay);
                        log.debug("{} req {} {}", .{ self.socket, Msg.seqFromId(req.msg_id), res });
                    } else {
                        return try self.close();
                    }
                },
                .touch => |msg_id| {
                    if (self.channel) |channel| {
                        self.in_flight -|= 1;
                        const res = try channel.touch(msg_id, self.identify.msg_timeout);
                        log.debug("{} touch {} {}", .{ self.socket, Msg.seqFromId(msg_id), res });
                    } else {
                        return try self.close();
                    }
                },
                .cls => {
                    self.ready_count = 0;
                    try self.respond(.cls);
                    log.debug("{} cls", .{self.socket});
                },
                .nop => {
                    log.debug("{} nop", .{self.socket});
                },
                .auth => {
                    log.err("{} `auth` is not supported operation", .{self.socket});
                },
                .version => unreachable, // handled in the parser
            }
        }

        const unparsed = parser.unparsed();
        if (unparsed.len > 0)
            try self.setRecvBuf(unparsed)
        else
            self.deinitRecvBuf();

        if (ready_changed)
            if (self.channel) |channel|
                try channel.ready(self);
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
        mem.writeInt(u32, hdr[4..8], @intFromEnum(protocol.FrameType.response), .big);
        @memcpy(hdr[8..][0..data.len], data);
        self.send_vec[0] = .{ .base = &self.send_header_buf, .len = data.len + 8 };
        try self.send(1);
    }

    pub fn sendMsgs(self: *Conn, msgs: []*Msg) !void {
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

    pub fn sendMsg(self: *Conn, msg: *Msg) !void {
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
        self.identify.deinit(self.allocator);
        self.listener.release(self);
    }
};
