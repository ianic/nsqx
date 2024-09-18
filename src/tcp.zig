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
const lookup = @import("lookup.zig");
pub const Server = @import("server.zig").ServerType(Conn, Io, lookup.Connector);
const Channel = Server.Channel;
const Msg = @import("server.zig").ChannelMsg;
const max_msgs_send_batch_size = @import("server.zig").max_msgs_send_batch_size;

const log = std.log.scoped(.tcp);

pub fn ListenerType(comptime ConnType: type) type {
    return struct {
        allocator: mem.Allocator,
        server: *Server,
        options: Options,
        io: *Io,
        op: ?*Op = null,
        conns: std.AutoHashMap(socket_t, *ConnType),
        stat: struct {
            accepted: u64 = 0,
            completed: u64 = 0,
        } = .{},

        const Self = @This();

        pub fn init(allocator: mem.Allocator, io: *Io, server: *Server, options: Options) !Self {
            return .{
                .allocator = allocator,
                .server = server,
                .options = options,
                .io = io,
                .conns = std.AutoHashMap(socket_t, *ConnType).init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            self.conns.deinit();
        }

        pub fn accept(self: *Self, socket: socket_t) !void {
            self.op = try self.io.accept(socket, self, accepted, failed);
        }

        fn accepted(self: *Self, socket: socket_t, addr: std.net.Address) Error!void {
            var conn = try self.allocator.create(ConnType);
            errdefer self.allocator.destroy(conn);
            try self.conns.put(socket, conn);
            conn.* = ConnType.init(self, socket, addr);
            try conn.recv();
            self.stat.accepted +%= 1;
        }

        fn failed(self: *Self, err: anyerror) Error!void {
            self.op = null;
            switch (err) {
                error.OperationCanceled => {},
                else => log.err("accept failed {}", .{err}),
            }
        }

        pub fn close(self: *Self) !void {
            if (self.op) |op|
                try op.cancel();

            var iter = self.conns.valueIterator();
            while (iter.next()) |e| {
                try e.*.close();
            }
        }

        pub fn remove(self: *Self, conn: *ConnType) void {
            assert(self.conns.remove(conn.socket));
            self.allocator.destroy(conn);
            self.stat.completed +%= 1;
        }
    };
}

pub const Listener = ListenerType(Conn);

pub const Conn = struct {
    allocator: mem.Allocator,
    listener: *Listener,
    io: *Io,
    socket: socket_t = 0,
    addr: std.net.Address,

    recv_op: ?*Op = null,
    send_op: ?*Op = null,
    ticker_op: ?*Op = null, // heartbeat ticker
    // if currently sending message store here response to send later
    pending_response: ?Response = null,
    ready_count: u32 = 0,
    in_flight: u32 = 0,
    outstanding_heartbeats: u8 = 0,

    recv_buf: RecvBuf, // holds unprocessed bytes from previous receive
    send_header_buf: [34]u8 = undefined, // message header
    send_vec: []posix.iovec_const = &.{}, // header and body for each message
    send_msghdr: posix.msghdr_const = .{ .iov = undefined, .iovlen = undefined, .name = null, .namelen = 0, .control = null, .controllen = 0, .flags = 0 },
    channel: ?*Channel = null,
    identify: protocol.Identify = .{},
    stat: struct {
        connected_at: u64 = 0,
        messages: u64 = 0,
        finish: u64 = 0,
        requeue: u64 = 0,
    } = .{},
    sent_at: u64 = 0, // Timestamp of the last finished send

    const Response = enum {
        ok,
        cls,
        heartbeat,
    };

    fn init(listener: *Listener, socket: socket_t, addr: std.net.Address) Conn {
        var conn = Conn{
            .allocator = listener.allocator,
            .listener = listener,
            .io = listener.io,
            .socket = socket,
            .addr = addr,
            .recv_buf = RecvBuf.init(listener.allocator),
        };
        conn.stat.connected_at = listener.io.now();
        return conn;
    }

    fn recv(self: *Conn) !void {
        self.send_vec = try self.allocator.alloc(posix.iovec_const, 2);
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
            self.send_vec.len / 2,
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
        var ready_changed: bool = false;

        var parser = protocol.Parser{ .buf = try self.recv_buf.append(bytes) };
        while (parser.next() catch |err| {
            log.err(
                "{} protocol parser failed {}, un-parsed: {d}",
                .{ self.socket, err, parser.unparsed()[0..@min(128, parser.unparsed().len)] },
            );
            return try self.close();
        }) |msg|
            self.msgRecived(msg, &ready_changed) catch |err| switch (err) {
                Error.OutOfMemory, Error.SubmissionQueueFull => |e| return e,
                else => {
                    log.err("{} message failed {}", .{ self.socket, err });
                    return try self.close();
                },
            };

        try self.recv_buf.set(parser.unparsed());
        if (ready_changed and self.send_op == null)
            if (self.channel) |channel| try channel.ready(self);
    }

    fn msgRecived(self: *Conn, msg: protocol.Message, ready_changed: *bool) !void {
        const server = self.listener.server;
        const options = self.listener.options;
        self.outstanding_heartbeats = 0;

        switch (msg) {
            .identify => {
                self.identify = try msg.parseIdentify(self.allocator, options);
                if (self.identify.heartbeat_interval != options.heartbeat_interval)
                    try self.initTicker(self.identify.heartbeat_interval);
                try self.respond(.ok);
                log.debug("{} identify {}", .{ self.socket, self.identify });
            },
            .sub => |arg| {
                self.channel = try server.sub(self, arg.topic, arg.channel);
                try self.respond(.ok);
                log.debug("{} subscribe: {s} {s}", .{ self.socket, arg.topic, arg.channel });
            },
            .spub => |arg| {
                try server.publish(arg.topic, arg.data);
                try self.respond(.ok);
                log.debug("{} publish: {s}", .{ self.socket, arg.topic });
            },
            .mpub => |arg| {
                try server.multiPublish(arg.topic, arg.msgs, arg.data);
                try self.respond(.ok);
                log.debug("{} multi publish: {s} messages: {}", .{ self.socket, arg.topic, arg.msgs });
            },
            .dpub => |arg| {
                try server.deferredPublish(arg.topic, arg.data, arg.delay);
                try self.respond(.ok);
                log.debug("{} deferred publish: {s} delay: {}", .{ self.socket, arg.topic, arg.delay });
            },
            .rdy => |count| {
                self.ready_count = count;
                ready_changed.* = true;
                log.debug("{} ready: {}", .{ self.socket, count });
            },
            .fin => |msg_id| {
                var channel = self.channel orelse return error.NotSubscribed;
                self.in_flight -|= 1;
                const res = try channel.fin(msg_id);
                if (res) self.stat.finish += 1;
                ready_changed.* = true;
                log.debug("{} fin {} {}", .{ self.socket, Msg.seqFromId(msg_id), res });
            },
            .req => |arg| {
                var channel = self.channel orelse return error.NotSubscribed;
                self.in_flight -|= 1;
                const res = try channel.req(arg.msg_id, arg.delay);
                if (res) self.stat.requeue += 1;
                log.debug("{} req {} {}", .{ self.socket, Msg.seqFromId(arg.msg_id), res });
            },
            .touch => |msg_id| {
                var channel = self.channel orelse return error.NotSubscribed;
                const res = try channel.touch(msg_id, self.identify.msg_timeout);
                log.debug("{} touch {} {}", .{ self.socket, Msg.seqFromId(msg_id), res });
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
        assert(msgs.len <= self.send_vec.len / 2);
        self.stat.messages += msgs.len;
        var n: usize = 0;
        for (msgs) |msg| {
            self.send_vec[n] = .{ .base = &msg.header, .len = msg.header.len };
            self.send_vec[n + 1] = .{ .base = msg.body().ptr, .len = msg.body().len };
            n += 2;
        }
        try self.send(n);
        self.in_flight += @intCast(msgs.len);
    }

    pub fn sendMsg(self: *Conn, msg: *Msg) !void {
        self.stat.messages += 1;
        self.send_vec[0] = .{ .base = &msg.header, .len = msg.header.len };
        self.send_vec[1] = .{ .base = msg.body.ptr, .len = msg.body.len };
        try self.send(2);
        self.in_flight += 1;
    }

    fn send(self: *Conn, vec_len: usize) !void {
        assert(self.send_op == null);
        self.send_msghdr.iov = self.send_vec.ptr;
        self.send_msghdr.iovlen = @intCast(vec_len);
        self.send_op = try self.io.sendv(self.socket, &self.send_msghdr, self, sent, sendFailed);
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

    fn sent(self: *Conn) Error!void {
        self.send_op = null;
        self.sent_at = self.io.now();
        try self.initSendVec();
        if (self.pending_response) |r| {
            try self.respond(r);
            self.pending_response = null;
        }
        if (self.channel) |channel| try channel.ready(self);
    }

    fn initSendVec(self: *Conn) !void {
        const new_len: usize = @as(usize, @intCast(@min(self.ready_count, max_msgs_send_batch_size))) * 2;
        if (new_len <= self.send_vec.len) return;
        self.allocator.free(self.send_vec);
        self.send_vec = try self.allocator.alloc(posix.iovec_const, new_len);
    }

    fn sendFailed(self: *Conn, err: anyerror) Error!void {
        self.send_op = null;
        self.sent_at = self.io.now();
        switch (err) {
            error.BrokenPipe, error.ConnectionResetByPeer => {},
            else => log.err("{} send failed {}", .{ self.socket, err }),
        }
        try self.close();
    }

    pub fn channelClosed(self: *Conn) void {
        self.channel = null;
        self.close() catch {};
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

        self.recv_buf.free();
        self.identify.deinit(self.allocator);
        self.allocator.free(self.send_vec);
        self.listener.remove(self);
    }
};

pub const RecvBuf = struct {
    allocator: mem.Allocator,
    buf: []u8 = &.{},

    const Self = @This();

    pub fn init(allocator: mem.Allocator) Self {
        return .{ .allocator = allocator };
    }

    pub fn append(self: *Self, bytes: []const u8) ![]const u8 {
        if (self.buf.len == 0) return bytes;
        const old_len = self.buf.len;
        self.buf = try self.allocator.realloc(self.buf, old_len + bytes.len);
        @memcpy(self.buf[old_len..], bytes);
        return self.buf;
    }

    pub fn set(self: *Self, bytes: []const u8) !void {
        if (bytes.len == 0) return self.free();
        if (self.buf.len == bytes.len and self.buf.ptr == bytes.ptr) return;

        const new_buf = try self.allocator.dupe(u8, bytes);
        self.free();
        self.buf = new_buf;
    }

    pub fn free(self: *Self) void {
        self.allocator.free(self.buf);
        self.buf = &.{};
    }
};
