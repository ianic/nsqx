const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const posix = std.posix;
const socket_t = std.posix.socket_t;
const fd_t = std.posix.fd_t;

const protocol = @import("protocol.zig");
const Options = @import("Options.zig");
const Io = @import("io.zig").Io;
const Op = @import("io.zig").Op;
const Error = @import("io.zig").Error;
const lookup = @import("lookup.zig");
pub const Server = @import("server.zig").ServerType(Conn, Io, lookup.Connector);
const Channel = Server.Channel;
const Msg = Server.Channel.Msg;

const log = std.log.scoped(.tcp);

pub fn ListenerType(comptime ConnType: type) type {
    return struct {
        allocator: mem.Allocator,
        server: *Server,
        options: Options,
        io: *Io,
        op: ?*Op = null,
        conns: std.AutoHashMap(socket_t, *ConnType),
        metric: struct {
            // Total number of
            accept: usize = 0, // accepted connections
            close: usize = 0, // closed (completed) connections
        } = .{},

        const Self = @This();

        pub fn init(
            self: *Self,
            allocator: mem.Allocator,
            io: *Io,
            server: *Server,
            options: Options,
            socket: socket_t,
        ) !void {
            self.* = .{
                .allocator = allocator,
                .server = server,
                .options = options,
                .io = io,
                .conns = std.AutoHashMap(socket_t, *ConnType).init(allocator),
            };
            try self.io.accept(socket, self, accepted, failed, &self.op);
        }

        pub fn deinit(self: *Self) void {
            self.conns.deinit();
        }

        fn accepted(self: *Self, socket: socket_t, addr: std.net.Address) Error!void {
            var conn = try self.allocator.create(ConnType);
            errdefer self.allocator.destroy(conn);
            try self.conns.ensureUnusedCapacity(1);
            try conn.init(self, socket, addr);
            self.conns.putAssumeCapacityNoClobber(socket, conn);
            self.metric.accept +%= 1;
        }

        fn failed(_: *Self, err: anyerror) Error!void {
            switch (err) {
                error.OperationCanceled => {},
                else => log.err("accept failed {}", .{err}),
            }
        }

        pub fn close(self: *Self) !void {
            try Op.cancel(self.op);
            var iter = self.conns.valueIterator();
            while (iter.next()) |e| {
                try e.*.close();
            }
        }

        pub fn remove(self: *Self, conn: *ConnType) void {
            assert(self.conns.remove(conn.socket));
            self.allocator.destroy(conn);
            self.metric.close +%= 1;
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
    ticker_op: ?*Op = null, // Heartbeat ticker
    // If send is in process store here response to send later
    pending_response: ?Response = null,
    ready_count: u32 = 0,
    in_flight: u32 = 0,
    outstanding_heartbeats: u8 = 0,
    send_vec: SendVec = .{},
    recv_buf: RecvBuf, // Unprocessed bytes from previous receive
    send_buf: [34]u8 = undefined, // Send buffer for control messages
    sent_at: u64 = 0, // Timestamp of the last finished send

    channel: ?*Channel = null,
    identify: protocol.Identify = .{},

    metric: struct {
        connected_at: u64 = 0,
        // Total number of
        send: usize = 0,
        finish: usize = 0,
        requeue: usize = 0,
    } = .{},

    const Response = enum {
        ok,
        close,
        heartbeat,
    };

    fn init(self: *Conn, listener: *Listener, socket: socket_t, addr: std.net.Address) !void {
        self.* = .{
            .allocator = listener.allocator,
            .listener = listener,
            .io = listener.io,
            .socket = socket,
            .addr = addr,
            .recv_buf = RecvBuf.init(listener.allocator),
            .metric = .{ .connected_at = listener.io.now() },
        };

        try self.send_vec.init(self.allocator);
        errdefer self.send_vec.deinit(self.allocator);
        try self.io.recv(self.socket, self, received, recvFailed, &self.recv_op);
        errdefer Op.cancel(self.recv_op) catch {};
        try self.initTicker(self.listener.options.max_heartbeat_interval);
    }

    fn initTicker(self: *Conn, heartbeat_interval: i64) !void {
        if (heartbeat_interval == 0) return;
        log.debug("{} heartbeat interval: {}", .{ self.socket, heartbeat_interval });
        try Op.cancel(self.ticker_op);
        const msec: i64 = @divTrunc(heartbeat_interval, 2);
        try self.io.ticker(msec, self, tick, tickerFailed, &self.ticker_op);
    }

    // Channel api -----------------

    /// Consumer setting
    pub fn msgTimeout(self: *Conn) u32 {
        return self.identify.msg_timeout;
    }

    /// Number of messages connection is ready to send
    pub fn ready(self: *Conn) u32 {
        if (self.send_op != null) return 0;
        if (self.in_flight >= self.ready_count) return 0;
        return @min(
            self.ready_count - self.in_flight,
            self.send_vec.maxMsgs(),
        );
    }

    /// Prepares single message to be sent. Fills vectored sending structures.
    /// msg_no must be 0,1,2
    pub fn prepareSend(self: *Conn, header: []const u8, body: []const u8, msg_no: u32) !void {
        if (msg_no == 0) try self.send_vec.prepInit(self.allocator, self.ready_count);
        self.send_vec.prep(header, body);
    }

    /// Sends all prepared messages.
    /// msgs must be number of prepared messages
    pub fn sendPrepared(self: *Conn, msgs: u32) !void {
        assert(msgs == self.send_vec.prepMsgs());
        try self.send();
        self.metric.send += msgs;
        self.in_flight += msgs;
    }

    /// When channel is deleted via web interface
    pub fn channelClosed(self: *Conn) void {
        self.channel = null;
        self.close() catch {};
    }

    // Channel api -----------------

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
        switch (err) {
            error.OperationCanceled => {},
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
                if (self.identify.heartbeat_interval != options.max_heartbeat_interval)
                    try self.initTicker(self.identify.heartbeat_interval);
                try self.respond(.ok);
                log.debug("{} identify {}", .{ self.socket, self.identify });
            },
            .subscribe => |arg| {
                self.channel = try server.subscribe(self, arg.topic, arg.channel);
                try self.respond(.ok);
                log.debug("{} subscribe: {s} {s}", .{ self.socket, arg.topic, arg.channel });
            },
            .publish => |arg| {
                if (arg.data.len > options.max_msg_size) return error.MessageSizeOverflow;
                try server.publish(arg.topic, arg.data);
                try self.respond(.ok);
                log.debug("{} publish: {s}", .{ self.socket, arg.topic });
            },
            .multi_publish => |arg| {
                if (arg.data.len / arg.msgs > options.max_msg_size) return error.MessageSizeOverflow;
                try server.multiPublish(arg.topic, arg.msgs, arg.data);
                try self.respond(.ok);
                log.debug("{} multi publish: {s} messages: {}", .{ self.socket, arg.topic, arg.msgs });
            },
            .deferred_publish => |arg| {
                try server.deferredPublish(arg.topic, arg.data, arg.delay);
                try self.respond(.ok);
                log.debug("{} deferred publish: {s} delay: {}", .{ self.socket, arg.topic, arg.delay });
            },
            .ready => |count| {
                self.ready_count = if (count > options.max_rdy_count) options.max_rdy_count else count;
                ready_changed.* = true;
                log.debug("{} ready: {}", .{ self.socket, count });
            },
            .finish => |msg_id| {
                var channel = self.channel orelse return error.NotSubscribed;
                self.in_flight -|= 1;
                const res = channel.finish(msg_id);
                if (res) self.metric.finish += 1;
                ready_changed.* = true;
                log.debug("{} finish {} {}", .{ self.socket, Msg.seqFromId(msg_id), res });
            },
            .requeue => |arg| {
                var channel = self.channel orelse return error.NotSubscribed;
                self.in_flight -|= 1;
                const delay = if (arg.delay > options.max_req_timeout)
                    options.max_req_timeout
                else
                    arg.delay;
                const res = try channel.requeue(arg.msg_id, delay);
                if (res) self.metric.requeue += 1;
                log.debug("{} requeue {} {}", .{ self.socket, Msg.seqFromId(arg.msg_id), res });
            },
            .touch => |msg_id| {
                var channel = self.channel orelse return error.NotSubscribed;
                const res = channel.touch(msg_id, self.identify.msg_timeout);
                log.debug("{} touch {} {}", .{ self.socket, Msg.seqFromId(msg_id), res });
            },
            .close => {
                self.ready_count = 0;
                try self.respond(.close);
                log.debug("{} close", .{self.socket});
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
        switch (err) {
            error.EndOfFile, error.OperationCanceled, error.ConnectionResetByPeer => {},
            else => log.err("{} recv failed {}", .{ self.socket, err }),
        }
        try self.close();
    }

    fn send(self: *Conn) !void {
        assert(self.send_op == null);
        try self.io.sendv(self.socket, self.send_vec.ptr(), self, sent, sendFailed, &self.send_op);
    }

    fn sent(self: *Conn) Error!void {
        self.sent_at = self.io.now();
        if (self.pending_response) |r| {
            try self.respond(r);
            self.pending_response = null;
            return;
        }
        if (self.channel) |channel| try channel.ready(self);
    }

    fn sendFailed(self: *Conn, err: anyerror) Error!void {
        self.sent_at = self.io.now();
        switch (err) {
            error.BrokenPipe, error.ConnectionResetByPeer => {},
            else => log.err("{} send failed {}", .{ self.socket, err }),
        }
        try self.close();
    }

    fn respond(self: *Conn, rsp: Response) !void {
        if (self.send_op != null) {
            self.pending_response = rsp;
            return;
        }
        switch (rsp) {
            .ok => try self.sendResponse("OK"),
            .heartbeat => try self.sendResponse("_heartbeat_"),
            .close => try self.sendResponse("CLOSE_WAIT"),
        }
    }

    fn sendResponse(self: *Conn, data: []const u8) !void {
        var hdr = &self.send_buf;
        assert(data.len <= hdr.len - 8);
        mem.writeInt(u32, hdr[0..4], @intCast(4 + data.len), .big);
        mem.writeInt(u32, hdr[4..8], @intFromEnum(protocol.FrameType.response), .big);
        @memcpy(hdr[8..][0..data.len], data);
        self.send_vec.prepOne(self.send_buf[0 .. data.len + 8]);
        try self.send();
    }

    fn close(self: *Conn) !void {
        log.debug("{} close", .{self.socket});
        if (self.channel) |channel| try channel.unsubscribe(self);
        try Op.cancel(self.ticker_op);
        try Op.cancel(self.recv_op);
        Op.unsubscribe(self.send_op);
        try self.io.close(self.socket);

        self.recv_buf.free();
        self.identify.deinit(self.allocator);
        self.send_vec.deinit(self.allocator);
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

/// Growable vectored send structures. Call prepInit then prep multiple times and
/// then use ptr in sendv.
const SendVec = struct {
    // iovlen in msghdr is limited by IOV_MAX in <limits.h>. On modern Linux
    // systems, the limit is 1024. Each message has header and body: 2 iovecs that
    // limits number of messages in a batch to 512.
    // ref: https://man7.org/linux/man-pages/man2/readv.2.html
    const max_msgs = 512;

    iov: []posix.iovec_const = &.{}, // header and body for each message
    msghdr: posix.msghdr_const = .{
        .iov = undefined,
        .iovlen = 0,
        .name = null,
        .namelen = 0,
        .control = null,
        .controllen = 0,
        .flags = 0,
    },

    const Self = @This();

    fn init(self: *Self, allocator: mem.Allocator) !void {
        // Start with one message space
        self.iov = try allocator.alloc(posix.iovec_const, 2);
        self.msghdr.iov = self.iov.ptr;
    }

    fn deinit(self: *Self, allocator: mem.Allocator) void {
        allocator.free(self.iov);
    }

    /// Max number of messages which can fit into iov.
    fn maxMsgs(self: *Self) usize {
        return self.iov.len / 2;
    }

    /// Number of prepared messages
    fn prepMsgs(self: *Self) usize {
        return @as(usize, @intCast(self.msghdr.iovlen)) / 2;
    }

    fn prepOne(self: *Self, header: []const u8) void {
        self.iov[0] = .{ .base = header.ptr, .len = header.len };
        self.msghdr.iovlen = 1;
    }

    /// Start of prepare multiple messages
    fn prepInit(self: *Self, allocator: mem.Allocator, want_msgs: usize) !void {
        self.msghdr.iovlen = 0;
        const new_len: usize = @as(usize, @intCast(@min(want_msgs, max_msgs))) * 2;
        if (new_len > self.iov.len) {
            allocator.free(self.iov);
            self.iov = try allocator.alloc(posix.iovec_const, new_len);
            self.msghdr.iov = self.iov.ptr;
        }
    }

    /// Puts each message(header/body) into iov
    fn prep(self: *Self, header: []const u8, body: []const u8) void {
        const n: usize = @intCast(self.msghdr.iovlen);
        self.iov[n] = .{ .base = header.ptr, .len = header.len };
        self.iov[n + 1] = .{ .base = if (body.len == 0) header.ptr else body.ptr, .len = body.len };
        self.msghdr.iovlen += 2;
    }

    /// Pointer to use in vectored send (sendv)
    fn ptr(self: *Self) *posix.msghdr_const {
        return &self.msghdr;
    }
};
