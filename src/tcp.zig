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
        socket: socket_t,
        io: *Io,
        op: Op = .{},
        conns: std.AutoHashMap(*ConnType, void),
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
                .socket = socket,
                .conns = std.AutoHashMap(*ConnType, void).init(allocator),
            };
            errdefer self.deinit();
            self.op = Op.accept(socket, self, onAccept, onAcceptFail);
            self.io.submit(&self.op);
        }

        pub fn deinit(self: *Self) void {
            var iter = self.conns.keyIterator();
            while (iter.next()) |e| {
                const conn = e.*;
                conn.deinit();
                self.allocator.destroy(conn);
            }
            self.conns.deinit();
        }

        fn onAccept(self: *Self, socket: socket_t, addr: std.net.Address) Error!void {
            var conn = try self.allocator.create(ConnType);
            errdefer self.allocator.destroy(conn);
            try self.conns.ensureUnusedCapacity(1);
            try conn.init(self, socket, addr);
            self.conns.putAssumeCapacityNoClobber(conn, {});
            self.metric.accept +%= 1;
        }

        fn onAcceptFail(self: *Self, err: anyerror) Error!void {
            log.err("accept failed {}", .{err});
            self.io.submit(&self.op);
        }

        pub fn remove(self: *Self, conn: *ConnType) void {
            assert(self.conns.remove(conn));
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

    recv_op: Op = .{},
    send_op: Op = .{},
    close_op: Op = .{},
    timer: *Io.Timer = undefined,
    // If send is in process store here response to send later
    pending_response: ?Response = null,
    ready_count: u32 = 0,
    in_flight: u32 = 0,
    heartbeat_interval: u32 = 0,
    outstanding_heartbeats: u8 = 0,
    send_vec: SendVec = .{},
    recv_buf: RecvBuf, // Unprocessed bytes from previous receive
    send_buf: [34]u8 = undefined, // Send buffer for control messages
    sent_at: u64 = 0, // Timestamp of the last finished send

    channel: ?*Channel = null,
    identify: protocol.Identify = .{},
    state: State = .connected,

    metric: struct {
        connected_at: u64 = 0,
        // Total number of
        send: usize = 0,
        finish: usize = 0,
        requeue: usize = 0,
    } = .{},

    const State = enum {
        connected,
        closing,
    };

    const Response = enum {
        ok,
        close,
        heartbeat,
    };

    // Until client set's connection heartbeat interval in identify message.
    const initial_heartbeat = 2000;

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
        self.timer = try self.io.initTimer(self, onTimer);
        errdefer self.timer.deinit();
        self.timer.setTicker(initial_heartbeat);
        self.send_op = Op.sendv(self.socket, self.send_vec.ptr(), self, onSend, onSendFail);
        self.recv_op = Op.recv(self.socket, self, onRecv, onRecvFail);
        self.io.submit(&self.recv_op);
        log.debug("{} connected", .{socket});
    }

    fn deinit(self: *Conn) void {
        self.recv_buf.free();
        self.identify.deinit(self.allocator);
        self.send_vec.deinit(self.allocator);
        self.timer.deinit();
    }

    // Channel api -----------------

    /// Consumer setting
    pub fn msgTimeout(self: *Conn) u32 {
        return self.identify.msg_timeout;
    }

    /// Number of messages connection is ready to send
    pub fn ready(self: *Conn) u32 {
        if (self.send_op.active()) return 0;
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
    // TODO not fallible any more
    pub fn sendPrepared(self: *Conn, msgs: u32) !void {
        assert(msgs == self.send_vec.prepMsgs());
        self.send();
        self.metric.send += msgs;
        self.in_flight += msgs;
    }

    /// When channel is deleted via web interface
    pub fn channelClosed(self: *Conn) void {
        self.channel = null;
        self.shutdown();
    }

    // IO callbacks -----------------

    fn onTimer(self: *Conn) void {
        if (self.state == .closing) return;
        if (self.outstanding_heartbeats > 4) {
            log.debug("{} no heartbeat, closing", .{self.socket});
            return self.shutdown();
        }
        if (self.outstanding_heartbeats > 0) {
            log.debug("{} heartbeat", .{self.socket});
            self.respond(.heartbeat) catch self.shutdown();
        }
        self.outstanding_heartbeats += 1;
    }

    fn onRecv(self: *Conn, bytes: []const u8) Error!void {
        // Any error is lack of resources, free this connection in the case of
        // any error.
        self.receivedData(bytes) catch |err| {
            log.err("{} recv failed {}", .{ self.socket, err });
            return self.shutdown();
        };
        if (!self.recv_op.hasMore()) self.shutdown();
    }

    fn onRecvFail(self: *Conn, err: anyerror) Error!void {
        switch (err) {
            error.EndOfFile, error.OperationCanceled, error.ConnectionResetByPeer => {},
            else => log.err("{} recv failed {}", .{ self.socket, err }),
        }
        self.shutdown();
    }

    fn onSend(self: *Conn) Error!void {
        self.sent_at = self.io.now();
        if (self.pending_response) |r| {
            try self.respond(r);
            self.pending_response = null;
            return;
        }
        if (self.channel) |channel| try channel.ready(self);
    }

    fn onSendFail(self: *Conn, err: anyerror) Error!void {
        self.sent_at = self.io.now();
        switch (err) {
            error.BrokenPipe, error.ConnectionResetByPeer => {},
            else => log.err("{} send failed {}", .{ self.socket, err }),
        }
        self.shutdown();
    }

    // ------------------------------

    fn receivedData(self: *Conn, bytes: []const u8) !void {
        var parser = protocol.Parser{ .buf = try self.recv_buf.append(bytes) };
        while (parser.next() catch |err| {
            log.err(
                "{} protocol parser failed {}, un-parsed: {d}",
                .{ self.socket, err, parser.unparsed()[0..@min(128, parser.unparsed().len)] },
            );
            return err;
        }) |msg| {
            try self.receivedMsg(msg);
        }

        try self.recv_buf.set(parser.unparsed());
        if (self.channel) |channel| if (self.ready() > 0) try channel.ready(self);
    }

    fn receivedMsg(self: *Conn, msg: protocol.Message) !void {
        const server = self.listener.server;
        const options = self.listener.options;
        self.outstanding_heartbeats = 0;

        switch (msg) {
            .identify => {
                const identify = try msg.parseIdentify(self.allocator, options);
                errdefer identify.deinit(self.allocator);
                try self.respond(.ok);
                self.timer.setTicker(identify.heartbeat_interval / 2);
                self.identify = identify;
                log.debug("{} identify {}", .{ self.socket, identify });
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
                if (arg.msgs == 0) return;
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
                log.debug("{} ready: {}", .{ self.socket, count });
            },
            .finish => |msg_id| {
                var channel = self.channel orelse return error.NotSubscribed;
                self.in_flight -|= 1;
                const res = channel.finish(msg_id);
                if (res) self.metric.finish += 1;
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

    fn send(self: *Conn) void {
        self.io.submit(&self.send_op);
    }

    fn respond(self: *Conn, rsp: Response) !void {
        if (self.send_op.active()) {
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
        self.send();
    }

    pub fn shutdown(self: *Conn) void {
        // log.debug("{} shutdown state: {s}", .{ self.socket, @tagName(self.state) });

        switch (self.state) {
            .connected => {
                // Start shutdown.
                self.ready_count = 0;
                self.pending_response = null;
                self.state = .closing;
                self.timer.cancel();
                if (self.channel) |channel| channel.unsubscribe(self);
                self.close_op = Op.shutdownClose(self.socket, self, shutdown);
                self.io.submit(&self.close_op);
            },
            .closing => {
                // Shutdown already in process.
                // Wait for all operation to finish.
                if (self.recv_op.active() or
                    self.send_op.active() or
                    self.close_op.active())
                    return;

                log.debug("{} closed", .{self.socket});
                self.deinit();
                self.listener.remove(self);
            },
        }
    }

    pub fn printStatus(self: *Conn) void {
        std.debug.print("  socket {} state: {s}, is done? recv: {} ticker: {} send: {} close: {}\n", .{
            self.socket,
            @tagName(self.state),
            self.recv_op.active(),
            false,
            self.send_op.active(),
            self.close_op.active(),
        });
    }
};

pub const RecvBuf = struct {
    allocator: mem.Allocator,
    buf: []u8 = &.{},

    const Self = @This();

    pub fn init(allocator: mem.Allocator) Self {
        return .{ .allocator = allocator };
    }

    pub fn free(self: *Self) void {
        self.allocator.free(self.buf);
        self.buf = &.{};
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
