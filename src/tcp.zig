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
const SendOp = @import("io.zig").SendOp;
const Error = @import("io.zig").Error;

const Broker = @import("main.zig").Broker;
const Channel = Broker.Channel;
const timer = @import("timer.zig");

const log = std.log.scoped(.tcp);

pub fn ListenerType(comptime ConnType: type) type {
    return struct {
        allocator: mem.Allocator,
        broker: *Broker,
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
            broker: *Broker,
            options: Options,
            socket: socket_t,
        ) !void {
            self.* = .{
                .allocator = allocator,
                .broker = broker,
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
            errdefer conn.deinit();
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
    send_op: SendOp = .{},
    timer_op: timer.Op = undefined,
    send_chunk: ?Channel.PullChunk = null,
    shutdown_op: Op = .{},
    pending_responses: std.ArrayList(protocol.Response),
    ready_count: u32 = 0,
    in_flight: u32 = 0,
    timer_ts: u64,
    heartbeat_interval: u32 = initial_heartbeat,
    outstanding_heartbeats: u8 = 0,
    recv_buf: RecvBuf, // Unprocessed bytes from previous receive
    send_buf: [34]u8 = undefined, // Send buffer for control messages

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

    // Until client set's connection heartbeat interval in identify message.
    const initial_heartbeat = 2000;

    fn init(self: *Conn, listener: *Listener, socket: socket_t, addr: std.net.Address) !void {
        const allocator = listener.allocator;
        self.* = .{
            .allocator = allocator,
            .listener = listener,
            .io = listener.io,
            .socket = socket,
            .addr = addr,
            .recv_buf = RecvBuf.init(listener.allocator),
            .metric = .{ .connected_at = listener.io.now() },
            .timer_ts = listener.io.tsFromDelay(initial_heartbeat),
            .pending_responses = std.ArrayList(protocol.Response).init(allocator),
        };
        try self.send_op.init(allocator, socket, self, onSend, onSendFail);
        errdefer self.send_op.deinit(allocator);

        self.timer_op.init(&listener.broker.timer_queue, self, Conn.onTimer);
        try self.timer_op.update(initial_heartbeat);

        self.recv_op = Op.recv(socket, self, onRecv, onRecvFail);
        self.io.submit(&self.recv_op);
        log.debug("{} connected {}", .{ socket, addr });
    }

    fn deinit(self: *Conn) void {
        self.recv_buf.free();
        self.identify.deinit(self.allocator);
        self.send_op.deinit(self.allocator);
        self.pending_responses.deinit();
        self.timer_op.deinit();
    }

    // Channel api -----------------

    /// Unique consumer identification. Used to know which in-flight message is
    /// sent to which consumer.
    pub fn id(self: *Conn) u32 {
        return @intCast(self.socket);
    }

    /// Setting from identify message.
    pub fn msgTimeout(self: *Conn) u32 {
        return self.identify.msg_timeout;
    }

    /// Is connection ready to send messages.
    pub fn ready(self: *Conn) bool {
        return !(self.send_op.active() or
            self.in_flight >= self.ready_count or
            self.state != .connected);
    }

    pub fn readyCount(self: *Conn) u32 {
        return if (self.ready()) self.ready_count - self.in_flight else 0;
    }

    /// Pull messages from channel and send.
    pub fn wakeup(self: *Conn) void {
        if (self.send_op.active() or self.state != .connected) return;

        { // prepare pending responses
            while (self.send_op.free() > 0) {
                const rsp = self.pending_responses.popOrNull() orelse break;
                self.send_op.prep(rsp.body());
            }
        }

        { // prepare messages
            if (self.channel) |channel| {
                const ready_count = self.readyCount();
                if (ready_count > 0 and self.send_op.free() > 0) {
                    if (channel.pull(self.id(), self.msgTimeout(), ready_count) catch |err| brk: {
                        log.err("{} failed to pull from channel {}", .{ self.socket, err });
                        break :brk null;
                    }) |res| {
                        self.send_op.prep(res.data);
                        self.send_chunk = res;
                        self.metric.send +%= res.count;
                        self.in_flight += res.count;
                    }
                }
            }
        }

        if (self.send_op.count() > 0)
            self.send_op.send(self.io);
    }

    // IO callbacks -----------------

    pub fn onTimer(self: *Conn, _: u64) !u64 {
        if (self.state == .closing) return timer.infinite;
        if (self.outstanding_heartbeats > 4) {
            log.debug("{} no heartbeat, closing", .{self.socket});
            self.shutdown();
            return timer.infinite;
        }
        if (self.outstanding_heartbeats > 0) {
            log.debug("{} heartbeat", .{self.socket});
            self.respond(.heartbeat) catch self.shutdown();
        }
        self.outstanding_heartbeats += 1;
        return self.io.tsFromDelay(self.heartbeat_interval);
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
        self.sendDone();
        self.wakeup();
    }

    fn sendDone(self: *Conn) void {
        if (self.send_chunk) |sc| {
            sc.done();
            self.send_chunk = null;
        }
    }

    fn onSendFail(self: *Conn, err: anyerror) Error!void {
        self.sendDone();
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
                "{} protocol parser failed with: {s}, un-parsed: '{d}'",
                .{ self.socket, @errorName(err), parser.unparsed()[0..@min(128, parser.unparsed().len)] },
            );
            switch (err) {
                error.Invalid => try self.respond(.invalid),
                error.InvalidName, error.InvalidNameCharacter => try self.respond(.bad_topic),
            }
            return err; // fatal, close connection on parser error
        }) |msg| {
            self.receivedMsg(msg) catch |err| {
                log.err(
                    "{} message error: {s}, operation: {s} ",
                    .{ self.socket, @errorName(err), @tagName(msg) },
                );
                switch (err) {
                    error.Invalid,
                    error.NotSubscribed,
                    => try self.respond(.invalid),
                    error.MessageSizeOverflow,
                    error.BadMessage,
                    => try self.respond(.bad_message),
                    error.StreamMemoryLimit => try self.respond(.pub_failed),
                    error.MessageNotInFlight => {
                        switch (msg) {
                            .finish => try self.respond(.fin_failed),
                            .touch => try self.respond(.touch_failed),
                            .requeue => try self.respond(.requeue_failed),
                            else => return err,
                        }
                    },
                    else => return err, // close connection on all other errors
                }
            };
        }

        try self.recv_buf.set(parser.unparsed());
        self.wakeup();
    }

    fn receivedMsg(self: *Conn, msg: protocol.Message) !void {
        const broker = self.listener.broker;
        const options = self.listener.options;
        self.outstanding_heartbeats = 0;

        switch (msg) {
            .identify => {
                const identify = try msg.parseIdentify(self.allocator, options);
                errdefer identify.deinit(self.allocator);
                try self.respond(.ok);
                self.identify = identify;
                self.heartbeat_interval = identify.heartbeat_interval / 2;
                log.debug("{} identify {}", .{ self.socket, identify });
            },
            .subscribe => |arg| {
                try broker.subscribe(self, arg.topic, arg.channel);
                try self.respond(.ok);
                log.debug("{} subscribe: {s} {s}", .{ self.socket, arg.topic, arg.channel });
            },
            .publish => |arg| {
                try broker.publish(arg.topic, arg.data);
                try self.respond(.ok);
                log.debug("{} publish: {s}", .{ self.socket, arg.topic });
            },
            .multi_publish => |arg| {
                if (arg.msgs == 0) return error.BadMessage;
                try broker.multiPublish(arg.topic, arg.msgs, arg.data);
                try self.respond(.ok);
                log.debug("{} multi publish: {s} messages: {}", .{ self.socket, arg.topic, arg.msgs });
            },
            .deferred_publish => |arg| {
                if (arg.delay > options.max_req_timeout) return error.Invalid;
                try broker.deferredPublish(arg.topic, arg.data, arg.delay);
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
                try channel.finish(self.id(), msg_id);
                self.metric.finish += 1;
                log.debug("{} finish {}", .{ self.socket, protocol.msg_id.decode(msg_id) });
            },
            .requeue => |arg| {
                var channel = self.channel orelse return error.NotSubscribed;
                self.in_flight -|= 1;
                const delay = if (arg.delay > options.max_req_timeout)
                    options.max_req_timeout
                else
                    arg.delay;
                try channel.requeue(self.id(), arg.msg_id, delay);
                self.metric.requeue += 1;
                log.debug("{} requeue {}", .{ self.socket, protocol.msg_id.decode(arg.msg_id) });
            },
            .touch => |msg_id| {
                var channel = self.channel orelse return error.NotSubscribed;
                try channel.touch(self.id(), msg_id, self.msgTimeout());
                log.debug("{} touch {}", .{ self.socket, protocol.msg_id.decode(msg_id) });
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
                return error.UnsupportedOperation;
            },
            .version => unreachable, // handled in the parser
        }
    }

    fn respond(self: *Conn, rsp: protocol.Response) !void {
        if (self.send_op.active())
            return try self.pending_responses.insert(0, rsp);

        self.send_op.prep(rsp.body());
        self.send_op.send(self.io);
    }

    fn onClose(self: *Conn, _: ?anyerror) void {
        self.shutdown();
    }

    pub fn shutdown(self: *Conn) void {
        switch (self.state) {
            .connected => {
                // Start shutdown.
                self.ready_count = 0; // stop sending
                self.state = .closing;
                self.shutdown_op = Op.shutdown(self.socket, self, onClose);
                self.io.submit(&self.shutdown_op);
            },
            .closing => {
                // Close recv if not closed by shutdown
                if (self.recv_op.active() and !self.shutdown_op.active()) {
                    self.shutdown_op = Op.cancel(&self.recv_op, self, onClose);
                    self.io.submit(&self.shutdown_op);
                    return;
                }
                // Wait for all operation to finish.
                if (self.recv_op.active() or
                    self.send_op.active() or
                    self.shutdown_op.active())
                    return;

                if (self.channel) |channel| channel.unsubscribe(self);

                log.debug("{} closed", .{self.socket});
                self.deinit();
                self.listener.remove(self);
            },
        }
    }

    pub fn printStatus(self: *Conn) void {
        std.debug.print("  socket {:>3} {s} state: {s}, capacity: {} {}  active:{s}{s}{s}\n", .{
            @as(u32, @intCast(self.socket)),
            if (self.channel != null) "sub" else "pub",
            @tagName(self.state),

            self.send_op.capacity(),
            self.pending_responses.capacity,

            if (self.recv_op.active()) " recv" else "",
            if (self.send_op.active()) " send" else "",
            if (self.shutdown_op.active()) " shutdown" else "",
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
