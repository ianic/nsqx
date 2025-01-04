const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const net = std.net;
const posix = std.posix;

const io = @import("io/io.zig");
const protocol = @import("protocol.zig");
const Options = @import("Options.zig");
const Broker = @import("main.zig").Broker;
const Channel = Broker.Channel;
const timer = @import("timer.zig");

const log = std.log.scoped(.tcp);

pub fn ListenerType(comptime ConnType: type) type {
    return struct {
        allocator: mem.Allocator,
        broker: *Broker,
        options: Options,
        socket: posix.socket_t,
        io_loop: *io.Loop,
        op: io.Op = .{},
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
            io_loop: *io.Loop,
            broker: *Broker,
            options: Options,
            socket: posix.socket_t,
        ) !void {
            self.* = .{
                .allocator = allocator,
                .broker = broker,
                .options = options,
                .io_loop = io_loop,
                .socket = socket,
                .conns = std.AutoHashMap(*ConnType, void).init(allocator),
            };
            errdefer self.deinit();
            self.op = io.Op.accept(socket, self, onAccept, onAcceptFail);
            self.io_loop.submit(&self.op);
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

        fn onAccept(self: *Self, socket: posix.socket_t, addr: net.Address) io.Error!void {
            var conn = try self.allocator.create(ConnType);
            errdefer self.allocator.destroy(conn);
            try self.conns.ensureUnusedCapacity(1);
            try conn.init(self, socket, addr);
            errdefer conn.deinit();
            self.conns.putAssumeCapacityNoClobber(conn, {});
            self.metric.accept +%= 1;
        }

        fn onAcceptFail(self: *Self, err: anyerror) io.Error!void {
            log.err("accept failed {}", .{err});
            self.io_loop.submit(&self.op);
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
    io_loop: *io.Loop,

    // TODO: remove this two, we have that in tcp
    socket: posix.socket_t = 0,
    addr: net.Address,

    tcp: io.Tcp(*Conn),
    timer_op: timer.Op = undefined,

    timer_ts: u64,
    heartbeat_interval: u32 = initial_heartbeat,
    outstanding_heartbeats: u8 = 0,

    consumer: ?Broker.Consumer = null,
    identify: protocol.Identify = .{},
    state: State = .connected,

    const State = enum {
        connected,
        closing,
    };

    // Until client set's connection heartbeat interval in identify message.
    const initial_heartbeat = 2000;

    fn init(self: *Conn, listener: *Listener, socket: posix.socket_t, addr: net.Address) !void {
        const allocator = listener.allocator;
        self.* = .{
            .allocator = allocator,
            .listener = listener,
            .io_loop = listener.io_loop,
            .socket = socket,
            .addr = addr,
            .timer_ts = listener.io_loop.tsFromDelay(initial_heartbeat),
            .tcp = io.Tcp(*Conn).init(allocator, listener.io_loop, self),
        };

        self.tcp.address = addr; // TODO: fix this
        self.tcp.connected(socket);

        self.timer_op.init(&listener.broker.timer_queue, self, Conn.onTimer);
        try self.timer_op.update(initial_heartbeat);

        log.debug("{} connected {}", .{ socket, addr });
    }

    fn deinit(self: *Conn) void {
        self.identify.deinit(self.allocator);
        self.timer_op.deinit();
        self.tcp.deinit();
    }

    // Channel api -----------------

    pub fn send(self: *Conn, buf: []const u8) !void {
        return self.tcp.send(buf);
    }

    pub fn close(self: *Conn) void {
        self.tcp.close();
    }

    // IO callbacks -----------------

    pub fn onSend(self: *Conn, buf: []const u8) void {
        _, const frame_type = protocol.parseFrame(buf) catch unreachable;
        if (frame_type != .message) return;
        if (self.consumer) |*consumer|
            consumer.onSend(buf);
    }

    pub fn onClose(self: *Conn) void {
        if (self.consumer) |*consumer| {
            consumer.unsubscribe();
            self.consumer = null;
        }
        self.deinit();
        self.listener.remove(self);
    }

    pub fn onTimer(self: *Conn, _: u64) !u64 {
        if (self.state == .closing) return timer.infinite;
        if (self.outstanding_heartbeats > 4) {
            log.debug("{} no heartbeat, closing", .{self.socket});
            self.close();
            return timer.infinite;
        }
        if (self.outstanding_heartbeats > 0) {
            log.debug("{} heartbeat", .{self.socket});
            self.respond(.heartbeat) catch self.close();
        }
        self.outstanding_heartbeats += 1;
        return self.io_loop.tsFromDelay(self.heartbeat_interval);
    }

    pub fn onRecv(self: *Conn, bytes: []const u8) io.Error!usize {
        // Any error is lack of resources, close this connection in the case
        // of error.
        return self.receivedData(bytes) catch brk: {
            self.close();
            break :brk 0;
        };
    }

    // ------------------------------

    fn receivedData(self: *Conn, bytes: []const u8) !usize {
        var parser = protocol.Parser{ .buf = bytes };
        while (parser.next() catch |err| {
            log.err(
                "{} protocol parser failed with: {s}, un-parsed: '{d}'",
                .{ self.socket, @errorName(err), parser.unparsed()[0..@min(128, parser.unparsed().len)] },
            );
            switch (err) {
                error.Invalid => try self.respond(.invalid),
                error.InvalidName, error.InvalidNameCharacter => try self.respond(.bad_topic),
                else => return err,
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
        if (self.consumer) |*consumer| consumer.pull();
        return parser.pos;
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
                if (self.consumer) |*consumer|
                    consumer.msg_timeout = self.identify.msg_timeout;
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
                var consumer = &(self.consumer orelse return error.NotSubscribed);
                consumer.ready_count = if (count > options.max_rdy_count) options.max_rdy_count else count;
                log.debug("{} ready: {}", .{ self.socket, count });
            },
            .finish => |msg_id| {
                var consumer = &(self.consumer orelse return error.NotSubscribed);
                try consumer.finish(msg_id);
                log.debug("{} finish {}", .{ self.socket, protocol.msg_id.decode(msg_id) });
            },
            .requeue => |arg| {
                const delay = if (arg.delay > options.max_req_timeout)
                    options.max_req_timeout
                else
                    arg.delay;
                var consumer = &(self.consumer orelse return error.NotSubscribed);
                try consumer.requeue(arg.msg_id, delay);
                log.debug("{} requeue {}", .{ self.socket, protocol.msg_id.decode(arg.msg_id) });
            },
            .touch => |msg_id| {
                var consumer = &(self.consumer orelse return error.NotSubscribed);
                try consumer.touch(msg_id);
                log.debug("{} touch {}", .{ self.socket, protocol.msg_id.decode(msg_id) });
            },
            .close => {
                if (self.consumer) |*consumer|
                    consumer.ready_count = 0;
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
        self.tcp.send(rsp.body()) catch |err| {
            log.err("{} respond {}", .{ self.socket, err });
            self.close();
        };
    }
};
