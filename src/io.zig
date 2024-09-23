const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const posix = std.posix;
const linux = std.os.linux;
const IoUring = linux.IoUring;
const socket_t = std.posix.socket_t;
const fd_t = std.posix.fd_t;
const errFromErrno = @import("errno.zig").toError;
const Atomic = std.atomic.Value;

const ns_per_ms = std.time.ns_per_ms;
const ns_per_s = std.time.ns_per_s;

const log = std.log.scoped(.io);

pub const Error = error{
    OutOfMemory,
    SubmissionQueueFull,
};

pub const Io = struct {
    allocator: mem.Allocator,
    op_pool: std.heap.MemoryPool(Op) = undefined,
    ring: IoUring = undefined,
    timestamp: u64 = 0,
    recv_buf_grp: IoUring.BufferGroup = undefined,
    metric: Metric = .{},
    metric_prev: Metric = .{},

    const Metric = struct {
        loops: usize = 0,
        cqes: usize = 0,
        send_bytes: usize = 0,
        recv_bytes: usize = 0,

        recv_buf_grp: struct {
            success: usize = 0,
            no_bufs: usize = 0,

            pub fn noBufs(self: @This()) f64 {
                const total = self.success + self.no_bufs;
                if (total == 0) return 0;
                return @as(f64, @floatFromInt(self.no_bufs)) / @as(f64, @floatFromInt(total)) * 100;
            }
        } = .{},

        all: Counter = .{},
        accept: Counter = .{},
        connect: Counter = .{},
        close: Counter = .{},
        recv: Counter = .{},
        writev: Counter = .{},
        sendv: Counter = .{},
        ticker: Counter = .{},

        // Counter of submitted/completed operations
        const Counter = struct {
            submitted: usize = 0,
            completed: usize = 0,
            restarted: usize = 0,
            max_active: usize = 0,

            // Current number of active operations
            pub fn active(self: Counter) usize {
                return self.submitted - self.restarted - self.completed;
            }
            fn submit(self: *Counter) void {
                self.submitted += 1;
                if (self.active() > self.max_active)
                    self.max_active = self.active();
            }
            fn complete(self: *Counter) void {
                self.completed += 1;
            }
            fn restart(self: *Counter) void {
                self.restarted += 1;
            }
            pub fn format(
                self: Counter,
                comptime fmt: []const u8,
                options: std.fmt.FormatOptions,
                writer: anytype,
            ) !void {
                _ = fmt;
                _ = options;

                try writer.print(
                    "active: {:>8}, max active: {:>8}, submitted: {:>8}, restarted: {:>8}, completed: {:>8}",
                    .{ self.active(), self.max_active, self.submitted, self.restarted, self.completed },
                );
            }

            fn write(prefix: []const u8, cur: Counter, prev: Counter, writer: anytype) !void {
                try writer.counter(prefix, "submitted", cur.submitted, prev.submitted);
                try writer.counter(prefix, "completed", cur.completed, prev.completed);
                try writer.counter(prefix, "restarted", cur.restarted, prev.restarted);
                try writer.gauge(prefix, "active", cur.active());
            }
        };

        fn submit(self: *Metric, op: *Op) void {
            self.all.submit();
            switch (op.args) {
                .accept => self.accept.submit(),
                .connect => self.connect.submit(),
                .close => self.close.submit(),
                .recv => self.recv.submit(),
                .writev => self.writev.submit(),
                .send, .sendv => self.sendv.submit(),
                .ticker, .timer => self.ticker.submit(),
                .socket => {},
            }
        }

        fn complete(self: *Metric, op: *Op) void {
            self.all.complete();
            switch (op.args) {
                .accept => self.accept.complete(),
                .connect => self.connect.complete(),
                .close => self.close.complete(),
                .recv => self.recv.complete(),
                .writev => self.writev.complete(),
                .send, .sendv => self.sendv.complete(),
                .ticker, .timer => self.ticker.complete(),
                .socket => {},
            }
        }

        fn restart(self: *Metric, op: *Op) void {
            self.all.restart();
            switch (op.args) {
                .accept => self.accept.restart(),
                .connect => self.connect.restart(),
                .close => self.close.restart(),
                .recv => self.recv.restart(),
                .writev => self.writev.restart(),
                .send, .sendv => self.sendv.restart(),
                .ticker, .timer => self.ticker.restart(),
                .socket => {},
            }
        }
    };

    pub fn writeMetrics(self: *Io, writer: anytype) !void {
        const cur = self.metric;
        const prev = self.metric_prev;
        const write = Metric.Counter.write;

        try write("io.op.all", cur.all, prev.all, writer);
        try write("io.op.send", cur.sendv, prev.sendv, writer);
        try write("io.op.recv", cur.recv, prev.recv, writer);
        try write("io.op.accept", cur.accept, prev.accept, writer);
        try write("io.op.connect", cur.connect, prev.connect, writer);
        try write("io.op.ticker", cur.ticker, prev.ticker, writer);
        {
            try writer.counter("io", "loops", cur.loops, prev.loops);
            try writer.counter("io", "cqes", cur.cqes, prev.cqes);

            try writer.counter("io", "send_bytes", cur.send_bytes, prev.send_bytes);
            try writer.counter("io", "recv_bytes", cur.recv_bytes, prev.recv_bytes);
        }
        {
            try writer.counter("io.recv_buf_grp", "success", cur.recv_buf_grp.success, prev.recv_buf_grp.success);
            try writer.counter("io.recv_buf_grp", "no_bufs", cur.recv_buf_grp.no_bufs, prev.recv_buf_grp.no_bufs);
        }
        self.metric_prev = cur;
    }

    pub fn init(self: *Io, ring_entries: u16, recv_buffers: u16, recv_buffer_len: u32) !void {
        self.ring = try IoUring.init(ring_entries, linux.IORING_SETUP_SQPOLL | linux.IORING_SETUP_SINGLE_ISSUER);
        self.timestamp = timestamp();
        if (recv_buffers > 0) {
            self.recv_buf_grp = try self.initBufferGroup(1, recv_buffers, recv_buffer_len);
        } else {
            self.recv_buf_grp.buffers_count = 0;
        }
        self.op_pool = try std.heap.MemoryPool(Op).initPreheated(self.allocator, 1024);
    }

    fn initBufferGroup(self: *Io, id: u16, count: u16, size: u32) !IoUring.BufferGroup {
        const buffers = try self.allocator.alloc(u8, count * size);
        errdefer self.allocator.free(buffers);
        return try IoUring.BufferGroup.init(&self.ring, id, buffers, size, count);
    }

    pub fn deinit(self: *Io) void {
        if (self.recv_buf_grp.buffers_count > 0) {
            self.allocator.free(self.recv_buf_grp.buffers);
            self.recv_buf_grp.deinit();
        }
        self.ring.deinit();
        self.op_pool.deinit();
    }

    fn acquire(self: *Io) !*Op {
        return try self.op_pool.create();
    }

    fn release(self: *Io, op: *Op) void {
        self.metric.complete(op);
        self.op_pool.destroy(op);
    }

    pub fn accept(
        self: *Io,
        socket: socket_t,
        context: anytype,
        comptime accepted: fn (@TypeOf(context), socket_t, std.net.Address) Error!void,
        comptime failed: fn (@TypeOf(context), anyerror) Error!void,
    ) !*Op {
        const op = try self.acquire();
        op.* = Op.accept(self, socket, context, accepted, failed);
        try op.prep();
        return op;
    }

    pub fn connect(
        self: *Io,
        socket: socket_t,
        addr: std.net.Address,
        context: anytype,
        comptime connected: fn (@TypeOf(context)) Error!void,
        comptime failed: fn (@TypeOf(context), anyerror) Error!void,
    ) !*Op {
        const op = try self.acquire();
        op.* = Op.connect(self, socket, addr, context, connected, failed);
        try op.prep();
        return op;
    }

    pub fn recv(
        self: *Io,
        socket: socket_t,
        context: anytype,
        comptime received: fn (@TypeOf(context), []const u8) Error!void,
        comptime failed: fn (@TypeOf(context), anyerror) Error!void,
    ) !*Op {
        const op = try self.acquire();
        op.* = Op.recv(self, socket, context, received, failed);
        try op.prep();
        return op;
    }

    pub fn writev(
        self: *Io,
        socket: socket_t,
        vec: []posix.iovec_const,
        context: anytype,
        comptime sent: fn (@TypeOf(context), usize) Error!void,
        comptime failed: fn (@TypeOf(context), anyerror) Error!void,
    ) !*Op {
        const op = try self.acquire();
        op.* = Op.writev(self, socket, vec, context, sent, failed);
        try op.prep();
        return op;
    }

    pub fn sendv(
        self: *Io,
        socket: socket_t,
        msghdr: *posix.msghdr_const,
        context: anytype,
        comptime sent: fn (@TypeOf(context)) Error!void,
        comptime failed: fn (@TypeOf(context), anyerror) Error!void,
    ) !*Op {
        const op = try self.acquire();
        op.* = Op.sendv(self, socket, msghdr, context, sent, failed);
        try op.prep();
        return op;
    }

    pub fn send(
        self: *Io,
        socket: socket_t,
        buf: []const u8,
        context: anytype,
        comptime sent: fn (@TypeOf(context)) Error!void,
        comptime failed: fn (@TypeOf(context), anyerror) Error!void,
    ) !*Op {
        const op = try self.acquire();
        op.* = Op.send(self, socket, buf, context, sent, failed);
        try op.prep();
        return op;
    }

    pub fn ticker(
        self: *Io,
        msec: i64, // miliseconds  TODO: use msec or nsec on both places
        context: anytype,
        comptime ticked: fn (@TypeOf(context)) Error!void,
        comptime failed: ?fn (@TypeOf(context), anyerror) Error!void,
    ) !*Op {
        const op = try self.acquire();
        op.* = Op.ticker(self, msec, context, ticked, failed);
        try op.prep();
        return op;
    }

    pub fn timer(
        self: *Io,
        nsec: u64, // delay in nanoseconds
        context: anytype,
        comptime ticked: fn (@TypeOf(context)) Error!void,
        comptime failed: ?fn (@TypeOf(context), anyerror) Error!void,
    ) !*Op {
        const op = try self.acquire();
        op.* = Op.timer(self, nsec, context, ticked, failed);
        try op.prep();
        return op;
    }

    pub fn socketCreate(
        self: *Io,
        domain: u32,
        socket_type: u32,
        protocol: u32,
        context: anytype,
        comptime success: fn (@TypeOf(context), socket_t) Error!void,
        comptime failed: ?fn (@TypeOf(context), anyerror) Error!void,
    ) !*Op {
        const op = try self.acquire();
        op.* = Op.socketCreate(self, domain, socket_type, protocol, context, success, failed);
        try op.prep();
        return op;
    }

    pub fn close(self: *Io, socket: socket_t) !void {
        const op = try self.acquire();
        op.* = Op.close(self, socket);
        try op.prep();
    }

    pub fn loop(self: *Io, run: Atomic(bool)) !void {
        while (run.load(.monotonic)) try self.tick();
    }

    pub fn tick(self: *Io) !void {
        var cqes: [256]std.os.linux.io_uring_cqe = undefined;
        self.metric.loops += 1;
        const n = try self.readCompletions(&cqes);
        if (n > 0) {
            const new_timestamp = timestamp();
            self.timestamp = if (new_timestamp == self.timestamp) new_timestamp + 1 else new_timestamp;
            try self.flushCompletions(cqes[0..n]);
        }
    }

    pub fn drain(self: *Io) !void {
        while (self.metric.all.active() > 0) {
            log.debug("draining active operations: {}", .{self.metric.all.active()});
            try self.tick();
        }
    }

    fn readCompletions(self: *Io, cqes: []linux.io_uring_cqe) !usize {
        _ = self.ring.submit() catch |err| switch (err) {
            error.SignalInterrupt => 0,
            else => return err,
        };
        return self.ring.copy_cqes(cqes, 1) catch |err| switch (err) {
            error.SignalInterrupt => 0,
            else => return err,
        };
    }

    fn flushCompletions(self: *Io, cqes: []linux.io_uring_cqe) !void {
        self.metric.cqes += cqes.len;
        for (cqes) |cqe| {
            if (cqe.user_data == 0) continue; // no op for this cqe
            const op: *Op = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
            if (op.context == 0) {
                if (!flagMore(cqe)) self.release(op);
                continue;
            }
            while (true) {
                const res = op.callback(op, cqe) catch |err| {
                    log.err("callback failed {}", .{err});
                    switch (err) {
                        error.SubmissionQueueFull => {
                            _ = self.ring.submit() catch |submit_err| switch (submit_err) {
                                error.SignalInterrupt => continue,
                                else => return submit_err,
                            };
                        },
                        else => return err,
                    }
                    return err;
                };
                if (!flagMore(cqe)) {
                    switch (res) {
                        .done => self.release(op),
                        .restart => {
                            self.metric.restart(op);
                            try op.prep();
                        },
                    }
                }
                break;
            }
        }
    }

    pub fn now(self: *Io) u64 {
        return self.timestamp;
    }

    pub fn initTimer(
        self: *Io,
        context: anytype,
        comptime cb: fn (@TypeOf(context)) Error!void,
    ) Timer {
        return Timer.init(self, context, cb);
    }

    pub const Timer = struct {
        io: *Io,
        op: ?*Op = null,
        fire_at: u64 = no_timeout,
        closed: bool = false,

        context: usize = 0,
        callback: *const fn (*Self) Error!void = undefined,

        pub const no_timeout: u64 = std.math.maxInt(u64);
        const Self = @This();

        pub fn init(
            io: *Io,
            context: anytype,
            comptime cb: fn (@TypeOf(context)) Error!void,
        ) Timer {
            const Context = @TypeOf(context);
            const wrapper = struct {
                fn complete(t: *Self) Error!void {
                    const ctx: Context = @ptrFromInt(t.context);
                    try cb(ctx);
                }
            };
            return .{
                .io = io,
                .context = @intFromPtr(context),
                .callback = wrapper.complete,
            };
        }

        pub fn now(self: *Self) u64 {
            return self.io.timestamp;
        }

        /// Set callback to be called at fire_at.
        pub fn set(self: *Self, fire_at: u64) !void {
            if (self.closed) return;
            if (fire_at == no_timeout) return;
            const now_ = self.now();
            if (fire_at <= now_) return;
            if (self.op != null and self.fire_at <= fire_at) return;

            const delay = fire_at - now_;
            try self.reset();
            self.op = try self.io.timer(delay, self, ticked, failed);
            self.fire_at = fire_at;
        }

        pub fn reset(self: *Self) !void {
            if (self.op) |op| {
                try op.cancel();
                op.unsubscribe(self);
                self.op = null;
            }
        }

        pub fn close(self: *Self) !void {
            if (self.closed) return;
            try self.reset();
            self.closed = true;
        }

        fn ticked(self: *Self) Error!void {
            self.op = null;
            try self.callback(self);
        }

        fn failed(self: *Self, err: anyerror) Error!void {
            self.op = null;
            log.err("timer failed {}", .{err});
        }
    };
};

fn flagMore(cqe: linux.io_uring_cqe) bool {
    return cqe.flags & linux.IORING_CQE_F_MORE > 0;
}

pub const Op = struct {
    io: *Io,
    context: u64 = 0,
    callback: *const fn (*Op, linux.io_uring_cqe) Error!CallbackResult = undefined,
    args: Args,

    const CallbackResult = enum {
        done,
        restart,
    };

    const Args = union(Kind) {
        accept: struct {
            socket: socket_t,
            addr: posix.sockaddr align(4) = undefined,
            addr_size: posix.socklen_t = @sizeOf(posix.sockaddr),
        },
        connect: struct {
            socket: socket_t,
            addr: std.net.Address,
        },
        close: socket_t,
        recv: socket_t,
        writev: struct {
            socket: socket_t,
            vec: []posix.iovec_const,
        },
        sendv: struct {
            socket: socket_t,
            msghdr: *posix.msghdr_const,
        },
        send: struct {
            socket: socket_t,
            buf: []const u8,
        },
        ticker: linux.kernel_timespec,
        timer: linux.kernel_timespec,
        socket: struct {
            domain: u32,
            socket_type: u32,
            protocol: u32,
        },
    };

    const Kind = enum {
        accept,
        connect,
        close,
        recv,
        writev,
        sendv,
        send,
        ticker,
        timer,
        socket,
    };

    pub fn cancel(op: *Op) !void {
        switch (op.args) {
            .timer, .ticker => _ = try op.io.ring.timeout_remove(0, @intFromPtr(op), 0),
            else => _ = try op.io.ring.cancel(0, @intFromPtr(op), 0),
        }
    }

    pub fn unsubscribe(op: *Op, context: anytype) void {
        if (op.context == @intFromPtr(context))
            op.context = 0;
    }

    fn prep(op: *Op) !void {
        op.io.metric.submit(op);
        const IORING_TIMEOUT_MULTISHOT = 1 << 6; // TODO: missing in linux. package
        switch (op.args) {
            .accept => |*arg| _ = try op.io.ring.accept_multishot(@intFromPtr(op), arg.socket, &arg.addr, &arg.addr_size, 0),
            .connect => |*arg| _ = try op.io.ring.connect(@intFromPtr(op), arg.socket, &arg.addr.any, arg.addr.getOsSockLen()),
            .close => |socket| _ = try op.io.ring.close(@intFromPtr(op), socket),
            .recv => |socket| _ = try op.io.recv_buf_grp.recv_multishot(@intFromPtr(op), socket, 0),
            .writev => |*arg| _ = try op.io.ring.writev(@intFromPtr(op), arg.socket, arg.vec, 0),
            .sendv => |*arg| _ = try op.io.ring.sendmsg(@intFromPtr(op), arg.socket, arg.msghdr, linux.MSG.WAITALL | linux.MSG.NOSIGNAL),
            .send => |*arg| _ = try op.io.ring.send(@intFromPtr(op), arg.socket, arg.buf, linux.MSG.WAITALL | linux.MSG.NOSIGNAL),
            .ticker => |*ts| _ = try op.io.ring.timeout(@intFromPtr(op), ts, 0, IORING_TIMEOUT_MULTISHOT),
            .timer => |*ts| _ = try op.io.ring.timeout(@intFromPtr(op), ts, 0, 0),
            .socket => |*arg| _ = try op.io.ring.socket(@intFromPtr(op), arg.domain, arg.socket_type, arg.protocol, 0),
        }
    }

    fn accept(
        io: *Io,
        socket: socket_t,
        context: anytype,
        comptime accepted: fn (@TypeOf(context), socket_t, std.net.Address) Error!void,
        comptime fail: fn (@TypeOf(context), anyerror) Error!void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, cqe: linux.io_uring_cqe) Error!CallbackResult {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS => {
                        // Note that for the multishot variants, setting addr and
                        // addrlen may not make a lot of sense, as the same value would
                        // be used for every accepted connection. This means that the
                        // data written to addr may be overwritten by a new connection
                        // before the application has had time to process a past
                        // connection.
                        // Ref: https://man7.org/linux/man-pages/man3/io_uring_prep_multishot_accept.3.html

                        const addr = std.net.Address.initPosix(&op.args.accept.addr);
                        try accepted(ctx, @intCast(cqe.res), addr);
                    },
                    .CONNABORTED, .INTR => {}, // continue
                    else => |errno| {
                        try fail(ctx, errFromErrno(errno));
                        return .done;
                    },
                }
                return .restart;
            }
        };
        return .{
            .io = io,
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .accept = .{ .socket = socket } },
        };
    }

    fn connect(
        io: *Io,
        socket: socket_t,
        addr: std.net.Address,
        context: anytype,
        comptime connected: fn (@TypeOf(context)) Error!void,
        comptime fail: fn (@TypeOf(context), anyerror) Error!void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, cqe: linux.io_uring_cqe) Error!CallbackResult {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS => try connected(ctx),
                    .INTR => return .restart,
                    else => |errno| try fail(ctx, errFromErrno(errno)),
                }
                return .done;
            }
        };
        return .{
            .io = io,
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .connect = .{ .socket = socket, .addr = addr } },
        };
    }

    fn recv(
        io: *Io,
        socket: socket_t,
        context: anytype,
        comptime received: fn (@TypeOf(context), []const u8) Error!void,
        comptime failed: fn (@TypeOf(context), anyerror) Error!void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, cqe: linux.io_uring_cqe) Error!CallbackResult {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS => {
                        const n: usize = @intCast(cqe.res);
                        if (n == 0) {
                            try failed(ctx, error.EndOfFile);
                            return .done;
                        }
                        op.io.metric.recv_bytes +%= n;

                        const buffer_id = cqe.buffer_id() catch unreachable;
                        const bytes = op.io.recv_buf_grp.get(buffer_id)[0..n];
                        defer op.io.recv_buf_grp.put(buffer_id);
                        try received(ctx, bytes);
                        op.io.metric.recv_buf_grp.success += 1;
                    },
                    .NOBUFS => {
                        op.io.metric.recv_buf_grp.no_bufs += 1;
                        //log.warn("{} recv buffer group NOBUFS temporary error", .{op.args.recv});
                    },
                    .INTR => {},
                    else => |errno| {
                        try failed(ctx, errFromErrno(errno));
                        return .done;
                    },
                }
                return .restart;
            }
        };
        return .{
            .io = io,
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .recv = socket },
        };
    }

    fn writev(
        io: *Io,
        socket: socket_t,
        vec: []posix.iovec_const,
        context: anytype,
        comptime sent: fn (@TypeOf(context), usize) Error!void,
        comptime failed: fn (@TypeOf(context), anyerror) Error!void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, cqe: linux.io_uring_cqe) Error!CallbackResult {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS => {
                        const n: usize = @intCast(cqe.res);
                        const v = resizeIovec(op.args.writev.vec, n);
                        if (v.len > 0) { // restart on short send
                            log.warn("short writev {}", .{n}); // TODO: remove
                            op.args.writev.vec = v;
                            return .restart;
                        }
                        try sent(ctx, n);
                    },
                    .INTR => return .restart,
                    else => |errno| try failed(ctx, errFromErrno(errno)),
                }
                return .done;
            }
        };
        return .{
            .io = io,
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .writev = .{ .socket = socket, .vec = vec } },
        };
    }

    fn sendv(
        io: *Io,
        socket: socket_t,
        msghdr: *posix.msghdr_const,
        context: anytype,
        comptime sent: fn (@TypeOf(context)) Error!void,
        comptime failed: fn (@TypeOf(context), anyerror) Error!void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, cqe: linux.io_uring_cqe) Error!CallbackResult {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS => {
                        const n: usize = @intCast(cqe.res);
                        op.io.metric.send_bytes +%= n;

                        // zero copy send handling
                        // if (cqe.flags & linux.IORING_CQE_F_MORE > 0) {
                        //     log.debug("{} send_zc f_more {}", .{ op.args.sendv.socket, n });
                        //     return .has_more;
                        // }
                        // if (cqe.flags & linux.IORING_CQE_F_NOTIF > 0) {
                        //     log.debug("{} send_zc f_notif {}", .{ op.args.sendv.socket, n });
                        //     try sent(ctx, n);
                        //     return .done;
                        // }

                        var m: usize = 0;
                        for (0..@intCast(op.args.sendv.msghdr.iovlen)) |i|
                            m += op.args.sendv.msghdr.iov[i].len;
                        // While sending with MSG_WAITALL we don't expect to get short send
                        if (n < m) {
                            log.warn("{} unexpected short send len: {} sent: {}", .{ op.args.sendv.socket, m, n });
                            var v: []posix.iovec_const = undefined;
                            v.ptr = @constCast(op.args.sendv.msghdr.iov);
                            v.len = @intCast(op.args.sendv.msghdr.iovlen);
                            v = resizeIovec(v, n);
                            if (v.len > 0) { // restart on short send
                                op.args.sendv.msghdr.iov = v.ptr;
                                op.args.sendv.msghdr.iovlen = @intCast(v.len);
                                return .restart;
                            }
                        }

                        try sent(ctx);
                    },
                    .INTR => return .restart,
                    else => |errno| try failed(ctx, errFromErrno(errno)),
                }
                return .done;
            }
        };
        return .{
            .io = io,
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{
                .sendv = .{ .socket = socket, .msghdr = msghdr },
            },
        };
    }

    fn send(
        io: *Io,
        socket: socket_t,
        buf: []const u8,
        context: anytype,
        comptime sent: fn (@TypeOf(context)) Error!void,
        comptime failed: fn (@TypeOf(context), anyerror) Error!void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, cqe: linux.io_uring_cqe) Error!CallbackResult {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS => {
                        const n: usize = @intCast(cqe.res);
                        op.io.metric.send_bytes +%= n;
                        const send_buf = op.args.send.buf;
                        if (n < send_buf.len) {
                            op.args.send.buf = send_buf[n..];
                            return .restart;
                        }
                        try sent(ctx);
                    },
                    .INTR => return .restart,
                    else => |errno| try failed(ctx, errFromErrno(errno)),
                }
                return .done;
            }
        };
        return .{
            .io = io,
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{
                .send = .{ .socket = socket, .buf = buf },
            },
        };
    }

    fn ticker(
        io: *Io,
        msec: i64, // milliseconds
        context: anytype,
        comptime ticked: fn (@TypeOf(context)) Error!void,
        comptime failed: ?fn (@TypeOf(context), anyerror) Error!void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, cqe: linux.io_uring_cqe) Error!CallbackResult {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS, .TIME => try ticked(ctx),
                    .INTR => {},
                    else => |errno| {
                        if (failed) |f| try f(ctx, errFromErrno(errno));
                        return .done;
                    },
                }
                return .restart;
            }
        };
        const sec: i64 = @divTrunc(msec, 1000);
        const nsec: i64 = (msec - sec * 1000) * ns_per_ms;
        return .{
            .io = io,
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .ticker = .{ .sec = sec, .nsec = nsec } },
        };
    }

    fn timer(
        io: *Io,
        nsec: u64,
        context: anytype,
        comptime ticked: fn (@TypeOf(context)) Error!void,
        comptime failed: ?fn (@TypeOf(context), anyerror) Error!void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, cqe: linux.io_uring_cqe) Error!CallbackResult {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS, .TIME => try ticked(ctx),
                    //.INTR => return .restart,
                    else => |errno| if (failed) |f| try f(ctx, errFromErrno(errno)),
                }
                return .done;
            }
        };
        const sec = nsec / ns_per_s;
        const ns = (nsec - sec * ns_per_s);
        return .{
            .io = io,
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .timer = .{ .sec = @intCast(sec), .nsec = @intCast(ns) } },
        };
    }

    fn socketCreate(
        io: *Io,
        domain: u32,
        socket_type: u32,
        protocol: u32,
        context: anytype,
        comptime success: fn (@TypeOf(context), socket_t) Error!void,
        comptime failed: ?fn (@TypeOf(context), anyerror) Error!void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, cqe: linux.io_uring_cqe) Error!CallbackResult {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS => try success(ctx, @intCast(cqe.res)),
                    else => |errno| if (failed) |f| try f(ctx, errFromErrno(errno)),
                }
                return .done;
            }
        };
        return .{
            .io = io,
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .socket = .{ .domain = domain, .socket_type = socket_type, .protocol = protocol } },
        };
    }

    fn close(
        io: *Io,
        socket: socket_t,
    ) Op {
        return .{
            .io = io,
            .context = 0,
            .callback = undefined,
            .args = .{ .close = socket },
        };
    }
};

const testing = std.testing;

// Remove prefix len from iovecs
fn resizeIovec(iovecs: []posix.iovec_const, prefix_len: usize) []posix.iovec_const {
    if (iovecs.len == 0) return iovecs[0..0];
    var i: usize = 0;
    var n: usize = prefix_len;
    while (n >= iovecs[i].len) {
        n -= iovecs[i].len;
        i += 1;
        if (i >= iovecs.len) return iovecs[0..0];
    }
    iovecs[i].base += n;
    iovecs[i].len -= n;
    return iovecs[i..];
}

test "resize iovec" {
    const data = "iso medo u ducan";
    var iovecs = [_]posix.iovec_const{
        .{ .base = data.ptr, .len = 4 },
        .{ .base = data[4..].ptr, .len = 5 },
        .{ .base = data[9..].ptr, .len = 7 },
    };

    var ret = resizeIovec(iovecs[0..], 6);
    try testing.expectEqual(2, ret.len);
    try testing.expectEqual(3, ret[0].len);
    try testing.expectEqual(7, ret[1].len);
    try testing.expectEqualStrings("do ", ret[0].base[0..ret[0].len]);
    try testing.expectEqualStrings("u ducan", ret[1].base[0..ret[1].len]);

    ret = resizeIovec(ret, 5);
    try testing.expectEqual(1, ret.len);
    try testing.expectEqualStrings("ducan", ret[0].base[0..ret[0].len]);

    ret = resizeIovec(ret, 5);
    try testing.expectEqual(0, ret.len);
}

test "ticker" {
    const allocator = testing.allocator;

    var io = Io{ .allocator = allocator };
    try io.init(4, 0, 0);
    defer io.deinit();

    const Ctx = struct {
        count: usize = 0,
        pub fn ticked(self: *@This()) Error!void {
            self.count += 1;
        }
    };
    var ctx = Ctx{};

    const delay = 123;
    const op = try io.ticker(delay, &ctx, Ctx.ticked, null);
    _ = op;
    const start = unixMilli();
    try io.tick();
    const elapsed = unixMilli() - start;
    try testing.expectEqual(1, ctx.count);
    try testing.expect(elapsed >= delay);
    // std.debug.print("elapsed: {}, delay: {}\n", .{ elapsed, delay });
}

test "timer" {
    const allocator = testing.allocator;
    var io = Io{ .allocator = allocator };
    try io.init(4, 0, 0);
    defer io.deinit();

    const Ctx = struct {
        count: usize = 0,
        pub fn ticked(self: *@This()) Error!void {
            self.count += 1;
        }
    };
    var ctx = Ctx{};

    var timer = io.initTimer(&ctx, Ctx.ticked);

    _ = try timer.set(1 + io.timestamp);
    try io.tick();
    try testing.expectEqual(1, ctx.count);

    _ = try timer.set(1 + io.timestamp);
    try io.tick();
    try testing.expectEqual(2, ctx.count);
}

fn timestamp() u64 {
    var ts: posix.timespec = undefined;
    posix.clock_gettime(.REALTIME, &ts) catch |err| switch (err) {
        error.UnsupportedClock, error.Unexpected => return 0, // "Precision of timing depends on hardware and OS".
    };
    return @as(u64, @intCast(ts.sec)) * ns_per_s + @as(u64, @intCast(ts.nsec));
}

fn unixMilli() i64 {
    var ts: posix.timespec = undefined;
    posix.clock_gettime(.REALTIME, &ts) catch |err| switch (err) {
        error.UnsupportedClock, error.Unexpected => return 0, // "Precision of timing depends on hardware and OS".
    };
    return ts.sec * 1000 + @divTrunc(ts.nsec, ns_per_ms);
}

test "send udp" {
    const allocator = testing.allocator;
    var io = Io{ .allocator = allocator };
    try io.init(4, 0, 0);
    defer io.deinit();

    const Ctx = struct {
        io: *Io,
        allocator: mem.Allocator,
        address: std.net.Address,

        socket: socket_t = 0,
        err: ?anyerror = null,
        callback_count: usize = 0,
        send_op: ?*Op = null,
        ticker_op: ?*Op = null,
        iter: BufferSizeIterator = .{ .buf = &.{}, .pos = 0, .size = 0 },

        const Self = @This();

        fn start(self: *Self) !void {
            const ping_interval = 2 * 1000; // in milliseconds
            self.ticker_op = try self.io.ticker(ping_interval, self, tick, tickerFailed);
        }

        fn tick(self: *Self) Error!void {
            if (self.socket == 0) {
                return try self.socketCreate();
            }
            if (self.send_op != null) return;
            if (self.iter.done()) try self.generate();
            try self.send();
        }

        fn tickerFailed(self: *Self, err: anyerror) Error!void {
            self.ticker_op = null;
            switch (err) {
                error.Canceled => {},
                else => {
                    log.err("ticker failed {}", .{err});
                    try self.start();
                },
            }
        }

        fn socketCreate(self: *Self) !void {
            _ = try self.io.socketCreate(
                self.address.any.family,
                posix.SOCK.DGRAM | posix.SOCK.CLOEXEC,
                0,
                self,
                socketCreated,
                connectFailed,
            );
        }

        fn socketCreated(self: *Self, socket: socket_t) Error!void {
            self.socket = socket;
            _ = try self.io.connect(self.socket, self.address, self, connected, connectFailed);
        }

        fn connectFailed(self: *Self, err: anyerror) Error!void {
            std.debug.print("connectFailed {}\n", .{err});
            try self.io.close(self.socket);
            self.socket = 0;
        }

        fn connected(self: *Self) Error!void {
            self.callback_count += 1;
        }

        fn generate(self: *Self) Error!void {
            var file = std.fs.cwd().openFile("/home/ianic/Code/tls.zig/example/cert/pg2600.txt", .{}) catch unreachable;
            defer file.close();
            const book = file.readToEndAlloc(self.allocator, 1024 * 1024 * 1024) catch unreachable;
            self.iter = BufferSizeIterator{ .buf = book, .size = 504 };
        }

        fn send(self: *Self) !void {
            if (self.iter.next()) |buf| {
                self.send_op = try self.io.send(self.socket, buf, self, sent, sendFailed);
            }
        }

        fn sent(self: *Self) Error!void {
            self.callback_count += 1;
            self.send_op = null;
            if (self.iter.done()) {
                self.allocator.free(self.iter.buf);
                self.iter = .{ .buf = &.{}, .pos = 0, .size = 0 };
                std.debug.print("sent {}\n", .{self.callback_count});
            } else {
                try self.send();
            }
        }

        fn sendFailed(self: *Self, err: anyerror) Error!void {
            self.send_op = null;
            std.debug.print("sendFailed {}\n", .{err});
        }
    };

    const addr = std.net.Address.initIp4([4]u8{ 127, 0, 0, 1 }, 8126);

    var ctx = Ctx{
        .allocator = allocator,
        .io = &io,
        .address = addr,
    };
    try ctx.start();

    // _ = try io.socketCreate(addr.any.family, posix.SOCK.DGRAM | posix.SOCK.CLOEXEC, 0, &ctx, Ctx.socketCreated, Ctx.failed);

    // try io.tick();
    // try testing.expect(ctx.socket > 0);
    // try testing.expect(ctx.err == null);

    // _ = try io.connect(ctx.socket, addr, &ctx, Ctx.connected, Ctx.failed);
    // try io.tick();
    // try testing.expectEqual(1, ctx.callback_count);

    while (true) {
        try io.tick();
    }

    // try io.tick();
    //std.debug.print("err: {}\n", .{ctx.err.?});
    //try testing.expect(ctx.err == null);
    //try testing.expectEqual(2, ctx.callback_count);
}

test "read file" {
    const allocator = testing.allocator;

    var file = try std.fs.cwd().openFile("/home/ianic/Code/tls.zig/example/cert/pg2600.txt", .{});
    defer file.close();

    const book = try file.readToEndAlloc(allocator, 1024 * 1024 * 1024);
    defer allocator.free(book);

    const buf = book[0 .. 1024 * 16 + 28 + 10];

    var iter = BufferSizeIterator{ .buf = buf, .size = 504 };
    while (iter.next()) |b| {
        std.debug.print("------\n{s}------\n", .{b});
    }
    try testing.expect(iter.done());

    // var start: usize = 0;
    // const max_len: usize = 508;

    // while (start <= buf.len) {
    //     const end = @min(start + max_len, buf.len);
    //     if (std.mem.lastIndexOfScalar(u8, buf[start..end], '\n')) |len| {
    //         const split = buf[start..][0 .. len + 1];
    //         std.debug.print("{} {s}", .{ split.len, split });
    //         start += split.len;
    //         continue;
    //     }
    //     const split = buf[start..];
    //     std.debug.print("leftover {} {s}", .{ split.len, split });
    //     break;
    // }
}

const BufferSizeIterator = struct {
    buf: []const u8,
    pos: usize = 0,
    size: usize,

    const Self = @This();

    pub fn next(self: *Self) ?[]const u8 {
        const end = @min(self.pos + self.size, self.buf.len);

        if (std.mem.lastIndexOfScalar(u8, self.buf[self.pos..end], '\n')) |sep| {
            const split = self.buf[self.pos..][0 .. sep + 1];
            self.pos += split.len;
            return split;
        }
        self.pos = self.buf.len;
        return null;
    }

    pub fn done(self: Self) bool {
        return self.pos == self.buf.len;
    }
};
