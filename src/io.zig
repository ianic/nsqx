const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const net = std.net;

const posix = std.posix;
const linux = std.os.linux;
const IoUring = linux.IoUring;
const socket_t = std.posix.socket_t;
const fd_t = std.posix.fd_t;
const errFromErrno = @import("errno.zig").toError;
const Atomic = std.atomic.Value;
const Options = @import("Options.zig").Io;

const ns_per_ms = std.time.ns_per_ms;
const ns_per_s = std.time.ns_per_s;

const log = std.log.scoped(.io);

pub const Error = error{
    OutOfMemory,
    SubmissionQueueFull,
};

pub const CallbackError = error{
    OutOfMemory,
    SubmissionQueueFull,
    MultishotCanceled,
    ShortSend, // TODO treat short sends as error because we have msg.wait_all flag set
};

pub const Io = struct {
    allocator: mem.Allocator,
    timer_pool: std.heap.MemoryPool(Timer) = undefined,
    ring: IoUring = undefined,
    timestamp: u64 = 0,
    recv_buf_grp: IoUring.BufferGroup = undefined,
    metric: Metric = .{},
    metric_prev: Metric = .{},
    cqe_buf: [256]linux.io_uring_cqe = undefined,
    cqe_buf_head: usize = 0,
    cqe_buf_tail: usize = 0,
    next: ?*Op = null,

    pub fn init(self: *Io, allocator: mem.Allocator, opt: Options) !void {
        var timer_pool = std.heap.MemoryPool(Timer).init(allocator);
        errdefer timer_pool.deinit();

        // Flags reference: https://nick-black.com/dankwiki/index.php/Io_uring
        const flags =
            // Create a kernel thread to poll on the submission queue. If the
            // submission queue is kept busy, this thread will reap SQEs without
            // the need for a system call.
            linux.IORING_SETUP_SQPOLL |
            // Hint to the kernel that only a single thread will submit
            // requests, allowing for optimizations.
            linux.IORING_SETUP_SINGLE_ISSUER;
        var ring = try IoUring.init(opt.entries, flags);
        errdefer ring.deinit();
        self.* = .{
            .allocator = allocator,
            .ring = ring,
            .timestamp = timestamp(),
            .timer_pool = timer_pool,
        };
        if (opt.recv_buffers > 0) {
            self.recv_buf_grp = try self.initBufferGroup(1, opt.recv_buffers, opt.recv_buffer_len);
        } else {
            self.recv_buf_grp.buffers_count = 0;
        }
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
        self.timer_pool.deinit();
        self.ring.deinit();
    }

    pub fn drain(self: *Io) !void {
        while (self.metric.all.active() > 0) {
            log.debug("draining active operations: {}", .{self.metric.all.active()});
            try self.tick();
        }
    }

    pub fn tick(self: *Io) !void {
        self.metric.loops += 1;
        // Submit prepared sqe-s to the kernel.
        _ = try self.ring.submit();
        // TODO: submit pending
        if (self.cqe_buf_head >= self.cqe_buf_tail) {
            // Get completions.
            self.cqe_buf_head = 0;
            self.cqe_buf_tail = 0;
            const n = try self.ring.copy_cqes(&self.cqe_buf, 1);
            self.cqe_buf_tail = n;
            self.metric.cqes += n;
        }
        if (self.cqe_buf_head < self.cqe_buf_tail) {
            // Process completions.
            const new_timestamp = timestamp();
            self.timestamp = if (new_timestamp <= self.timestamp) self.timestamp + 1 else new_timestamp;
            try self.flushCompletions();
        }
    }

    fn flushCompletions(self: *Io) Error!void {
        while (self.cqe_buf_head < self.cqe_buf_tail) : (self.cqe_buf_head += 1) {
            const cqe = self.cqe_buf[self.cqe_buf_head];
            // There is no op for this cqe
            if (cqe.user_data == 0) continue;
            const has_more = flagMore(cqe);

            const op: *Op = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
            if (has_more) op.flags |= Op.flag_has_more else op.flags &= ~Op.flag_has_more;
            op.flags &= ~Op.flag_submitted;
            const op_kind: Op.Kind = op.args;

            // Do operation callback
            op.callback(op, self, cqe) catch |err| switch (err) {
                error.MultishotCanceled, error.ShortSend => {
                    self.metric.restart(op_kind);
                    self.submit(op);
                    continue;
                },
                // Retry cqe callback
                error.OutOfMemory, error.SubmissionQueueFull => |e| return e,
            };
            // NOTE: op can be already deallocated here don't use it after callback
            if (!has_more) self.metric.complete(op_kind);
        }
    }

    pub fn submit(self: *Io, op: *Op) void {
        assert(op.flags & Op.flag_submitted == 0);
        op.flags |= Op.flag_submitted;
        op.prep(self) catch |err| {
            log.debug("fail to submit operation {}", .{err});
            // add to linked list
            op.next = self.next;
            self.next = op;
            // TODO not parsing that list
            unreachable;
        };
    }

    pub fn now(self: *Io) u64 {
        return self.timestamp;
    }

    pub fn initTimer(
        self: *Io,
        context: anytype,
        comptime cb: fn (@TypeOf(context)) void,
    ) !*Timer {
        const tmr = try self.timer_pool.create();
        errdefer self.allocator.destroy(tmr);
        tmr.* = Timer.init(self, context, cb);
        return tmr;
    }

    pub const Timer = struct {
        io: *Io,
        timer_op: Op = .{},
        cancel_op: Op = .{},
        fire_at: u64 = no_timeout,
        state: enum {
            single,
            multi,
            reset,
            cancel,
            close,
        } = .cancel,
        args: Op.TimerArgs = .{ .ts = .{ .sec = 0, .nsec = 0 }, .count = 0, .flags = 0 },

        context: usize = 0,
        callback: *const fn (*Self) void = undefined,

        pub const no_timeout: u64 = std.math.maxInt(u64);
        const IORING_TIMEOUT_MULTISHOT = 1 << 6; // TODO: missing in linux. package
        const Self = @This();

        pub fn init(
            io: *Io,
            context: anytype,
            comptime cb: fn (@TypeOf(context)) void,
        ) Self {
            const Context = @TypeOf(context);
            const wrapper = struct {
                fn complete(t: *Self) void {
                    const ctx: Context = @ptrFromInt(t.context);
                    cb(ctx);
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

        /// Set multi shot ticker to fire every delay milliseconds.
        pub fn setTicker(self: *Self, delay_ms: u32) void {
            self.prepArgs(@as(u64, @intCast(delay_ms)) * ns_per_ms, IORING_TIMEOUT_MULTISHOT);
            self.fire_at = no_timeout;
            self.set_();
        }

        /// Set one shot timer after delay milliseconds.
        pub fn set(self: *Self, delay_ms: u32) void {
            self.setAbs(@as(u64, @intCast(delay_ms)) * ns_per_ms + self.now());
        }

        /// Set one shot timer at an absolute timestamp (nanoseconds). If ts is
        /// after already set timer it is ignored, shorter timer is preserved.
        /// So this can be called multiple times and shortest timer will be
        /// used.
        pub fn setAbs(self: *Self, ts: u64) void {
            if (ts == no_timeout) return;
            if (ts <= self.now()) return;
            if (self.timer_op.active() and self.fire_at <= ts) return;

            self.fire_at = ts;
            self.prepArgs(ts - self.now(), 0);
            self.set_();
        }

        fn prepArgs(self: *Self, delay_ns: u64, flags: u32) void {
            const sec = delay_ns / ns_per_s;
            const nsec = (delay_ns - sec * ns_per_s);
            self.args = .{
                .ts = .{ .sec = @intCast(sec), .nsec = @intCast(nsec) },
                .count = 0,
                .flags = flags,
            };
        }

        fn set_(self: *Self) void {
            if (self.timer_op.active()) return self.reset();

            self.timer_op = Op.timer(self.args, self, onTimer, onTimerFail);
            self.io.submit(&self.timer_op);
            self.state = if (self.args.flags & IORING_TIMEOUT_MULTISHOT == 0) .single else .multi;
        }

        fn reset(self: *Self) void {
            self.cancel_();
            self.state = .reset;
        }

        /// Cancel any running timer.
        pub fn cancel(self: *Self) void {
            self.cancel_();
            self.state = .cancel;
        }

        fn cancel_(self: *Self) void {
            if (self.cancel_op.active()) return;
            if (!self.timer_op.active()) return;
            self.cancel_op = Op.cancel(&self.timer_op, self, onCancel);
            self.io.submit(&self.cancel_op);
        }

        /// Cancel pending operation and deinit self.
        pub fn deinit(self: *Self) void {
            self.state = .close;
            if (self.timer_op.active()) return self.cancel_();
            if (self.cancel_op.active()) return;
            self.io.timer_pool.destroy(self);
        }

        fn onTimer(self: *Self) Error!void {
            switch (self.state) {
                .close => return self.deinit(),
                .reset => return self.set_(),
                .cancel => {},
                .single => {
                    self.fire_at = no_timeout;
                    self.state = .cancel;
                    self.callback(self);
                },
                .multi => {
                    if (!self.timer_op.hasMore()) self.set_();
                    self.callback(self);
                },
            }
        }

        fn onTimerFail(self: *Self, _: anyerror) Error!void {
            switch (self.state) {
                .close => self.deinit(),
                .reset => self.set_(),
                .cancel => {},
                .single => {
                    self.state = .cancel;
                },
                .multi => if (!self.timer_op.hasMore()) self.set_(),
            }
        }

        fn onCancel(self: *Self) void {
            switch (self.state) {
                .close => self.deinit(),
                else => {},
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
        try write("io.op.timer", cur.timer, prev.timer, writer);
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
};

fn flagMore(cqe: linux.io_uring_cqe) bool {
    return cqe.flags & linux.IORING_CQE_F_MORE > 0;
}

pub const Op = struct {
    context: u64 = 0,
    callback: *const fn (*Op, *Io, linux.io_uring_cqe) CallbackError!void = undefined,
    args: Args = undefined,
    next: ?*Op = null,
    flags: u8 = 0,

    const flag_submitted: u8 = 1 << 0;
    const flag_has_more: u8 = 1 << 1;

    const Args = union(Kind) {
        accept: struct {
            socket: socket_t,
            addr: posix.sockaddr align(4) = undefined,
            addr_size: posix.socklen_t = @sizeOf(posix.sockaddr),
        },
        connect: struct {
            socket: socket_t,
            addr: *net.Address,
        },
        close: socket_t,
        recv: socket_t,
        sendv: struct {
            socket: socket_t,
            msghdr: *posix.msghdr_const,
        },
        send: struct {
            socket: socket_t,
            buf: []const u8,
        },
        timer: TimerArgs,
        socket: struct {
            domain: u32,
            socket_type: u32,
            protocol: u32,
        },
        shutdown: socket_t,
        cancel: *Op,
    };
    const TimerArgs = struct {
        ts: linux.kernel_timespec,
        count: u32,
        flags: u32,
    };

    const Kind = enum {
        accept,
        connect,
        close,
        recv,
        sendv,
        send,
        timer,
        socket,
        shutdown,
        cancel,
    };

    pub fn active(op: Op) bool {
        return op.flags & flag_submitted > 0 or
            op.flags & flag_has_more > 0;
    }

    pub fn hasMore(op: Op) bool {
        return op.flags & flag_has_more > 0;
    }

    fn prep(op: *Op, io: *Io) !void {
        // TODO timeout on connect or any other non multishot operation can be implemented like
        //sqe.flags |= linux.IOSQE_IO_LINK;
        //_ = try io.ring.link_timeout(1, &timeout_ts, linux.IORING_TIMEOUT_ETIME_SUCCESS);

        switch (op.args) {
            .accept => |*arg| _ = try io.ring.accept_multishot(@intFromPtr(op), arg.socket, &arg.addr, &arg.addr_size, 0),
            .connect => |*arg| _ = try io.ring.connect(@intFromPtr(op), arg.socket, &arg.addr.any, arg.addr.getOsSockLen()),
            .close => |socket| _ = try io.ring.close(@intFromPtr(op), socket),
            .recv => |socket| _ = try io.recv_buf_grp.recv_multishot(@intFromPtr(op), socket, 0),
            .sendv => |*arg| _ = try io.ring.sendmsg(@intFromPtr(op), arg.socket, arg.msghdr, linux.MSG.WAITALL | linux.MSG.NOSIGNAL),
            .send => |*arg| _ = try io.ring.send(@intFromPtr(op), arg.socket, arg.buf, linux.MSG.WAITALL | linux.MSG.NOSIGNAL),
            .timer => |*arg| _ = try io.ring.timeout(@intFromPtr(op), &arg.ts, arg.count, arg.flags),
            .socket => |*arg| _ = try io.ring.socket(@intFromPtr(op), arg.domain, arg.socket_type, arg.protocol, 0),
            .shutdown => |socket| _ = try io.ring.shutdown(@intFromPtr(op), socket, 2),
            .cancel => |op_to_cancel| switch (op_to_cancel.args) {
                .timer => _ = try io.ring.timeout_remove(@intFromPtr(op), @intFromPtr(op_to_cancel), 0),
                else => _ = try io.ring.cancel(@intFromPtr(op), @intFromPtr(op_to_cancel), 0),
            },
        }
        io.metric.submit(op.args);
    }

    // Multishot operations -----------------

    pub fn accept(
        socket: socket_t,
        context: anytype,
        comptime success: fn (@TypeOf(context), socket_t, net.Address) Error!void,
        comptime fail: fn (@TypeOf(context), anyerror) Error!void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, _: *Io, cqe: linux.io_uring_cqe) CallbackError!void {
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

                        const addr = net.Address.initPosix(&op.args.accept.addr);
                        try success(ctx, @intCast(cqe.res), addr);
                    },
                    .CONNABORTED, .INTR => {}, // continue
                    else => |errno| return try fail(ctx, errFromErrno(errno)),
                }
                if (!flagMore(cqe)) return error.MultishotCanceled;
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .accept = .{ .socket = socket } },
        };
    }

    pub fn recv(
        socket: socket_t,
        context: anytype,
        comptime success: fn (@TypeOf(context), []const u8) Error!void,
        comptime fail: fn (@TypeOf(context), anyerror) Error!void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, io: *Io, cqe: linux.io_uring_cqe) CallbackError!void {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS => {
                        const n: usize = @intCast(cqe.res);
                        if (n == 0)
                            return try fail(ctx, error.EndOfFile);

                        io.metric.recv_bytes +%= n;
                        const buffer_id = cqe.buffer_id() catch unreachable;
                        const bytes = io.recv_buf_grp.get(buffer_id)[0..n];
                        try success(ctx, bytes);
                        io.recv_buf_grp.put(buffer_id);
                        io.metric.recv_buf_grp.success +%= 1;

                        // TODO ovaj note vise ne stoji
                        // NOTE: recv is not restarted if there is no more
                        // multishot cqe's (like accept, timer). Check op_field
                        // in callback if null multishot is terminated.
                        return;
                    },
                    .NOBUFS => {
                        // log.info("recv nobufs", .{});
                        io.metric.recv_buf_grp.no_bufs +%= 1;
                    },
                    .INTR => {},
                    else => |errno| return try fail(ctx, errFromErrno(errno)),
                }
                if (!flagMore(cqe)) return error.MultishotCanceled;
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .recv = socket },
        };
    }

    // Single shot operations -----------------

    pub fn connect(
        socket: socket_t,
        addr: *net.Address,
        context: anytype,
        comptime success: fn (@TypeOf(context)) Error!void,
        comptime fail: fn (@TypeOf(context), anyerror) Error!void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, _: *Io, cqe: linux.io_uring_cqe) CallbackError!void {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS => return try success(ctx),
                    else => |errno| return try fail(ctx, errFromErrno(errno)),
                }
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .connect = .{ .socket = socket, .addr = addr } },
        };
    }

    pub fn sendv(
        socket: socket_t,
        msghdr: *posix.msghdr_const,
        context: anytype,
        comptime success: fn (@TypeOf(context)) Error!void,
        comptime fail: fn (@TypeOf(context), anyerror) Error!void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, io: *Io, cqe: linux.io_uring_cqe) CallbackError!void {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS => {
                        const n: usize = @intCast(cqe.res);
                        io.metric.send_bytes +%= n;

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
                                return error.ShortSend;
                            }
                        }

                        try success(ctx);
                    },
                    else => |errno| try fail(ctx, errFromErrno(errno)),
                }
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{
                .sendv = .{ .socket = socket, .msghdr = msghdr },
            },
        };
    }

    pub fn send(
        socket: socket_t,
        buf: []const u8,
        context: anytype,
        comptime success: fn (@TypeOf(context)) Error!void,
        comptime fail: fn (@TypeOf(context), anyerror) Error!void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, io: *Io, cqe: linux.io_uring_cqe) CallbackError!void {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS => {
                        const n: usize = @intCast(cqe.res);
                        io.metric.send_bytes +%= n;
                        const send_buf = op.args.send.buf;
                        if (n < send_buf.len) {
                            op.args.send.buf = send_buf[n..];
                            return error.ShortSend;
                        }
                        try success(ctx);
                    },
                    else => |errno| try fail(ctx, errFromErrno(errno)),
                }
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{
                .send = .{ .socket = socket, .buf = buf },
            },
        };
    }

    fn timer(
        args: TimerArgs,
        context: anytype,
        comptime success: fn (@TypeOf(context)) Error!void,
        comptime fail: fn (@TypeOf(context), anyerror) Error!void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, _: *Io, cqe: linux.io_uring_cqe) CallbackError!void {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS, .TIME => try success(ctx),
                    else => |errno| try fail(ctx, errFromErrno(errno)),
                }
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .timer = args },
        };
    }

    pub fn socketCreate(
        domain: u32,
        socket_type: u32,
        protocol: u32,
        context: anytype,
        comptime success: fn (@TypeOf(context), socket_t) Error!void,
        comptime fail: fn (@TypeOf(context), anyerror) Error!void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, _: *Io, cqe: linux.io_uring_cqe) CallbackError!void {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS => try success(ctx, @intCast(cqe.res)),
                    else => |errno| try fail(ctx, errFromErrno(errno)),
                }
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .socket = .{ .domain = domain, .socket_type = socket_type, .protocol = protocol } },
        };
    }

    pub fn shutdownClose(
        socket: socket_t,
        context: anytype,
        comptime done: fn (@TypeOf(context)) void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, io: *Io, cqe: linux.io_uring_cqe) CallbackError!void {
                _ = cqe;
                const ctx: Context = @ptrFromInt(op.context);
                op.* = Op.close(op.args.shutdown, ctx, done);
                io.submit(op);
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .shutdown = socket },
        };
    }

    fn close(
        socket: socket_t,
        context: anytype,
        comptime done: fn (@TypeOf(context)) void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, _: *Io, cqe: linux.io_uring_cqe) CallbackError!void {
                _ = cqe;
                const ctx: Context = @ptrFromInt(op.context);
                done(ctx);
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .close = socket },
        };
    }

    pub fn cancel(
        op_to_cancel: *Op,
        context: anytype,
        comptime done: fn (@TypeOf(context)) void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, _: *Io, cqe: linux.io_uring_cqe) CallbackError!void {
                _ = cqe;
                const ctx: Context = @ptrFromInt(op.context);
                done(ctx);
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .cancel = op_to_cancel },
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

test "timer" {
    const allocator = testing.allocator;
    var io: Io = undefined;
    try io.init(allocator, .{ .entries = 4, .recv_buffers = 0, .recv_buffer_len = 0 });
    defer io.deinit();

    const Ctx = struct {
        count: usize = 0,
        pub fn onTimer(self: *@This()) void {
            self.count += 1;
        }
    };
    var ctx = Ctx{};
    var timer = try io.initTimer(&ctx, Ctx.onTimer);
    defer timer.deinit();

    timer.set(1);
    try io.tick();
    try testing.expectEqual(1, ctx.count);

    timer.set(std.math.maxInt(u32)); // really long
    timer.set(1); // reset and set again
    try io.drain();
    try testing.expectEqual(2, ctx.count);
}

test "ticker" {
    const allocator = testing.allocator;
    var io: Io = undefined;
    try io.init(allocator, .{ .entries = 4, .recv_buffers = 0, .recv_buffer_len = 0 });
    defer io.deinit();

    const Ctx = struct {
        count: usize = 0,
        pub fn onTimer(self: *@This()) void {
            self.count += 1;
        }
    };
    var ctx = Ctx{};
    var timer = try io.initTimer(&ctx, Ctx.onTimer);
    defer timer.deinit();

    try testing.expectEqual(.cancel, timer.state);
    timer.setTicker(1);
    try testing.expectEqual(.multi, timer.state);
    try io.tick();
    try testing.expectEqual(1, ctx.count);
    try io.tick();
    try testing.expectEqual(2, ctx.count);
    try io.tick();
    try testing.expectEqual(3, ctx.count);
    try testing.expectEqual(.multi, timer.state);
    try testing.expect(timer.timer_op.active());
    timer.set(1); // reset and set one shot
    try testing.expectEqual(.reset, timer.state);
    try io.tick();
    try testing.expectEqual(.single, timer.state);
    try testing.expectEqual(3, ctx.count);
    try io.tick();
    try testing.expectEqual(.cancel, timer.state);
    try testing.expectEqual(4, ctx.count);
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

test "size" {
    const Accept = struct {
        socket: socket_t,
        addr: posix.sockaddr align(4) = undefined,
        addr_size: posix.socklen_t = @sizeOf(posix.sockaddr),
    };
    const Connect = struct {
        socket: socket_t,
        addr: *net.Address,
    };
    const Writev = struct {
        socket: socket_t,
        vec: []posix.iovec_const,
    };
    const Sendv = struct {
        socket: socket_t,
        msghdr: *posix.msghdr_const,
    };
    const Send = struct {
        socket: socket_t,
        buf: []const u8,
    };

    try testing.expectEqual(64, @sizeOf(Op));
    try testing.expectEqual(32, @sizeOf(Op.Args));
    try testing.expectEqual(24, @sizeOf(Op.TimerArgs));
    try testing.expectEqual(24, @sizeOf(Accept));
    try testing.expectEqual(24, @sizeOf(Writev));
    try testing.expectEqual(24, @sizeOf(Send));
    try testing.expectEqual(16, @sizeOf(Connect));
    try testing.expectEqual(16, @sizeOf(Sendv));
    try testing.expectEqual(112, @sizeOf(net.Address));
    try testing.expectEqual(16, @sizeOf(linux.kernel_timespec));
    try testing.expectEqual(8, @sizeOf(?*Op));

    try testing.expectEqual(192, @sizeOf(Io.Timer));
}

test "pointer" {
    const O = struct {
        const Self = @This();

        ptr: *?*Self,
    };
    const Ctx = struct {
        op: ?*O = null,
    };

    var ctx = Ctx{};
    var o = O{
        .ptr = &ctx.op,
    };
    try testing.expect(ctx.op == null);
    ctx.op = &o;
    try testing.expect(ctx.op != null);
    o.ptr.* = null;
    try testing.expect(ctx.op == null);
}

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
    timer: Counter = .{},

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

    fn submit(self: *Metric, kind: Op.Kind) void {
        self.all.submit();
        switch (kind) {
            .accept => self.accept.submit(),
            .connect => self.connect.submit(),
            .close => self.close.submit(),
            .recv => self.recv.submit(),
            .send, .sendv => self.sendv.submit(),
            .timer => self.timer.submit(),
            .cancel, .socket, .shutdown => {},
        }
    }

    fn complete(self: *Metric, kind: Op.Kind) void {
        self.all.complete();
        switch (kind) {
            .accept => self.accept.complete(),
            .connect => self.connect.complete(),
            .close => self.close.complete(),
            .recv => self.recv.complete(),
            .send, .sendv => self.sendv.complete(),
            .timer => self.timer.complete(),
            .cancel, .socket, .shutdown => {},
        }
    }

    fn restart(self: *Metric, kind: Op.Kind) void {
        self.all.restart();
        switch (kind) {
            .accept => self.accept.restart(),
            .connect => self.connect.restart(),
            .close => self.close.restart(),
            .recv => self.recv.restart(),
            .send, .sendv => self.sendv.restart(),
            .timer => self.timer.restart(),
            .cancel, .socket, .shutdown => {},
        }
    }
};
