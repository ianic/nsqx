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

pub const Io = struct {
    allocator: mem.Allocator,
    op_pool: std.heap.MemoryPool(Op) = undefined,
    ring: IoUring = undefined,
    timestamp: u64 = 0,
    recv_buf_grp: IoUring.BufferGroup = undefined,
    metric: Metric = .{},
    metric_prev: Metric = .{},
    cqe_buf: []linux.io_uring_cqe = undefined,
    cqe_buf_head: usize = 0,
    cqe_buf_tail: usize = 0,

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

    pub fn init(self: *Io, allocator: mem.Allocator, opt: Options) !void {
        const cqe_buf = try allocator.alloc(linux.io_uring_cqe, opt.entries);
        errdefer allocator.free(cqe_buf);
        var op_pool = try std.heap.MemoryPool(Op).initPreheated(allocator, 1024);
        errdefer op_pool.deinit();
        var ring = try IoUring.init(opt.entries, linux.IORING_SETUP_SQPOLL | linux.IORING_SETUP_SINGLE_ISSUER);
        errdefer ring.deinit();
        self.* = .{
            .allocator = allocator,
            .ring = ring,
            .timestamp = timestamp(),
            .op_pool = op_pool,
            .cqe_buf = cqe_buf,
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
        self.ring.deinit();
        self.op_pool.deinit();
        self.allocator.free(self.cqe_buf);
    }

    fn acquire(self: *Io) !*Op {
        return try self.op_pool.create();
    }

    fn release(self: *Io, op: *Op) void {
        self.op_pool.destroy(op);
    }

    /// Multishot accept operation
    pub fn accept(
        self: *Io,
        socket: socket_t,
        context: anytype,
        comptime accepted: fn (@TypeOf(context), socket_t, net.Address) Error!void,
        comptime failed: fn (@TypeOf(context), anyerror) Error!void,
        op_field: *?*Op,
    ) !void {
        const op = try self.acquire();
        errdefer self.release(op);
        op.* = Op.accept(self, socket, context, accepted, failed, op_field);
        try op.prep();
        op_field.* = op;
    }

    pub fn connect(
        self: *Io,
        socket: socket_t,
        addr: *net.Address,
        context: anytype,
        comptime connected: fn (@TypeOf(context)) Error!void,
        comptime failed: fn (@TypeOf(context), anyerror) Error!void,
        op_field: *?*Op,
    ) !void {
        const op = try self.acquire();
        errdefer self.release(op);
        op.* = Op.connect(self, socket, addr, context, connected, failed, op_field);
        try op.prep();
        op_field.* = op;
    }

    /// Multishot receive operation
    pub fn recv(
        self: *Io,
        socket: socket_t,
        context: anytype,
        comptime received: fn (@TypeOf(context), []const u8) Error!void,
        comptime failed: fn (@TypeOf(context), anyerror) Error!void,
        op_field: *?*Op,
    ) !void {
        const op = try self.acquire();
        errdefer self.release(op);
        op.* = Op.recv(self, socket, context, received, failed, op_field);
        try op.prep();
        op_field.* = op;
    }

    // Unused making it private for now
    fn writev(
        self: *Io,
        socket: socket_t,
        vec: []posix.iovec_const,
        context: anytype,
        comptime sent: fn (@TypeOf(context), usize) Error!void,
        comptime failed: fn (@TypeOf(context), anyerror) Error!void,
    ) !*Op {
        const op = try self.acquire();
        errdefer self.release(op);
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
        op_field: *?*Op,
    ) !void {
        const op = try self.acquire();
        errdefer self.release(op);
        op.* = Op.sendv(self, socket, msghdr, context, sent, failed, op_field);
        try op.prep();
        op_field.* = op;
    }

    fn setField(context: anytype, op_field: *?*Op, op: *Op) void {
        assert(@field(context, op_field) == null);
        @field(context, op_field) = op;
    }

    pub fn send(
        self: *Io,
        socket: socket_t,
        buf: []const u8,
        context: anytype,
        comptime sent: fn (@TypeOf(context)) Error!void,
        comptime failed: fn (@TypeOf(context), anyerror) Error!void,
        op_field: *?*Op,
    ) !void {
        const op = try self.acquire();
        errdefer self.release(op);
        op.* = Op.send(self, socket, buf, context, sent, failed, op_field);
        try op.prep();
        op_field.* = op;
    }

    /// Multi shot
    pub fn ticker(
        self: *Io,
        msec: i64, // miliseconds  TODO: use msec or nsec on both places
        context: anytype,
        comptime ticked: fn (@TypeOf(context)) Error!void,
        comptime failed: ?fn (@TypeOf(context), anyerror) Error!void,
        op_field: *?*Op,
    ) !void {
        const op = try self.acquire();
        errdefer self.release(op);
        op.* = Op.ticker(self, msec, context, ticked, failed, op_field);
        try op.prep();
        op_field.* = op;
    }

    /// One shot
    pub fn timer(
        self: *Io,
        nsec: u64, // delay in nanoseconds
        context: anytype,
        comptime ticked: fn (@TypeOf(context)) Error!void,
        comptime failed: ?fn (@TypeOf(context), anyerror) Error!void,
        op_field: *?*Op,
    ) !void {
        const op = try self.acquire();
        errdefer self.release(op);
        op.* = Op.timer(self, nsec, context, ticked, failed, op_field);
        try op.prep();
        op_field.* = op;
    }

    pub fn socketCreate(
        self: *Io,
        domain: u32,
        socket_type: u32,
        protocol: u32,
        context: anytype,
        comptime success: fn (@TypeOf(context), socket_t) Error!void,
        comptime failed: ?fn (@TypeOf(context), anyerror) Error!void,
        op_field: *?*Op,
    ) !void {
        const op = try self.acquire();
        errdefer self.release(op);
        op.* = Op.socketCreate(self, domain, socket_type, protocol, context, success, failed, op_field);
        try op.prep();
        op_field.* = op;
    }

    pub fn close(self: *Io, socket: socket_t) !void {
        const op = try self.acquire();
        errdefer self.release(op);
        op.* = Op.close(self, socket);
        try op.prep();
    }

    fn loop(self: *Io, run: Atomic(bool)) !void {
        while (run.load(.monotonic)) try self.tick();
    }

    pub fn drain(self: *Io) !void {
        while (self.metric.all.active() > 0) {
            log.debug("draining active operations: {}", .{self.metric.all.active()});
            try self.tick();
        }
    }

    pub fn tick(self: *Io) !void {
        self.metric.loops += 1;
        _ = try self.ring.submit();
        if (self.cqe_buf_tail <= self.cqe_buf_head) {
            self.cqe_buf_head = 0;
            self.cqe_buf_tail = 0;
            const n = try self.ring.copy_cqes(self.cqe_buf, 1);
            self.cqe_buf_tail = n;
            self.metric.cqes += n;
        }
        if (self.cqe_buf_tail > self.cqe_buf_head) {
            const new_timestamp = timestamp();
            self.timestamp = if (new_timestamp == self.timestamp) new_timestamp + 1 else new_timestamp;
            try self.flushCompletions();
        }
    }

    fn flushCompletions(self: *Io) !void {
        while (self.cqe_buf_head < self.cqe_buf_tail) : (self.cqe_buf_head += 1) {
            const cqe = self.cqe_buf[self.cqe_buf_head];

            // There is no op for this cqe
            if (cqe.user_data == 0) continue;

            const op: *Op = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
            const res = if (op.context != 0)
                try op.callback(op, cqe) // Do operation callback
            else
                .done; // There is no callback for this op
            if (flagMore(cqe)) continue; // Wait for next cqe of the same operation
            switch (res) {
                .done => {
                    self.release(op);
                    self.metric.complete(op);
                },
                .restart => {
                    try op.prep();
                    self.metric.restart(op);
                },
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
            try self.io.timer(delay, self, ticked, failed, &self.op);
            self.fire_at = fire_at;
        }

        pub fn reset(self: *Self) !void {
            try Op.cancel(self.op);
        }

        pub fn close(self: *Self) !void {
            if (self.closed) return;
            try self.reset();
            self.closed = true;
        }

        fn ticked(self: *Self) Error!void {
            try self.callback(self);
        }

        fn failed(_: *Self, err: anyerror) Error!void {
            log.err("timer failed {}", .{err});
        }
    };
};

fn flagMore(cqe: linux.io_uring_cqe) bool {
    return cqe.flags & linux.IORING_CQE_F_MORE > 0;
}

pub const Op = struct {
    io: *Io,
    context: u64,
    op_field: *?*Op,
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
            addr: *net.Address,
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

    pub fn unsubscribe(maybe_op: ?*Op) void {
        if (maybe_op) |op| {
            op.context = 0;
            op.op_field.* = null;
        }
    }

    pub fn cancel(maybe_op: ?*Op) !void {
        if (maybe_op) |op| {
            switch (op.args) {
                .timer, .ticker => _ = try op.io.ring.timeout_remove(0, @intFromPtr(op), 0),
                else => _ = try op.io.ring.cancel(0, @intFromPtr(op), 0),
            }
            Op.unsubscribe(op);
        }
    }

    fn prep(op: *Op) !void {
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
        op.io.metric.submit(op);
    }

    fn accept(
        io: *Io,
        socket: socket_t,
        context: anytype,
        comptime accepted: fn (@TypeOf(context), socket_t, net.Address) Error!void,
        comptime fail: fn (@TypeOf(context), anyerror) Error!void,
        op_field: *?*Op,
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

                        const addr = net.Address.initPosix(&op.args.accept.addr);
                        try accepted(ctx, @intCast(cqe.res), addr);
                    },
                    .CONNABORTED, .INTR => {}, // continue
                    else => |errno| {
                        op.op_field.* = null;
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
            .op_field = op_field,
            .callback = wrapper.complete,
            .args = .{ .accept = .{ .socket = socket } },
        };
    }

    fn connect(
        io: *Io,
        socket: socket_t,
        addr: *net.Address,
        context: anytype,
        comptime connected: fn (@TypeOf(context)) Error!void,
        comptime fail: fn (@TypeOf(context), anyerror) Error!void,
        op_field: *?*Op,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, cqe: linux.io_uring_cqe) Error!CallbackResult {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS => {
                        op.op_field.* = null;
                        try connected(ctx);
                    },
                    .INTR => return .restart,
                    else => |errno| {
                        op.op_field.* = null;
                        try fail(ctx, errFromErrno(errno));
                    },
                }
                return .done;
            }
        };
        return .{
            .io = io,
            .context = @intFromPtr(context),
            .op_field = op_field,
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
        op_field: *?*Op,
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
                        op.io.metric.recv_buf_grp.success +%= 1;
                    },
                    .NOBUFS => {
                        // log.info("recv nobufs", .{});
                        op.io.metric.recv_buf_grp.no_bufs +%= 1;
                    },
                    .INTR => {},
                    else => |errno| {
                        op.op_field.* = null;
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
            .op_field = op_field,
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
        op_field: *?*Op,
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

                        op.op_field.* = null;
                        try sent(ctx);
                    },
                    .INTR => return .restart,
                    else => |errno| {
                        op.op_field.* = null;
                        try failed(ctx, errFromErrno(errno));
                    },
                }
                return .done;
            }
        };
        return .{
            .io = io,
            .context = @intFromPtr(context),
            .op_field = op_field,
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
        op_field: *?*Op,
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
                        op.op_field.* = null;
                        try sent(ctx);
                    },
                    .INTR => return .restart,
                    else => |errno| {
                        op.op_field.* = null;
                        try failed(ctx, errFromErrno(errno));
                    },
                }
                return .done;
            }
        };
        return .{
            .io = io,
            .context = @intFromPtr(context),
            .op_field = op_field,
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
        op_field: *?*Op,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, cqe: linux.io_uring_cqe) Error!CallbackResult {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS, .TIME => try ticked(ctx),
                    .INTR => {},
                    else => |errno| {
                        op.op_field.* = null;
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
            .op_field = op_field,
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
        op_field: *?*Op,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, cqe: linux.io_uring_cqe) Error!CallbackResult {
                const ctx: Context = @ptrFromInt(op.context);
                op.op_field.* = null;
                switch (cqe.err()) {
                    .SUCCESS, .TIME => try ticked(ctx),
                    .INTR => return .restart,
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
            .op_field = op_field,
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
        op_field: *?*Op,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, cqe: linux.io_uring_cqe) Error!CallbackResult {
                const ctx: Context = @ptrFromInt(op.context);
                op.op_field.* = null;
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
            .op_field = op_field,
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
            .op_field = undefined,
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
    var io: Io = undefined;
    try io.init(allocator, .{ .entries = 4, .recv_buffers = 0, .recv_buffer_len = 0 });
    defer io.deinit();

    const Ctx = struct {
        count: usize = 0,
        op: ?*Op = null,
        pub fn ticked(self: *@This()) Error!void {
            self.count += 1;
        }
    };
    var ctx = Ctx{};

    const delay = 123;
    const op = try io.ticker(delay, &ctx, Ctx.ticked, null, &ctx.op);
    try testing.expect(ctx.op != null);
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
    var io: Io = undefined;
    try io.init(allocator, .{ .entries = 4, .recv_buffers = 0, .recv_buffer_len = 0 });
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
    try testing.expectEqual(24, @sizeOf(Accept));
    try testing.expectEqual(16, @sizeOf(Connect));
    try testing.expectEqual(24, @sizeOf(Writev));
    try testing.expectEqual(16, @sizeOf(Sendv));
    try testing.expectEqual(24, @sizeOf(Send));
    try testing.expectEqual(112, @sizeOf(net.Address));
    try testing.expectEqual(16, @sizeOf(linux.kernel_timespec));
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
