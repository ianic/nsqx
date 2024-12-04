const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const net = std.net;

const posix = std.posix;
const linux = std.os.linux;
const IoUring = linux.IoUring;
const socket_t = std.posix.socket_t;
const errFromErrno = @import("errno.zig").toError;
const Options = @import("Options.zig").Io;
const Fifo = @import("fifo.zig").Fifo;

const ns_per_ms = std.time.ns_per_ms;
const ns_per_s = std.time.ns_per_s;

const log = std.log.scoped(.io);

pub const Error = error{OutOfMemory};

pub const Io = struct {
    allocator: mem.Allocator,
    ring: IoUring = undefined,
    timestamp: u64 = 0,
    recv_buf_grp: IoUring.BufferGroup = undefined,
    metric: Metric = .{},
    cqe_buf: [256]linux.io_uring_cqe = undefined,
    cqe_buf_head: usize = 0,
    cqe_buf_tail: usize = 0,
    pending: Fifo(Op) = .{},
    loop_timer: LoopTimer,

    pub fn init(self: *Io, allocator: mem.Allocator, options: Options) !void {
        // Flags reference: https://nick-black.com/dankwiki/index.php/Io_uring
        const flags =
            // Create a kernel thread to poll on the submission queue. If the
            // submission queue is kept busy, this thread will reap SQEs without
            // the need for a system call.
            linux.IORING_SETUP_SQPOLL |
            // Hint to the kernel that only a single thread will submit
            // requests, allowing for optimizations.
            linux.IORING_SETUP_SINGLE_ISSUER;
        var ring = try IoUring.init(options.entries, flags);
        errdefer ring.deinit();
        self.* = .{
            .allocator = allocator,
            .ring = ring,
            .timestamp = timestamp(),
            .loop_timer = .{ .io = self },
        };
        if (options.recv_buffers > 0) {
            self.recv_buf_grp = try self.initBufferGroup(1, options.recv_buffers, options.recv_buffer_len);
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
    }

    pub fn drain(self: *Io) !void {
        while (self.metric.all.active() > 0) {
            log.debug("draining active operations: {}", .{self.metric.all.active()});
            try self.tick();
        }
    }

    pub fn tickTs(self: *Io, ts: u64) !void {
        self.loop_timer.set(ts);
        try self.tick();
    }

    pub fn tick(self: *Io) !void {
        self.metric.loops.inc(1);
        // Submit prepared sqe-s to the kernel.
        _ = try self.ring.submit();
        // Prepare pending operations
        while (self.pending.peek()) |op| {
            try op.prep(self);
            _ = self.pending.pop();
        }

        if (self.cqe_buf_head >= self.cqe_buf_tail) {
            // Get completions.
            self.cqe_buf_head = 0;
            self.cqe_buf_tail = 0;
            const n = try self.ring.copy_cqes(&self.cqe_buf, 1);
            self.cqe_buf_tail = n;
            self.metric.cqes.inc(n);
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
            const has_more = flagMore(cqe);
            const op: *Op = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
            const op_kind: Op.Kind = op.args;
            // Set op flags
            if (has_more) op.flags |= Op.flag_has_more else op.flags &= ~Op.flag_has_more;
            op.flags &= ~Op.flag_submitted;
            // Do operation callback
            try op.callback(op, self, cqe);
            // NOTE: op can be already deallocated here don't use it after callback
            if (!has_more) self.metric.complete(op_kind);
        }
    }

    fn restart(self: *Io, op: *Op) void {
        self.metric.restart(op.args);
        self.submit(op);
    }

    pub fn submit(self: *Io, op: *Op) void {
        assert(op.flags & Op.flag_submitted == 0);
        op.flags |= Op.flag_submitted;
        op.prep(self) catch |err| {
            log.debug("fail to submit operation {}", .{err});
            self.pending.push(op);
        };
    }

    pub fn now(self: *Io) u64 {
        return self.timestamp;
    }

    pub fn tsFromDelay(self: *Io, delay_ms: u32) u64 {
        return self.timestamp + @as(u64, @intCast(delay_ms)) * std.time.ns_per_ms;
    }

    const LoopTimer = struct {
        io: *Io,
        timer_op: Op = .{},
        cancel_op: Op = .{},
        args: Op.TimerArgs = .{},

        const Self = @This();

        pub fn set(self: *Self, ts: u64) void {
            if (self.timer_op.active()) {
                if (self.args.value() <= ts) return;
                return self.cancel();
            }
            self.args.abs(ts);
            self.timer_op = Op.timer(self.args, self, onTimer, onTimerFail);
            self.io.submit(&self.timer_op);
        }

        fn cancel(self: *Self) void {
            if (!self.timer_op.active()) return;
            if (self.cancel_op.active()) return;
            self.cancel_op = Op.cancel(&self.timer_op, self, onCancel);
            self.io.submit(&self.cancel_op);
        }

        fn onCancel(_: *Self, _: ?anyerror) void {}
        fn onTimer(_: *Self) Error!void {}
        fn onTimerFail(_: *Self, _: anyerror) Error!void {}
    };

    pub fn writeMetrics(self: *Io, writer: anytype) !void {
        var m = &self.metric;

        try m.all.write("io.op.all", writer);
        try m.sendv.write("io.op.send", writer);
        try m.recv.write("io.op.recv", writer);
        try m.accept.write("io.op.accept", writer);
        try m.connect.write("io.op.connect", writer);
        try m.timer.write("io.op.timer", writer);
        {
            try writer.add("io", "loops", &m.loops);
            try writer.add("io", "cqes", &m.cqes);
            try writer.add("io", "send_bytes", &m.send_bytes);
            try writer.add("io", "recv_bytes", &m.recv_bytes);
        }
        {
            try writer.add("io.recv_buf_grp", "success", &m.recv_buf_grp.success);
            try writer.add("io.recv_buf_grp", "no_bufs", &m.recv_buf_grp.no_bufs);
        }
    }
};

fn flagMore(cqe: linux.io_uring_cqe) bool {
    return cqe.flags & linux.IORING_CQE_F_MORE > 0;
}

pub const Op = struct {
    context: u64 = 0,
    callback: *const fn (*Op, *Io, linux.io_uring_cqe) Error!void = undefined,
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
        connect: SocketArgs,
        close: struct {
            socket: socket_t,
            err: ?anyerror,
        },
        recv: socket_t,
        sendv: struct {
            socket: socket_t,
            msghdr: *posix.msghdr_const,
            zero_copy: bool,
        },
        send: struct {
            socket: socket_t,
            buf: []const u8,
        },
        timer: TimerArgs,
        socket: SocketArgs,
        shutdown: socket_t,
        cancel: *Op,
    };

    const SocketArgs = struct {
        domain: u32,
        socket_type: u32 = posix.SOCK.STREAM | posix.SOCK.CLOEXEC,
        protocol: u32 = 0,
        socket: socket_t = 0,
        addr: *std.net.Address,
    };
    const TimerArgs = struct {
        ts: linux.kernel_timespec = .{ .sec = 0, .nsec = 0 },
        count: u32 = 0,
        flags: u32 = 0,

        const IORING_TIMEOUT_MULTISHOT = 1 << 6; // TODO: missing in linux. package

        fn prep(self: *TimerArgs, delay_ns: u64) void {
            const sec = delay_ns / ns_per_s;
            const nsec = (delay_ns - sec * ns_per_s);
            self.ts = .{ .sec = @intCast(sec), .nsec = @intCast(nsec) };
            self.count = 0;
            self.flags = 0;
        }

        fn ticker(self: *TimerArgs, interval_ms: u32) void {
            const sec = interval_ms / std.time.ms_per_s;
            const nsec = (interval_ms - sec * std.time.ms_per_s) * std.time.ns_per_ms;
            self.ts = .{ .sec = @intCast(sec), .nsec = @intCast(nsec) };
            self.count = 0;
            self.flags = IORING_TIMEOUT_MULTISHOT;
        }

        fn abs(self: *TimerArgs, ts_ns: u64) void {
            const sec = ts_ns / ns_per_s;
            const nsec = (ts_ns - sec * ns_per_s);
            self.ts = .{ .sec = @intCast(sec), .nsec = @intCast(nsec) };
            self.count = 0;
            self.flags = linux.IORING_TIMEOUT_ABS | linux.IORING_TIMEOUT_REALTIME;
        }

        fn value(self: TimerArgs) u64 {
            return @as(u64, @intCast(self.ts.sec)) * ns_per_s + @as(u64, @intCast(self.ts.nsec));
        }
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
        switch (op.args) {
            .accept => |*arg| _ = try io.ring.accept_multishot(@intFromPtr(op), arg.socket, &arg.addr, &arg.addr_size, 0),
            .connect => |*arg| _ = try io.ring.connect(@intFromPtr(op), arg.socket, &arg.addr.any, arg.addr.getOsSockLen()),
            .close => |*arg| _ = try io.ring.close(@intFromPtr(op), arg.socket),
            .recv => |socket| _ = try io.recv_buf_grp.recv_multishot(@intFromPtr(op), socket, 0),
            .sendv => |*arg| _ = if (arg.zero_copy)
                try io.ring.sendmsg_zc(@intFromPtr(op), arg.socket, arg.msghdr, linux.MSG.WAITALL | linux.MSG.NOSIGNAL)
            else
                try io.ring.sendmsg(@intFromPtr(op), arg.socket, arg.msghdr, linux.MSG.WAITALL | linux.MSG.NOSIGNAL),
            .send => |*arg| _ = try io.ring.send(@intFromPtr(op), arg.socket, arg.buf, linux.MSG.WAITALL | linux.MSG.NOSIGNAL),
            .timer => |*arg| _ = try io.ring.timeout(@intFromPtr(op), &arg.ts, arg.count, arg.flags),
            .socket => |*arg| _ = try io.ring.socket(@intFromPtr(op), arg.domain, arg.socket_type, arg.protocol, 0),
            .shutdown => |socket| _ = try io.ring.shutdown(@intFromPtr(op), socket, posix.SHUT.RDWR),
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
            fn complete(op: *Op, io: *Io, cqe: linux.io_uring_cqe) Error!void {
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
                if (!flagMore(cqe)) io.restart(op);
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
            fn complete(op: *Op, io: *Io, cqe: linux.io_uring_cqe) Error!void {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS => {
                        const n: usize = @intCast(cqe.res);
                        if (n == 0)
                            return try fail(ctx, error.EndOfFile);

                        const buffer_id = cqe.buffer_id() catch unreachable;
                        const bytes = io.recv_buf_grp.get(buffer_id)[0..n];
                        try success(ctx, bytes);
                        io.recv_buf_grp.put(buffer_id);
                        io.metric.recv_bytes.inc(n);
                        io.metric.recv_buf_grp.success.inc(1);

                        // NOTE: recv is not restarted if there is no more
                        // multishot cqe's (like accept). Check op.hasMore
                        // in callback and submit if needed.
                        return;
                    },
                    .NOBUFS => {
                        io.metric.recv_buf_grp.no_bufs.inc(1);
                    },
                    .INTR => {},
                    else => |errno| return try fail(ctx, errFromErrno(errno)),
                }
                if (!flagMore(cqe)) return io.restart(op);
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .recv = socket },
        };
    }

    // Single shot operations -----------------

    pub fn sendv(
        socket: socket_t,
        msghdr: *posix.msghdr_const,
        context: anytype,
        comptime success: fn (@TypeOf(context)) Error!void,
        comptime fail: fn (@TypeOf(context), anyerror) Error!void,
    ) Op {
        return sendv_(socket, msghdr, context, success, fail, false);
    }

    pub fn sendv_zc(
        socket: socket_t,
        msghdr: *posix.msghdr_const,
        context: anytype,
        comptime success: fn (@TypeOf(context)) Error!void,
        comptime fail: fn (@TypeOf(context), anyerror) Error!void,
    ) Op {
        return sendv_(socket, msghdr, context, success, fail, true);
    }

    fn sendv_(
        socket: socket_t,
        msghdr: *posix.msghdr_const,
        context: anytype,
        comptime success: fn (@TypeOf(context)) Error!void,
        comptime fail: fn (@TypeOf(context), anyerror) Error!void,
        zero_copy: bool,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, io: *Io, cqe: linux.io_uring_cqe) Error!void {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS => {
                        const sent_bytes: usize = @intCast(cqe.res);
                        io.metric.send_bytes.inc(sent_bytes);

                        var data_len: usize = 0;
                        const mh = op.args.sendv.msghdr;
                        for (0..@intCast(mh.iovlen)) |i| data_len += mh.iov[i].len;

                        if (op.args.sendv.zero_copy) {
                            if (cqe.flags & linux.IORING_CQE_F_NOTIF > 0) {
                                op.args.sendv.msghdr.iovlen = 0;
                                return try success(ctx);
                            } else if (cqe.flags & linux.IORING_CQE_F_MORE > 0) {
                                op.args.sendv.msghdr.iovlen = 0;
                                if (sent_bytes < data_len) {
                                    log.err("{} unexpected short send len: {} sent: {}", .{ op.args.sendv.socket, data_len, sent_bytes });
                                    return fail(ctx, error.ShortSend);
                                }
                                return;
                            }
                            unreachable;
                        }

                        // While sending with MSG_WAITALL we don't expect to get short send
                        if (sent_bytes < data_len) {
                            log.warn("{} unexpected short send len: {} sent: {}", .{ op.args.sendv.socket, data_len, sent_bytes });
                            var v: []posix.iovec_const = undefined;
                            v.ptr = @constCast(op.args.sendv.msghdr.iov);
                            v.len = @intCast(op.args.sendv.msghdr.iovlen);
                            v = resizeIovec(v, sent_bytes);
                            if (v.len > 0) { // restart on short send
                                op.args.sendv.msghdr.iov = v.ptr;
                                op.args.sendv.msghdr.iovlen = @intCast(v.len);
                                return io.restart(op);
                            }
                        }

                        op.args.sendv.msghdr.iovlen = 0;
                        try success(ctx);
                    },
                    else => |errno| {
                        op.args.sendv.msghdr.iovlen = 0;
                        try fail(ctx, errFromErrno(errno));
                    },
                }
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{
                .sendv = .{ .socket = socket, .msghdr = msghdr, .zero_copy = zero_copy },
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
            fn complete(op: *Op, io: *Io, cqe: linux.io_uring_cqe) Error!void {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS => {
                        const n: usize = @intCast(cqe.res);
                        io.metric.send_bytes.inc(n);
                        const send_buf = op.args.send.buf;
                        if (n < send_buf.len) {
                            op.args.send.buf = send_buf[n..];
                            return io.restart(op);
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
            fn complete(op: *Op, _: *Io, cqe: linux.io_uring_cqe) Error!void {
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

    pub fn ticker(
        interval_ms: u32,
        context: anytype,
        comptime success: fn (@TypeOf(context)) void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, io: *Io, cqe: linux.io_uring_cqe) Error!void {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS, .TIME => success(ctx),
                    .CANCELED => return, // don't restart
                    else => {},
                }
                if (!flagMore(cqe)) return io.restart(op);
            }
        };
        var args: TimerArgs = .{};
        args.ticker(interval_ms);
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .timer = args },
        };
    }

    // Create socket and connect.
    pub fn connect(
        args: SocketArgs,
        context: anytype,
        comptime success: fn (@TypeOf(context), socket_t) Error!void,
        comptime fail: fn (@TypeOf(context), ?anyerror) void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, io: *Io, cqe: linux.io_uring_cqe) Error!void {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS => {
                        const socket: socket_t = @intCast(cqe.res);
                        var a = op.args.socket;
                        a.socket = socket;
                        op.* = Op.connectSocket(a, ctx, success, fail);
                        io.submit(op);
                    },
                    else => |errno| {
                        log.err("socket create failed {}", .{errFromErrno(errno)});
                        fail(ctx, errFromErrno(errno));
                    },
                }
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .socket = args },
        };
    }

    fn connectSocket(
        args: SocketArgs,
        context: anytype,
        comptime success: fn (@TypeOf(context), socket_t) Error!void,
        comptime fail: fn (@TypeOf(context), ?anyerror) void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, io: *Io, cqe: linux.io_uring_cqe) Error!void {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS => return try success(ctx, op.args.connect.socket),
                    else => |errno| {
                        const err = errFromErrno(errno);
                        op.* = Op.close(op.args.connect.socket, err, ctx, fail);
                        io.submit(op);
                    },
                }
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .connect = args },
        };
    }

    // Shutdown and close socket
    pub fn shutdown(
        socket: socket_t,
        context: anytype,
        comptime done: fn (@TypeOf(context), ?anyerror) void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, io: *Io, cqe: linux.io_uring_cqe) Error!void {
                const err: ?anyerror = switch (cqe.err()) {
                    .SUCCESS => null,
                    else => |errno| errFromErrno(errno),
                };
                const ctx: Context = @ptrFromInt(op.context);
                op.* = Op.close(op.args.shutdown, err, ctx, done);
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
        err: ?anyerror,
        context: anytype,
        comptime done: fn (@TypeOf(context), ?anyerror) void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, _: *Io, cqe: linux.io_uring_cqe) Error!void {
                _ = cqe;
                const ctx: Context = @ptrFromInt(op.context);
                done(ctx, op.args.close.err);
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .close = .{ .socket = socket, .err = err } },
        };
    }

    pub fn cancel(
        op_to_cancel: *Op,
        context: anytype,
        comptime done: fn (@TypeOf(context), ?anyerror) void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, _: *Io, cqe: linux.io_uring_cqe) Error!void {
                const err: ?anyerror = switch (cqe.err()) {
                    .SUCCESS => null,
                    else => |errno| errFromErrno(errno),
                };
                const ctx: Context = @ptrFromInt(op.context);
                done(ctx, err);
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .cancel = op_to_cancel },
        };
    }
};

pub const SendOp = struct {
    op: Op = .{},

    // Vectored send structure
    iov: []posix.iovec_const = &.{},
    // Zeroed msghdr
    msghdr: posix.msghdr_const = .{ .iov = undefined, .iovlen = 0, .name = null, .namelen = 0, .control = null, .controllen = 0, .flags = 0 },

    // iovlen in msghdr is limited by IOV_MAX in <limits.h>. On modern Linux
    // systems, the limit is 1024. Each message has header and body: 2 iovecs that
    // limits number of messages in a batch to 512.
    // ref: https://man7.org/linux/man-pages/man2/readv.2.html
    const max_capacity = 1024;
    // Start with few messages (header and body) capacity
    const initial_capacity = 8;
    const Self = @This();

    pub fn init(
        self: *Self,
        allocator: mem.Allocator,
        socket: socket_t,
        context: anytype,
        comptime success: fn (@TypeOf(context)) Error!void,
        comptime fail: fn (@TypeOf(context), anyerror) Error!void,
    ) !void {
        self.iov = try allocator.alloc(posix.iovec_const, initial_capacity);
        self.msghdr.iov = self.iov.ptr;
        self.op = Op.sendv(socket, &self.msghdr, context, success, fail);
    }

    pub fn deinit(self: *Self, allocator: mem.Allocator) void {
        allocator.free(self.iov);
    }

    pub fn active(self: *Self) bool {
        return self.op.active();
    }

    /// Maximum buffer which can be sent in one go.
    pub fn capacity(self: *Self) usize {
        return self.iov.len;
    }

    /// Submit io operation with all prepared buffers.
    pub fn send(self: *Self, io: *Io) void {
        self.op.args.sendv.zero_copy = false;
        self.send_(io);
    }

    fn send_(self: *Self, io: *Io) void {
        assert(self.msghdr.iov == self.iov.ptr);
        assert(self.op.args.sendv.msghdr == &self.msghdr);
        io.submit(&self.op);
        // io will reset msghdr to 0 after operation completion
    }

    pub fn send_zc(self: *Self, io: *Io) void {
        self.op.args.sendv.zero_copy = true;
        self.send_(io);
    }

    /// Extends iov to want_capacity if that is less then max_capacity.
    pub fn ensureCapacity(self: *Self, allocator: mem.Allocator, want_capacity: usize) !void {
        assert(!self.op.active());
        const new_capacity: usize = @min(want_capacity, max_capacity);
        if (new_capacity <= self.iov.len) return;

        const new_iov = try allocator.alloc(posix.iovec_const, new_capacity);
        allocator.free(self.iov);
        self.iov = new_iov;
        self.msghdr.iov = self.iov.ptr;
    }

    /// Prepare single buffer to be sent. Puts data into next iov.
    pub fn prep(self: *Self, data: []const u8) void {
        assert(!self.op.active());
        assert(data.len > 0);
        const n: usize = @intCast(self.msghdr.iovlen);
        self.iov[n] = .{ .base = data.ptr, .len = data.len };
        self.msghdr.iovlen += 1;
    }

    /// Number of prepared buffers.
    pub fn count(self: *Self) usize {
        return @intCast(self.msghdr.iovlen);
    }

    pub fn free(self: *Self) usize {
        return self.capacity() - self.count();
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

test "loop timer" {
    if (true) return error.SkipZigTest;

    const allocator = testing.allocator;
    var io: Io = undefined;
    try io.init(allocator, .{ .entries = 4, .recv_buffers = 0, .recv_buffer_len = 0 });
    defer io.deinit();

    const start = io.timestamp;
    try io.tickTs(io.timestamp + 100 * std.time.ns_per_ms);
    std.debug.print("1: {}\n", .{(io.timestamp - start) / std.time.ns_per_ms});
    try io.tickTs(io.timestamp + 200 * std.time.ns_per_ms);
    std.debug.print("2: {}\n", .{(io.timestamp - start) / std.time.ns_per_ms});
    io.loop_timer.set(io.timestamp + 200 * std.time.ns_per_ms);
    try io.tickTs(io.timestamp + 199 * std.time.ns_per_ms);
    std.debug.print("3: {}\n", .{(io.timestamp - start) / std.time.ns_per_ms});
    try io.tickTs(io.timestamp + 100 * std.time.ns_per_ms);
    std.debug.print("4: {}\n", .{(io.timestamp - start) / std.time.ns_per_ms});
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

    const Socket = struct {
        domain: u32,
        socket_type: u32,
        protocol: u32,
        socket: socket_t,
        address: *std.net.Address,
    };

    try testing.expectEqual(64, @sizeOf(Op));
    try testing.expectEqual(32, @sizeOf(Op.Args));
    try testing.expectEqual(24, @sizeOf(Op.TimerArgs));
    try testing.expectEqual(24, @sizeOf(Accept));
    try testing.expectEqual(24, @sizeOf(Writev));
    try testing.expectEqual(24, @sizeOf(Send));
    try testing.expectEqual(24, @sizeOf(Socket));
    try testing.expectEqual(16, @sizeOf(Connect));
    try testing.expectEqual(16, @sizeOf(Sendv));
    try testing.expectEqual(112, @sizeOf(net.Address));
    try testing.expectEqual(16, @sizeOf(linux.kernel_timespec));
    try testing.expectEqual(8, @sizeOf(?*Op));
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

const statsd = @import("statsd.zig");

const Metric = struct {
    loops: statsd.Counter = .{},
    cqes: statsd.Counter = .{},
    send_bytes: statsd.Counter = .{},
    recv_bytes: statsd.Counter = .{},

    recv_buf_grp: struct {
        success: statsd.Counter = .{},
        no_bufs: statsd.Counter = .{},

        pub fn noBufsPercent(self: @This()) f64 {
            const total = self.success.value + self.no_bufs.value;
            if (total == 0) return 0;
            return @as(f64, @floatFromInt(self.no_bufs.value)) / @as(f64, @floatFromInt(total)) * 100;
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
        submitted: statsd.Counter = .{},
        completed: statsd.Counter = .{},
        restarted: statsd.Counter = .{},

        // Current number of active operations
        pub fn active(self: Counter) usize {
            return self.submitted.value - self.completed.value;
        }
        fn submit(self: *Counter) void {
            self.submitted.inc(1);
        }
        fn complete(self: *Counter) void {
            self.completed.inc(1);
        }
        fn restart(self: *Counter) void {
            self.restarted.inc(1);
        }

        fn write(self: *Counter, prefix: []const u8, writer: anytype) !void {
            try writer.add(prefix, "submitted", &self.submitted);
            try writer.add(prefix, "completed", &self.completed);
            try writer.add(prefix, "restarted", &self.restarted);
            try writer.gauge(prefix, "active", self.active());
        }

        pub fn jsonStringify(self: *const Counter, jws: anytype) !void {
            try jws.beginObject();
            try jws.objectField("active");
            try jws.write(self.active());
            try jws.objectField("submit");
            try jws.write(self.submitted);
            try jws.objectField("complete");
            try jws.write(self.completed);
            if (self.restarted.value > 0) {
                try jws.objectField("restart");
                try jws.write(self.restarted);
            }
            try jws.endObject();
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
