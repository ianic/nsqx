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

pub const Io = struct {
    allocator: mem.Allocator,
    ring: IoUring = undefined,
    timestamp: u64 = 0,
    recv_buf_grp: IoUring.BufferGroup = undefined,
    recv_buf_grp_stat: struct {
        success: usize = 0,
        no_bufs: usize = 0,

        pub fn noBufs(self: @This()) f64 {
            const total = self.success + self.no_bufs;
            if (total == 0) return 0;
            return @as(f64, @floatFromInt(self.no_bufs)) / @as(f64, @floatFromInt(total)) * 100;
        }
    } = .{},
    stat: Stat = .{},

    const Stat = struct {
        loops: usize = 0,
        cqes: usize = 0,

        all: Counter = .{},
        accept: Counter = .{},
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
        };

        fn submit(self: *Stat, op: *Op) void {
            self.all.submit();
            switch (op.args) {
                .accept => self.accept.submit(),
                .close => self.close.submit(),
                .recv => self.recv.submit(),
                .writev => self.writev.submit(),
                .sendv => self.sendv.submit(),
                .ticker, .timer => self.ticker.submit(),
            }
        }

        fn complete(self: *Stat, op: *Op) void {
            self.all.complete();
            switch (op.args) {
                .accept => self.accept.complete(),
                .close => self.close.complete(),
                .recv => self.recv.complete(),
                .writev => self.writev.complete(),
                .sendv => self.sendv.complete(),
                .ticker, .timer => self.ticker.complete(),
            }
        }

        fn restart(self: *Stat, op: *Op) void {
            self.all.restart();
            switch (op.args) {
                .accept => self.accept.restart(),
                .close => self.close.restart(),
                .recv => self.recv.restart(),
                .writev => self.writev.restart(),
                .sendv => self.sendv.restart(),
                .ticker, .timer => self.ticker.restart(),
            }
        }
    };

    pub fn init(self: *Io, ring_entries: u16, recv_buffers: u16, recv_buffer_len: u32) !void {
        self.ring = try IoUring.init(ring_entries, linux.IORING_SETUP_SQPOLL | linux.IORING_SETUP_SINGLE_ISSUER);
        self.timestamp = timestamp();
        if (recv_buffers > 0) {
            self.recv_buf_grp = try self.initBufferGroup(1, recv_buffers, recv_buffer_len);
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

    fn acquire(self: *Io) !*Op {
        return try self.allocator.create(Op);
    }

    fn release(self: *Io, op: *Op) void {
        self.stat.complete(op);
        self.allocator.destroy(op);
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
        comptime sent: fn (@TypeOf(context), usize) Error!void,
        comptime failed: fn (@TypeOf(context), anyerror) Error!void,
    ) !*Op {
        const op = try self.acquire();
        op.* = Op.sendv(self, socket, msghdr, context, sent, failed);
        try op.prep();
        return op;
    }

    pub fn ticker(
        self: *Io,
        msec: i64, // miliseconds
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
        self.stat.loops += 1;
        const n = try self.readCompletions(&cqes);
        if (n > 0) {
            self.timestamp = timestamp();
            try self.flushCompletions(cqes[0..n]);
        }
    }

    pub fn drain(self: *Io) !void {
        while (self.stat.all.active() > 0) {
            log.debug("draining active operations: {}\n", .{self.stat.all.active()});
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
        self.stat.cqes += cqes.len;
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
                            self.stat.restart(op);
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

pub const Error = error{
    OutOfMemory,
    SubmissionQueueFull,
    NoBufferSelected,
};

const PrepError = error{
    SubmissionQueueFull,
};

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
        ticker: linux.kernel_timespec,
        timer: linux.kernel_timespec,
    };

    const Kind = enum {
        accept,
        close,
        recv,
        writev,
        sendv,
        ticker,
        timer,
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

    fn prep(op: *Op) PrepError!void {
        op.io.stat.submit(op);
        const IORING_TIMEOUT_MULTISHOT = 1 << 6; // TODO: missing in linux. package
        switch (op.args) {
            .accept => |*arg| _ = try op.io.ring.accept_multishot(@intFromPtr(op), arg.socket, &arg.addr, &arg.addr_size, 0),
            .close => |socket| _ = try op.io.ring.close(@intFromPtr(op), socket),
            .recv => |socket| _ = try op.io.recv_buf_grp.recv_multishot(@intFromPtr(op), socket, 0),
            .writev => |*arg| _ = try op.io.ring.writev(@intFromPtr(op), arg.socket, arg.vec, 0),
            .sendv => |*arg| _ = try op.io.ring.sendmsg(@intFromPtr(op), arg.socket, arg.msghdr, linux.MSG.WAITALL | linux.MSG.NOSIGNAL),
            .ticker => |*ts| _ = try op.io.ring.timeout(@intFromPtr(op), ts, 0, IORING_TIMEOUT_MULTISHOT),
            .timer => |*ts| _ = try op.io.ring.timeout(@intFromPtr(op), ts, 0, 0),
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

                        const buffer_id = try cqe.buffer_id();
                        const bytes = op.io.recv_buf_grp.get(buffer_id)[0..n];
                        defer op.io.recv_buf_grp.put(buffer_id);
                        try received(ctx, bytes);
                        op.io.recv_buf_grp_stat.success += 1;
                    },
                    .NOBUFS => {
                        op.io.recv_buf_grp_stat.no_bufs += 1;
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

                        // zero send handling
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
            .args = .{
                .sendv = .{ .socket = socket, .msghdr = msghdr },
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
    defer allocator.destroy(op);
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

test "lookup connect" {
    //if (true) return error.SkipZigTest;

    const allocator = testing.allocator;
    var io = Io{ .allocator = allocator };
    try io.init(4, 2, 1024);
    defer io.deinit();

    const Ctx = struct {
        count: usize = 0,
        err: ?anyerror = null,
        socket: socket_t,
        io: *Io,
        send_vec: [2]posix.iovec_const = undefined,
        send_msghdr: posix.msghdr_const = .{ .iov = undefined, .iovlen = 1, .name = null, .namelen = 0, .control = null, .controllen = 0, .flags = 0 },
        send_op: ?*Op = null,
        recv_op: ?*Op = null,
        const Self = @This();
        fn connected(self: *Self) Error!void {
            std.debug.print("connected\n", .{});
            self.count += 1;
        }
        fn send(self: *Self, header: []const u8, body: []const u8) !void {
            self.send_vec[0] = .{ .base = header.ptr, .len = header.len };
            self.send_vec[1] = .{ .base = body.ptr, .len = body.len };
            self.send_msghdr.iov = &self.send_vec;
            self.send_msghdr.iovlen = 2;
            self.send_op = try self.io.sendv(self.socket, &self.send_msghdr, self, sent, sendFailed);
        }
        fn sent(self: *Self, n: usize) Error!void {
            std.debug.print("sent {}\n", .{n});
            self.send_op = null;
            self.count += 1;
        }
        fn sendFailed(self: *Self, err: anyerror) Error!void {
            self.send_op = null;
            self.err = err;
            try self.close();
        }
        fn connectFailed(self: *Self, err: anyerror) Error!void {
            self.err = err;
        }
        pub fn recv(self: *Self) !void {
            self.recv_op = try self.io.recv(self.socket, self, received, recvFailed);
        }

        fn received(self: *Self, bytes: []const u8) Error!void {
            _ = self;
            std.debug.print("received {s}\n", .{bytes});
        }

        fn recvFailed(self: *Self, err: anyerror) Error!void {
            self.recv_op = null;
            switch (err) {
                error.EndOfFile => {},
                error.ConnectionResetByPeer => {},
                else => log.err("{} recv failed {}", .{ self.socket, err }),
            }
            try self.close();
        }

        pub fn close(self: *Self) !void {
            if (self.send_op) |op| {
                try op.cancel();
                return;
            }
            if (self.recv_op) |op| {
                try op.cancel();
                op.unsubscribe(self);
            }
            try self.io.close(self.socket);
            log.debug("{} close", .{self.socket});
        }
    };

    const address = try std.net.Address.parseIp4("127.0.0.1", 4160);
    const socket = try posix.socket(address.any.family, posix.SOCK.STREAM | posix.SOCK.CLOEXEC, 0);

    var ctx = Ctx{ .socket = socket, .io = &io };
    _ = try io.connect(socket, address, &ctx, Ctx.connected, Ctx.connectFailed);

    try io.tick();

    try testing.expect(ctx.err == null);
    try testing.expectEqual(1, ctx.count);

    var header: []const u8 = "  V1";
    var body: []const u8 = "";
    try ctx.send(header, body);
    try ctx.recv();

    try io.tick();
    try testing.expectEqual(2, ctx.count);

    header = "IDENTIFY\n";
    body = "\x00\x00\x00\x63{\"broadcast_address\":\"hydra\",\"hostname\":\"hydra\",\"http_port\":4151,\"tcp_port\":4150,\"version\":\"1.3.0\"}";
    try ctx.send(header, body);

    try io.tick();
    try testing.expectEqual(3, ctx.count);

    header = "REGISTER ";
    body = "pero zdero\n";
    try ctx.send(header, body);

    try io.tick();
    try testing.expectEqual(4, ctx.count);

    try io.drain();
}
