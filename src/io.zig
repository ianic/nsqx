const std = @import("std");
const mem = std.mem;
const log = std.log;

const posix = std.posix;
const linux = std.os.linux;
const IoUring = linux.IoUring;
const socket_t = std.posix.socket_t;
const fd_t = std.posix.fd_t;
const errFromErrno = @import("errno.zig").toError;
const Atomic = std.atomic.Value;

const ns_per_ms = std.time.ns_per_ms;
const ns_per_s = std.time.ns_per_s;

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
        op: *Op,
        socket: socket_t,
        context: anytype,
        comptime accepted: fn (@TypeOf(context), socket_t) Error!void,
        comptime failed: fn (@TypeOf(context), anyerror) Error!void,
    ) !void {
        op.* = Op.accept(self, socket, context, accepted, failed);
        try op.prep();
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
        accept: socket_t,
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
            .ticker => _ = try op.io.ring.timeout_remove(0, @intFromPtr(op), 0),
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
            .accept => |socket| _ = try op.io.ring.accept_multishot(@intFromPtr(op), socket, null, null, 0),
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
        comptime accepted: fn (@TypeOf(context), socket_t) Error!void,
        comptime fail: fn (@TypeOf(context), anyerror) Error!void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, cqe: linux.io_uring_cqe) Error!CallbackResult {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS => try accepted(ctx, @intCast(cqe.res)),
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
            .args = .{ .accept = socket },
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

    _ = try io.timer(1, &ctx, Ctx.ticked, null);
    try io.tick();
    try testing.expectEqual(1, ctx.count);

    _ = try io.timer(1, &ctx, Ctx.ticked, null);
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

const TimerOp = struct {
    context: usize,
    fire_at: u64,
    user_data: u64,
    callback: *const fn (*TimerOp) Error!void,

    fn init(
        context: anytype,
        comptime cb: fn (@TypeOf(context), u64) Error!void,
        fire_at: u64,
        user_data: u64,
    ) TimerOp {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *TimerOp) Error!void {
                const ctx: Context = @ptrFromInt(op.context);
                try cb(ctx, op.user_data);
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .fire_at = fire_at,
            .user_data = user_data,
        };
    }

    fn less(_: void, a: *TimerOp, b: *TimerOp) std.math.Order {
        return std.math.order(a.fire_at, b.fire_at);
    }
};

pub const Timer = struct {
    allocator: mem.Allocator,
    io: *Io,
    io_op: ?*Op = null,
    queue: std.PriorityQueue(*TimerOp, void, TimerOp.less),

    const Self = @This();

    pub fn init(allocator: mem.Allocator, io: *Io) Timer {
        return .{
            .allocator = allocator,
            .io = io,
            .queue = std.PriorityQueue(*TimerOp, void, TimerOp.less).init(allocator, {}),
        };
    }

    pub fn deinit(self: *Self) void {
        while (self.queue.removeOrNull()) |op| {
            self.allocator.destroy(op);
        }
        self.queue.deinit();
    }

    pub fn now(self: *Self) u64 {
        return self.io.timestamp;
    }

    /// Set callback to be called at fire_at. User data will be passed to the
    /// callback.
    pub fn set(
        self: *Self,
        context: anytype,
        comptime cb: fn (@TypeOf(context), u64) Error!void,
        fire_at: u64, // future timestamp in nanoseconds
        user_data: u64,
    ) !void {
        const current = if (self.queue.peek()) |op| op.fire_at else std.math.maxInt(u64);
        { // add new op
            const op = try self.allocator.create(TimerOp);
            op.* = TimerOp.init(context, cb, fire_at, user_data);
            try self.queue.add(op);
        }
        if (fire_at < current) {
            if (self.io_op) |io_op| {
                try io_op.cancel();
                io_op.unsubscribe(self);
            }
            try self.ticked();
        }
    }

    /// Success event handler. Fire expired callbacks and arm next timer.
    fn ticked(self: *Self) Error!void {
        const ts = self.now();
        var at: u64 = 0;
        while (self.queue.peek()) |op| {
            if (op.fire_at <= ts) {
                { // fire callback
                    try op.callback(op);
                    _ = self.queue.remove();
                    self.allocator.destroy(op);
                }
                continue;
            }
            at = op.fire_at;
            break;
        }
        if (at == 0) {
            self.io_op = null;
            return;
        }
        self.io_op = try self.io.timer(at - ts, self, Timer.ticked, Timer.failed);
    }

    /// Fail event handler.
    fn failed(self: *Self, err: anyerror) Error!void {
        log.err("timer failed {}", .{err});
        try self.ticked();
    }

    /// Removes all scheduled timer operations for given context.
    pub fn clear(self: *Self, context: anytype) !void {
        const ctx: usize = @intFromPtr(context);
        outer: while (true) {
            var it = self.queue.iterator();
            var idx: usize = 0;
            while (it.next()) |e| {
                if (e.context == ctx) {
                    const op = self.queue.removeIndex(idx);
                    self.allocator.destroy(op);
                    continue :outer;
                }
                idx += 1;
            }
            break;
        }
    }
};

test "Timer set/clear" {
    const allocator = testing.allocator;
    var io = Io{ .allocator = allocator };
    try io.init(4, 0, 0);
    defer io.deinit();

    const Ctx = struct {
        created_at: u64,
        fired_at: u64 = 0,
        fn onTimer(self: *@This(), user_data: u64) !void {
            _ = user_data;
            self.fired_at = timestamp();
        }
        fn delay(self: *@This()) u64 {
            if (self.fired_at == 0) return 0;
            return self.fired_at - self.created_at;
        }
    };

    var timer = Timer.init(testing.allocator, &io);
    var ctx1 = Ctx{ .created_at = timer.now() };
    var ctx2 = Ctx{ .created_at = timer.now() };
    var ctx3 = Ctx{ .created_at = timer.now() };

    defer timer.deinit();
    const ctx1_delay = 1 * ns_per_ms;
    const ctx2_delay = 2 * ns_per_ms;
    const ctx3_delay = 3 * ns_per_ms;
    try timer.set(&ctx3, Ctx.onTimer, ctx3_delay + timer.now(), 0);
    try timer.set(&ctx1, Ctx.onTimer, ctx1_delay + timer.now(), 0);
    try timer.set(&ctx2, Ctx.onTimer, ctx2_delay + timer.now(), 0);
    try timer.set(&ctx2, Ctx.onTimer, ctx2_delay + timer.now(), 0);

    try testing.expectEqual(4, timer.queue.count());

    try io.tick(); // cancel
    try io.tick(); // timer 1
    try testing.expectEqual(3, timer.queue.count());
    try timer.clear(&ctx2);
    try testing.expectEqual(1, timer.queue.count());
    try io.tick(); // timer 2, noop
    try io.tick(); // timer 3

    const tolerance = ns_per_ms / 2;
    // std.debug.print("ctx1.delay(): {}\n", .{ctx1.delay()});
    try testing.expect(ctx1.delay() > ctx1_delay and ctx1.delay() < ctx1_delay + tolerance);
    try testing.expectEqual(0, ctx2.fired_at);
    //std.debug.print("ctx3.delay(): {}\n", .{ctx3.delay()});
    try testing.expect(ctx3.delay() > ctx3_delay and ctx3.delay() < ctx3_delay + tolerance);
}
