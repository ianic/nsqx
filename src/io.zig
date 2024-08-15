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

pub const Io = struct {
    allocator: mem.Allocator,
    ring: IoUring = undefined,
    recv_buf_grp: IoUring.BufferGroup = undefined,
    op_pool: std.heap.MemoryPool(Op) = undefined,
    op_stat: struct {
        active: usize = 0,
        completed: usize = 0,
    } = .{},

    pub fn init(self: *Io, ring_entries: u16, recv_buffers: u16, recv_buffer_len: u32) !void {
        self.ring = try IoUring.init(ring_entries, linux.IORING_SETUP_SQPOLL & linux.IORING_SETUP_SINGLE_ISSUER);
        self.recv_buf_grp = try self.initBufferGroup(1, recv_buffers, recv_buffer_len);
        self.op_pool = std.heap.MemoryPool(Op).init(self.allocator);
    }

    fn initBufferGroup(self: *Io, id: u16, count: u16, size: u32) !IoUring.BufferGroup {
        const buffers = try self.allocator.alloc(u8, count * size);
        errdefer self.allocator.free(buffers);
        return try IoUring.BufferGroup.init(&self.ring, id, buffers, size, count);
    }

    pub fn deinit(self: *Io) void {
        self.allocator.free(self.recv_buf_grp.buffers);
        self.recv_buf_grp.deinit();
        self.op_pool.deinit();
        self.ring.deinit();
    }

    fn acquire(self: *Io) !*Op {
        self.op_stat.active += 1;
        return try self.op_pool.create();
    }

    fn release(self: *Io, op: *Op) void {
        self.op_stat.active -= 1;
        self.op_stat.completed +%= 1;
        self.op_pool.destroy(op);
    }

    pub fn accept(
        self: *Io,
        socket: socket_t,
        context: anytype,
        comptime accepted: fn (@TypeOf(context), socket_t) Error!void,
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
        //vec: []posix.iovec_const,
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
        sec: i64,
        context: anytype,
        comptime tick: fn (@TypeOf(context)) Error!void,
        comptime failed: ?fn (@TypeOf(context), anyerror) Error!void,
    ) !*Op {
        const op = try self.acquire();
        op.* = Op.ticker(self, sec, context, tick, failed);
        try op.prep();
        return op;
    }

    pub fn close(self: *Io, socket: socket_t) !void {
        const op = try self.acquire();
        op.* = Op.close(self, socket);
        try op.prep();
    }

    pub fn loop(self: *Io, run: Atomic(bool)) !void {
        var cqes: [256]std.os.linux.io_uring_cqe = undefined;
        var i: usize = 0;
        while (run.load(.monotonic)) : (i +%= 1) {
            const n = try self.readCompletions(&cqes);
            if (n > 0)
                try self.flushCompletions(cqes[0..n]);
            if (i % 1024 == 0)
                log.debug("loop round: {}, active ops: {}, completed ops: {}", .{ i, self.op_stat.active, self.op_stat.completed });
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
        for (cqes) |cqe| {
            if (cqe.user_data == 0) continue; // no op for this cqe
            const op: *Op = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
            if (op.context == 0) {
                self.release(op);
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
                switch (res) {
                    .done => self.release(op),
                    .restart => try op.prep(),
                    .has_more => {},
                }
                break;
            }
        }
    }
};

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
        has_more,
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
    };

    const Kind = enum {
        accept,
        close,
        recv,
        writev,
        sendv,
        ticker,
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
        const IORING_TIMEOUT_MULTISHOT = 1 << 6; // TODO: missing in linux. package
        switch (op.args) {
            .accept => |socket| _ = try op.io.ring.accept_multishot(@intFromPtr(op), socket, null, null, 0),
            .close => |socket| _ = try op.io.ring.close(@intFromPtr(op), socket),
            .recv => |socket| _ = try op.io.recv_buf_grp.recv_multishot(@intFromPtr(op), socket, 0),
            .writev => |*args| _ = try op.io.ring.writev(@intFromPtr(op), args.socket, args.vec, 0),
            .sendv => |*args| _ = try op.io.ring.sendmsg(@intFromPtr(op), args.socket, args.msghdr, linux.MSG.WAITALL | linux.MSG.NOSIGNAL),
            .ticker => |ts| _ = try op.io.ring.timeout(@intFromPtr(op), &ts, 0, IORING_TIMEOUT_MULTISHOT),
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
                if (cqe.flags & linux.IORING_CQE_F_MORE == 0)
                    return .restart;
                return .has_more;
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
                        try received(ctx, bytes);
                        op.io.recv_buf_grp.put(buffer_id);
                    },
                    .NOBUFS => log.warn("recv buffer group NOBUFS temporary error", .{}),
                    .INTR => {},
                    else => |errno| {
                        try failed(ctx, errFromErrno(errno));
                        return .done;
                    },
                }
                if (cqe.flags & linux.IORING_CQE_F_MORE == 0)
                    return .restart;
                return .has_more;
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
                            log.warn("unexpected short send len: {} sent: {}", .{ m, n });
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
        sec: i64,
        context: anytype,
        comptime tick: fn (@TypeOf(context)) Error!void,
        comptime failed: ?fn (@TypeOf(context), anyerror) Error!void,
    ) Op {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(op: *Op, cqe: linux.io_uring_cqe) Error!CallbackResult {
                const ctx: Context = @ptrFromInt(op.context);
                switch (cqe.err()) {
                    .SUCCESS, .TIME => try tick(ctx),
                    .INTR => {},
                    else => |errno| {
                        if (failed) |f| try f(ctx, errFromErrno(errno));
                        return .done;
                    },
                }
                if (cqe.flags & linux.IORING_CQE_F_MORE == 0)
                    return .restart;
                return .has_more;
            }
        };
        return .{
            .io = io,
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .args = .{ .ticker = .{ .sec = sec, .nsec = 0 } },
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
