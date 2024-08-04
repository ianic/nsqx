const std = @import("std");
const linux = std.os.linux;
const IoUring = linux.IoUring;
const socket_t = std.posix.socket_t;
const fd_t = std.posix.fd_t;
const errFromErrno = @import("errno.zig").toError;

pub const Error = error{
    OutOfMemory,
    SubmissionQueueFull,
    NoBufferSelected,
    NoSpaceLeft, // bufPrint
};

pub const Completion = struct {
    context: u64 = 0,
    callback: *const fn (*Completion, linux.io_uring_cqe) Error!void = undefined,
    buf_grp: ?*IoUring.BufferGroup = null,
    state: State = .submitted,

    const State = enum {
        /// Submitted by application to the kernel. Can be prepared but not yet
        /// submitted to the kernel, submitted to the kernel (we can't
        /// distinguish these two if kernel is pulling from submission queue) or
        /// processed by the kernel and in completion queue.
        submitted,
        /// Completed by the kernel. Pulled from completion queue and being
        /// processed by application.
        completed,
    };

    pub fn process(cqes: []linux.io_uring_cqe) !void {
        for (cqes) |cqe| {
            const c: *Completion = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
            c.state = .completed;
            try c.callback(c, cqe);
        }
    }

    // Signal handler should handle this errors:
    //   error.MultishotFinished       - restart multishot receive
    //   all other errors              - log error and close
    pub fn accept(
        context: anytype,
        comptime success: fn (@TypeOf(context), socket_t) Error!void,
        comptime signal: fn (@TypeOf(context), anyerror) Error!void,
    ) Completion {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, cqe: linux.io_uring_cqe) Error!void {
                const ctx: Context = @ptrFromInt(completion.context);
                switch (cqe.err()) {
                    .SUCCESS => try success(ctx, @intCast(cqe.res)),
                    .CONNABORTED, .INTR => {}, // continue
                    else => |errno| return try signal(ctx, errFromErrno(errno)), // close
                }
                if (cqe.flags & linux.IORING_CQE_F_MORE == 0) {
                    try signal(ctx, error.MultishotFinished);
                } else {
                    completion.state = .submitted;
                }
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
        };
    }

    // Signal handler should handle this errors:
    //   error.EndOfFile               - as 0 bytes receive, close
    //   error.MultishotFinished       - restart multishot receive
    //   error.NoBufferSpaceAvailable  - for monitoring mybe more provided buffers is needed
    //   all other errors              - log error and close
    pub fn recv(
        context: anytype,
        comptime success: fn (@TypeOf(context), []const u8) Error!void,
        comptime signal: fn (@TypeOf(context), anyerror) Error!void,
        buf_grp: *IoUring.BufferGroup,
    ) Completion {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, cqe: linux.io_uring_cqe) Error!void {
                const ctx: Context = @ptrFromInt(completion.context);
                switch (cqe.err()) {
                    .SUCCESS => {
                        const n: usize = @intCast(cqe.res);
                        if (n == 0) return try signal(ctx, error.EndOfFile); // signal EOF

                        const buffer_id = try cqe.buffer_id();
                        const grp = completion.buf_grp.?;
                        const bytes = grp.get(buffer_id)[0..n];
                        try success(ctx, bytes);
                        grp.put(buffer_id);
                    },
                    else => |errno| return try signal(ctx, errFromErrno(errno)), // signal other errors
                }
                if (cqe.flags & linux.IORING_CQE_F_MORE == 0) {
                    try signal(ctx, error.MultishotFinished);
                } else {
                    completion.state = .submitted;
                }
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
            .buf_grp = buf_grp,
        };
    }

    pub fn timeout(
        context: anytype,
        comptime success: fn (@TypeOf(context)) Error!void,
        comptime signal: fn (@TypeOf(context), anyerror) Error!void,
    ) Completion {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, cqe: linux.io_uring_cqe) Error!void {
                const ctx: Context = @ptrFromInt(completion.context);
                switch (cqe.err()) {
                    .SUCCESS, .TIME => try success(ctx),
                    else => |errno| return try signal(ctx, errFromErrno(errno)),
                }
                if (cqe.flags & linux.IORING_CQE_F_MORE == 0) {
                    try signal(ctx, error.MultishotFinished);
                } else {
                    completion.state = .submitted;
                }
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
        };
    }

    pub const read = successUsize;
    pub const send = successUsize;

    pub fn successUsize(
        context: anytype,
        comptime success: fn (@TypeOf(context), usize) Error!void,
        comptime fail: fn (@TypeOf(context), anyerror) Error!void,
    ) Completion {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, cqe: linux.io_uring_cqe) Error!void {
                const ctx: Context = @ptrFromInt(completion.context);
                switch (cqe.err()) {
                    .SUCCESS => try success(ctx, @intCast(cqe.res)),
                    else => |errno| try fail(ctx, errFromErrno(errno)),
                }
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
        };
    }

    pub fn open(
        context: anytype,
        comptime success: fn (@TypeOf(context), fd_t) Error!void,
        comptime fail: fn (@TypeOf(context), anyerror) Error!void,
    ) Completion {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, cqe: linux.io_uring_cqe) Error!void {
                const ctx: Context = @ptrFromInt(completion.context);
                switch (cqe.err()) {
                    .SUCCESS => try success(ctx, @intCast(cqe.res)),
                    else => |errno| try fail(ctx, errFromErrno(errno)),
                }
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
        };
    }

    pub const statx = simple;
    pub const close = simple;

    pub fn simple(
        context: anytype,
        comptime callback: fn (@TypeOf(context), ?anyerror) Error!void,
    ) Completion {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, cqe: linux.io_uring_cqe) Error!void {
                const ctx: Context = @ptrFromInt(completion.context);
                switch (cqe.err()) {
                    .SUCCESS => try callback(ctx, null),
                    else => |errno| try callback(ctx, errFromErrno(errno)),
                }
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
        };
    }

    pub fn generic(
        context: anytype,
        comptime success: fn (@TypeOf(context), result: i32) Error!void,
        comptime fail: fn (@TypeOf(context), anyerror) Error!void,
    ) Completion {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, cqe: linux.io_uring_cqe) Error!void {
                const ctx: Context = @ptrFromInt(completion.context);
                switch (cqe.err()) {
                    .SUCCESS => try success(ctx, cqe.res),
                    else => |errno| try fail(ctx, errFromErrno(errno)),
                }
            }
        };
        return .{
            .context = @intFromPtr(context),
            .callback = wrapper.complete,
        };
    }
};

const testing = std.testing;

test "Completion" {
    const T = struct {
        socket: socket_t = 0,
        err: ?anyerror = null,
        multishot_finished: usize = 0,

        const Self = @This();
        fn onRecv(self: *Self, socket: socket_t) Error!void {
            self.socket = socket;
        }
        fn onRecvSignal(self: *Self, err: anyerror) Error!void {
            switch (err) {
                error.MultishotFinished => {
                    self.multishot_finished += 1;
                },
                else => self.err = err,
            }
        }
    };
    var inst: T = .{};

    var c = Completion.accept(&inst, T.onRecv, T.onRecvSignal);

    try c.callback(&c, .{ .res = 123, .flags = linux.IORING_CQE_F_MORE, .user_data = 0 });
    try testing.expectEqual(123, inst.socket);
    try testing.expect(inst.err == null);
    try testing.expectEqual(0, inst.multishot_finished);

    try c.callback(&c, .{ .res = -2, .flags = linux.IORING_CQE_F_MORE, .user_data = 0 });
    try testing.expectEqual(error.NoSuchFileOrDirectory, inst.err.?);
    try testing.expectEqual(0, inst.multishot_finished);

    try c.callback(&c, .{ .res = -32, .flags = 0, .user_data = 0 });
    try testing.expectEqual(error.BrokenPipe, inst.err.?);
    try testing.expectEqual(0, inst.multishot_finished);

    inst.err = null;
    try c.callback(&c, .{ .res = -4, .flags = 0, .user_data = 0 }); // INTR
    try testing.expect(inst.err == null);
    try testing.expectEqual(1, inst.multishot_finished);
}

const CompletionPool = struct {
    pool: std.heap.MemoryPool(Completion),

    fn init(allocator: std.mem.Allocator) CompletionPool {
        return .{
            .pool = std.heap.MemoryPool(Completion).init(allocator),
        };
    }

    fn get(self: *CompletionPool) !*Completion {
        return try self.pool.create();
    }

    fn putBack(self: *CompletionPool, completion: *Completion) !void {
        self.pool.destroy(completion);
    }

    fn deinit(self: *CompletionPool) void {
        self.pool.deinit();
    }
};

test "a" {
    std.debug.print("align: {}\n", .{@alignOf(*Completion)});
}
