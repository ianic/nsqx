const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const log = std.log;
const posix = std.posix;
const linux = std.os.linux;
const IoUring = linux.IoUring;
const socket_t = std.posix.socket_t;
const fd_t = std.posix.fd_t;
const Atomic = std.atomic.Value;

const Completion = @import("completion.zig").Completion;
const Error = @import("completion.zig").Error;
const protocol = @import("protocol.zig");

const recv_buffers = 16;
const recv_buffer_len = 4096;
const port = 4150;
const ring_entries: u16 = 16;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const addr = std.net.Address.initIp4([4]u8{ 127, 0, 0, 1 }, port);
    const socket = (try addr.listen(.{ .reuse_address = true })).stream.handle;

    var ring = try IoUring.init(ring_entries, linux.IORING_SETUP_SQPOLL & linux.IORING_SETUP_SINGLE_ISSUER);
    defer ring.deinit();

    var listener = try Listener.init(allocator, &ring, socket);
    defer listener.deinit();
    try listener.accept();

    catchSignals();
    var cqes: [ring_entries]std.os.linux.io_uring_cqe = undefined;
    while (!interrupted()) {
        const n = try readCompletions(&ring, &cqes);
        if (n > 0)
            try flushCompletions(&ring, cqes[0..n]);
    }
}

fn interrupted() bool {
    const sig = signal.load(.monotonic);
    if (sig != 0) {
        signal.store(0, .release);
        log.info("signal {} received", .{sig});
        switch (sig) {
            posix.SIG.TERM, posix.SIG.INT => return true,
            else => {}, // ignore USR1, USR2, PIPE
        }
    }
    return false;
}

fn readCompletions(ring: *IoUring, cqes: []linux.io_uring_cqe) !usize {
    _ = ring.submit() catch |err| switch (err) {
        error.SignalInterrupt => 0,
        else => return err,
    };
    return ring.copy_cqes(cqes, 1) catch |err| switch (err) {
        error.SignalInterrupt => 0,
        else => return err,
    };
}

fn flushCompletions(ring: *IoUring, cqes: []linux.io_uring_cqe) !void {
    for (cqes) |cqe| {
        const c: *Completion = @ptrFromInt(@as(usize, @intCast(cqe.user_data)));
        c.state = .completed;
        while (true) {
            c.callback(c, cqe) catch |err| {
                log.err("callback failed {}", .{err});
                switch (err) {
                    error.SubmissionQueueFull => {
                        _ = ring.submit() catch |submit_err| switch (submit_err) {
                            error.SignalInterrupt => continue,
                            else => return submit_err,
                        };
                    },
                    else => return err,
                }
            };
            break;
        }
    }
}

var signal = Atomic(c_int).init(0);

fn catchSignals() void {
    var act = posix.Sigaction{
        .handler = .{
            .handler = struct {
                fn wrapper(sig: c_int) callconv(.C) void {
                    signal.store(sig, .release);
                }
            }.wrapper,
        },
        .mask = posix.empty_sigset,
        .flags = 0,
    };
    posix.sigaction(posix.SIG.TERM, &act, null);
    posix.sigaction(posix.SIG.INT, &act, null);
    posix.sigaction(posix.SIG.USR1, &act, null);
    posix.sigaction(posix.SIG.USR2, &act, null);
    posix.sigaction(posix.SIG.PIPE, &act, null);
}

const Listener = struct {
    allocator: mem.Allocator,
    socket: socket_t,
    accept_completion: Completion = undefined,

    conns_pool: std.heap.MemoryPool(Conn),
    conns: std.AutoHashMap(socket_t, *Conn),

    ring: *IoUring,
    recv_buf_grp: IoUring.BufferGroup,

    fn init(allocator: mem.Allocator, ring: *IoUring, socket: socket_t) !Listener {
        return .{
            .allocator = allocator,
            .ring = ring,
            .socket = socket,
            .conns_pool = std.heap.MemoryPool(Conn).init(allocator),
            .conns = std.AutoHashMap(socket_t, *Conn).init(allocator),
            .recv_buf_grp = try initBufferGroup(allocator, ring, 1, recv_buffers, recv_buffer_len),
        };
    }

    fn initBufferGroup(allocator: mem.Allocator, ring: *IoUring, id: u16, count: u16, size: u32) !IoUring.BufferGroup {
        const buffers = try allocator.alloc(u8, count * size);
        errdefer allocator.free(buffers);
        return try IoUring.BufferGroup.init(ring, id, buffers, size, count);
    }

    fn deinit(self: *Listener) void {
        self.allocator.free(self.recv_buf_grp.buffers);
        self.recv_buf_grp.deinit();
        self.conns_pool.deinit();
        self.conns.deinit();
    }

    fn accept(self: *Listener) !void {
        self.accept_completion = Completion.accept(self, Listener.onAccept, Listener.onAcceptErr);
        _ = try self.ring.accept_multishot(@intFromPtr(&self.accept_completion), self.socket, null, null, 0);
    }

    fn onAccept(self: *Listener, socket: socket_t) Error!void {
        var conn = try self.conns_pool.create();
        conn.* = Conn{ .allocator = self.allocator, .socket = socket, .listener = self };
        try self.conns.put(socket, conn);
        try conn.recv();
    }

    fn onAcceptErr(self: *Listener, err: anyerror) Error!void {
        switch (err) {
            error.MultishotFinished => return try self.accept(),
            else => log.err("server accept {}", .{err}),
        }
        self.accept_completion = Completion.close(self, Listener.onClose);
        _ = try self.ring.close(@intFromPtr(&self.accept_completion), self.socket);
    }

    fn onClose(_: *Listener, _: ?anyerror) Error!void {}

    fn remove(self: *Listener, conn: *Conn) void {
        _ = self.conns.remove(conn.socket);
        self.conns_pool.destroy(conn);
    }
};

const response_ok = "\x00\x00\x00\x06" ++ // size
    "\x00\x00\x00\x00" ++ // frame type response
    "OK";

const Conn = struct {
    allocator: mem.Allocator,
    recv_completion: Completion = undefined,
    send_completion: Completion = undefined,
    socket: socket_t = 0,
    listener: *Listener,
    send_buf: []const u8 = &.{},
    recv_buf: ?[]u8 = null,

    fn recv(conn: *Conn) !void {
        conn.recv_completion = Completion.recv(conn, Conn.onRecv, Conn.onRecvSignal, &conn.listener.recv_buf_grp);
        _ = try conn.listener.recv_buf_grp.recv_multishot(@intFromPtr(&conn.recv_completion), conn.socket, 0);
    }

    fn onRecv(conn: *Conn, bytes: []const u8) Error!void {
        if (conn.recv_buf) |old_buf| {
            const new_buf = try conn.allocator.realloc(old_buf, old_buf.len + bytes.len);
            @memcpy(new_buf[old_buf.len..], bytes);
            conn.recv_buf = new_buf;
        }

        var parser = protocol.Parser{ .buf = if (conn.recv_buf) |recv_buf| recv_buf else bytes };
        while (parser.next() catch |err| brk: {
            log.err("protocol parser failed {}", .{err});
            try conn.recvClose();
            break :brk null;
        }) |msg| {
            switch (msg) {
                .identify => |data| {
                    log.debug("identify: {s}", .{data});
                    try conn.send(response_ok);
                },
                .sub => |sub| {
                    log.debug("subscribe: {s} {s}", .{ sub.topic, sub.channel });
                },
                .rdy => |count| {
                    log.debug("ready: {}", .{count});
                },
                else => {
                    std.debug.print("{}\n", .{msg});
                    unreachable;
                },
            }
        }

        const unparsed = parser.unparsed();
        if (unparsed.len > 0) {
            const new_buf = try conn.allocator.alloc(u8, unparsed.len);
            @memcpy(new_buf, unparsed);
            conn.deinitRecvBuf();
            conn.recv_buf = new_buf;
        } else {
            conn.deinitRecvBuf();
        }
    }

    fn deinitRecvBuf(conn: *Conn) void {
        if (conn.recv_buf) |recv_buf| {
            conn.allocator.free(recv_buf);
            conn.recv_buf = null;
        }
    }

    fn onRecvSignal(conn: *Conn, err: anyerror) Error!void {
        switch (err) {
            error.NoBufferSpaceAvailable,
            error.InterruptedSystemCall,
            error.MultishotFinished,
            => {
                log.warn("recv failed {}, restarting", .{err});
                return try conn.recv();
            },
            error.EndOfFile, error.ConnectionResetByPeer => {}, // don't log and close
            else => log.err("{} recv {}", .{ conn.socket, err }), // log and close
        }
        try conn.recvClose();
    }

    fn send(conn: *Conn, bytes: []const u8) !void {
        conn.send_buf = bytes;
        conn.send_completion = Completion.send(conn, Conn.onSend, Conn.onSendErr);
        _ = try conn.listener.ring.send(@intFromPtr(&conn.send_completion), conn.socket, conn.send_buf, 0);
    }

    fn onSend(self: *Conn, n: usize) Error!void {
        if (n < self.send_buf.len) {
            try self.send(self.send_buf[n..]);
        } else {
            self.send_buf = &.{};
        }
    }

    fn onSendErr(conn: *Conn, err: anyerror) Error!void {
        switch (err) {
            error.BrokenPipe, error.ConnectionResetByPeer => {},
            error.InterruptedSystemCall => return try conn.send(conn.send_buf),
            else => log.err("{} send {}", .{ conn.socket, err }),
        }
        try conn.sendClose();
    }

    fn sendClose(conn: *Conn) !void {
        conn.send_completion = Completion.close(conn, Conn.onClose);
        _ = try conn.listener.ring.close(@intFromPtr(&conn.send_completion), conn.socket);
    }

    fn recvClose(conn: *Conn) !void {
        conn.recv_completion = Completion.close(conn, Conn.onClose);
        _ = try conn.listener.ring.close(@intFromPtr(&conn.recv_completion), conn.socket);
    }

    fn onClose(conn: *Conn, _: ?anyerror) Error!void {
        if (conn.recv_completion.state == .submitted or
            conn.send_completion.state == .submitted) return;
        conn.deinitRecvBuf();
        conn.listener.remove(conn);
    }
};
