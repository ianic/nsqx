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

const recv_buffers = 4096;
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
        if (cqe.user_data == 0) continue;
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
        try conn.init();
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
    send_completion: Completion = .{ .state = .completed },
    heartbeat_completion: Completion = undefined,
    socket: socket_t = 0,
    listener: *Listener,
    recv_buf: ?[]u8 = null,
    msg_header_buf: [34]u8 = undefined,
    send_vec: [2]posix.iovec_const = undefined,
    ready_count: u32 = 0,
    unanswered_heartbeats: u8 = 0,
    heartbeat_interval: linux.kernel_timespec = .{ .sec = 5, .nsec = 0 },

    msg_id: usize = 0,

    fn init(self: *Conn) !void {
        try self.recv();
        try self.heartbeatSet();
    }

    fn recv(self: *Conn) !void {
        self.recv_completion = Completion.recv(self, Conn.onRecv, Conn.onRecvSignal, &self.listener.recv_buf_grp);
        _ = try self.listener.recv_buf_grp.recv_multishot(@intFromPtr(&self.recv_completion), self.socket, 0);
    }

    fn recvCancel(self: *Conn) !void {
        _ = try self.listener.ring.cancel(0, @intFromPtr(&self.recv_completion), 0);
    }

    fn heartbeatSet(self: *Conn) !void {
        self.heartbeat_completion = Completion.timeout(self, Conn.onHeartbeatTimeout, Conn.onHeartbeatTimeoutErr);
        const IORING_TIMEOUT_MULTISHOT = 1 << 6;
        _ = try self.listener.ring.timeout(
            @intFromPtr(&self.heartbeat_completion),
            &self.heartbeat_interval,
            0,
            IORING_TIMEOUT_MULTISHOT,
        );
    }

    fn heartbeatRemove(self: *Conn) !void {
        _ = try self.listener.ring.timeout_remove(0, @intFromPtr(&self.heartbeat_completion), 0);
    }

    fn testSendMsg(self: *Conn) !void {
        var id: [16]u8 = .{0} ** 16;
        mem.writeInt(usize, id[8..], self.msg_id, .big);
        self.msg_id += 1;
        const m = Message{ .body = "Hello world!", .id = id };
        try self.sendMsg(m);
    }

    fn onRecv(self: *Conn, bytes: []const u8) Error!void {
        var parser = protocol.Parser{ .buf = try self.appendRecvBuf(bytes) };
        while (parser.next() catch |err| brk: {
            log.err("protocol parser failed {} '{s}'", .{ err, parser.buf });
            try self.recvCancel();
            break :brk null;
        }) |msg| {
            self.unanswered_heartbeats = 0;
            switch (msg) {
                .identify => |data| {
                    log.debug("identify: {s}", .{data});
                    try self.sendOk();
                },
                .sub => |sub| {
                    log.debug("subscribe: {s} {s}", .{ sub.topic, sub.channel });
                    // try self.sendOk();
                },
                .rdy => |count| {
                    log.debug("ready: {}", .{count});
                    self.ready_count = count;
                    try self.testSendMsg();
                },
                .fin => |_| {
                    std.debug.print("F", .{});
                    //log.debug("fin: {x}", .{msg_id});
                },
                .cls => {
                    try self.sendResponse("CLOSE_WAIT");
                },
                .nop => {
                    log.debug("nop", .{});
                },
                else => {
                    std.debug.print("{}\n", .{msg});
                    unreachable;
                },
            }
        }

        const unparsed = parser.unparsed();
        if (unparsed.len > 0)
            try self.setRecvBuf(unparsed)
        else
            self.deinitRecvBuf();
    }

    fn appendRecvBuf(self: *Conn, bytes: []const u8) ![]const u8 {
        if (self.recv_buf) |old_buf| {
            const new_buf = try self.allocator.realloc(old_buf, old_buf.len + bytes.len);
            @memcpy(new_buf[old_buf.len..], bytes);
            self.recv_buf = new_buf;
            return new_buf;
        }
        return bytes;
    }

    fn setRecvBuf(self: *Conn, bytes: []const u8) !void {
        const new_buf = try self.allocator.dupe(u8, bytes);
        self.deinitRecvBuf();
        self.recv_buf = new_buf;
    }

    fn deinitRecvBuf(self: *Conn) void {
        if (self.recv_buf) |recv_buf| {
            self.allocator.free(recv_buf);
            self.recv_buf = null;
        }
    }

    fn onRecvSignal(self: *Conn, err: anyerror) Error!void {
        switch (err) {
            error.NoBufferSpaceAvailable,
            error.InterruptedSystemCall,
            error.MultishotFinished,
            => {
                log.warn("{} recv failed {}, restarting", .{ self.socket, err });
                return try self.recv();
            },
            error.OperationCanceled, error.EndOfFile, error.ConnectionResetByPeer => {}, // don't log and close
            else => log.err("{} recv {}", .{ self.socket, err }), // log and close
        }
        log.err("{} recv {}", .{ self.socket, err });
        try self.close(&self.recv_completion);
    }

    fn onHeartbeatTimeout(self: *Conn) Error!void {
        if (self.unanswered_heartbeats > 2)
            return self.heartbeatRemove();
        if (!self.sending() and self.unanswered_heartbeats > 0)
            try self.sendHeartbeat();
        self.unanswered_heartbeats += 1;
    }

    fn onHeartbeatTimeoutErr(self: *Conn, err: anyerror) Error!void {
        switch (err) {
            error.InterruptedSystemCall,
            error.MultishotFinished,
            => {
                log.warn("heartbeat failed {}, restarting", .{err});
                return try self.recv();
            },
            error.OperationCanceled => {},
            else => log.err("{} heartbeat failed {}", .{ self.socket, err }),
        }
        log.err("{} heartbeat failed {}", .{ self.socket, err });
        return try self.close(&self.heartbeat_completion);
    }

    fn sending(self: *Conn) bool {
        return self.send_completion.state == .submitted;
    }

    fn sendOk(self: *Conn) !void {
        try self.sendResponse("OK");
    }

    fn sendHeartbeat(self: *Conn) !void {
        try self.sendResponse("_heartbeat_");
    }

    fn sendResponse(self: *Conn, data: []const u8) !void {
        var hdr = &self.msg_header_buf;
        assert(data.len <= hdr.len - 8);
        mem.writeInt(u32, hdr[0..4], @intCast(4 + data.len), .big);
        mem.writeInt(u32, hdr[4..8], @intFromEnum(FrameType.response), .big);
        @memcpy(hdr[8..][0..data.len], data);
        self.send_vec[0] = .{ .base = &self.msg_header_buf, .len = data.len + 8 };
        self.send_vec[1].len = 0;
        try self.send();
    }

    fn sendMsg(self: *Conn, msg: Message) !void {
        var hdr = &self.msg_header_buf;
        mem.writeInt(u32, hdr[0..4], @intCast(msg.body.len + 30), .big);
        mem.writeInt(u32, hdr[4..8], @intFromEnum(FrameType.message), .big);
        mem.writeInt(u64, hdr[8..16], msg.timestamp, .big);
        mem.writeInt(u16, hdr[16..18], msg.attempts, .big);
        hdr[18..34].* = msg.id;
        self.send_vec[0] = .{ .base = &self.msg_header_buf, .len = hdr.len };
        self.send_vec[1] = .{ .base = msg.body.ptr, .len = msg.body.len };
        try self.send();
    }

    fn send(self: *Conn) !void {
        assert(self.send_completion.state != .submitted);
        self.send_completion = Completion.send(self, Conn.onSend, Conn.onSendErr);
        _ = try self.listener.ring.writev(@intFromPtr(&self.send_completion), self.socket, self.send_vec[0..2], 0);
    }

    fn onSend(self: *Conn, n: usize) Error!void {
        const send_len = self.send_vec[0].len + self.send_vec[1].len;
        // log.debug("onSend {} {}", .{ send_len, n });
        if (n < send_len) {
            log.debug("onSend short send n: {} len: {} vec0: {} vec1: {}", .{ n, send_len, self.send_vec[0].len, self.send_vec[1].len });
            if (n > self.send_vec[0].len) {
                const n1 = n - self.send_vec[0].len;
                self.send_vec[1].base += n1;
                self.send_vec[1].len -= n1;
                self.send_vec[0].len = 0;
            } else {
                self.send_vec[0].base += n;
                self.send_vec[0].len -= n;
            }
            try self.send();
        } else {
            self.send_vec[0].len = 0;
            self.send_vec[1].len = 0;
            if (send_len == 46)
                try self.testSendMsg();
        }
    }

    fn onSendErr(conn: *Conn, err: anyerror) Error!void {
        switch (err) {
            error.BrokenPipe, error.ConnectionResetByPeer => {},
            error.InterruptedSystemCall => return try conn.send(),
            else => log.err("{} send {}", .{ conn.socket, err }),
        }
        try conn.close(&conn.send_completion);
    }

    fn close(self: *Conn, completion: *Completion) !void {
        if (self.heartbeat_completion.state == .submitted)
            try self.heartbeatRemove();
        completion.* = Completion.close(self, Conn.onClose);
        _ = try self.listener.ring.close(@intFromPtr(completion), self.socket);
    }

    fn onClose(self: *Conn, _: ?anyerror) Error!void {
        log.debug("{} onClose closing", .{self.socket});
        if (self.recv_completion.state == .submitted or
            self.send_completion.state == .submitted or
            self.heartbeat_completion.state == .submitted) return;
        log.debug("{} onClose closed", .{self.socket});
        self.deinitRecvBuf();
        self.listener.remove(self);
    }
};

const FrameType = enum(u32) {
    response = 0,
    err = 1,
    message = 2,
};

const Message = struct {
    id: [16]u8 = .{0} ** 16,
    timestamp: u64 = 0,
    attempts: u16 = 0,
    body: []const u8,
};
