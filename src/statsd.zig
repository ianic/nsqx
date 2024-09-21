const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const posix = std.posix;
const socket_t = std.posix.socket_t;
const testing = std.testing;

const Io = @import("io.zig").Io;
const Op = @import("io.zig").Op;
const Error = @import("io.zig").Error;

const Server = @import("tcp.zig").Server;

const log = std.log.scoped(.statsd);

pub const Connector = struct {
    io: *Io,
    allocator: mem.Allocator,
    address: std.net.Address,
    socket: socket_t = 0,
    send_op: ?*Op = null,
    ticker_op: ?*Op = null,
    iter: BufferSizeIterator = .{ .buf = &.{}, .pos = 0, .size = 0 },
    send_interval: i64 = 5 * 1000, // in milliseconds

    const Self = @This();

    pub fn start(self: *Self) !void {
        self.ticker_op = try self.io.ticker(self.send_interval, self, tick, tickerFailed);
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
                // try self.start();
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
        log.err("connect failed {}", .{err});
        try self.io.close(self.socket);
        self.socket = 0;
    }

    fn connected(self: *Self) Error!void {
        _ = self;
    }

    fn generate(self: *Self) Error!void {
        var writer = MetricWriter.init(self.allocator);
        self.io.writeMetrics(&writer) catch |err| {
            log.err("io.writeMetrics error {}", .{err});
            return;
        };
        const book = try writer.toOwned();
        self.iter = BufferSizeIterator{ .buf = book, .size = 504, .pos = 0 };
    }

    fn send(self: *Self) !void {
        if (self.iter.next()) |buf| {
            self.send_op = try self.io.send(self.socket, buf, self, sent, sendFailed);
        }
    }

    fn sent(self: *Self) Error!void {
        self.send_op = null;
        if (self.iter.done()) {
            log.debug("sent {} bytes", .{self.iter.buf.len});
            self.allocator.free(self.iter.buf);
            self.iter = .{ .buf = &.{}, .pos = 0, .size = 0 };
        } else {
            try self.send();
        }
    }

    fn sendFailed(self: *Self, err: anyerror) Error!void {
        self.send_op = null;
        log.err("send failed {}", .{err});
    }

    pub fn close(self: *Self) !void {
        if (self.ticker_op) |op| try op.cancel();
    }
};

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

pub const MetricWriter = struct {
    list: std.ArrayList(u8),

    const Self = @This();
    pub fn init(allocator: mem.Allocator) Self {
        return .{
            .list = std.ArrayList(u8).init(allocator),
        };
    }

    pub fn counter(self: *Self, name: []const u8, current: usize, previous: usize) !void {
        const writer = self.list.writer().any();
        try writer.print("{s}:{d}|c\n", .{ name, current -| previous });
    }
    pub fn gauge(self: *Self, name: []const u8, value: u64) !void {
        const writer = self.list.writer().any();
        try writer.print("{s}:{d}|g\n", .{ name, value });
    }

    pub fn toOwned(self: *Self) ![]const u8 {
        const buf = try self.list.toOwnedSlice();
        self.list.deinit();
        return buf;
    }
};
