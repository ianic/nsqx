const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const posix = std.posix;
const socket_t = std.posix.socket_t;
const testing = std.testing;

const Io = @import("io.zig").Io;
const Op = @import("io.zig").Op;
const Error = @import("io.zig").Error;
const Options = @import("Options.zig");
const Server = @import("tcp.zig").Server;

const log = std.log.scoped(.statsd);

pub const Connector = struct {
    allocator: mem.Allocator,
    io: *Io,
    server: *Server,
    options: Options.Statsd,
    address: std.net.Address,
    socket: socket_t = 0,
    send_op: ?*Op = null,
    ticker_op: ?*Op = null,
    connect_op: ?*Op = null,
    iter: BufferSizeIterator = .{},
    prefix: []const u8,

    const Self = @This();

    pub fn init(self: *Self, allocator: mem.Allocator, io: *Io, server: *Server, options: Options) !void {
        self.* = .{
            .allocator = allocator,
            .io = io,
            .server = server,
            .options = options.statsd,
            .address = options.statsd.address.?,
            .prefix = try fmtPrefix(allocator, options.statsd.prefix, options.broadcastAddress(), options.broadcast_tcp_port),
        };
        errdefer self.deinit();
        try self.io.ticker(self.options.interval, self, onTick, onTickerFail, &self.ticker_op);
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.prefix);
    }

    fn onTick(self: *Self) Error!void {
        if (self.socket == 0) {
            return try self.connect();
        }
        if (self.send_op != null) return;
        if (self.iter.done()) try self.generate();
        try self.send();
    }

    fn onTickerFail(self: *Self, err: anyerror) Error!void {
        switch (err) {
            error.OperationCanceled => {},
            else => log.err("ticker failed {}", .{err}),
        }
        try self.io.ticker(self.options.interval, self, onTick, onTickerFail, &self.ticker_op);
    }

    fn connect(self: *Self) !void {
        try self.io.socketCreate(
            self.address.any.family,
            posix.SOCK.DGRAM | posix.SOCK.CLOEXEC,
            0,
            self,
            onSocketCreate,
            onConnectFail,
            &self.connect_op,
        );
    }

    fn onSocketCreate(self: *Self, socket: socket_t) Error!void {
        self.socket = socket;
        try self.io.connect(socket, &self.address, self, onConnect, onConnectFail, &self.connect_op);
    }

    fn onConnectFail(self: *Self, err: anyerror) Error!void {
        log.err("connect failed {}", .{err});
        if (self.socket != 0) {
            try self.io.close(self.socket);
            self.socket = 0;
        }
    }

    fn onConnect(_: *Self) Error!void {}

    fn generate(self: *Self) Error!void {
        var writer = MetricWriter.init(self.allocator, self.prefix);
        errdefer writer.deinit();
        self.server.writeMetrics(&writer) catch |err| {
            log.err("server write metrics error {}", .{err});
            return;
        };
        self.io.writeMetrics(&writer) catch |err| {
            log.err("io write metrics error {}", .{err});
            return;
        };
        writeMem(&writer) catch |err| {
            log.err("mem write metrics error {}", .{err});
            return;
        };
        writeStatm(&writer) catch |err| {
            log.err("statm write metrics error {}", .{err});
            return;
        };
        const book = try writer.toOwned();
        self.iter = BufferSizeIterator{ .buf = book, .size = self.options.udp_packet_size };
    }

    fn send(self: *Self) !void {
        if (self.iter.next()) |buf|
            try self.io.send(self.socket, buf, self, onSend, onSendFail, &self.send_op);
    }

    fn onSend(self: *Self) Error!void {
        if (self.iter.done()) {
            log.debug("sent {} bytes", .{self.iter.buf.len});
            self.allocator.free(self.iter.buf);
            self.iter = .{};
        } else {
            try self.send();
        }
    }

    fn onSendFail(self: *Self, err: anyerror) Error!void {
        self.allocator.free(self.iter.buf);
        self.iter = .{};
        log.err("send failed {}", .{err});
    }
};

const BufferSizeIterator = struct {
    buf: []const u8 = &.{},
    pos: usize = 0,
    size: usize = 0,

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
    prefix: []const u8,

    const Self = @This();
    pub fn init(allocator: mem.Allocator, prefix: []const u8) Self {
        return .{
            .list = std.ArrayList(u8).init(allocator),
            .prefix = prefix,
        };
    }

    pub fn counter(self: *Self, prefix: []const u8, metric: []const u8, current: usize, previous: usize) !void {
        const writer = self.list.writer().any();
        if (self.prefix.len > 0)
            try writer.print("{s}.{s}.{s}:{d}|c\n", .{ self.prefix, prefix, metric, current -| previous })
        else
            try writer.print("{s}.{s}:{d}|c\n", .{ prefix, metric, current -| previous });
    }
    pub fn gauge(self: *Self, prefix: []const u8, metric: []const u8, value: usize) !void {
        const writer = self.list.writer().any();
        if (self.prefix.len > 0)
            try writer.print("{s}.{s}.{s}:{d}|g\n", .{ self.prefix, prefix, metric, value })
        else
            try writer.print("{s}.{s}:{d}|g\n", .{ prefix, metric, value });
    }

    pub fn toOwned(self: *Self) ![]const u8 {
        const buf = try self.list.toOwnedSlice();
        self.list.deinit();
        return buf;
    }

    pub fn deinit(self: *Self) void {
        self.list.deinit();
    }
};

///  arena     The total amount of memory allocated by means other than mmap(2) (i.e., memory
///            allocated on the heap).  This figure includes both in-use blocks and  blocks  on
///            the free list.
///
///  ordblks   The number of ordinary (i.e., non-fastbin) free blocks.
///
///  smblks    The number of fastbin free blocks (see mallopt(3)).
///
///  hblks     The  number of blocks currently allocated using mmap(2).  (See the discussion of
///            M_MMAP_THRESHOLD in mallopt(3).)
///
///  hblkhd    The number of bytes in blocks currently allocated using mmap(2).
///
///  usmblks   The "highwater mark" for allocated space—that is, the maximum  amount  of  space
///            that  was  ever  allocated.   This  field  is  maintained  only  in nonthreading
///            environments.
///
///  fsmblks   The total number of bytes in fastbin free blocks.
///
///  uordblks  The total number of bytes used by in-use allocations.
///
///  fordblks  The total number of bytes in free blocks.
///
///  keepcost  The total amount of releasable free space at the top of the heap.  This  is  the
///            maximum  number  of  bytes  that  could  ideally  (i.e., ignoring page alignment
///            restrictions, and so on) be released by malloc_trim(3).
fn writeMem(writer: anytype) !void {
    const c = @cImport(@cInclude("malloc.h"));
    const mi = c.mallinfo2();

    try writer.gauge("mem.malloc", "arena", mi.arena);
    try writer.gauge("mem.malloc", "ordblks", mi.ordblks);
    try writer.gauge("mem.malloc", "smblks", mi.smblks);
    try writer.gauge("mem.malloc", "hblks", mi.hblks);
    try writer.gauge("mem.malloc", "hblkhd", mi.hblkhd);
    try writer.gauge("mem.malloc", "usmblks", mi.usmblks);
    try writer.gauge("mem.malloc", "fsmblks", mi.fsmblks);
    try writer.gauge("mem.malloc", "uordblks", mi.uordblks);
    try writer.gauge("mem.malloc", "fordblks", mi.fordblks);
    try writer.gauge("mem.malloc", "keepcost", mi.keepcost);
}

/// Read /proc/self/statm and output as statsd metrics.
///   size       (1) total program size
///              (same as VmSize in /proc/[pid]/status)
///   resident   (2) resident set size
///              (same as VmRSS in /proc/[pid]/status)
///   share      (3) shared pages (i.e., backed by a file)
///   text       (4) text (code)
///   lib        (5) library (unused in Linux 2.6)
///   data       (6) data + stack
///   dt         (7) dirty pages (unused in Linux 2.6)
fn writeStatm(writer: anytype) !void {
    var file = try std.fs.openFileAbsolute("/proc/self/statm", .{});
    defer file.close();
    var buf: [64]u8 = undefined;
    const n = try file.readAll(&buf);

    var iter = std.mem.splitScalar(u8, buf[0..n], ' ');
    const page_size: usize = 4096;
    const size = intFromStr(iter.next()) * page_size; // vmsize
    const rss = intFromStr(iter.next()) * page_size; // vmrss
    const share = intFromStr(iter.next()) * page_size; // rssfile
    const text = intFromStr(iter.next()) * page_size;
    _ = iter.next();
    const data = intFromStr(iter.next()) * page_size;

    try writer.gauge("mem", "size", size);
    try writer.gauge("mem", "rss", rss);
    try writer.gauge("mem", "share", share);
    try writer.gauge("mem", "text", text);
    try writer.gauge("mem", "data", data);
}

fn intFromStr(str: ?[]const u8) usize {
    if (str) |s| return std.fmt.parseInt(usize, s, 10) catch return 0;
    return 0;
}

fn fmtPrefix(allocator: mem.Allocator, template: []const u8, address: []const u8, port: u16) ![]const u8 {
    if (mem.indexOf(u8, template, "%s")) |_| {
        const addr = try std.mem.replaceOwned(u8, allocator, address, ".", "_");
        defer allocator.free(addr);
        const hostname = try std.fmt.allocPrint(allocator, "{s}_{d}", .{ addr, port });
        defer allocator.free(hostname);
        return try std.mem.replaceOwned(u8, allocator, template, "%s", hostname);
    }
    return allocator.dupe(u8, template);
}

test fmtPrefix {
    const allocator = testing.allocator;

    var prefix = try fmtPrefix(allocator, "nsq.%s", "hydra", 4150);
    try testing.expectEqualStrings("nsq.hydra_4150", prefix);
    allocator.free(prefix);

    prefix = try fmtPrefix(allocator, "foo.bar.%s.stats", "hydra.my.local", 1234);
    try testing.expectEqualStrings("foo.bar.hydra_my_local_1234.stats", prefix);
    allocator.free(prefix);

    prefix = try fmtPrefix(allocator, "foo", "hydra.my.local", 1234);
    try testing.expectEqualStrings("foo", prefix);
    allocator.free(prefix);

    prefix = try fmtPrefix(allocator, "", "hydra.my.local", 1234);
    try testing.expectEqualStrings("", prefix);
    allocator.free(prefix);
}
