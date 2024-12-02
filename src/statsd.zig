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
const Broker = @import("main.zig").Broker;

const log = std.log.scoped(.statsd);

pub const Connector = struct {
    allocator: mem.Allocator,
    io: *Io,
    broker: *Broker,
    options: Options.Statsd,
    address: std.net.Address,
    socket: socket_t = 0,
    connect_op: Op = .{},
    send_op: Op = .{},
    ticker_op: Op = .{},

    iter: BufferSizeIterator = .{},
    prefix: []const u8,

    const Self = @This();

    pub fn init(self: *Self, allocator: mem.Allocator, io: *Io, broker: *Broker, options: Options) !void {
        self.* = .{
            .allocator = allocator,
            .io = io,
            .broker = broker,
            .options = options.statsd,
            .address = options.statsd.address.?,
            .prefix = try fmtPrefix(allocator, options.statsd.prefix, options.broadcastAddress(), options.broadcast_tcp_port),
        };
        errdefer self.deinit();
        // Start endless ticker
        self.ticker_op = Op.ticker(self.options.interval, self, onTick);
        self.io.submit(&self.ticker_op);
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.prefix);
        self.allocator.free(self.iter.buf);
    }

    fn onTick(self: *Self) void {
        self.tick() catch |err| {
            log.err("tick failed {}", .{err});
        };
    }

    fn tick(self: *Self) !void {
        if (self.socket == 0) return try self.connect();
        if (self.send_op.active()) return;

        if (self.iter.done()) try self.generate();
        self.send();
    }

    fn connect(self: *Self) !void {
        if (self.connect_op.active()) return;
        self.connect_op = Op.connect(
            .{
                .domain = self.address.any.family,
                .socket_type = posix.SOCK.DGRAM | posix.SOCK.CLOEXEC,
                .addr = &self.address,
            },
            self,
            onConnect,
            onConnectFail,
        );
        self.io.submit(&self.connect_op);
    }

    fn onConnect(self: *Self, socket: socket_t) Error!void {
        self.socket = socket;
    }

    fn onConnectFail(_: *Self, err: ?anyerror) void {
        if (err) |e| log.err("connect failed {}", .{e});
    }

    fn generate(self: *Self) Error!void {
        var writer = MetricWriter.init(self.allocator, self.prefix);
        errdefer writer.deinit();
        self.broker.writeMetrics(&writer) catch |err| {
            log.err("broker write metrics error {s}", .{@errorName(err)});
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
        const buf = try writer.toOwned();
        self.iter = BufferSizeIterator{ .buf = buf, .size = self.options.udp_packet_size };
    }

    fn send(self: *Self) void {
        if (self.iter.next()) |buf| {
            self.send_op = Op.send(self.socket, buf, self, onSend, onSendFail);
            self.io.submit(&self.send_op);
        }
    }

    fn onSend(self: *Self) Error!void {
        if (self.iter.done()) {
            log.debug("sent {} bytes", .{self.iter.buf.len});
            self.allocator.free(self.iter.buf);
            self.iter = .{};
        } else {
            self.send();
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
        try self.write(prefix, metric, 'c', current -| previous);
    }

    fn write(self: *Self, prefix: []const u8, metric: []const u8, typ: u8, value: usize) !void {
        const writer = self.list.writer().any();
        if (self.prefix.len > 0)
            try writer.print("{s}.{s}.{s}:{d}|{c}\n", .{ self.prefix, prefix, metric, value, typ })
        else
            try writer.print("{s}.{s}:{d}|{c}\n", .{ prefix, metric, value, typ });
    }

    pub fn gauge(self: *Self, prefix: []const u8, metric: []const u8, value: usize) !void {
        try self.write(prefix, metric, 'g', value);
    }

    pub fn add(self: *Self, prefix: []const u8, metric: []const u8, value: anytype) !void {
        switch (@TypeOf(value)) {
            Gauge, *Gauge => try self.write(prefix, metric, 'g', value.value),
            *Counter => {
                if (value.diff() == 0) return;
                try self.write(prefix, metric, 'c', value.diffReset());
            },
            else => unreachable,
        }
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
    // try writer.gauge("mem.malloc", "ordblks", mi.ordblks);
    // try writer.gauge("mem.malloc", "smblks", mi.smblks);
    // try writer.gauge("mem.malloc", "hblks", mi.hblks);
    try writer.gauge("mem.malloc", "hblkhd", mi.hblkhd);
    // try writer.gauge("mem.malloc", "usmblks", mi.usmblks);
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
    // const share = intFromStr(iter.next()) * page_size; // rssfile
    // const text = intFromStr(iter.next()) * page_size;
    // _ = iter.next();
    // const data = intFromStr(iter.next()) * page_size;

    try writer.gauge("mem", "size", size);
    try writer.gauge("mem", "rss", rss);
    // try writer.gauge("mem", "share", share);
    // try writer.gauge("mem", "text", text);
    // try writer.gauge("mem", "data", data);
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

pub const Gauge = struct {
    const Self = @This();

    value: usize = 0,

    pub fn set(self: *Self, v: usize) void {
        self.value = v;
    }

    pub fn inc(self: *Self, v: usize) void {
        self.value +%= v;
    }

    pub fn dec(self: *Self, v: usize) void {
        self.value -|= v;
    }
};

pub const Counter = struct {
    const Self = @This();

    value: usize = 0,
    prev: usize = 0,

    pub fn inc(self: *Self, v: usize) void {
        self.value +%= v;
    }

    pub fn diff(self: Self) usize {
        return self.value - self.prev;
    }

    pub fn reset(self: *Self) void {
        self.prev = self.value;
    }

    pub fn diffReset(self: *Self) usize {
        defer self.reset();
        return self.diff();
    }
};

test "write metrics" {
    {
        var mv = MetricWriter.init(testing.allocator, "nsq");
        defer mv.deinit();
        try mv.gauge("topic", "depth", 123);
        try mv.counter("topic", "bytes", 789, 456);
        try testing.expectEqualStrings("nsq.topic.depth:123|g\nnsq.topic.bytes:333|c\n", mv.list.items);
    }
    {
        var mv = MetricWriter.init(testing.allocator, "nsq");
        defer mv.deinit();
        var g: Gauge = .{};
        g.inc(123);
        try mv.add("topic", "depth", &g);
        var c: Counter = .{};
        c.inc(789);
        c.prev = 456;
        try mv.add("topic", "bytes", &c);
        try testing.expectEqualStrings("nsq.topic.depth:123|g\nnsq.topic.bytes:333|c\n", mv.list.items);
        try testing.expectEqual(789, c.prev);
    }

    {
        var mv = MetricWriter.init(testing.allocator, "nsq");
        defer mv.deinit();
        var t: struct {
            g: Gauge = .{},
            c: Counter = .{},

            fn write(self: *@This(), writer: anytype) !void {
                try writer.add("topic", "depth", self.g);
                try writer.add("topic", "bytes", &self.c);
            }
        } = .{};
        t.g.inc(123);
        t.c.inc(789);
        t.c.prev = 456;
        try t.write(&mv);
        try testing.expectEqualStrings("nsq.topic.depth:123|g\nnsq.topic.bytes:333|c\n", mv.list.items);
        try testing.expectEqual(789, t.c.prev);
        try testing.expectEqual(789, t.c.value);
    }
}
