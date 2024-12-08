const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;
const testing = std.testing;
const math = std.math;
const maxInt = math.maxInt;
const log = std.log.scoped(.store);

const Counter = @import("statsd.zig").Counter;
const Gauge = @import("statsd.zig").Gauge;

pub const Options = struct {
    initial_page_size: u32,
    max_page_size: u32 = 0, // prevents growth
    max_mem: u64 = maxInt(u64),

    ack_policy: AckPolicy = .none,
    retention_policy: RetentionPolicy = .all,
};

const Page = struct {
    const Self = @This();

    buf: []u8, // allocated page buffer
    first_sequence: u64, // sequence of message at offset 0
    ref_count: u32 = 0, // reference counter
    offsets: std.ArrayList(u32), // offset of each message in the buf

    fn alloc(self: *Self, bytes_count: u32) ![]u8 {
        assert(self.free() >= bytes_count);
        const wp = self.writePos();
        try self.offsets.append(wp + bytes_count);
        return self.buf[wp..][0..bytes_count];
    }

    fn append(self: *Self, bytes: []const u8) !void {
        assert(self.free() >= bytes.len);
        const wp = self.writePos();
        try self.offsets.append(wp + @as(u32, @intCast(bytes.len)));
        @memcpy(self.buf[wp..][0..bytes.len], bytes);
    }

    fn writePos(self: Self) u32 {
        return if (self.offsets.items.len == 0) 0 else self.offsets.getLast();
    }

    fn free(self: Self) u32 {
        return self.capacity() - self.writePos();
    }

    // Allocated bytes
    pub fn capacity(self: Self) u32 {
        return @intCast(self.buf.len);
    }

    // Number of messages in page
    pub fn messagesCount(self: Self) u32 {
        return @intCast(self.offsets.items.len);
    }

    // Used bytes
    pub fn bytesSize(self: Self) u32 {
        return self.writePos();
    }

    fn release(self: *Self, refs: u32) void {
        self.ref_count -= refs;
    }

    // With explicit ack_policy reference is raised for each message and one more
    // for the first message. That first reference should be released when
    // buffer is no more required by kernel. Other references should be released
    // when that message is no more needed.
    fn pull(self: *Self, sequence: u64, ready_count: u32, ack_policy: AckPolicy) PullResult {
        assert(ready_count > 0);
        assert(sequence < self.last());
        const msgs_count = self.offsets.items.len;

        if (sequence < self.first()) {
            const cnt = @min(msgs_count, ready_count);
            const end_idx = cnt - 1;
            if (ack_policy == .explicit) self.ref_count += cnt + 1;
            const data = self.buf[0..self.offsets.items[end_idx]];
            return .{
                .data = data,
                .count = cnt,
                .sequence = .{ .from = self.first(), .to = self.first() + end_idx },
            };
        }
        assert(sequence >= self.first());

        const start_idx: u32 = @intCast(sequence - self.first());
        const end_idx: u32 = @min(msgs_count - 1, start_idx + ready_count);
        const cnt: u32 = end_idx - start_idx;
        if (ack_policy == .explicit) self.ref_count += cnt + 1;
        const data = self.buf[self.offsets.items[start_idx]..self.offsets.items[end_idx]];
        return .{
            .data = data,
            .count = cnt,
            .sequence = .{ .from = sequence + 1, .to = sequence + cnt },
        };
    }

    fn first(self: Self) u64 {
        return self.first_sequence;
    }

    fn last(self: Self) u64 {
        return self.first_sequence + self.offsets.items.len - 1;
    }

    fn contains(self: Self, sequence: u64) bool {
        return self.first() <= sequence and self.last() >= sequence;
    }

    fn message(self: *Self, sequence: u64) []const u8 {
        assert(self.contains(sequence));
        const idx: u32 = @intCast(sequence - self.first());
        if (idx == 0)
            return self.buf[0..self.offsets.items[idx]];
        return self.buf[self.offsets.items[idx - 1]..self.offsets.items[idx]];
    }
};

pub const PullResult = struct {
    data: []const u8,
    count: u32,
    sequence: struct {
        from: u64,
        to: u64,
    },

    pub fn revert(self: PullResult, stream: *Stream) void {
        if (stream.options.ack_policy == .explicit) {
            const page = stream.findPage(self.sequence.from).?;
            page.ref_count -= (self.count + 1);
        }
    }
};

pub const DeliverPolicy = union(enum) {
    // Start receiving from the earliest available message in the stream.
    all: void,
    // Start receiving messages created after the consumer was created.
    new: void,
    // Start receiving after some sequence.
    from_sequence: u64,
    // last
};

pub const AckPolicy = enum {
    // Fin is expected for each message.
    // Reference counter raised for each message returned in next.
    explicit,
    none,
};

pub const RetentionPolicy = union(enum) {
    // No page deletion.
    all: void,
    // Remove pages with zero reference count.
    interest: void,
    // Don't delete from sequence, before by interest.
    from_sequence: u64,
};

pub const Metric = struct {
    // Current number of messages in the stream.
    msgs: Gauge = .{},
    // Size in bytes of the current messages.
    bytes: Gauge = .{},
    // Allocated bytes for messages.
    capacity: Gauge = .{},
    // Total number of messages.
    total_msgs: Counter = .{},
    // Total size of all messages.
    total_bytes: Counter = .{},
    // Allocation limit
    max_mem: u64 = maxInt(u64),

    parent: ?*Metric = null,

    fn ensureCapacity(self: *Metric, size: usize) !void {
        if (self.capacity.value + size > self.max_mem) {
            log.err("max memory reached; {} bytes", .{self.max_mem});
            return error.StreamMemoryLimit;
        }
        if (self.parent) |parent| try parent.ensureCapacity(size);
    }

    fn alloc(self: *Metric, capacity: usize) void {
        self.capacity.inc(capacity);
        if (self.parent) |parent| parent.alloc(capacity);
    }

    fn free(self: *Metric, msgs: usize, bytes: usize, capacity: usize) void {
        self.msgs.dec(msgs);
        self.bytes.dec(bytes);
        self.capacity.dec(capacity);
        if (self.parent) |parent| parent.free(msgs, bytes, capacity);
    }

    fn append(self: *Metric, msgs: usize, bytes: usize) void {
        self.total_msgs.inc(msgs);
        self.total_bytes.inc(bytes);
        self.msgs.inc(msgs);
        self.bytes.inc(bytes);
        if (self.parent) |parent| parent.append(msgs, bytes);
    }

    fn dec(self: *Metric, msgs: usize, bytes: usize) void {
        self.msgs.dec(msgs);
        self.bytes.dec(bytes);
        if (self.parent) |parent| parent.dec(msgs, bytes);
    }

    pub fn jsonStringify(self: *const Metric, jws: anytype) !void {
        try jws.beginObject();
        try jws.objectField("msgs");
        try jws.write(self.msgs);
        try jws.objectField("bytes");
        try jws.write(self.bytes);
        try jws.objectField("capacity");
        try jws.write(self.capacity);
        try jws.objectField("total_msgs");
        try jws.write(self.total_msgs);
        try jws.objectField("total_bytes");
        try jws.write(self.total_bytes);
        try jws.endObject();
    }
};

pub const Stream = struct {
    const Self = @This();

    allocator: mem.Allocator,
    options: Options,
    pages: std.ArrayList(Page),
    subscribers: u32 = 0,
    last_sequence: u64 = 0,
    page_size: u32,
    metric: Metric = .{},

    pub fn init(allocator: mem.Allocator, options: Options, parent_metric: ?*Metric) Self {
        return .{
            .allocator = allocator,
            .options = options,
            .page_size = options.initial_page_size,
            .pages = std.ArrayList(Page).init(allocator),
            .metric = .{ .parent = parent_metric, .max_mem = options.max_mem },
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.pages.items) |*page| {
            self.metric.free(page.messagesCount(), page.bytesSize(), page.capacity());
            self.allocator.free(page.buf);
            page.offsets.deinit();
        }
        self.pages.deinit();
    }

    const AllocResult = struct {
        data: []u8,
        sequence: u64,
    };

    fn addPage(self: *Self, min_size: u32) !void {
        assert(min_size > 0);

        // increase page_size if needed
        if (self.pages.items.len > 2 and self.page_size < self.options.max_page_size) {
            self.page_size = @min(
                self.page_size * 2,
                self.options.max_page_size,
            );
        }

        const size = @max(self.page_size, min_size);
        try self.metric.ensureCapacity(size);

        // append new page
        var offsets = std.ArrayList(u32).init(self.allocator);
        try offsets.ensureTotalCapacity(if (self.lastPage()) |page| page.messagesCount() else 128);
        errdefer offsets.deinit();
        const buf = try self.allocator.alloc(u8, size);
        errdefer self.allocator.free(buf);
        try self.pages.append(
            Page{
                .ref_count = if (self.pages.items.len == 0) self.subscribers else 0,
                .first_sequence = 1 +% self.last_sequence,
                .buf = buf,
                .offsets = offsets,
            },
        );
        self.metric.alloc(buf.len);
    }

    pub fn ensureUnusedCapacity(self: *Self, bytes_count: u32) !void {
        if (self.pages.items.len == 0 or self.pages.getLast().free() < bytes_count)
            try self.addPage(bytes_count);

        const page = self.lastPage().?;
        try page.offsets.ensureUnusedCapacity(1);
    }

    pub fn alloc(self: *Self, bytes_count: u32) !AllocResult {
        try self.ensureUnusedCapacity(bytes_count);

        const page = self.lastPage().?;
        const data = try page.alloc(bytes_count);

        self.last_sequence +%= 1;
        self.metric.append(1, bytes_count);
        assert(self.last_sequence == page.last());
        return .{ .data = data, .sequence = self.last_sequence };
    }

    pub fn append(self: *Self, bytes: []const u8) !void {
        const res = try self.alloc(@intCast(bytes.len));
        @memcpy(res.data, bytes);
    }

    fn lastPage(self: *Self) ?*Page {
        if (self.pages.items.len == 0) return null;
        return &self.pages.items[self.pages.items.len - 1];
    }

    pub fn lastSequence(self: *Self) u64 {
        if (self.pages.items.len == 0) return 0;
        return self.lastPage().last();
    }

    fn firstPage(self: *Self) ?*Page {
        if (self.pages.items.len == 0) return null;
        return &self.pages.items[0];
    }

    fn nextPage(self: *Self, sequence: *u64) ?[]const u8 {
        const max_ready_count = maxInt(u32);
        return self.pull(sequence, max_ready_count);
    }

    fn containsSequence(sequence: u64, page: Page) math.Order {
        if (page.contains(sequence)) return .eq;
        return if (page.first_sequence > sequence) .lt else .gt;
    }

    fn findPage(self: Self, sequence: u64) ?*Page {
        if (std.sort.binarySearch(Page, self.pages.items, sequence, containsSequence)) |i| {
            return &self.pages.items[i];
        }
        return null;
    }

    pub fn subscribe(self: *Self, delivery_policy: DeliverPolicy) u64 {
        self.subscribers += 1;
        switch (delivery_policy) {
            .all => {
                if (self.firstPage()) |page| {
                    page.ref_count += 1;
                    return page.first_sequence - 1;
                }
                return 0;
            },
            .new => {
                if (self.lastPage()) |page| {
                    page.ref_count += 1;
                    return page.last();
                }
                return self.last_sequence;
            },
            .from_sequence => |seq| {
                if (self.findPage(seq)) |page| {
                    page.ref_count += 1;
                    return seq;
                }
                return self.last_sequence;
            },
        }
    }

    pub fn unsubscribe(self: *Self, sequence: u64) void {
        self.subscribers -= 1;
        if (self.findPage(sequence)) |page| {
            page.ref_count -= 1;
            if (page.ref_count == 0) self.cleanupPages();
            return;
        }
        if (self.firstPage()) |page| {
            page.ref_count -= 1;
            if (page.ref_count == 0) self.cleanupPages();
            return;
        }
        assert(self.pages.items.len == 0);
    }

    fn cleanupPages(self: *Self) void {
        if (self.options.retention_policy != .interest) return;
        if (self.subscribers == 0) return;
        self.removeFreePages();
    }

    // Remove pages which have zero references.
    fn removeFreePages(self: *Self) void {
        while (self.pages.items.len > 0 and self.pages.items[0].ref_count == 0) {
            var page = &self.pages.items[0];
            self.metric.free(page.messagesCount(), page.bytesSize(), page.capacity());
            self.allocator.free(page.buf);
            page.offsets.deinit();
            _ = self.pages.orderedRemove(0);
        }
    }

    pub fn empty(self: *Self) void {
        self.removeFreePages();
    }

    pub fn pull(self: *Self, sequence: u64, ready_count: u32) ?PullResult {
        if (self.pages.items.len == 0 or
            ready_count == 0 or
            sequence == self.last_sequence)
            return null;

        var cleanup = false;
        const page = brk: {
            if (self.firstPage()) |page| if (sequence < page.first()) {
                break :brk page;
            };

            if (self.findPage(sequence)) |page| {
                if (page.last() > sequence) break :brk page;

                if (self.findPage(sequence + 1)) |next_page| {
                    page.ref_count -= 1;
                    next_page.ref_count += 1;
                    cleanup = page.ref_count == 0;
                    break :brk next_page;
                }
            }

            return null;
        };

        defer if (cleanup) self.cleanupPages();
        return page.pull(sequence, ready_count, self.options.ack_policy);
    }

    pub fn hasMore(self: *Self, sequence: u64) bool {
        return self.pages.items.len > 0 and sequence < self.last_sequence;
    }

    pub fn message(self: *Self, sequence: u64) []const u8 {
        const page = self.findPage(sequence).?;
        return page.message(sequence);
    }

    fn findPageByNo(self: *Self, no: u32) ?*Page {
        const first_no = self.pages.items[0].no;
        if (no >= first_no) {
            const idx = no - first_no;
            if (idx < self.pages.items.len)
                return &self.pages.items[idx];
        }
        return null;
    }

    pub fn release(self: *Self, sequence: u64) void {
        const page = self.findPage(sequence) orelse {
            log.err("release: page not found for sequence: {}", .{sequence});
            return;
        };
        page.ref_count -= 1;
        if (page.ref_count == 0) self.cleanupPages();
    }

    pub fn acquire(self: *Self, sequence: u64) void {
        const page = self.findPage(sequence).?;
        page.ref_count += 1;
    }

    pub fn dump(self: Self, file: std.fs.File) !void {
        var header: [12]u8 = undefined;
        mem.writeInt(u64, header[0..8], self.last_sequence, .little);
        mem.writeInt(u32, header[8..12], @intCast(self.pages.items.len), .little);
        try file.writeAll(header[0..12]);

        for (self.pages.items) |page| {
            const wp = page.writePos();
            mem.writeInt(u64, header[0..8], page.first_sequence, .little);
            mem.writeInt(u32, header[8..12], wp, .little);
            try file.writeAll(header[0..12]);
            try file.writeAll(page.buf[0..wp]);
        }
    }

    pub fn restore(self: *Self, file: std.fs.File) !void {
        const rdr = file.reader();

        var header: [12]u8 = undefined;
        try rdr.readNoEof(header[0..12]);
        self.last_sequence = mem.readInt(u64, header[0..8], .little);
        const pages_count = mem.readInt(u32, header[8..12], .little);

        assert(self.pages.items.len == 0);
        for (0..pages_count) |_| {
            try rdr.readNoEof(header[0..12]);
            const first_sequence = mem.readInt(u64, header[0..8], .little);
            const buf_len = mem.readInt(u32, header[8..12], .little);

            var page = Page{
                .ref_count = 0,
                .first_sequence = first_sequence,
                .buf = try self.allocator.alloc(u8, buf_len),
                .offsets = std.ArrayList(u32).init(self.allocator),
            };
            try rdr.readNoEof(page.buf);
            {
                var pos: u32 = 0;
                while (pos < buf_len) {
                    const size = mem.readInt(u32, page.buf[pos..][0..4], .big);
                    pos += size + 4;
                    try page.offsets.append(pos);
                }
            }
            try self.pages.append(page);
            self.metric.alloc(buf_len);
            self.metric.append(page.messagesCount(), page.bytesSize());
        }
    }
};

test "topic usage" {
    var broker_metric: Metric = .{};
    var stream = Stream.init(
        testing.allocator,
        .{ .initial_page_size = 8, .ack_policy = .explicit },
        &broker_metric,
    );
    defer stream.deinit();
    stream.last_sequence = 100;
    stream.options.retention_policy = .interest;

    {
        // page 0
        try stream.append("0123");
        try stream.append("45");
        try stream.append("67");
        // page 1
        try stream.append("89");
        try stream.append("0123");
        // page 2
        try stream.append("4567");
    }
    try testing.expectEqual(6, stream.metric.msgs.value);
    try testing.expectEqual(6, broker_metric.msgs.value);
    try testing.expectEqual(18, stream.metric.bytes.value);
    try testing.expectEqual(24, stream.metric.capacity.value);

    // first subscriber gets all the messages
    var sub_1_seq = stream.subscribe(.all);
    try testing.expectEqual(100, sub_1_seq);
    try testing.expectEqual(1, stream.pages.items[0].ref_count);
    try testing.expectEqual(1, stream.subscribers);
    stream.unsubscribe(sub_1_seq);
    try testing.expectEqual(0, stream.pages.items[0].ref_count);
    try testing.expectEqual(0, stream.subscribers);
    sub_1_seq = stream.subscribe(.all);

    // other subscribers get new messages
    const sub_2_seq = stream.subscribe(.new);
    try testing.expectEqual(106, sub_2_seq);
    try testing.expect(stream.pull(sub_2_seq, 1) == null);

    // references hold
    try testing.expectEqual(2, stream.subscribers);
    try testing.expectEqual(1, stream.pages.items[0].ref_count);
    try testing.expectEqual(0, stream.pages.items[1].ref_count);
    try testing.expectEqual(1, stream.pages.items[2].ref_count);

    // subscriber 1
    {
        const res1 = stream.pull(sub_1_seq, 10).?;
        try testing.expectEqualStrings("01234567", res1.data);
        sub_1_seq = res1.sequence.to;

        // each message holds reference when ack is explicit
        try testing.expectEqual(5, stream.pages.items[0].ref_count);
        try testing.expectEqual(0, stream.pages.items[1].ref_count);
        try testing.expectEqual(1, stream.pages.items[2].ref_count);

        const res2 = stream.pull(sub_1_seq, 10).?;
        try testing.expectEqualStrings("890123", res2.data);

        // subscriber's reference is moved to the second page
        try testing.expectEqual(4, stream.pages.items[0].ref_count);
        try testing.expectEqual(4, stream.pages.items[1].ref_count);
        try testing.expectEqual(1, stream.pages.items[2].ref_count);

        // fin messages in flight release first page
        try testing.expectEqual(3, stream.pages.items.len);
        for (res1.sequence.from..res1.sequence.to + 1) |seq| stream.release(seq);
        stream.release(res1.sequence.from);
        try testing.expectEqual(2, stream.pages.items.len);

        try testing.expectEqual(3, stream.metric.msgs.value);
        try testing.expectEqual(3, broker_metric.msgs.value);
        try testing.expectEqual(10, stream.metric.bytes.value);
        try testing.expectEqual(16, stream.metric.capacity.value);
    }
}

test "append/alloc" {
    var stream = Stream.init(testing.allocator, .{ .initial_page_size = 8, .ack_policy = .explicit }, null);
    stream.last_sequence = 100;
    defer stream.deinit();
    try testing.expectEqual(0, stream.pages.items.len);

    // page 1
    try stream.append("01234");
    try testing.expectEqual(1, stream.pages.items.len);
    try testing.expectEqual(101, stream.pages.items[0].first_sequence);
    try testing.expectEqual(101, stream.last_sequence);
    try testing.expectEqual(8, stream.pages.items[0].buf.len);

    try stream.append("5");
    try testing.expectEqual(102, stream.last_sequence);
    try testing.expectEqual(101, stream.pages.items[0].first_sequence);
    try testing.expectEqual(102, stream.pages.items[0].last());
    //try store.append("6");
    var res = try stream.alloc(2);
    try testing.expectEqual(103, stream.last_sequence);
    try testing.expectEqual(103, stream.pages.items[0].last());

    // page 2
    res = try stream.alloc(2);
    try testing.expectEqual(2, stream.pages.items.len);
    try testing.expectEqual(2, res.data.len);
    try testing.expectEqual(104, res.sequence);
    try testing.expectEqual(104, stream.last_sequence);
    try testing.expectEqual(104, stream.pages.items[1].first_sequence);
    try testing.expectEqual(8, stream.pages.items[1].buf.len);

    // page 3 of size 9
    res = try stream.alloc(9);
    try testing.expectEqual(3, stream.pages.items.len);
    try testing.expectEqual(9, res.data.len);
    try testing.expectEqual(105, res.sequence);
    try testing.expectEqual(105, stream.last_sequence);
    try testing.expectEqual(9, stream.pages.items[2].buf.len);
    try testing.expectEqual(105, stream.pages.items[2].first_sequence);
}

test "delivery policy" {
    var stream = Stream.init(testing.allocator, .{ .initial_page_size = 8, .ack_policy = .none }, null);
    stream.last_sequence = 100;
    defer stream.deinit();

    {
        // page 0
        try stream.append("0123");
        try stream.append("45");
        try stream.append("67");
        // page 1
        try stream.append("89");
        try stream.append("0123");
        // page 2
        try stream.append("4567");
    }

    // deliver policy all
    for (0..2) |_| {
        var sequence = stream.subscribe(.all);
        try testing.expectEqual(100, sequence);
        try testing.expectEqual(1, stream.subscribers);
        var res = stream.pull(sequence, 2).?;
        try testing.expectEqualStrings("012345", res.data);
        try testing.expectEqual(101, res.sequence.from);
        try testing.expectEqual(102, res.sequence.to);
        try testing.expectEqual(1, stream.pages.items[0].ref_count);
        try testing.expectEqual(0, stream.pages.items[1].ref_count);
        try testing.expectEqual(0, stream.pages.items[2].ref_count);
        sequence = res.sequence.to;

        res = stream.pull(sequence, 2).?;
        try testing.expectEqualStrings("67", res.data);
        try testing.expectEqual(103, res.sequence.from);
        try testing.expectEqual(103, res.sequence.to);
        try testing.expectEqual(1, stream.pages.items[0].ref_count);
        try testing.expectEqual(0, stream.pages.items[1].ref_count);
        try testing.expectEqual(0, stream.pages.items[2].ref_count);
        sequence = res.sequence.to;

        res = stream.pull(sequence, 10).?;
        try testing.expectEqualStrings("890123", res.data);
        try testing.expectEqual(104, res.sequence.from);
        try testing.expectEqual(105, res.sequence.to);
        try testing.expectEqual(0, stream.pages.items[0].ref_count);
        try testing.expectEqual(1, stream.pages.items[1].ref_count);
        try testing.expectEqual(0, stream.pages.items[2].ref_count);
        sequence = res.sequence.to;

        res = stream.pull(sequence, 10).?;
        try testing.expectEqualStrings("4567", res.data);
        try testing.expectEqual(106, res.sequence.from);
        try testing.expectEqual(106, res.sequence.to);
        try testing.expectEqual(0, stream.pages.items[0].ref_count);
        try testing.expectEqual(0, stream.pages.items[1].ref_count);
        try testing.expectEqual(1, stream.pages.items[2].ref_count);
        sequence = res.sequence.to;

        stream.unsubscribe(sequence);
        try testing.expectEqual(0, stream.pages.items[0].ref_count);
        try testing.expectEqual(0, stream.pages.items[1].ref_count);
        try testing.expectEqual(0, stream.pages.items[2].ref_count);
        try testing.expectEqual(0, stream.subscribers);
    }

    // deliver policy new
    {
        var sequence = stream.subscribe(.new);
        try testing.expectEqual(106, sequence);
        try testing.expectEqual(0, stream.pages.items[0].ref_count);
        try testing.expectEqual(0, stream.pages.items[1].ref_count);
        try testing.expectEqual(1, stream.pages.items[2].ref_count);
        try testing.expect(stream.pull(sequence, 2) == null);

        try stream.append("89012"); // page 3
        try stream.append("3");
        try stream.append("4");
        try stream.append("5");
        const res = stream.pull(sequence, 10).?;
        try testing.expectEqualStrings("89012345", res.data);
        try testing.expectEqual(107, res.sequence.from);
        try testing.expectEqual(110, res.sequence.to);
        try testing.expectEqual(0, stream.pages.items[0].ref_count);
        try testing.expectEqual(0, stream.pages.items[1].ref_count);
        try testing.expectEqual(0, stream.pages.items[2].ref_count);
        try testing.expectEqual(1, stream.pages.items[3].ref_count);
        sequence = res.sequence.to;

        stream.unsubscribe(sequence);
        try testing.expectEqual(0, stream.pages.items[3].ref_count);
    }

    // delivery policy from
    {
        var sequence = stream.subscribe(.{ .from_sequence = 104 });
        try testing.expectEqual(0, stream.pages.items[0].ref_count);
        try testing.expectEqual(1, stream.pages.items[1].ref_count);
        try testing.expectEqual(0, stream.pages.items[2].ref_count);
        try testing.expectEqual(0, stream.pages.items[3].ref_count);

        const res = stream.pull(sequence, 10).?;
        try testing.expectEqualStrings("0123", res.data);
        try testing.expectEqual(105, res.sequence.from);
        try testing.expectEqual(105, res.sequence.to);
        sequence = res.sequence.to;
        stream.unsubscribe(sequence);
        try testing.expectEqual(0, stream.pages.items[0].ref_count);
        try testing.expectEqual(0, stream.pages.items[1].ref_count);
        try testing.expectEqual(0, stream.pages.items[2].ref_count);
        try testing.expectEqual(0, stream.pages.items[3].ref_count);
    }
}

test "retention policy" {
    var stream = Stream.init(
        testing.allocator,
        .{ .initial_page_size = 8, .ack_policy = .none, .retention_policy = .interest },
        null,
    );
    stream.last_sequence = 100;
    defer stream.deinit();

    {
        // page 0
        try stream.append("0123");
        try stream.append("45");
        try stream.append("67");
        // page 1
        try stream.append("89");
        try stream.append("0123");
        // page 2
        try stream.append("4567");
    }

    var sequence_all_1 = stream.subscribe(.all);
    try testing.expectEqual(100, sequence_all_1);

    var sequence_all_2 = stream.subscribe(.all);
    try testing.expectEqual(100, sequence_all_2);
    try testing.expectEqual(2, stream.subscribers);
    try testing.expectEqual(2, stream.pages.items[0].ref_count);
    try testing.expectEqual(0, stream.pages.items[1].ref_count);
    try testing.expectEqual(0, stream.pages.items[2].ref_count);

    var res = stream.pull(sequence_all_1, 10).?;
    try testing.expectEqualStrings("01234567", res.data);
    sequence_all_1 = res.sequence.to;

    try testing.expectEqual(2, stream.pages.items[0].ref_count);
    try testing.expectEqual(0, stream.pages.items[1].ref_count);
    try testing.expectEqual(0, stream.pages.items[2].ref_count);

    res = stream.pull(sequence_all_1, 10).?;
    try testing.expectEqualStrings("890123", res.data);
    sequence_all_1 = res.sequence.to;
    try testing.expectEqual(1, stream.pages.items[0].ref_count);
    try testing.expectEqual(1, stream.pages.items[1].ref_count);
    try testing.expectEqual(0, stream.pages.items[2].ref_count);

    res = stream.pull(sequence_all_2, 10).?;
    try testing.expectEqualStrings("01234567", res.data);
    sequence_all_2 = res.sequence.to;
    res = stream.pull(sequence_all_2, 1).?;
    try testing.expectEqualStrings("89", res.data);
    sequence_all_2 = res.sequence.to;

    try testing.expectEqual(2, stream.pages.items.len);
    try testing.expectEqual(2, stream.pages.items[0].ref_count);
    try testing.expectEqual(0, stream.pages.items[1].ref_count);

    try testing.expectEqual(104, sequence_all_2);
    try testing.expectEqual(105, sequence_all_1);

    stream.unsubscribe(sequence_all_2);
    try testing.expectEqual(2, stream.pages.items.len);
    try testing.expectEqual(1, stream.pages.items[0].ref_count);
    try testing.expectEqual(0, stream.pages.items[1].ref_count);

    stream.unsubscribe(sequence_all_1);
    try testing.expectEqual(2, stream.pages.items.len);

    try testing.expectEqual(106, stream.last_sequence);
    try stream.append("8");
    try testing.expectEqual(2, stream.pages.items.len);
    try testing.expectEqual(107, stream.last_sequence);
    stream.empty();
    try testing.expectEqual(0, stream.pages.items.len);
}

test "ack policy" {
    var stream = Stream.init(testing.allocator, .{ .initial_page_size = 8, .ack_policy = .explicit }, null);
    stream.last_sequence = 100;
    defer stream.deinit();

    {
        // page 0
        try stream.append("0");
        try stream.append("1");
        try stream.append("2");
        try stream.append("3");
        try stream.append("45");
        try stream.append("67");
    }

    try testing.expectEqual(0, stream.pages.items[0].ref_count);
    var res = stream.pull(100, 2).?;
    try testing.expectEqual(101, res.sequence.from);
    try testing.expectEqual(102, res.sequence.to);
    try testing.expectEqual(3, stream.pages.items[0].ref_count);

    res = stream.pull(102, 2).?;
    try testing.expectEqual(103, res.sequence.from);
    try testing.expectEqual(104, res.sequence.to);
    try testing.expectEqual(6, stream.pages.items[0].ref_count);

    res = stream.pull(102, 2).?;
    try testing.expectEqual(103, res.sequence.from);
    try testing.expectEqual(104, res.sequence.to);
    try testing.expectEqual(9, stream.pages.items[0].ref_count);

    res = stream.pull(104, 10).?;
    try testing.expectEqual(105, res.sequence.from);
    try testing.expectEqual(106, res.sequence.to);
    try testing.expectEqual(12, stream.pages.items[0].ref_count);

    res = stream.pull(100, 6).?;
    try testing.expectEqual(101, res.sequence.from);
    try testing.expectEqual(106, res.sequence.to);
    try testing.expectEqual(19, stream.pages.items[0].ref_count);

    try testing.expectEqualStrings("0", stream.message(101));
    try testing.expectEqualStrings("1", stream.message(102));
    try testing.expectEqualStrings("45", stream.message(105));
    try testing.expectEqualStrings("67", stream.message(106));
}

test "subscribe/unsubscribe" {
    var stream = Stream.init(testing.allocator, .{ .initial_page_size = 8, .ack_policy = .explicit }, null);
    stream.last_sequence = 100;
    defer stream.deinit();

    {
        const sequence = stream.subscribe(.all);
        try testing.expectEqual(sequence, 0);
        try testing.expectEqual(1, stream.subscribers);
        stream.unsubscribe(sequence);
        try testing.expectEqual(0, stream.subscribers);
    }

    {
        const sequence = stream.subscribe(.new);
        try testing.expectEqual(100, sequence);
        try testing.expectEqual(1, stream.subscribers);
        stream.unsubscribe(sequence);
        try testing.expectEqual(0, stream.subscribers);
    }
    try stream.append("0");
    {
        const sequence = stream.subscribe(.all);
        try testing.expectEqual(100, sequence);
        stream.unsubscribe(sequence);
        try testing.expectEqual(0, stream.subscribers);
    }
    {
        const sequence = stream.subscribe(.new);
        try testing.expectEqual(101, sequence);
        stream.unsubscribe(sequence);
        try testing.expectEqual(0, stream.subscribers);
    }
}

test "subscribe/unsubscribe interest" {
    var stream = Stream.init(
        testing.allocator,
        .{ .initial_page_size = 8, .ack_policy = .explicit, .retention_policy = .interest },
        null,
    );
    stream.last_sequence = 100;
    defer stream.deinit();

    // subscribe, no pages
    var sequence = stream.subscribe(.all);
    try testing.expectEqual(sequence, 0);
    try testing.expectEqual(1, stream.subscribers);

    // first page gets all pending subscribers to ref count
    try stream.append("01234567");
    try testing.expectEqual(1, stream.pages.items[0].ref_count);

    // new to the end
    sequence = stream.subscribe(.new);
    try testing.expectEqual(sequence, 101);
    try testing.expectEqual(2, stream.pages.items[0].ref_count);

    // unsubscribe releases ref count
    stream.unsubscribe(100);
    try testing.expectEqual(1, stream.subscribers);
    try testing.expectEqual(1, stream.pages.items[0].ref_count);

    // first page gets subscriber
    sequence = stream.subscribe(.all);
    try testing.expectEqual(sequence, 100);
    try testing.expectEqual(2, stream.subscribers);
    try testing.expectEqual(2, stream.pages.items[0].ref_count);

    // add page 2
    try stream.append("01234567");
    try testing.expectEqual(2, stream.subscribers);
    try testing.expectEqual(2, stream.pages.items[0].ref_count);
    try testing.expectEqual(0, stream.pages.items[1].ref_count);

    // subscribe delivery_policy = .new ref count to last page
    sequence = stream.subscribe(.new);
    try testing.expectEqual(sequence, 102);
    try testing.expectEqual(2, stream.pages.items[0].ref_count);
    try testing.expectEqual(1, stream.pages.items[1].ref_count);

    // unsubscribe of all removes page
    try testing.expectEqual(2, stream.pages.items.len);
    stream.unsubscribe(100);
    stream.unsubscribe(101);
    try testing.expectEqual(1, stream.pages.items.len);
    try testing.expectEqual(1, stream.pages.items[0].ref_count);
}
