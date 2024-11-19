const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;
const testing = std.testing;
const log = std.log.scoped(.storeg);

const Page = struct {
    const Self = @This();

    buf: []u8,
    first_sequence: u64 = 0,
    rc: u32 = 0, // reference counter
    offsets: std.ArrayList(u32),

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

    fn capacity(self: Self) u32 {
        return @intCast(self.buf.len);
    }

    fn free(self: Self) u32 {
        return self.capacity() - self.writePos();
    }

    // With explicit ack_policy reference is raised for each message and one more
    // for the first message. That first reference should be released when
    // buffer is no more required by kernel. Other references should be released
    // when that message is no more needed.
    fn next(self: *Self, sequence: u64, ready_count: u32, ack_policy: AckPolicy) NextResult {
        assert(ready_count > 0);
        assert(sequence < self.last());
        const msgs_count = self.offsets.items.len;

        if (sequence < self.first()) {
            const no_msgs = @min(msgs_count, ready_count);
            const end_idx = no_msgs - 1;
            if (ack_policy == .explicit) self.rc += no_msgs + 1;
            const data = self.buf[0..self.offsets.items[end_idx]];
            return .{
                .data = data,
                .sequence = .{ .from = self.first(), .to = self.first() + end_idx },
            };
        }
        assert(sequence >= self.first());

        const start_idx: u32 = @intCast(sequence - self.first());
        const end_idx: u32 = @min(msgs_count - 1, start_idx + ready_count);
        const no_msgs: u32 = end_idx - start_idx;
        if (ack_policy == .explicit) self.rc += no_msgs + 1;
        const data = self.buf[self.offsets.items[start_idx]..self.offsets.items[end_idx]];
        return .{
            .data = data,
            .sequence = .{ .from = sequence + 1, .to = sequence + no_msgs },
        };
    }

    fn first(self: *Self) u64 {
        return self.first_sequence;
    }

    fn last(self: Self) u64 {
        return self.first_sequence + self.offsets.items.len - 1;
    }

    fn contains(self: *Self, sequence: u64) bool {
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

pub const NextResult = struct {
    data: []const u8,
    sequence: struct {
        from: u64,
        to: u64,
    },
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

pub const Options = struct {
    page_size: u32,
    ack_policy: AckPolicy = .none,
    deliver_policy: DeliverPolicy = .{ .all = {} },
    retention_policy: RetentionPolicy = .{ .all = {} },
};

pub const Store = struct {
    const Self = @This();

    allocator: mem.Allocator,
    options: Options,
    pages: std.ArrayList(Page),
    consumers: u32 = 0,
    last_sequence: u64 = 0,

    pub fn init(allocator: mem.Allocator, options: Options) Self {
        assert(options.page_size > 0);
        return .{
            .allocator = allocator,
            .options = options,
            .pages = std.ArrayList(Page).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        for (self.pages.items) |*page| {
            self.allocator.free(page.buf);
            page.offsets.deinit();
        }
        self.pages.deinit();
    }

    const AllocResult = struct {
        data: []u8,
        sequence: u64,
    };

    pub fn alloc(self: *Self, bytes_count: u32) !AllocResult {
        if (self.pages.items.len == 0 or self.pages.getLast().free() < bytes_count) {
            // if (self.pages.items.len > 2) {
            //     self.options.page_size *|= 2;
            // }
            // std.debug.print("{} page size {}\n", .{ self.pages.items.len, self.options.page_size });
            // add new page
            try self.pages.append(
                Page{
                    .rc = if (self.pages.items.len == 0) self.consumers else 0,
                    .first_sequence = 1 + self.last_sequence,
                    .buf = try self.allocator.alloc(u8, @max(self.options.page_size, bytes_count)),
                    .offsets = std.ArrayList(u32).init(self.allocator),
                },
            );
        }
        self.last_sequence += 1;
        const page = self.lastPage().?;
        const data = try page.alloc(bytes_count);
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
        const max_ready_count = std.math.maxInt(u32);
        return self.next(sequence, max_ready_count);
    }

    fn isEmpty(self: Self) bool {
        return self.pages.items.len == 0;
    }

    fn findPage(self: Self, sequence: u64) ?*Page {
        for (self.pages.items) |*page| {
            if (page.contains(sequence)) return page;
        }
        return null;
    }

    pub fn subscribe(self: *Self) u64 {
        self.consumers += 1;
        switch (self.options.deliver_policy) {
            .new => {
                if (self.lastPage()) |p| p.rc += 1;
            },
            .all => {
                if (self.firstPage()) |p| {
                    p.rc += 1;
                    return p.first() - 1;
                }
            },
            .from_sequence => |seq| {
                if (self.findPage(seq)) |p| {
                    p.rc += 1;
                    return seq;
                }
            },
        }
        return self.last_sequence;
    }

    pub fn unsubscribe(self: *Self, sequence: u64) void {
        self.consumers -= 1;
        self.fin(sequence);
        // if (self.findPage(sequence)) |p| {
        //     p.rc -= 1;
        //     if (p.rc == 0) self.cleanupPages();
        // }
    }

    const FindResult = struct {
        page: *Page,
        cleared: bool = false,
    };

    fn findReadPage(self: *Self, sequence: u64) ?FindResult {
        if (self.firstPage()) |fp| {
            if (sequence < fp.first()) return .{ .page = fp };
        }

        if (self.findPage(sequence)) |page| {
            if (page.last() > sequence) return .{ .page = page };

            if (self.findPage(sequence + 1)) |next_page| {
                page.rc -= 1;
                next_page.rc += 1;
                return .{ .page = next_page, .cleared = page.rc == 0 };
            }
        }
        return null;
    }

    fn cleanupPages(self: *Self) void {
        if (self.options.retention_policy != .interest) return;
        while (self.pages.items.len > 0 and self.pages.items[0].rc == 0) {
            var fp = self.pages.orderedRemove(0);
            // log.warn("page remove {}-{}", .{ fp.first(), fp.last() });
            self.allocator.free(fp.buf);
            fp.offsets.deinit();
        }
    }

    pub fn next(self: *Self, sequence: u64, ready_count: u32) ?NextResult {
        if (self.pages.items.len == 0 or
            ready_count == 0 or
            sequence == self.last_sequence)
            return null;

        if (self.findReadPage(sequence)) |res| {
            defer if (res.cleared) self.cleanupPages();
            return res.page.next(sequence, ready_count, self.options.ack_policy);
        }

        unreachable;
    }

    pub fn hasNext(self: *Self, sequence: u64) bool {
        return self.pages.items.len > 0 and sequence < self.last_sequence;
    }

    pub fn message(self: *Self, sequence: u64) []const u8 {
        const page = self.findPage(sequence).?;
        return page.message(sequence);
    }

    pub fn fin(self: *Self, sequence: u64) void {
        const page = self.findPage(sequence).?;
        page.rc -= 1;
        if (page.rc == 0) self.cleanupPages();

        // var i: usize = self.pages.items.len;
        // while (i > 0) {
        //     i -= 1;
        //     var page = &self.pages.items[i];
        //     if (page.contains(sequence)) {
        //         page.rc -= 1;
        //         if (page.rc == 0) self.cleanupPages();
        //         return;
        //     }
        // }
    }
};

test "topic usage" {
    var store = Store.init(testing.allocator, .{
        .page_size = 8,
        .ack_policy = .explicit,
        .deliver_policy = .all,
        .retention_policy = .all,
    });
    defer store.deinit();
    store.last_sequence = 100;

    {
        // page 0
        try store.append("0123");
        try store.append("45");
        try store.append("67");
        // page 1
        try store.append("89");
        try store.append("0123");
        // page 2
        try store.append("4567");
    }

    // first subscriber gets all the messages
    var sub_1_seq = store.subscribe();
    try testing.expectEqual(100, sub_1_seq);

    // change policy after first subscriber
    store.options.retention_policy = .interest;
    store.options.deliver_policy = .new;

    // other subscribers get new messages
    const sub_2_seq = store.subscribe();
    try testing.expectEqual(106, sub_2_seq);
    try testing.expect(store.next(sub_2_seq, 1) == null);

    // references hold
    try testing.expectEqual(1, store.pages.items[0].rc);
    try testing.expectEqual(0, store.pages.items[1].rc);
    try testing.expectEqual(1, store.pages.items[2].rc);

    // subscriber 1
    {
        const res1 = store.next(sub_1_seq, 10).?;
        try testing.expectEqualStrings("01234567", res1.data);
        sub_1_seq = res1.sequence.to;

        // each message holds reference when ack is explicit
        try testing.expectEqual(5, store.pages.items[0].rc);
        try testing.expectEqual(0, store.pages.items[1].rc);
        try testing.expectEqual(1, store.pages.items[2].rc);

        const res2 = store.next(sub_1_seq, 10).?;
        try testing.expectEqualStrings("890123", res2.data);

        // subscriber's reference is moved to the second page
        try testing.expectEqual(4, store.pages.items[0].rc);
        try testing.expectEqual(4, store.pages.items[1].rc);
        try testing.expectEqual(1, store.pages.items[2].rc);

        // fin messages in flight release first page
        try testing.expectEqual(3, store.pages.items.len);
        for (res1.sequence.from..res1.sequence.to + 1) |seq| store.fin(seq);
        store.fin(res1.sequence.from);
        try testing.expectEqual(2, store.pages.items.len);
    }
}

test "append/alloc" {
    var store = Store.init(testing.allocator, .{ .page_size = 8 });
    store.last_sequence = 100;
    defer store.deinit();
    try testing.expectEqual(0, store.pages.items.len);

    // page 1
    try store.append("01234");
    try testing.expectEqual(1, store.pages.items.len);
    try testing.expectEqual(101, store.pages.items[0].first_sequence);
    try testing.expectEqual(101, store.last_sequence);
    try testing.expectEqual(8, store.pages.items[0].buf.len);

    try store.append("5");
    try testing.expectEqual(102, store.last_sequence);
    try testing.expectEqual(101, store.pages.items[0].first_sequence);
    try testing.expectEqual(102, store.pages.items[0].last());
    try store.append("6");
    try testing.expectEqual(103, store.last_sequence);
    try testing.expectEqual(103, store.pages.items[0].last());

    // page 2
    var res = try store.alloc(2);
    try testing.expectEqual(2, store.pages.items.len);
    try testing.expectEqual(2, res.data.len);
    try testing.expectEqual(104, res.sequence);
    try testing.expectEqual(104, store.last_sequence);
    try testing.expectEqual(104, store.pages.items[1].first_sequence);
    try testing.expectEqual(8, store.pages.items[1].buf.len);

    // page 3 of size 9
    res = try store.alloc(9);
    try testing.expectEqual(3, store.pages.items.len);
    try testing.expectEqual(9, res.data.len);
    try testing.expectEqual(105, res.sequence);
    try testing.expectEqual(105, store.last_sequence);
    try testing.expectEqual(9, store.pages.items[2].buf.len);
    try testing.expectEqual(105, store.pages.items[2].first_sequence);
}

test "delivery policy" {
    var store = Store.init(testing.allocator, .{ .page_size = 8, .deliver_policy = .all });
    store.last_sequence = 100;
    defer store.deinit();

    {
        // page 0
        try store.append("0123");
        try store.append("45");
        try store.append("67");
        // page 1
        try store.append("89");
        try store.append("0123");
        // page 2
        try store.append("4567");
    }

    for (0..2) |_| {
        var sequence = store.subscribe();
        try testing.expectEqual(100, sequence);
        try testing.expectEqual(1, store.consumers);
        var res = store.next(sequence, 2).?;
        try testing.expectEqualStrings("012345", res.data);
        try testing.expectEqual(101, res.sequence.from);
        try testing.expectEqual(102, res.sequence.to);
        try testing.expectEqual(1, store.pages.items[0].rc);
        try testing.expectEqual(0, store.pages.items[1].rc);
        try testing.expectEqual(0, store.pages.items[2].rc);
        sequence = res.sequence.to;

        res = store.next(sequence, 2).?;
        try testing.expectEqualStrings("67", res.data);
        try testing.expectEqual(103, res.sequence.from);
        try testing.expectEqual(103, res.sequence.to);
        try testing.expectEqual(1, store.pages.items[0].rc);
        try testing.expectEqual(0, store.pages.items[1].rc);
        try testing.expectEqual(0, store.pages.items[2].rc);
        sequence = res.sequence.to;

        res = store.next(sequence, 10).?;
        try testing.expectEqualStrings("890123", res.data);
        try testing.expectEqual(104, res.sequence.from);
        try testing.expectEqual(105, res.sequence.to);
        try testing.expectEqual(0, store.pages.items[0].rc);
        try testing.expectEqual(1, store.pages.items[1].rc);
        try testing.expectEqual(0, store.pages.items[2].rc);
        sequence = res.sequence.to;

        res = store.next(sequence, 10).?;
        try testing.expectEqualStrings("4567", res.data);
        try testing.expectEqual(106, res.sequence.from);
        try testing.expectEqual(106, res.sequence.to);
        try testing.expectEqual(0, store.pages.items[0].rc);
        try testing.expectEqual(0, store.pages.items[1].rc);
        try testing.expectEqual(1, store.pages.items[2].rc);
        sequence = res.sequence.to;

        store.unsubscribe(sequence);
        try testing.expectEqual(0, store.pages.items[0].rc);
        try testing.expectEqual(0, store.pages.items[1].rc);
        try testing.expectEqual(0, store.pages.items[2].rc);
        try testing.expectEqual(0, store.consumers);
    }

    store.options.deliver_policy = .new;
    {
        var sequence = store.subscribe();
        try testing.expectEqual(106, sequence);
        try testing.expectEqual(0, store.pages.items[0].rc);
        try testing.expectEqual(0, store.pages.items[1].rc);
        try testing.expectEqual(1, store.pages.items[2].rc);
        try testing.expect(store.next(sequence, 2) == null);

        try store.append("89012"); // page 3
        try store.append("3");
        try store.append("4");
        try store.append("5");
        const res = store.next(sequence, 10).?;
        try testing.expectEqualStrings("89012345", res.data);
        try testing.expectEqual(107, res.sequence.from);
        try testing.expectEqual(110, res.sequence.to);
        try testing.expectEqual(0, store.pages.items[0].rc);
        try testing.expectEqual(0, store.pages.items[1].rc);
        try testing.expectEqual(0, store.pages.items[2].rc);
        try testing.expectEqual(1, store.pages.items[3].rc);
        sequence = res.sequence.to;

        store.unsubscribe(sequence);
        try testing.expectEqual(0, store.pages.items[3].rc);
    }

    store.options.deliver_policy = .{ .from_sequence = 104 };
    {
        var sequence = store.subscribe();
        try testing.expectEqual(0, store.pages.items[0].rc);
        try testing.expectEqual(1, store.pages.items[1].rc);
        try testing.expectEqual(0, store.pages.items[2].rc);
        try testing.expectEqual(0, store.pages.items[3].rc);

        const res = store.next(sequence, 10).?;
        try testing.expectEqualStrings("0123", res.data);
        try testing.expectEqual(105, res.sequence.from);
        try testing.expectEqual(105, res.sequence.to);
        sequence = res.sequence.to;
        store.unsubscribe(sequence);
        try testing.expectEqual(0, store.pages.items[0].rc);
        try testing.expectEqual(0, store.pages.items[1].rc);
        try testing.expectEqual(0, store.pages.items[2].rc);
        try testing.expectEqual(0, store.pages.items[3].rc);
    }
}

test "retention policy" {
    var store = Store.init(testing.allocator, .{ .page_size = 8, .deliver_policy = .all, .retention_policy = .all });
    store.last_sequence = 100;
    defer store.deinit();

    {
        // page 0
        try store.append("0123");
        try store.append("45");
        try store.append("67");
        // page 1
        try store.append("89");
        try store.append("0123");
        // page 2
        try store.append("4567");
    }

    var sequence_all_1 = store.subscribe();
    try testing.expectEqual(100, sequence_all_1);
    try testing.expectEqual(1, store.consumers);

    var sequence_all_2 = store.subscribe();
    try testing.expectEqual(100, sequence_all_2);
    try testing.expectEqual(2, store.consumers);

    store.options.retention_policy = .interest;
    store.options.deliver_policy = .new;

    var res = store.next(sequence_all_1, 10).?;
    try testing.expectEqualStrings("01234567", res.data);
    sequence_all_1 = res.sequence.to;
    res = store.next(sequence_all_1, 10).?;
    try testing.expectEqualStrings("890123", res.data);
    sequence_all_1 = res.sequence.to;

    try testing.expectEqual(1, store.pages.items[0].rc);
    try testing.expectEqual(1, store.pages.items[1].rc);
    try testing.expectEqual(0, store.pages.items[2].rc);

    res = store.next(sequence_all_2, 10).?;
    try testing.expectEqualStrings("01234567", res.data);
    sequence_all_2 = res.sequence.to;
    res = store.next(sequence_all_2, 1).?;
    try testing.expectEqualStrings("89", res.data);
    sequence_all_2 = res.sequence.to;

    try testing.expectEqual(2, store.pages.items.len);
    try testing.expectEqual(2, store.pages.items[0].rc);
    try testing.expectEqual(0, store.pages.items[1].rc);

    try testing.expectEqual(104, sequence_all_2);
    try testing.expectEqual(105, sequence_all_1);

    store.unsubscribe(sequence_all_2);
    try testing.expectEqual(2, store.pages.items.len);
    try testing.expectEqual(1, store.pages.items[0].rc);
    try testing.expectEqual(0, store.pages.items[1].rc);

    store.unsubscribe(sequence_all_1);
    try testing.expectEqual(0, store.pages.items.len);

    try testing.expectEqual(106, store.last_sequence);
    try store.append("8");
    try testing.expectEqual(1, store.pages.items.len);
    try testing.expectEqual(107, store.last_sequence);
    store.cleanupPages();
    try testing.expectEqual(0, store.pages.items.len);
}

test "ack policy" {
    var store = Store.init(testing.allocator, .{
        .page_size = 8,
        .ack_policy = .explicit,
        .deliver_policy = .all,
        .retention_policy = .all,
    });
    store.last_sequence = 100;
    defer store.deinit();

    {
        // page 0
        try store.append("0");
        try store.append("1");
        try store.append("2");
        try store.append("3");
        try store.append("45");
        try store.append("67");
    }

    try testing.expectEqual(0, store.pages.items[0].rc);
    var res = store.next(100, 2).?;
    try testing.expectEqual(101, res.sequence.from);
    try testing.expectEqual(102, res.sequence.to);
    try testing.expectEqual(3, store.pages.items[0].rc);

    res = store.next(102, 2).?;
    try testing.expectEqual(103, res.sequence.from);
    try testing.expectEqual(104, res.sequence.to);
    try testing.expectEqual(6, store.pages.items[0].rc);

    res = store.next(102, 2).?;
    try testing.expectEqual(103, res.sequence.from);
    try testing.expectEqual(104, res.sequence.to);
    try testing.expectEqual(9, store.pages.items[0].rc);

    res = store.next(104, 10).?;
    try testing.expectEqual(105, res.sequence.from);
    try testing.expectEqual(106, res.sequence.to);
    try testing.expectEqual(12, store.pages.items[0].rc);

    res = store.next(100, 6).?;
    try testing.expectEqual(101, res.sequence.from);
    try testing.expectEqual(106, res.sequence.to);
    try testing.expectEqual(19, store.pages.items[0].rc);

    try testing.expectEqualStrings("0", store.message(101));
    try testing.expectEqualStrings("1", store.message(102));
    try testing.expectEqualStrings("45", store.message(105));
    try testing.expectEqualStrings("67", store.message(106));
}
