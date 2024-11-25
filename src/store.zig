const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;
const testing = std.testing;
const maxInt = std.math.maxInt;
const log = std.log.scoped(.store);

pub var stat: struct {
    pages: u32 = 0,
    capacity: u64 = 0,
    max_pages: u32 = 0,

    const Self = @This();

    fn assertAddPage(self: Self) !void {
        if (self.max_pages > 0 and self.pages == self.max_pages) {
            log.err("max number of pages per broker {} reached", .{self.pages});
            return error.BrokerOutOfPages;
        }
    }

    fn inc(self: *Self, bytes: usize) void {
        self.pages += 1;
        self.capacity += bytes;
    }

    fn dec(self: *Self, bytes: usize) void {
        self.pages -= 1;
        self.capacity -= bytes;
    }
} = .{};

pub const Page = struct {
    const Self = @This();

    buf: []u8,
    no: u32,
    first_sequence: u64,
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

    pub fn writePos(self: Self) u32 {
        return if (self.offsets.items.len == 0) 0 else self.offsets.getLast();
    }

    pub fn capacity(self: Self) u32 {
        return @intCast(self.buf.len);
    }

    fn free(self: Self) u32 {
        return self.capacity() - self.writePos();
    }

    pub fn messagesCount(self: Self) u32 {
        return @intCast(self.offsets.items.len);
    }

    pub fn release(self: *Self, refs: u32) void {
        self.rc -= refs;
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
            const count = @min(msgs_count, ready_count);
            const end_idx = count - 1;
            if (ack_policy == .explicit) self.rc += count + 1;
            const data = self.buf[0..self.offsets.items[end_idx]];
            return .{
                .data = data,
                .count = count,
                .sequence = .{ .from = self.first(), .to = self.first() + end_idx },
                .page = self.no,
            };
        }
        assert(sequence >= self.first());

        const start_idx: u32 = @intCast(sequence - self.first());
        const end_idx: u32 = @min(msgs_count - 1, start_idx + ready_count);
        const count: u32 = end_idx - start_idx;
        if (ack_policy == .explicit) self.rc += count + 1;
        const data = self.buf[self.offsets.items[start_idx]..self.offsets.items[end_idx]];
        return .{
            .data = data,
            .count = count,
            .sequence = .{ .from = sequence + 1, .to = sequence + count },
            .page = self.no,
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
    count: u32,
    sequence: struct {
        from: u64,
        to: u64,
    },
    page: u32,

    pub fn revert(self: NextResult, store: *Store, sequence: u64) void {
        if (store.options.ack_policy == .explicit) {
            const page = store.findPageByNo(self.page).?;
            page.rc -= (self.count + 1);
        }
        if (sequence == 0) store.consumers.head += 1;
        if (self.sequence.to == store.last_sequence) store.consumers.tail -= 1;
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

pub const Options = struct {
    initial_page_size: u32,
    max_page_size: u32 = 0,
    max_pages: u32 = maxInt(u32),

    ack_policy: AckPolicy = .none,
    deliver_policy: DeliverPolicy = .{ .all = {} },
    retention_policy: RetentionPolicy = .{ .all = {} },
};

pub const Store = struct {
    const Self = @This();

    allocator: mem.Allocator,
    options: Options,
    pages: std.ArrayList(Page),
    consumers: struct {
        count: u32 = 0,
        head: u32 = 0,
        tail: u32 = 0,
    } = .{},
    last_sequence: u64 = 0,
    last_page: u32 = 0,
    page_size: u32,

    pub fn init(allocator: mem.Allocator, options: Options) Self {
        return .{
            .allocator = allocator,
            .options = options,
            .page_size = options.initial_page_size,
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
        page: u32,
    };

    fn addPage(self: *Self, min_size: u32) !void {
        assert(min_size > 0);
        const pages = self.pages.items.len;
        if (pages >= self.options.max_pages) {
            log.err("max number of pages per topic {} reached", .{pages});
            return error.TopicOutOfPages;
        }
        try stat.assertAddPage();

        // increase page_size if needed
        if (self.pages.items.len > 2 and self.page_size < self.options.max_page_size) {
            self.page_size = @min(
                self.page_size * 2,
                self.options.max_page_size,
            );
        }

        // append new page
        var offsets = std.ArrayList(u32).init(self.allocator);
        try offsets.ensureTotalCapacity(if (self.lastPage()) |page| page.messagesCount() else 128);
        errdefer offsets.deinit();
        const buf = try self.allocator.alloc(u8, @max(self.page_size, min_size));
        errdefer self.allocator.free(buf);
        try self.pages.append(
            Page{
                .rc = 0,
                .first_sequence = 1 +% self.last_sequence,
                .no = 1 +% self.last_page,
                .buf = buf,
                .offsets = offsets,
            },
        );

        self.last_page +%= 1;
        stat.inc(buf.len);
    }

    pub fn alloc(self: *Self, bytes_count: u32) !AllocResult {
        if (self.pages.items.len == 0 or self.pages.getLast().free() < bytes_count)
            try self.addPage(bytes_count);

        const page = self.lastPage().?;
        const data = try page.alloc(bytes_count);

        self.last_sequence +%= 1;
        self.consumers.tail = 0;
        assert(self.last_sequence == page.last());
        return .{ .data = data, .sequence = self.last_sequence, .page = page.no };
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

    pub fn empty(self: Self) bool {
        return self.pages.items.len == 0;
    }

    fn findPage(self: Self, sequence: u64) ?*Page {
        for (self.pages.items) |*page| {
            if (page.contains(sequence)) return page;
        }
        return null;
    }

    pub fn subscribe(self: *Self) u64 {
        self.consumers.count += 1;
        switch (self.options.deliver_policy) {
            .new => {
                if (self.lastPage()) |p| {
                    p.rc += 1;
                    self.consumers.tail += 1;
                    return self.last_sequence;
                } else {
                    self.consumers.head += 1;
                    return 0;
                }
            },
            .all => {
                self.consumers.head += 1;
                return 0;
                // if (self.firstPage()) |p| {
                //     p.rc += 1;
                //     return p.first() - 1;
                // }
            },
            .from_sequence => |seq| {
                // TODO...
                if (self.findPage(seq)) |p| {
                    p.rc += 1;
                    return seq;
                }
            },
        }
        return self.last_sequence;
    }

    pub fn subscribeAt(self: *Self, sequence: u64) void {
        self.consumers.count += 1;
        self.acquire(sequence);
        if (sequence == 0) self.consumers.head += 1;
        if (sequence == self.last_sequence) self.consumers.tail += 1;
    }

    pub fn unsubscribe(self: *Self, sequence: u64) void {
        self.consumers.count -= 1;
        if (sequence == 0) {
            self.consumers.head -= 1;
            return;
        }
        if (sequence == self.last_sequence) {
            self.consumers.tail -= 1;
        }
        self.fin(sequence);
    }

    fn cleanupPages(self: *Self) void {
        if (self.options.retention_policy != .interest or
            self.consumers.head > 0) return;

        while (self.pages.items.len > 0 and self.pages.items[0].rc == 0) {
            var fp = self.pages.items[0];
            self.allocator.free(fp.buf);
            fp.offsets.deinit();
            stat.dec(fp.buf.len);
            _ = self.pages.orderedRemove(0);
        }
    }

    pub fn next(self: *Self, sequence: u64, ready_count: u32) ?NextResult {
        if (self.pages.items.len == 0 or
            ready_count == 0 or
            sequence == self.last_sequence)
            return null;

        var cleanup = false;
        const page = brk: {
            if (self.firstPage()) |first_page| if (sequence < first_page.first()) {
                if (sequence == 0) self.consumers.head -= 1;
                first_page.rc += 1;
                break :brk first_page;
            };

            if (self.findPage(sequence)) |page| {
                if (page.last() > sequence) break :brk page;

                if (self.findPage(sequence + 1)) |next_page| {
                    page.rc -= 1;
                    next_page.rc += 1;
                    cleanup = page.rc == 0;
                    break :brk next_page;
                }
            }

            return null;
        };

        const res = page.next(sequence, ready_count, self.options.ack_policy);
        if (cleanup) self.cleanupPages();
        if (res.sequence.to == self.last_sequence) self.consumers.tail += 1;
        return res;
    }

    pub fn hasMore(self: *Self, sequence: u64) bool {
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

    pub fn release(self: *Self, no: u32, sequence: u64) void {
        const page = brk: {
            if (self.findPageByNo(no)) |page| break :brk page;
            if (self.findPage(sequence)) |page| break :brk page;

            log.err("release: page not found page: {} sequence: {}", .{ no, sequence });
            return;
        };
        page.rc -= 1;
        if (page.rc == 0) self.cleanupPages();
    }

    pub fn acquire(self: *Self, sequence: u64) void {
        const page = self.findPage(sequence).?;
        page.rc += 1;
    }

    pub fn dump(self: Self, file: std.fs.File) !void {
        var header: [16]u8 = undefined;
        mem.writeInt(u64, header[0..8], self.last_sequence, .little);
        mem.writeInt(u32, header[8..12], self.last_page, .little);
        mem.writeInt(u32, header[12..16], @intCast(self.pages.items.len), .little);
        try file.writeAll(header[0..16]);

        for (self.pages.items) |page| {
            const wp = page.writePos();
            mem.writeInt(u64, header[0..8], page.first_sequence, .little);
            mem.writeInt(u32, header[8..12], page.no, .little);
            mem.writeInt(u32, header[12..16], wp, .little);
            try file.writeAll(header[0..16]);
            try file.writeAll(page.buf[0..wp]);
        }
    }

    pub fn restore(self: *Self, file: std.fs.File) !void {
        const rdr = file.reader();

        var header: [16]u8 = undefined;
        try rdr.readNoEof(header[0..16]);
        self.last_sequence = mem.readInt(u64, header[0..8], .little);
        self.last_page = mem.readInt(u32, header[8..12], .little);
        const pages_count = mem.readInt(u32, header[12..16], .little);

        assert(self.pages.items.len == 0);
        for (0..pages_count) |_| {
            try rdr.readNoEof(header[0..16]);
            const first_sequence = mem.readInt(u64, header[0..8], .little);
            const no = mem.readInt(u32, header[8..12], .little);
            const buf_len = mem.readInt(u32, header[12..16], .little);

            var page = Page{
                .rc = 0,
                .first_sequence = first_sequence,
                .no = no,
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
        }
    }
};

test "topic usage" {
    var store = Store.init(testing.allocator, .{
        .initial_page_size = 8,
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
    try testing.expectEqual(0, sub_1_seq);
    try testing.expectEqual(0, store.pages.items[0].rc);
    try testing.expectEqual(1, store.consumers.head);
    store.unsubscribe(sub_1_seq);
    try testing.expectEqual(0, store.pages.items[0].rc);
    try testing.expectEqual(0, store.consumers.head);
    sub_1_seq = store.subscribe();

    // change policy after first subscriber
    store.options.retention_policy = .interest;
    store.options.deliver_policy = .new;

    // other subscribers get new messages
    const sub_2_seq = store.subscribe();
    try testing.expectEqual(106, sub_2_seq);
    try testing.expect(store.next(sub_2_seq, 1) == null);

    // references hold
    try testing.expectEqual(1, store.consumers.head);
    try testing.expectEqual(0, store.pages.items[0].rc);
    try testing.expectEqual(0, store.pages.items[1].rc);
    try testing.expectEqual(1, store.pages.items[2].rc);

    // subscriber 1
    {
        const res1 = store.next(sub_1_seq, 10).?;
        try testing.expectEqualStrings("01234567", res1.data);
        sub_1_seq = res1.sequence.to;

        // each message holds reference when ack is explicit
        try testing.expectEqual(0, store.consumers.head);
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
    var store = Store.init(testing.allocator, .{ .initial_page_size = 8 });
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
    //try store.append("6");
    var res = try store.alloc(2);
    try testing.expectEqual(1, res.page);
    try testing.expectEqual(103, store.last_sequence);
    try testing.expectEqual(103, store.pages.items[0].last());

    // page 2
    res = try store.alloc(2);
    try testing.expectEqual(2, res.page);
    try testing.expectEqual(2, store.pages.items.len);
    try testing.expectEqual(2, res.data.len);
    try testing.expectEqual(104, res.sequence);
    try testing.expectEqual(104, store.last_sequence);
    try testing.expectEqual(104, store.pages.items[1].first_sequence);
    try testing.expectEqual(8, store.pages.items[1].buf.len);

    // page 3 of size 9
    res = try store.alloc(9);
    try testing.expectEqual(3, res.page);
    try testing.expectEqual(3, store.pages.items.len);
    try testing.expectEqual(9, res.data.len);
    try testing.expectEqual(105, res.sequence);
    try testing.expectEqual(105, store.last_sequence);
    try testing.expectEqual(9, store.pages.items[2].buf.len);
    try testing.expectEqual(105, store.pages.items[2].first_sequence);
}

test "delivery policy" {
    var store = Store.init(testing.allocator, .{ .initial_page_size = 8, .deliver_policy = .all });
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
        try testing.expectEqual(0, sequence);
        try testing.expectEqual(1, store.consumers.count);
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
        try testing.expectEqual(0, store.consumers.count);
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
    var store = Store.init(testing.allocator, .{ .initial_page_size = 8, .deliver_policy = .all, .retention_policy = .all });
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
    try testing.expectEqual(0, sequence_all_1);
    try testing.expectEqual(1, store.consumers.count);

    var sequence_all_2 = store.subscribe();
    try testing.expectEqual(0, sequence_all_2);
    try testing.expectEqual(2, store.consumers.count);

    store.options.retention_policy = .interest;
    store.options.deliver_policy = .new;

    try testing.expectEqual(2, store.consumers.head);
    try testing.expectEqual(0, store.pages.items[0].rc);
    try testing.expectEqual(0, store.pages.items[1].rc);
    try testing.expectEqual(0, store.pages.items[2].rc);

    var res = store.next(sequence_all_1, 10).?;
    try testing.expectEqualStrings("01234567", res.data);
    sequence_all_1 = res.sequence.to;

    try testing.expectEqual(1, store.pages.items[0].rc);
    try testing.expectEqual(0, store.pages.items[1].rc);
    try testing.expectEqual(0, store.pages.items[2].rc);

    res = store.next(sequence_all_1, 10).?;
    try testing.expectEqualStrings("890123", res.data);
    sequence_all_1 = res.sequence.to;

    try testing.expectEqual(0, store.pages.items[0].rc);
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
        .initial_page_size = 8,
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
    try testing.expectEqual(4, store.pages.items[0].rc);

    res = store.next(102, 2).?;
    try testing.expectEqual(103, res.sequence.from);
    try testing.expectEqual(104, res.sequence.to);
    try testing.expectEqual(7, store.pages.items[0].rc);

    res = store.next(102, 2).?;
    try testing.expectEqual(103, res.sequence.from);
    try testing.expectEqual(104, res.sequence.to);
    try testing.expectEqual(10, store.pages.items[0].rc);

    res = store.next(104, 10).?;
    try testing.expectEqual(105, res.sequence.from);
    try testing.expectEqual(106, res.sequence.to);
    try testing.expectEqual(13, store.pages.items[0].rc);

    res = store.next(100, 6).?;
    try testing.expectEqual(101, res.sequence.from);
    try testing.expectEqual(106, res.sequence.to);
    try testing.expectEqual(21, store.pages.items[0].rc);

    try testing.expectEqualStrings("0", store.message(101));
    try testing.expectEqualStrings("1", store.message(102));
    try testing.expectEqualStrings("45", store.message(105));
    try testing.expectEqualStrings("67", store.message(106));
}

test "subscribe/unsubscribe" {
    var store = Store.init(testing.allocator, .{
        .initial_page_size = 8,
        .ack_policy = .explicit,
        .deliver_policy = .new,
        .retention_policy = .all,
    });
    store.last_sequence = 100;
    defer store.deinit();

    store.options.deliver_policy = .all;
    {
        const sequence = store.subscribe();
        try testing.expectEqual(sequence, 0);
        try testing.expectEqual(1, store.consumers.count);
        try testing.expectEqual(1, store.consumers.head);
        try testing.expectEqual(0, store.consumers.tail);
        store.unsubscribe(sequence);
        try testing.expectEqual(0, store.consumers.count);
        try testing.expectEqual(0, store.consumers.head);
        try testing.expectEqual(0, store.consumers.tail);
    }
    store.options.deliver_policy = .new;
    {
        const sequence = store.subscribe();
        try testing.expectEqual(sequence, 0);
        try testing.expectEqual(1, store.consumers.count);
        try testing.expectEqual(1, store.consumers.head);
        try testing.expectEqual(0, store.consumers.tail);
        store.unsubscribe(sequence);
        try testing.expectEqual(0, store.consumers.count);
        try testing.expectEqual(0, store.consumers.head);
        try testing.expectEqual(0, store.consumers.tail);
    }
    try store.append("0");
    store.options.deliver_policy = .all;
    {
        const sequence = store.subscribe();
        try testing.expectEqual(sequence, 0);
        try testing.expectEqual(1, store.consumers.count);
        try testing.expectEqual(1, store.consumers.head);
        try testing.expectEqual(0, store.consumers.tail);
        store.unsubscribe(sequence);
        try testing.expectEqual(0, store.consumers.count);
        try testing.expectEqual(0, store.consumers.head);
        try testing.expectEqual(0, store.consumers.tail);
    }
    store.options.deliver_policy = .new;
    {
        const sequence = store.subscribe();
        try testing.expectEqual(sequence, 101);
        try testing.expectEqual(1, store.consumers.count);
        try testing.expectEqual(0, store.consumers.head);
        try testing.expectEqual(1, store.consumers.tail);
        store.unsubscribe(sequence);
        try testing.expectEqual(0, store.consumers.count);
        try testing.expectEqual(0, store.consumers.head);
        try testing.expectEqual(0, store.consumers.tail);
    }
}
