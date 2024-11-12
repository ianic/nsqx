const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;
const testing = std.testing;

const Page = struct {
    const Self = @This();

    buf: []u8,
    sequence: u64 = 0,
    rc: u32 = 0, // reference counter
    offsets: std.ArrayList(u32),

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

    fn next(self: *Self, sequence: *u64, ready_count: u32) ?[]const u8 {
        assert(ready_count > 0);
        assert(sequence.* < self.last());
        const msgs_count = self.offsets.items.len;

        if (sequence.* < self.first()) {
            const no_msgs = @min(msgs_count, ready_count);
            const end_idx = no_msgs - 1;
            if (ready_count != chunk_ready_count) self.rc += no_msgs;
            sequence.* = self.first() + end_idx;
            return self.buf[0..self.offsets.items[end_idx]];
        }

        const start_idx: u32 = @intCast(sequence.* - self.sequence);
        const end_idx: u32 = @min(msgs_count - 1, start_idx + ready_count);
        const no_msgs: u32 = end_idx - start_idx;
        if (ready_count != chunk_ready_count) self.rc += no_msgs;
        sequence.* += no_msgs;

        return self.buf[self.offsets.items[start_idx]..self.offsets.items[end_idx]];
    }

    fn first(self: *Self) u64 {
        return self.sequence;
    }

    fn last(self: Self) u64 {
        return self.sequence + self.offsets.items.len - 1;
    }

    fn contains(self: *Self, sequence: u64) bool {
        return self.first() <= sequence and self.last() >= sequence;
    }
};

// Start receiving from the earliest available message in the stream.
pub const sequence_deliver_all = 0;
// Start receiving messages created after the consumer was created.
pub const sequence_deliver_new = std.math.maxInt(u64);
// Don't hold reference for each message
const chunk_ready_count = std.math.maxInt(u32);

pub const Store = struct {
    const Self = @This();

    allocator: mem.Allocator,
    page_size: u32,
    pages: std.ArrayList(Page),

    pub fn init(allocator: mem.Allocator, page_size: u32) Self {
        return .{
            .allocator = allocator,
            .page_size = page_size,
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

    pub fn append(self: *Self, bytes: []const u8) !void {
        if (self.pages.items.len == 0 or
            self.pages.getLast().free() < bytes.len)
        { // add new page
            try self.pages.append(
                Page{
                    .sequence = 1 + if (self.pages.items.len == 0) 0 else self.pages.getLast().last(),
                    .buf = try self.allocator.alloc(u8, @max(self.page_size, bytes.len)),
                    .offsets = std.ArrayList(u32).init(self.allocator),
                },
            );
        }
        // Append to last page
        return self.lastPage().append(bytes);
    }

    fn lastPage(self: *Self) *Page {
        return &self.pages.items[self.pages.items.len - 1];
    }

    fn nextChunk(self: *Self, sequence: *u64) ?[]const u8 {
        return self.next(sequence, chunk_ready_count);
    }

    pub fn next(self: *Self, sequence: *u64, ready_count: u32) ?[]const u8 {
        if (self.pages.items.len == 0 or ready_count == 0) return null;

        // Deliver all:
        if (sequence.* == sequence_deliver_all) {
            var seg = &self.pages.items[0];
            seg.rc += 1;
            sequence.* = seg.first() - 1;
            return seg.next(sequence, ready_count);
        }

        const last_page = self.lastPage();
        const last_sequence = last_page.last();

        // Deliver new: start receiving messages created after the consumer was created.
        if (sequence.* == sequence_deliver_new) {
            sequence.* = last_sequence;
            last_page.rc += 1;
            return null;
        }

        assert(sequence.* <= last_sequence);
        if (sequence.* == last_sequence) return null;

        // Find page which contains previous sequence
        var i: usize = self.pages.items.len;
        while (i > 0) {
            i -= 1;
            var seg = &self.pages.items[i];
            if (seg.contains(sequence.*)) {
                if (seg.last() == sequence.*) {
                    seg.rc -= 1;
                    seg = &self.pages.items[i + 1];
                    seg.rc += 1;
                    return seg.next(sequence, ready_count);
                }
                return seg.next(sequence, ready_count);
            }
        }

        // Sequence not in any page
        unreachable;
    }

    pub fn fin(self: *Self, sequence: u64) void {
        var i: usize = self.pages.items.len;
        while (i > 0) {
            i -= 1;
            var seg = &self.pages.items[i];
            if (seg.contains(sequence)) {
                seg.rc -= 1;
                return;
            }
        }
    }
};

test "append/next/nextChunk" {
    var store = Store.init(testing.allocator, 8);
    defer store.deinit();
    {
        try testing.expectEqual(0, store.pages.items.len);
        try store.append("0123");
        try testing.expectEqual(1, store.pages.items.len);
        try testing.expectEqual(1, store.lastPage().first());
        try testing.expectEqual(1, store.lastPage().last());
        try store.append("45");
        try testing.expectEqual(1, store.lastPage().first());
        try testing.expectEqual(2, store.lastPage().last());
        try store.append("67");
        try testing.expectEqual(3, store.lastPage().last());
        try store.append("89");
        try testing.expectEqual(2, store.pages.items.len);
        try testing.expectEqual(4, store.lastPage().first());
        try testing.expectEqual(4, store.lastPage().last());
    }
    {
        var sequence: u64 = 0;
        try testing.expect((store.next(&sequence, 0)) == null);
        try testing.expectEqual(0, sequence);
        try testing.expectEqual(0, store.pages.items[0].rc);

        try testing.expectEqualStrings("0123", store.next(&sequence, 1).?);
        try testing.expectEqual(1, sequence);
        try testing.expectEqual(2, store.pages.items[0].rc);
        try testing.expectEqualStrings("4567", store.next(&sequence, 3).?);
        try testing.expectEqual(3, sequence);
        try testing.expectEqual(4, store.pages.items[0].rc);
        store.fin(1);
        store.fin(2);
        store.fin(3);
        try testing.expectEqual(1, store.pages.items[0].rc);

        try testing.expectEqualStrings("89", store.next(&sequence, 123).?);
        try testing.expectEqual(4, sequence);
        try testing.expectEqual(0, store.pages.items[0].rc);
        try testing.expectEqual(2, store.pages.items[1].rc);
    }
    { // chunk read
        var sequence: u64 = 0;
        try testing.expectEqualStrings("01234567", store.nextChunk(&sequence).?);
        try testing.expectEqual(1, store.pages.items[0].rc);
        try testing.expectEqual(2, store.pages.items[1].rc);
        try testing.expectEqual(3, sequence);
        try testing.expectEqualStrings("89", store.nextChunk(&sequence).?);
        try testing.expectEqual(0, store.pages.items[0].rc);
        try testing.expectEqual(3, store.pages.items[1].rc);
    }
}
