const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;
const testing = std.testing;

// TODO: rename Segment to Page
// onda mozes koristiti skracenicu seg

const Segment = struct {
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
            self.rc += no_msgs;
            sequence.* = self.first() + end_idx;
            return self.buf[0..self.offsets.items[end_idx]];
        }

        const start_idx: u32 = @intCast(sequence.* - self.sequence);
        const end_idx: u32 = @min(msgs_count - 1, start_idx + ready_count);
        const no_msgs: u32 = end_idx - start_idx;
        self.rc += no_msgs;
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

const Store = struct {
    const Self = @This();

    allocator: mem.Allocator,
    segment_size: u32,
    segments: std.ArrayList(Segment),

    fn init(allocator: mem.Allocator, segment_size: u32) Self {
        return .{
            .allocator = allocator,
            .segment_size = segment_size,
            .segments = std.ArrayList(Segment).init(allocator),
        };
    }

    fn deinit(self: *Self) void {
        for (self.segments.items) |*seg| {
            self.allocator.free(seg.buf);
            seg.offsets.deinit();
        }
        self.segments.deinit();
    }

    fn append(self: *Self, bytes: []const u8) !void {
        if (self.segments.items.len == 0 or
            self.segments.getLast().free() < bytes.len)
        { // add new segment
            try self.segments.append(
                Segment{
                    .sequence = 1 + if (self.segments.items.len == 0) 0 else self.segments.getLast().last(),
                    .buf = try self.allocator.alloc(u8, @max(self.segment_size, bytes.len)),
                    .offsets = std.ArrayList(u32).init(self.allocator),
                },
            );
        }
        // append to last segment
        return self.lastSegment().append(bytes);
    }

    fn lastSegment(self: *Self) *Segment {
        return &self.segments.items[self.segments.items.len - 1];
    }

    fn nextChunk(self: *Self, sequence: *u64) ?[]const u8 {
        _ = self;
        _ = sequence;
        return &.{};
    }

    // Start receiving from the earliest available message in the stream.
    const sequence_deliver_all = 0;
    // Start receiving messages created after the consumer was created.
    const sequence_deliver_new = std.math.maxInt(u64);

    fn next(self: *Self, sequence: *u64, ready_count: u32) ?[]const u8 {
        if (self.segments.items.len == 0 or ready_count == 0) return null;

        // Deliver all:
        if (sequence.* == sequence_deliver_all) {
            var seg = &self.segments.items[0];
            seg.rc += 1;
            sequence.* = seg.first() - 1;
            return seg.next(sequence, ready_count);
        }

        const last_segment = self.lastSegment();
        const last_sequence = last_segment.last();

        // Deliver new: start receiving messages created after the consumer was created.
        if (sequence.* == sequence_deliver_new) {
            sequence.* = last_sequence;
            last_segment.rc += 1;
            return null;
        }

        assert(sequence.* <= last_sequence);
        if (sequence.* == last_sequence) return null;

        // Find segment which contains previous sequence
        var i: usize = self.segments.items.len;
        while (i > 0) {
            i -= 1;
            var seg = &self.segments.items[i];
            if (seg.contains(sequence.*)) {
                if (seg.last() == sequence.*) {
                    seg.rc -= 1;
                    seg = &self.segments.items[i + 1];
                    seg.rc += 1;
                    return seg.next(sequence, ready_count);
                }
                return seg.next(sequence, ready_count);
            }
        }

        // Sequence not in any segment
        unreachable;
    }

    fn fin(self: *Self, sequence: u64) void {
        var i: usize = self.segments.items.len;
        while (i > 0) {
            i -= 1;
            var seg = &self.segments.items[i];
            if (seg.contains(sequence)) {
                seg.rc -= 1;
                return;
            }
        }
    }
};

test "write" {
    var store = Store.init(testing.allocator, 8);
    defer store.deinit();
    {
        try testing.expectEqual(0, store.segments.items.len);
        try store.append("0123");
        try testing.expectEqual(1, store.segments.items.len);
        try testing.expectEqual(1, store.lastSegment().first());
        try testing.expectEqual(1, store.lastSegment().last());
        try store.append("45");
        try testing.expectEqual(1, store.lastSegment().first());
        try testing.expectEqual(2, store.lastSegment().last());
        try store.append("67");
        try testing.expectEqual(3, store.lastSegment().last());
        try store.append("89");
        try testing.expectEqual(2, store.segments.items.len);
        try testing.expectEqual(4, store.lastSegment().first());
        try testing.expectEqual(4, store.lastSegment().last());
    }
    {
        var sequence: u64 = 0;
        try testing.expect((store.next(&sequence, 0)) == null);
        try testing.expectEqual(0, sequence);
        try testing.expectEqual(0, store.segments.items[0].rc);

        try testing.expectEqualStrings("0123", store.next(&sequence, 1).?);
        try testing.expectEqual(1, sequence);
        try testing.expectEqual(2, store.segments.items[0].rc);
        try testing.expectEqualStrings("4567", store.next(&sequence, 3).?);
        try testing.expectEqual(3, sequence);
        try testing.expectEqual(4, store.segments.items[0].rc);
        store.fin(1);
        store.fin(2);
        store.fin(3);
        try testing.expectEqual(1, store.segments.items[0].rc);

        try testing.expectEqualStrings("89", store.next(&sequence, 123).?);
        try testing.expectEqual(4, sequence);
        try testing.expectEqual(0, store.segments.items[0].rc);
        try testing.expectEqual(2, store.segments.items[1].rc);
    }
}
