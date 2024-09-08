const std = @import("std");
const debug = std.debug;
const assert = debug.assert;
const testing = std.testing;

pub fn CompactedTopic(comptime Data: type, comptime Consumer: type) type {
    return struct {
        const Self = @This();

        pub const Node = struct {
            next: ?*Node = null,
            rc: usize = 0, // reference count
            sequence: u64, // stream sequence
            key: u64 = 0, // compaction key
            kind: Kind = .upsert, // node kind; used in compaction
            data: *Data,
            // If the data is owned by this node it will be destroyed on node destroy.
            data_owned: bool = true,

            const Kind = enum {
                upsert,
                delete,
            };

            fn incRc(self: *Node) void {
                self.rc += 1;
            }

            fn decRc(self: *Node, allocator: std.mem.Allocator) void {
                assert(self.rc > 0);
                const nn = self.next;
                self.rc -= 1;
                if (self.rc == 0) {
                    self.deinit(allocator);
                    if (nn) |n| n.decRc(allocator);
                }
            }

            fn deinit(self: *Node, allocator: std.mem.Allocator) void {
                // std.debug.print("deinit node {} {}\n", .{ self.data.*, self.data_owned });
                if (self.data_owned) allocator.destroy(self.data);
                allocator.destroy(self);
            }
        };

        sequence: u64 = 0,
        first: ?*Node = null,
        last: ?*Node = null,

        allocator: std.mem.Allocator,
        consumers: std.AutoHashMap(*Consumer, ConsumerState),

        const ConsumerState = struct {
            current: ?*Node = null,
            next: ?*Node = null,
            sequence: u64 = 0,

            fn deinit(self: *ConsumerState, allocator: std.mem.Allocator) void {
                if (self.current) |n| n.decRc(allocator);
                if (self.next) |n| n.decRc(allocator);
            }
        };

        pub fn subscribe(self: *Self, consumer: *Consumer) !void {
            try self.consumers.put(consumer, .{
                .next = self.first,
                .sequence = 0,
            });
            if (self.first) |n| n.incRc();
        }

        pub fn unsubscribe(self: *Self, consumer: *Consumer) void {
            if (self.consumers.fetchRemove(consumer)) |kv|
                kv.value.deinit(self.allocator);
        }

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .allocator = allocator,
                .consumers = std.AutoHashMap(*Consumer, ConsumerState).init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            { // Remove all consumers, decrements references on next/current nodes
                var iter = self.consumers.iterator();
                while (iter.next()) |e| {
                    e.value_ptr.deinit(self.allocator);
                }
            }
            self.consumers.deinit();
            { // Remove first node reference, will trigger deinit on all childs
                if (self.first) |n| n.decRc(self.allocator);
                self.first = null;
            }
        }

        pub fn next(self: *Self, consumer: *Consumer) ?*Data {
            if (self.consumers.getPtr(consumer)) |state| {
                if (state.current) |node| {
                    state.sequence = node.sequence;
                    node.decRc(self.allocator);
                    state.current = null;
                }
                while (state.next) |node| {
                    state.next = node.next;
                    if (state.next) |n| n.incRc();
                    if (node.sequence <= state.sequence) {
                        node.decRc(self.allocator);
                        continue;
                    }
                    state.current = node;
                    return node.data;
                }
            }
            return null;
        }

        pub fn append(self: *Self, data: *Data, key: u64, kind: Node.Kind) !void {
            const node = try self.push(data, key, kind);
            try self.notify(node);
        }

        fn push(self: *Self, data: *Data, key: u64, kind: Node.Kind) !*Node {
            const node = try self.allocator.create(Node);
            self.sequence += 1;
            node.* = Node{
                .data = data,
                .rc = 1, // previous node or self.first holds reference
                .sequence = self.sequence,
                .key = key,
                .kind = kind,
            };

            if (self.last == null) {
                assert(self.first == null);
                self.first = node;
                self.last = node;
            } else {
                assert(self.first != null);
                self.last.?.next = node;
                self.last = node;
            }
            return node;
        }

        fn notify(self: *Self, node: *Node) !void {
            var iter = self.consumers.iterator();
            while (iter.next()) |e| {
                const consumer = e.key_ptr.*;
                const state = e.value_ptr;
                if (state.next == null) {
                    node.incRc();
                    state.next = node;
                    try consumer.wakeup();
                }
            }
        }

        fn compact(self: *Self) !void {
            if (self.first == null) return;
            assert(self.last != null);

            // key -> last sequence for that key
            var keys = std.AutoHashMap(u64, u64).init(self.allocator);
            defer keys.deinit();
            { // fill keys map
                var current = self.first;
                while (current) |node| : (current = node.next) {
                    if (node.kind == .delete) {
                        _ = keys.remove(node.key);
                    } else {
                        try keys.put(node.key, node.sequence);
                    }
                }
            }
            var first: ?*Node = null;
            {
                var current = self.first;
                while (current) |node| : (current = node.next) {
                    if (first) |f| if (f == current) break;
                    if (keys.get(node.key)) |sequence| {
                        if (node.sequence == sequence) {
                            node.data_owned = false;
                            const new_node = try self.push(node.data, node.key, node.kind);
                            new_node.sequence = sequence;
                            if (first == null) first = new_node;
                        }
                    }
                }
            }

            if (first) |n| n.incRc(); // referenced by first and by previous node
            if (first == null) self.last = null; // if all deleted
            self.first.?.decRc(self.allocator);
            self.first = first;
        }
    };
}

test "wakeup, state management" {
    const Consumer = struct {
        wakeup_count: usize = 0,
        const Self = @This();
        fn wakeup(self: *Self) !void {
            self.wakeup_count += 1;
        }
    };
    var topic = CompactedTopic(usize, Consumer).init(testing.allocator);
    defer topic.deinit();
    { // first consumer, check wakeup call
        var c1 = Consumer{};
        try topic.subscribe(&c1);
        try testing.expectEqual(1, topic.consumers.count());

        var v = try testing.allocator.create(usize);
        v.* = 42;
        try testing.expectEqual(0, c1.wakeup_count);
        try topic.append(v, 1, .upsert);
        try testing.expectEqual(1, c1.wakeup_count);

        v = try testing.allocator.create(usize);
        v.* = 43;
        try topic.append(v, 2, .upsert);
        try testing.expectEqual(1, c1.wakeup_count);

        try testing.expectEqual(42, topic.next(&c1).?.*);
        try testing.expectEqual(43, topic.next(&c1).?.*);
        try testing.expect(topic.next(&c1) == null);
    }
    { // new consumer, check state
        var c2 = Consumer{};
        try topic.subscribe(&c2);
        try testing.expectEqual(2, topic.consumers.count());

        const state = topic.consumers.getPtr(&c2).?;
        try testing.expect(state.current == null);
        try testing.expectEqual(42, state.next.?.data.*);

        try testing.expectEqual(42, topic.next(&c2).?.*);
        try testing.expectEqual(42, state.current.?.data.*);
        try testing.expectEqual(43, state.next.?.data.*);

        try testing.expectEqual(43, topic.next(&c2).?.*);
        try testing.expectEqual(43, state.current.?.data.*);
        try testing.expect(state.next == null);

        try testing.expect(topic.next(&c2) == null);
        try testing.expect(state.current == null);
        try testing.expect(state.next == null);
    }
}

test "compact" {
    const Consumer = struct {
        wakeup_count: usize = 0,
        const Self = @This();
        fn wakeup(self: *Self) !void {
            self.wakeup_count += 1;
        }
    };

    var topic = CompactedTopic(usize, Consumer).init(testing.allocator);
    defer topic.deinit();

    { // add nodes
        var v = try testing.allocator.create(usize);
        v.* = 42;
        try topic.append(v, 1, .upsert);

        v = try testing.allocator.create(usize);
        v.* = 43;
        try topic.append(v, 2, .upsert);

        v = try testing.allocator.create(usize);
        v.* = 44;
        try topic.append(v, 3, .upsert);

        v = try testing.allocator.create(usize);
        v.* = 45;
        try topic.append(v, 1, .upsert);

        v = try testing.allocator.create(usize);
        v.* = 46;
        try topic.append(v, 2, .delete);

        v = try testing.allocator.create(usize);
        v.* = 47;
        try topic.append(v, 2, .upsert);

        v = try testing.allocator.create(usize);
        v.* = 48;
        try topic.append(v, 3, .delete);
    }

    var c1 = Consumer{};
    try topic.subscribe(&c1);
    try testing.expectEqual(42, topic.next(&c1).?.*);

    try topic.compact();

    { // new consumer gets compacted nodes
        var c2 = Consumer{};
        try topic.subscribe(&c2);
        try testing.expectEqual(45, topic.next(&c2).?.*);
        try testing.expectEqual(47, topic.next(&c2).?.*);
        try testing.expect(topic.next(&c2) == null);
    }

    // old consumers gets all updates, but not compacted
    try testing.expectEqual(43, topic.next(&c1).?.*);
    try testing.expectEqual(44, topic.next(&c1).?.*);
    try testing.expectEqual(45, topic.next(&c1).?.*);
    try testing.expectEqual(46, topic.next(&c1).?.*);
    try testing.expectEqual(47, topic.next(&c1).?.*);
    try testing.expectEqual(48, topic.next(&c1).?.*);
    try testing.expect(topic.next(&c1) == null);
}
