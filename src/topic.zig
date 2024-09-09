const std = @import("std");
const debug = std.debug;
const assert = debug.assert;
const testing = std.testing;

pub fn CompactedTopic(
    comptime Data: type,
    comptime Consumer: type,
    comptime wakeupCallback: fn (*Consumer) anyerror!void,
) type {
    return struct {
        const Self = @This();

        pub const Node = struct {
            next: ?*Node = null,
            rc: usize = 0, // reference count
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
                if (self.data_owned) allocator.destroy(self.data);
                allocator.destroy(self);
            }
        };

        first: ?*Node = null,
        last: ?*Node = null,
        keys: std.AutoHashMap(u64, *Node),
        nodes_count: u64 = 0,

        allocator: std.mem.Allocator,
        consumers: std.AutoHashMap(*Consumer, ConsumerState),

        const ConsumerState = struct {
            current: ?*Node = null,
            next: ?*Node = null,

            fn deinit(self: *ConsumerState, allocator: std.mem.Allocator) void {
                if (self.current) |n| n.decRc(allocator);
                if (self.next) |n| n.decRc(allocator);
            }
        };

        pub fn subscribe(self: *Self, consumer: *Consumer) !void {
            try self.consumers.put(consumer, .{ .next = self.first });
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
                .keys = std.AutoHashMap(u64, *Node).init(allocator),
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
            self.keys.deinit();
        }

        pub fn next(self: *Self, consumer: *Consumer) ?*Data {
            if (self.consumers.getPtr(consumer)) |state| {
                if (state.current) |node| {
                    node.decRc(self.allocator);
                    state.current = null;
                }
                while (state.next) |node| {
                    state.next = node.next;
                    if (state.next) |n| n.incRc();
                    state.current = node;
                    return node.data;
                }
            }
            return null;
        }

        pub fn append(self: *Self, data: *Data, key: u64, kind: Node.Kind) !void {
            const node = try self.create(data, key, kind);
            try self.keysPut(node);
            try self.notify(node);
        }

        fn keysPut(self: *Self, node: *Node) !void {
            if (node.kind == .delete)
                _ = self.keys.remove(node.key)
            else
                try self.keys.put(node.key, node);
            self.nodes_count += 1;
        }

        fn create(self: *Self, data: *Data, key: u64, kind: Node.Kind) !*Node {
            const node = try self.allocator.create(Node);
            node.* = Node{
                .data = data,
                .rc = 1, // previous node or self.first holds reference
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
                    try wakeupCallback(consumer);
                }
            }
        }

        fn count(self: *Self) !usize {
            var i: usize = 0;
            var node = self.first;
            while (node) |n| : (node = n.next) i += 1;
            return i;
        }

        fn compact(self: *Self) !void {
            if (self.first == null) return;
            assert(self.last != null);

            // All keys are different, compaction will do nothing
            if (self.keys.count() >= self.nodes_count) return;

            const first = self.first.?;
            const last = self.last.?;

            // Everything deleted. Only the last node is left after compaction.
            if (self.keys.count() == 0) {
                last.incRc();
                self.last = last;
                first.decRc(self.allocator);
                self.first = last;

                self.keys.deinit();
                self.keys = std.AutoHashMap(u64, *Node).init(self.allocator);
                self.nodes_count = 0;
                try self.keysPut(last);
                return;
            }

            // start new list
            self.first = null;
            self.last = null;

            self.nodes_count = 0;
            {
                var node = first;
                while (node != last) : (node = node.next.?) {
                    if (self.keys.get(node.key)) |last_node| {
                        if (node == last_node) {
                            node.data_owned = false;
                            _ = try self.create(node.data, node.key, node.kind);
                        }
                    }
                }
            }

            self.keys.deinit();
            self.keys = std.AutoHashMap(u64, *Node).init(self.allocator);
            {
                var node = self.first;
                while (node) |n| : (node = n.next) {
                    try self.keysPut(n);
                }
            }

            // connect two lists into last node
            self.last.?.next = last;
            last.incRc();
            self.last = last;
            first.decRc(self.allocator);
            try self.keysPut(last);
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
    var topic = CompactedTopic(usize, Consumer, Consumer.wakeup).init(testing.allocator);
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
        dummy_count: usize = 0,
        fn dummy(self: *@This()) !void {
            self.dummy_count += 1;
        }
    };

    var topic = CompactedTopic(usize, Consumer, Consumer.dummy).init(testing.allocator);
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

        v = try testing.allocator.create(usize);
        v.* = 49;
        try topic.append(v, 4, .upsert);
    }

    var c1 = Consumer{};
    try topic.subscribe(&c1);
    try testing.expectEqual(42, topic.next(&c1).?.*);

    try testing.expectEqual(8, topic.nodes_count);
    try topic.compact();
    try testing.expectEqual(3, topic.nodes_count);

    { // new consumer gets compacted nodes
        var c2 = Consumer{};
        try topic.subscribe(&c2);
        try testing.expectEqual(45, topic.next(&c2).?.*);
        try testing.expectEqual(47, topic.next(&c2).?.*);
        try testing.expectEqual(49, topic.next(&c2).?.*);
        try testing.expect(topic.next(&c2) == null);
    }

    // old consumers gets all updates, but not compacted
    try testing.expectEqual(43, topic.next(&c1).?.*);
    try testing.expectEqual(44, topic.next(&c1).?.*);
    try testing.expectEqual(45, topic.next(&c1).?.*);
    try testing.expectEqual(46, topic.next(&c1).?.*);
    try testing.expectEqual(47, topic.next(&c1).?.*);
    try testing.expectEqual(48, topic.next(&c1).?.*);
    try testing.expectEqual(49, topic.next(&c1).?.*);
    try testing.expect(topic.next(&c1) == null);
}

test "compact deletes all" {
    const Consumer = struct {
        dummy_count: usize = 0,
        fn dummy(self: *@This()) !void {
            self.dummy_count += 1;
        }
    };

    var topic = CompactedTopic(usize, Consumer, Consumer.dummy).init(testing.allocator);
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
        try topic.append(v, 1, .delete);

        v = try testing.allocator.create(usize);
        v.* = 46;
        try topic.append(v, 2, .delete);

        v = try testing.allocator.create(usize);
        v.* = 47;
        try topic.append(v, 3, .delete);
    }

    try testing.expectEqual(6, topic.nodes_count);
    try topic.compact();
    try testing.expectEqual(1, topic.nodes_count);

    var c1 = Consumer{};
    try topic.subscribe(&c1);
    try testing.expectEqual(47, topic.next(&c1).?.*);
    try testing.expect(topic.next(&c1) == null);
}
