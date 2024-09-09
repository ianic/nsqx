const std = @import("std");
const debug = std.debug;
const assert = debug.assert;
const testing = std.testing;

const NodeKind = enum {
    upsert,
    delete,
};

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

            const Kind = NodeKind;

            fn incRc(self: *Node) void {
                self.rc += 1;
            }

            fn release(self: *Node, allocator: std.mem.Allocator) void {
                assert(self.rc > 0);
                const nn = self.next;
                self.rc -= 1;
                if (self.rc == 0) {
                    self.deinit(allocator);
                    if (nn) |n| n.release(allocator);
                }
            }

            fn deinit(self: *Node, allocator: std.mem.Allocator) void {
                if (self.data_owned) allocator.destroy(self.data);
                allocator.destroy(self);
            }

            fn acquire(node: ?*Node) ?*Node {
                if (node) |n| n.incRc();
                return node;
            }
        };

        first: ?*Node = null,
        last: ?*Node = null,
        keys: std.AutoHashMap(u64, *Node),
        nodes_count: u32 = 0,

        allocator: std.mem.Allocator,
        consumers: std.AutoHashMap(*Consumer, ConsumerState),

        const ConsumerState = struct {
            current: ?*Node = null,
            next: ?*Node = null,

            fn deinit(self: *ConsumerState, allocator: std.mem.Allocator) void {
                if (self.current) |n| n.release(allocator);
                if (self.next) |n| n.release(allocator);
            }
        };

        pub fn subscribe(self: *Self, consumer: *Consumer) !void {
            try self.consumers.put(consumer, .{ .next = Node.acquire(self.first) });
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
                if (self.last) |n| n.release(self.allocator);
                if (self.first) |n| n.release(self.allocator);
                self.first = null;
            }
            self.keys.deinit();
        }

        pub fn next(self: *Self, consumer: *Consumer) ?*Data {
            if (self.consumers.getPtr(consumer)) |state| {
                if (state.current) |node| {
                    node.release(self.allocator);
                    if (self.last) |last| if (node == last and node.rc == 1) {
                        self.last = null;
                        node.release(self.allocator);
                    };
                    state.current = null;
                }
                while (state.next) |node| {
                    state.next = Node.acquire(node.next);
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

        fn keysInit(self: *Self) !void {
            self.keys.deinit();
            self.keys = std.AutoHashMap(u64, *Node).init(self.allocator);
            self.nodes_count = 0;
        }

        fn create(self: *Self, data: *Data, key: u64, kind: Node.Kind) !*Node {
            const node = try self.allocator.create(Node);
            node.* = Node{
                .data = data,
                .rc = 2, // self.last and previous node (or self.first) has references
                .key = key,
                .kind = kind,
            };

            if (self.last) |last| {
                last.release(self.allocator);
                last.next = node;
            }
            self.last = node;
            if (self.first == null) self.first = node;
            return node;
        }

        fn notify(self: *Self, node: *Node) !void {
            var iter = self.consumers.iterator();
            while (iter.next()) |e| {
                const consumer = e.key_ptr.*;
                const state = e.value_ptr;
                if (state.next == null) {
                    state.next = Node.acquire(node);
                    try wakeupCallback(consumer);
                }
            }
        }

        // Percent of the nodes which will persist after compact. Client can use
        // node_count and load factor to decide is it a good time for
        // compaction.
        pub fn loadFactor(self: *Self) u32 {
            if (self.nodes_count == 0) return 100;
            var key_count = self.keys.count();
            if (key_count == 0) return 0;
            if (self.last) |last| if (last.kind == .delete) {
                key_count += 2;
            };
            return (key_count * 100) / self.nodes_count;
        }

        pub fn compact(self: *Self) !void {
            if (self.first == null) return;
            assert(self.last != null);

            // All keys are different, compaction will do nothing
            if (self.keys.count() >= self.nodes_count) return;

            const first = self.first.?;
            const last = self.last.?;

            // Everything deleted
            if (self.keys.count() == 0) {
                first.release(self.allocator);
                self.first = null;
                if (last.rc == 1) {
                    last.release(self.allocator);
                    self.last = null;
                }
                try self.keysInit();
                return;
            }

            { // Create new list
                self.first = null;
                self.last = null;
                var last_upsert: ?*Node = null; // last upsert for the last.key
                var node = first;
                while (node != last) : (node = node.next.?) {
                    if (self.keys.get(node.key)) |last_node| {
                        if (node == last_node) {
                            node.data_owned = false;
                            _ = try self.create(node.data, node.key, node.kind);
                        }
                    }
                    if (last.key == node.key and node.kind == .upsert)
                        last_upsert = node;
                }
                // If the last is delete we need to preserve previous upsert also.
                // Last must be always preserve so we can connect two list at some node.
                if (last.kind == .delete) {
                    if (last_upsert) |n| {
                        n.data_owned = false;
                        _ = try self.create(n.data, n.key, n.kind);
                    }
                }
            }
            { // Update keys for the new list
                try self.keysInit();
                var node = self.first;
                while (node) |n| : (node = n.next) {
                    try self.keysPut(n);
                }
                try self.keysPut(last);
            }

            // Connect two lists into last node
            if (self.last) |new_last| {
                new_last.next = Node.acquire(last);
                new_last.release(self.allocator);
            }
            if (self.first == null) self.first = Node.acquire(last);
            self.last = last;
            first.release(self.allocator);
        }
    };
}

test "wakeup, state management" {
    var topic = CompactedTopic(usize, TestConsumer, TestConsumer.wakeup).init(testing.allocator);
    defer topic.deinit();
    { // first consumer, check wakeup call
        var c1 = TestConsumer{};
        try topic.subscribe(&c1);
        try testing.expectEqual(1, topic.consumers.count());

        try testing.expectEqual(0, c1.wakeup_count);
        try addNode(&topic, 42, 1, .upsert);
        try testing.expectEqual(1, c1.wakeup_count);

        try addNode(&topic, 43, 2, .upsert);
        try testing.expectEqual(1, c1.wakeup_count);

        try testing.expectEqual(42, topic.next(&c1).?.*);
        try testing.expectEqual(43, topic.next(&c1).?.*);
        try testing.expect(topic.next(&c1) == null);
    }
    { // new consumer, check state
        var c2 = TestConsumer{};
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
    var topic = CompactedTopic(usize, TestConsumer, TestConsumer.wakeup).init(testing.allocator);
    defer topic.deinit();

    try addNode(&topic, 42, 1, .upsert);
    try addNode(&topic, 43, 2, .upsert);
    try addNode(&topic, 44, 3, .upsert);
    try addNode(&topic, 45, 1, .upsert);
    try addNode(&topic, 46, 2, .delete);
    try addNode(&topic, 47, 2, .upsert);
    try addNode(&topic, 48, 3, .delete);
    try addNode(&topic, 49, 4, .upsert);

    var c1 = TestConsumer{};
    try topic.subscribe(&c1);
    try testing.expectEqual(42, topic.next(&c1).?.*);

    try testing.expectEqual(8, topic.nodes_count);
    try testing.expectEqual(37, topic.loadFactor());
    try topic.compact();
    try testing.expectEqual(3, topic.nodes_count);

    { // new consumer gets compacted nodes
        var c2 = TestConsumer{};
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

test "compact removes all" {
    var topic = CompactedTopic(usize, TestConsumer, TestConsumer.wakeup).init(testing.allocator);
    defer topic.deinit();

    try addNode(&topic, 42, 1, .upsert);
    try addNode(&topic, 43, 2, .upsert);
    try addNode(&topic, 44, 3, .upsert);
    try addNode(&topic, 45, 1, .delete);
    try addNode(&topic, 46, 2, .delete);
    try addNode(&topic, 47, 3, .delete);

    var c1 = TestConsumer{};
    try topic.subscribe(&c1);

    try testing.expectEqual(6, topic.nodes_count);
    try testing.expectEqual(0, topic.loadFactor());
    try topic.compact();
    try testing.expectEqual(0, topic.nodes_count);
    try testing.expectEqual(100, topic.loadFactor());

    var c2 = TestConsumer{};
    try topic.subscribe(&c2);
    try testing.expect(topic.next(&c2) == null);

    try testing.expect(topic.last != null);
    try testing.expectEqual(42, topic.next(&c1).?.*);
}

fn addNode(topic: anytype, value: usize, key: u64, kind: NodeKind) !void {
    const v = try testing.allocator.create(usize);
    v.* = value;
    try topic.append(v, key, kind);
}

const TestConsumer = struct {
    wakeup_count: usize = 0,
    fn wakeup(self: *@This()) !void {
        self.wakeup_count += 1;
    }
};

test "compact removes all but one" {
    var topic = CompactedTopic(usize, TestConsumer, TestConsumer.wakeup).init(testing.allocator);
    defer topic.deinit();

    try addNode(&topic, 42, 1, .upsert);
    try addNode(&topic, 43, 1, .upsert);
    try addNode(&topic, 44, 1, .upsert);
    try addNode(&topic, 45, 1, .delete);
    try addNode(&topic, 46, 1, .upsert);

    var c1 = TestConsumer{};
    try topic.subscribe(&c1);

    try testing.expectEqual(5, topic.nodes_count);
    try testing.expectEqual(20, topic.loadFactor());
    try topic.compact();
    try testing.expectEqual(1, topic.nodes_count);
    try testing.expectEqual(100, topic.loadFactor());

    var c2 = TestConsumer{};
    try topic.subscribe(&c2);
    try testing.expectEqual(46, topic.next(&c2).?.*);
    try testing.expect(topic.next(&c2) == null);

    try testing.expect(topic.last != null);
    try testing.expectEqual(42, topic.next(&c1).?.*);
    try testing.expectEqual(43, topic.next(&c1).?.*);
    try testing.expectEqual(44, topic.next(&c1).?.*);
    try testing.expectEqual(45, topic.next(&c1).?.*);
    try testing.expectEqual(46, topic.next(&c1).?.*);
    try testing.expect(topic.next(&c1) == null);
}

test "compact removes all but one, last is delete" {
    var topic = CompactedTopic(usize, TestConsumer, TestConsumer.wakeup).init(testing.allocator);
    defer topic.deinit();

    try addNode(&topic, 42, 1, .upsert);
    try addNode(&topic, 43, 1, .delete);
    try addNode(&topic, 44, 1, .upsert);
    try addNode(&topic, 45, 2, .upsert);
    try addNode(&topic, 46, 2, .delete);

    var c1 = TestConsumer{};
    try topic.subscribe(&c1);

    try testing.expectEqual(5, topic.nodes_count);
    try testing.expectEqual(60, topic.loadFactor());
    try topic.compact();
    try testing.expectEqual(3, topic.nodes_count);
    try testing.expectEqual(100, topic.loadFactor());

    var c2 = TestConsumer{};
    try topic.subscribe(&c2);
    try testing.expectEqual(44, topic.next(&c2).?.*);
    try testing.expectEqual(45, topic.next(&c2).?.*);
    try testing.expectEqual(46, topic.next(&c2).?.*);
    try testing.expect(topic.next(&c2) == null);

    try testing.expect(topic.last != null);
    try testing.expectEqual(42, topic.next(&c1).?.*);
    try testing.expectEqual(43, topic.next(&c1).?.*);
    try testing.expectEqual(44, topic.next(&c1).?.*);
    try testing.expectEqual(45, topic.next(&c1).?.*);
    try testing.expectEqual(46, topic.next(&c1).?.*);
    try testing.expect(topic.next(&c1) == null);
}
