const std = @import("std");
const debug = std.debug;
const assert = debug.assert;
const testing = std.testing;

pub fn SimpleTopic(
    comptime Data: type,
    comptime Consumer: type,
    comptime wakeupCallback: fn (*Consumer) anyerror!void,
) type {
    return struct {
        const Self = @This();

        pub const Node = struct {
            next: ?*Node = null,
            rc: usize = 0, // reference count
            data: *Data,

            fn unbox(self: *Node, allocator: std.mem.Allocator) void {
                assert(self.rc > 0);
                const nn = self.next;
                self.rc -= 1;
                if (self.rc == 0) {
                    allocator.destroy(self.data);
                    allocator.destroy(self);
                    if (nn) |n| n.unbox(allocator);
                }
            }

            fn box(node: ?*Node) ?*Node {
                if (node) |n| n.rc += 1;
                return node;
            }
        };

        last: ?*Node = null,
        allocator: std.mem.Allocator,
        consumers: std.AutoHashMap(*Consumer, ConsumerState),

        const ConsumerState = struct {
            current: ?*Node = null,
            next: ?*Node = null,

            fn deinit(self: *ConsumerState, allocator: std.mem.Allocator) void {
                if (self.current) |n| n.unbox(allocator);
                if (self.next) |n| n.unbox(allocator);
            }
        };

        pub fn subscribe(self: *Self, consumer: *Consumer) !void {
            try self.consumers.put(consumer, .{});
        }

        pub fn unsubscribe(self: *Self, consumer: *Consumer) void {
            if (self.consumers.getPtr(consumer)) |state| {
                state.deinit(self.allocator);
                _ = self.consumers.remove(consumer);
            }
            if (self.consumers.count() == 0) self.last = null;
        }

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{
                .allocator = allocator,
                .consumers = std.AutoHashMap(*Consumer, ConsumerState).init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            { // Deinit all consumers, decrements references on next/current nodes
                var iter = self.consumers.iterator();
                while (iter.next()) |e| e.value_ptr.deinit(self.allocator);
            }
            self.consumers.deinit();
        }

        pub fn next(self: *Self, consumer: *Consumer) ?*Data {
            if (self.consumers.getPtr(consumer)) |state| {
                if (state.current) |node| {
                    if (self.last) |last| if (node == last and node.rc == 1) {
                        self.last = null;
                    };
                    node.unbox(self.allocator);
                    state.current = null;
                }
                if (state.next) |node| {
                    state.next = Node.box(node.next);
                    state.current = node;
                    return node.data;
                }
            }
            return null;
        }

        pub fn append(self: *Self, data: *Data) !void {
            if (self.consumers.count() == 0) {
                self.allocator.destroy(data);
                return;
            }
            const node = try self.allocator.create(Node);
            node.* = Node{ .data = data, .rc = 0 };

            if (self.last) |last| last.next = Node.box(node);
            self.last = node;
            try self.notify(node);
        }

        fn notify(self: *Self, node: *Node) !void {
            var iter = self.consumers.iterator();
            while (iter.next()) |e| {
                const consumer = e.key_ptr.*;
                const state = e.value_ptr;
                if (state.next == null) {
                    state.next = Node.box(node);
                    try wakeupCallback(consumer);
                }
            }
        }
    };
}

fn addNode(topic: anytype, value: usize) !void {
    const v = try testing.allocator.create(usize);
    v.* = value;
    try topic.append(v);
}

const TestConsumer = struct {
    wakeup_count: usize = 0,
    fn wakeup(self: *@This()) !void {
        self.wakeup_count += 1;
    }
};

test SimpleTopic {
    var topic = SimpleTopic(usize, TestConsumer, TestConsumer.wakeup).init(testing.allocator);
    defer topic.deinit();

    var c1 = TestConsumer{};
    { // Consumers gets wakeup if state.next is null
        try topic.subscribe(&c1);
        try testing.expectEqual(1, topic.consumers.count());

        try testing.expectEqual(0, c1.wakeup_count);
        try addNode(&topic, 42);
        try testing.expectEqual(1, c1.wakeup_count);

        try addNode(&topic, 43);
        try testing.expectEqual(1, c1.wakeup_count);

        try testing.expectEqual(42, topic.next(&c1).?.*);
        try testing.expectEqual(43, topic.next(&c1).?.*);
        try testing.expect(topic.next(&c1) == null);
    }
    try addNode(&topic, 44);
    try testing.expectEqual(44, topic.last.?.data.*);

    var c2 = TestConsumer{};
    { // New consumer gets only new nodes, don't get 44
        try topic.subscribe(&c2);
        try testing.expectEqual(2, topic.consumers.count());

        const state = topic.consumers.getPtr(&c2).?;
        try testing.expect(state.current == null);
        try testing.expect(state.next == null);
        try testing.expect(topic.next(&c2) == null);
    }
    // existing Consumer follows nodes list
    try testing.expectEqual(44, topic.next(&c1).?.*);

    { // deinit last node
        try addNode(&topic, 45);
        try testing.expectEqual(45, topic.next(&c1).?.*);
        try testing.expect(topic.next(&c1) == null);
        try testing.expectEqual(45, topic.next(&c2).?.*);

        try testing.expectEqual(45, topic.last.?.data.*);
        try testing.expect(topic.next(&c2) == null);
        try testing.expect(topic.last == null);
    }

    { // noop append when no consumers
        topic.unsubscribe(&c1);
        topic.unsubscribe(&c2);
        try addNode(&topic, 46);
        try testing.expect(topic.last == null);
    }

    { // start again when we have consumers
        try topic.subscribe(&c1);
        try topic.subscribe(&c2);
        try addNode(&topic, 47);
        try testing.expectEqual(47, topic.last.?.data.*);
        try testing.expectEqual(47, topic.next(&c1).?.*);
        try testing.expect(topic.next(&c1) == null);
        try testing.expectEqual(47, topic.next(&c2).?.*);
        try testing.expect(topic.next(&c2) == null);
        try testing.expect(topic.last == null);
    }

    // topic.deinit should deallocate nodes and data
    try addNode(&topic, 48);
    try addNode(&topic, 49);
}
