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
            data: Data,

            fn unbox(self: *Node, allocator: std.mem.Allocator) void {
                assert(self.rc > 0);
                const nn = self.next;
                self.rc -= 1;
                if (self.rc == 0) {
                    switch (@typeInfo(Data)) {
                        .pointer => |ptr_info| switch (ptr_info.size) {
                            .Slice => allocator.free(self.data),
                            .One => allocator.destroy(self.data),
                            else => @compileError("invalid type given"),
                        },
                        else => {},
                    }
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
        first: ?*Node = null,
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
            try self.consumers.put(consumer, .{ .next = Node.box(self.first) });
        }

        pub fn unsubscribe(self: *Self, consumer: *Consumer) void {
            if (self.consumers.getPtr(consumer)) |state| {
                state.deinit(self.allocator);
                _ = self.consumers.remove(consumer);
            }
            self.checkLast();
        }

        pub fn setFirst(self: *Self, data: Data) !void {
            self.unsetFrist();
            const node = try self.allocator.create(Node);
            node.* = .{ .data = data, .rc = 0, .next = null };
            self.first = Node.box(node);
        }

        pub fn unsetFrist(self: *Self) void {
            if (self.first) |n| {
                n.unbox(self.allocator);
                self.first = null;
            }
        }

        pub fn hasFirst(self: *Self) bool {
            return self.first != null;
        }

        fn checkLast(self: *Self) void {
            if (self.last) |last| if (last.rc == 1) {
                last.unbox(self.allocator);
                self.last = null;
            };
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
            self.unsetFrist();
            if (self.last) |n| n.unbox(self.allocator);
        }

        pub fn next(self: *Self, consumer: *Consumer) ?Data {
            if (self.consumers.getPtr(consumer)) |state| {
                if (state.current) |node| {
                    node.unbox(self.allocator);
                    state.current = null;
                    self.checkLast();
                }
                if (state.next) |node| {
                    state.next = Node.box(node.next);
                    state.current = node;
                    return node.data;
                }
            }
            return null;
        }

        pub fn hasConsumers(self: *Self) bool {
            return self.consumers.count() > 0;
        }

        pub fn append(self: *Self, data: Data) !bool {
            if (!self.hasConsumers()) return false;

            const node = try self.allocator.create(Node);
            node.* = Node{ .data = data, .rc = 0, .next = null };

            if (self.last) |last| {
                last.unbox(self.allocator);
                last.next = Node.box(node);
            }
            self.last = Node.box(node);
            if (self.first) |first| if (first.next == null) {
                first.next = Node.box(node);
            };
            try self.notify(node);
            return true;
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
    assert(try topic.append(v));
}

const TestConsumer = struct {
    wakeup_count: usize = 0,
    fn wakeup(self: *@This()) !void {
        self.wakeup_count += 1;
    }
};

test SimpleTopic {
    // Using `*usize` instead of `usize` for Data to test that node data is also
    // destroyed with node.
    var topic = SimpleTopic(*usize, TestConsumer, TestConsumer.wakeup).init(testing.allocator);
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
        {
            const v = try testing.allocator.create(usize);
            v.* = 46;
            assert(try topic.append(v) == false);
            testing.allocator.destroy(v);
        }
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

test "unsubscribe nulls last" {
    var topic = SimpleTopic(*usize, TestConsumer, TestConsumer.wakeup).init(testing.allocator);
    defer topic.deinit();

    var c1 = TestConsumer{};
    try topic.subscribe(&c1);

    var c2 = TestConsumer{};
    try topic.subscribe(&c2);

    try addNode(&topic, 42);
    try addNode(&topic, 43);
    try addNode(&topic, 44);
    {
        try testing.expectEqual(42, topic.next(&c1).?.*);
        try testing.expectEqual(43, topic.next(&c1).?.*);
        try testing.expectEqual(44, topic.next(&c1).?.*);
        try testing.expect(topic.next(&c1) == null);
    }
    try testing.expectEqual(42, topic.next(&c2).?.*);
    {
        // Unsubscribe of the last consumer which holds references releases all
        // nodes, and nulls last.
        try testing.expectEqual(44, topic.last.?.data.*);
        topic.unsubscribe(&c2);
        try testing.expect(topic.last == null);
    }

    { // Next of last consumer also nulls last
        try addNode(&topic, 45);
        try testing.expectEqual(45, topic.next(&c1).?.*);
        try testing.expectEqual(45, topic.last.?.data.*);
        try testing.expect(topic.next(&c1) == null);
        try testing.expect(topic.last == null);
    }
}

test "first message" {
    var topic = SimpleTopic(*usize, TestConsumer, TestConsumer.wakeup).init(testing.allocator);
    defer topic.deinit();

    var c1 = TestConsumer{};
    try topic.subscribe(&c1);

    const f = try testing.allocator.create(usize);
    f.* = 1;
    try topic.setFirst(f);

    var c2 = TestConsumer{};
    try topic.subscribe(&c2);

    try addNode(&topic, 42);
    try testing.expectEqual(42, topic.next(&c1).?.*);

    try testing.expectEqual(2, topic.first.?.rc);
    try testing.expectEqual(1, topic.next(&c2).?.*);
    try testing.expectEqual(42, topic.next(&c2).?.*);
    try testing.expectEqual(1, topic.first.?.rc);

    topic.unsetFrist();
    try testing.expect(topic.first == null);
}
