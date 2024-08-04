const std = @import("std");
const mem = std.mem;
const Message = @import("protocol.zig").Message;

pub fn Channel(Consumer: type) type {
    return struct {
        const Self = @This();

        allocator: mem.Allocator,
        consumers: std.AutoArrayHashMap(Consumer, ConsumerState),

        pub fn init(allocator: mem.Allocator) Self {
            return .{
                .allocator = allocator,
                .consumers = std.AutoArrayHashMap(Consumer, ConsumerState).init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            self.consumers.deinit();
        }

        pub fn recv(self: *Self, cnm: Consumer, msg: Message) !void {
            switch (msg) {
                .sub => {
                    _ = try self.consumers.getOrPut(cnm);
                },
                .rdy => |count| {
                    if (self.consumers.getPtr(cnm)) |state| {
                        state.ready = count;
                    }
                },
                .fin => {},
                .req => {},
                .touch => {},
                .cls => {},
                .nop => {},
                else => unreachable,
            }
        }
    };
}

const ConsumerState = struct {
    // counters
    ready: usize = 0,
    in_flight: usize = 0,
    finish: usize = 0,
    requeue: usize = 0,
};

const testing = std.testing;

test "sub" {
    var consumer = struct {}{};

    var channel = Channel(*@TypeOf(consumer)).init(testing.allocator);
    defer channel.deinit();

    try testing.expectEqual(0, channel.consumers.count());
    try channel.recv(&consumer, .{ .sub = .{ .topic = "", .channel = "" } });
    try testing.expectEqual(1, channel.consumers.count());
    try channel.recv(&consumer, .{ .rdy = 123 });

    // sub of already existing consumer
    try channel.recv(&consumer, .{ .sub = .{ .topic = "t", .channel = "c" } });
    try testing.expectEqual(1, channel.consumers.count());
    try testing.expectEqual(123, channel.consumers.getPtr(&consumer).?.ready);
}
