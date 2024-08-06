const std = @import("std");
const mem = std.mem;
const posix = std.posix;
const log = std.log;

pub const Message = struct {
    id: [16]u8 = .{0} ** 16,
    timestamp: u64 = 0,
    attempts: u16 = 0,
    body: []const u8,
};

pub fn Server(Consumer: type) type {
    const TopicType = Topic(Consumer);
    const ChannelType = Channel(Consumer);

    return struct {
        const Self = @This();

        allocator: mem.Allocator,
        topics: std.StringHashMap(*TopicType),

        pub fn init(allocator: mem.Allocator) Self {
            return .{
                .allocator = allocator,
                .topics = std.StringHashMap(*TopicType).init(allocator),
            };
        }

        pub fn sub(self: *Self, consumer: Consumer, topic: []const u8, channel: []const u8) !*ChannelType {
            if (self.topics.get(topic)) |t|
                return try t.sub(consumer, channel);

            const t = try self.allocator.create(TopicType);
            errdefer self.allocator.destroy(t);
            t.* = TopicType.init(self.allocator);
            const key = try self.allocator.dupe(u8, topic);
            errdefer self.allocator.free(key);
            try self.topics.put(key, t);
            log.debug("created topic {s}", .{topic});
            return try t.sub(consumer, channel);
        }

        pub fn deinit(self: *Self) void {
            var iter = self.topics.iterator();
            while (iter.next()) |e| {
                const topic = e.value_ptr.*;
                topic.deinit();
                self.allocator.destroy(topic);
                self.allocator.free(e.key_ptr.*);
            }
            self.topics.deinit();
        }
    };
}

fn Topic(Consumer: type) type {
    const ChannelType = Channel(Consumer);

    return struct {
        const Self = @This();

        allocator: mem.Allocator,
        messages: std.ArrayList(Message),
        channels: std.StringHashMap(*ChannelType),
        msg_id: u64 = 0,

        pub fn init(allocator: mem.Allocator) Self {
            return .{
                .allocator = allocator,
                .channels = std.StringHashMap(*ChannelType).init(allocator),
                .messages = std.ArrayList(Message).init(allocator),
            };
        }

        fn sub(self: *Self, consumer: Consumer, channel: []const u8) !*ChannelType {
            if (self.channels.get(channel)) |c| {
                try c.sub(consumer);
                return c;
            }

            const c = try self.allocator.create(ChannelType);
            errdefer self.allocator.destroy(c);
            c.* = ChannelType.init(self.allocator);
            const key = try self.allocator.dupe(u8, channel);
            errdefer self.allocator.free(key);
            try self.channels.put(key, c);
            try c.sub(consumer);
            log.debug("created channel {s}", .{channel});
            return c;
        }

        fn deinit(self: *Self) void {
            var iter = self.channels.iterator();
            while (iter.next()) |e| {
                const channel = e.value_ptr.*;
                channel.deinit();
                self.allocator.destroy(channel);
                self.allocator.free(e.key_ptr.*);
            }
            self.channels.deinit();
            for (self.messages.items) |msg| {
                self.allocator.free(msg.body);
            }
            self.messages.deinit();
        }

        pub fn publish(self: *Self, msg_body: []const u8) !void {
            const id: [16]u8 = .{0} ** 16;
            mem.writeInt(u64, id[8..], self.msg_id, .big);

            var ts: posix.timespec = undefined;
            posix.clock_gettime(.REALTIME, &ts) catch |err| switch (err) {
                error.UnsupportedClock, error.Unexpected => return 0, // "Precision of timing depends on hardware and OS".
            };
            const timestamp: u64 = ts.sec * std.time.ns_per_s + ts.nsec;

            const msg = Message{
                .id = id,
                .timestamp = timestamp,
                .body = try self.allocator.dupe(msg_body),
            };
            try self.messages.append(msg);
            self.msg_id +%= 1;

            for (self.channels) |channel| {
                //if (channel.msg_id == 0 or channel.msg_id == last) {
                channel.publish(&msg);
                //                }
            }
        }
    };
}

pub fn Channel(Consumer: type) type {
    return struct {
        const Self = @This();

        allocator: mem.Allocator,
        consumers: std.ArrayList(Consumer),

        pub fn init(allocator: mem.Allocator) Self {
            return .{
                .allocator = allocator,
                .consumers = std.ArrayList(Consumer).init(allocator),
            };
        }

        fn publish(self: *Self, msg: *Message) void {
            for (self.consumers) |consumer| {
                if (consumer.ready_count > 0) {
                    consumer.send(msg) catch unreachable; // TODO
                    return;
                }
            }
        }

        fn sub(self: *Self, consumer: Consumer) !void {
            try self.consumers.append(consumer);
        }

        pub fn unsub(self: *Self, consumer: Consumer) void {
            for (self.consumers.items, 0..) |item, i| {
                if (item.socket == consumer.socket) {
                    _ = self.consumers.swapRemove(i);
                    return;
                }
            }
        }

        pub fn ready(self: *Self, consumer: Consumer) !void {
            _ = self;
            _ = consumer;
        }

        fn deinit(self: *Self) void {
            self.consumers.deinit();
        }
    };
}
