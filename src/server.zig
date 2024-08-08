const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const posix = std.posix;
const log = std.log;
const math = std.math;
const testing = std.testing;

pub const Message = struct {
    id: [16]u8 = .{0} ** 16,
    timestamp: u64 = 0,
    attempts: u16 = 0,
    body: []const u8,
};

const TopicMsg = struct {
    sequence: u64,
    timestamp: u64,
    body: []const u8,

    const frame_type = 2;

    fn asChannelMsg(self: TopicMsg) ChannelMsg {
        var header: [34]u8 = .{0} ** 34;
        mem.writeInt(u32, header[0..4], @intCast(self.body.len + 30), .big); // size
        mem.writeInt(u32, header[4..8], frame_type, .big); // frame type
        mem.writeInt(u64, header[8..16], self.timestamp, .big); // timestamp
        mem.writeInt(u16, header[16..18], 1, .big); // attempts
        mem.writeInt(u64, header[26..34], self.sequence, .big); // message id
        return .{
            .header = header,
            .body = self.body,
        };
    }
};

pub const ChannelMsg = struct {
    header: [34]u8,
    body: []const u8,

    fn incAttempts(self: *ChannelMsg) void {
        const buf = self.header[16..18];
        const v = mem.readInt(u16, buf, .big) +% 1;
        mem.writeInt(u16, buf, v, .big);
    }

    fn sequence(self: ChannelMsg) u64 {
        return mem.readInt(u64, self.header[26..34], .big);
    }

    test incAttempts {
        const t = TopicMsg{
            .sequence = 0x01020304050607,
            .timestamp = 0x08090a0b0c0d0e0f,
            .body = &.{ 0xaa, 0xbb, 0xcc, 0xcc },
        };
        var c = t.asChannelMsg();
        try testing.expectEqual(t.sequence, c.sequence());

        const expected: [34]u8 = .{
            0x00, 0x00, 0x00, 0x022, 0x00, 0x00, 0x00, 0x02,
            0x08, 0x09, 0x0a, 0x0b,  0x0c, 0x0d, 0x0e, 0x0f,
            0x00, 0x01, 0x00, 0x00,  0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01,  0x02, 0x03, 0x04, 0x05,
            0x06, 0x07,
        };
        try testing.expectEqualSlices(u8, &expected, &c.header);
        try testing.expectEqual(1, c.header[17]);
        c.incAttempts();
        try testing.expectEqualSlices(u8, expected[0..17], c.header[0..17]);
        try testing.expectEqual(2, c.header[17]);
        try testing.expectEqualSlices(u8, expected[18..], c.header[18..]);
        c.incAttempts();
        try testing.expectEqual(3, c.header[17]);
    }
};

test {
    _ = ChannelMsg;
}

pub fn ServerType(Consumer: type) type {
    return struct {
        const Server = @This();
        const Topic = TopicType();
        pub const Channel = ChannelType();

        allocator: mem.Allocator,
        topics: std.StringHashMap(*Topic),

        pub fn init(allocator: mem.Allocator) Server {
            return .{
                .allocator = allocator,
                .topics = std.StringHashMap(*Topic).init(allocator),
            };
        }

        pub fn sub(self: *Server, consumer: Consumer, topic: []const u8, channel: []const u8) !*Channel {
            const t = try self.getTopic(topic);
            return try t.sub(consumer, channel);
        }

        fn getTopic(self: *Server, topic: []const u8) !*Topic {
            if (self.topics.get(topic)) |t| return t;

            const t = try self.allocator.create(Topic);
            errdefer self.allocator.destroy(t);
            t.* = Topic.init(self.allocator);
            const key = try self.allocator.dupe(u8, topic);
            errdefer self.allocator.free(key);
            try self.topics.put(key, t);
            log.debug("created topic {s}", .{topic});
            return t;
        }

        pub fn publish(self: *Server, consumer: Consumer, topic: []const u8, data: []const u8) !void {
            const t = try self.getTopic(topic);
            return try t.publish(consumer, data);
        }

        pub fn deinit(self: *Server) void {
            var iter = self.topics.iterator();
            while (iter.next()) |e| {
                const topic = e.value_ptr.*;
                topic.deinit();
                self.allocator.destroy(topic);
                self.allocator.free(e.key_ptr.*);
            }
            self.topics.deinit();
        }

        fn TopicType() type {
            return struct {
                allocator: mem.Allocator,
                messages: std.ArrayList(TopicMsg),
                channels: std.StringHashMap(*Channel),
                sequence: u64 = 0,
                messages_start_seq: u64 = 1,

                pub fn init(allocator: mem.Allocator) Topic {
                    return .{
                        .allocator = allocator,
                        .channels = std.StringHashMap(*Channel).init(allocator),
                        .messages = std.ArrayList(TopicMsg).init(allocator),
                    };
                }

                fn sub(self: *Topic, consumer: Consumer, channel: []const u8) !*Channel {
                    if (self.channels.get(channel)) |c| {
                        try c.sub(consumer);
                        return c;
                    }
                    const c = try self.allocator.create(Channel);
                    errdefer self.allocator.destroy(c);
                    c.* = Channel.init(self.allocator, self);
                    const key = try self.allocator.dupe(u8, channel);
                    errdefer self.allocator.free(key);
                    try self.channels.put(key, c);
                    try c.sub(consumer);
                    log.debug("created channel {s}", .{channel});
                    return c;
                }

                fn deinit(self: *Topic) void {
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

                pub fn publish(self: *Topic, _: Consumer, data: []const u8) !void {
                    const msg = TopicMsg{
                        .sequence = self.sequence + 1,
                        .timestamp = timestamp(),
                        .body = try self.allocator.dupe(u8, data),
                    };
                    try self.messages.append(msg);
                    self.sequence = msg.sequence;

                    var iter = self.channels.valueIterator();
                    while (iter.next()) |channel| {
                        try channel.*.wakeup();
                    }
                }

                fn timestamp() u64 {
                    var ts: posix.timespec = undefined;
                    posix.clock_gettime(.REALTIME, &ts) catch |err| switch (err) {
                        error.UnsupportedClock, error.Unexpected => return 0, // "Precision of timing depends on hardware and OS".
                    };
                    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
                }

                fn getMsg(self: *Topic, seq: usize) TopicMsg {
                    const idx = seq - self.messages_start_seq;
                    const tm = self.messages.items[idx];
                    assert(tm.sequence == seq);
                    return tm;
                }
            };
        }

        fn lessThan(context: void, a: u64, b: u64) math.Order {
            _ = context;
            return math.order(a, b);
        }

        pub fn ChannelType() type {
            return struct {
                allocator: mem.Allocator,
                topic: *Topic,
                consumers: std.ArrayList(Consumer),
                next_consumer: usize = 0,
                in_flight_msgs: std.AutoArrayHashMap(u64, *ChannelMsg),
                finished_seqences: std.PriorityQueue(u64, void, lessThan),
                window_start: u64 = 0,
                window_end: u64 = 0,
                iterator: ConsumersIterator = .{},

                pub fn init(allocator: mem.Allocator, topic: *Topic) Channel {
                    return .{
                        .allocator = allocator,
                        .topic = topic,
                        .consumers = std.ArrayList(Consumer).init(allocator),
                        .in_flight_msgs = std.AutoArrayHashMap(u64, *ChannelMsg).init(allocator),
                        .finished_seqences = std.PriorityQueue(u64, void, lessThan).init(allocator, {}),
                        .window_start = topic.sequence,
                        .window_end = topic.sequence,
                    };
                }

                fn wakeup(self: *Channel) !void {
                    var iter = self.consumersIterator();
                    while (iter.next()) |consumer| {
                        if (try self.getMsg()) |msg| {
                            // TODO: handle error return message to queue
                            try consumer.sendMsg(msg);
                        } else {
                            break;
                        }
                    }
                }

                // Iterates over ready consumers. Returns null when there is no
                // ready consumers.
                fn consumersIterator(self: *Channel) *ConsumersIterator {
                    self.iterator.init(self.consumers.items);
                    return &self.iterator;
                }

                const ConsumersIterator = struct {
                    consumers: []Consumer = undefined,
                    not_ready_count: usize = 0,
                    idx: usize = 0,

                    fn init(self: *ConsumersIterator, consumers: []Consumer) void {
                        self.consumers = consumers;
                        self.not_ready_count = 0;
                    }

                    fn next(self: *ConsumersIterator) ?Consumer {
                        const count = self.consumers.len;
                        if (count == 0) return null;
                        while (true) {
                            if (self.not_ready_count == count) return null;
                            self.idx += 1;
                            if (self.idx >= count) self.idx = 0;
                            const consumer = self.consumers[self.idx];
                            if (consumer.ready()) return consumer;
                            self.not_ready_count += 1;
                        }
                    }
                };

                pub fn fin(self: *Channel, msg_id: [16]u8) !bool {
                    const seq = mem.readInt(u64, msg_id[8..], .big);
                    const ok = self.in_flight_msgs.swapRemove(seq);
                    if (ok) {
                        try self.finished_seqences.add(seq);
                        while (self.finished_seqences.peek() == self.window_start + 1) {
                            self.window_start = self.finished_seqences.remove();
                        }
                    }
                    log.debug("fin {}-{}", .{ self.window_start, self.window_end });
                    return ok;
                }

                fn sub(self: *Channel, consumer: Consumer) !void {
                    try self.consumers.append(consumer);
                }

                pub fn unsub(self: *Channel, consumer: Consumer) void {
                    // TODO remove in_flight messages of that consumer
                    for (self.consumers.items, 0..) |item, i| {
                        if (item.socket == consumer.socket) {
                            _ = self.consumers.swapRemove(i);
                            return;
                        }
                    }
                }

                pub fn ready(self: *Channel, consumer: Consumer) !void {
                    // TODO send more than one message
                    if (consumer.ready() == 0) return;
                    if (try self.getMsg()) |msg| {
                        try consumer.sendMsg(msg);
                    }
                }

                fn getMsg(self: *Channel) !?*ChannelMsg {
                    if (self.window_end < self.topic.sequence) {
                        const topic_msg = self.topic.getMsg(self.window_end + 1);
                        const msg = try self.allocator.create(ChannelMsg);
                        msg.* = topic_msg.asChannelMsg();
                        try self.in_flight_msgs.put(topic_msg.sequence, msg);
                        self.window_end = topic_msg.sequence;
                        log.debug("get {}-{}", .{ self.window_start, self.window_end });
                        return msg;
                    }
                    return null;
                }

                fn deinit(self: *Channel) void {
                    self.consumers.deinit();
                    for (self.in_flight_msgs.values()) |msg| self.allocator.destroy(msg);
                    self.in_flight_msgs.deinit();
                    self.finished_seqences.deinit();
                }
            };
        }
    };
}

test "channel consumers iterator" {
    const allocator = testing.allocator;
    const T = struct {
        _ready: bool = true,
        fn ready(self: @This()) bool {
            return self._ready;
        }
    };

    var server = ServerType(*T).init(allocator);
    defer server.deinit();

    var consumer1: T = .{};
    var channel = try server.sub(&consumer1, "topic", "channel");

    var iter = channel.consumersIterator();
    try testing.expectEqual(&consumer1, iter.next().?);
    try testing.expectEqual(&consumer1, iter.next().?);
    consumer1._ready = false;
    try testing.expectEqual(null, iter.next());

    consumer1._ready = true;
    var consumer2: T = .{};
    var consumer3: T = .{};
    channel = try server.sub(&consumer2, "topic", "channel");
    channel = try server.sub(&consumer3, "topic", "channel");
    try testing.expectEqual(3, channel.consumers.items.len);

    iter = channel.consumersIterator();
    try testing.expectEqual(3, iter.consumers.len);
    try testing.expectEqual(&consumer2, iter.next().?);
    try testing.expectEqual(&consumer3, iter.next().?);
    try testing.expectEqual(&consumer1, iter.next().?);
    try testing.expectEqual(&consumer2, iter.next().?);

    iter = channel.consumersIterator();
    try testing.expectEqual(&consumer3, iter.next().?);
    try testing.expectEqual(&consumer1, iter.next().?);
    try testing.expectEqual(&consumer2, iter.next().?);
}
