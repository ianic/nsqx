const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const posix = std.posix;
const log = std.log;
const math = std.math;
const testing = std.testing;
const socket_t = posix.socket_t;
const ns_per_s = std.time.ns_per_s;

// TODO: can't set this larger than 512, getting InvalidArgument in send
// why?
pub const max_msgs_send_batch_size = 512;

const TopicMsg = struct {
    sequence: u64,
    timestamp: u64,
    body: []const u8,
    next: ?*TopicMsg = null,

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
    in_flight_socket: socket_t = 0,
    timeout: u64 = 0,

    fn incAttempts(self: *ChannelMsg) void {
        const buf = self.header[16..18];
        const v = mem.readInt(u16, buf, .big) +% 1;
        mem.writeInt(u16, buf, v, .big);
    }

    fn sequence(self: ChannelMsg) u64 {
        return mem.readInt(u64, self.header[26..34], .big);
    }

    fn attempts(self: ChannelMsg) u16 {
        const buf = self.header[16..18];
        return mem.readInt(u16, buf, .big);
    }

    fn id(self: ChannelMsg) [16]u8 {
        return self.header[18..34].*;
    }

    fn less(_: void, a: *ChannelMsg, b: *ChannelMsg) math.Order {
        return math.order(a.sequence(), b.sequence());
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

    pub fn seqFromId(msg_id: [16]u8) u64 {
        return mem.readInt(u64, msg_id[8..], .big);
    }

    fn idFromSeq(seq: u64) [16]u8 {
        var msg_id: [16]u8 = .{0} ** 16;
        mem.writeInt(u64, msg_id[8..16], seq, .big);
        return msg_id;
    }
};

fn timestamp() u64 {
    var ts: posix.timespec = undefined;
    posix.clock_gettime(.REALTIME, &ts) catch |err| switch (err) {
        error.UnsupportedClock, error.Unexpected => return 0, // "Precision of timing depends on hardware and OS".
    };
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

fn lessU64(context: void, a: u64, b: u64) math.Order {
    _ = context;
    return math.order(a, b);
}

test {
    _ = ChannelMsg;
    _ = TopicMsgList;
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

        fn getTopic(self: *Server, topic_name: []const u8) !*Topic {
            if (self.topics.get(topic_name)) |t| return t;

            const topic = try self.allocator.create(Topic);
            errdefer self.allocator.destroy(topic);
            topic.* = Topic.init(self.allocator);
            const key = try self.allocator.dupe(u8, topic_name);
            errdefer self.allocator.free(key);
            try self.topics.put(key, topic);
            log.debug("created topic {s}", .{topic_name});
            return topic;
        }

        pub fn publish(self: *Server, topic_name: []const u8, data: []const u8) !void {
            const topic = try self.getTopic(topic_name);
            try topic.publish(data);
        }

        pub fn multiPublish(self: *Server, topic_name: []const u8, msgs: u32, data: []const u8) !void {
            const topic = try self.getTopic(topic_name);
            try topic.multiPublish(msgs, data);
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
                messages: TopicMsgList,
                channels: std.StringHashMap(*Channel),
                sequence: u64 = 0,
                next: ?*TopicMsg = null,
                fin_count: usize = 0, // fin's in channels

                pub fn init(allocator: mem.Allocator) Topic {
                    return .{
                        .allocator = allocator,
                        .channels = std.StringHashMap(*Channel).init(allocator),
                        .messages = .{},
                    };
                }

                fn sub(self: *Topic, consumer: Consumer, channel_name: []const u8) !*Channel {
                    if (self.channels.get(channel_name)) |c| {
                        try c.sub(consumer);
                        return c;
                    }
                    const channel = try self.allocator.create(Channel);
                    errdefer self.allocator.destroy(channel);
                    channel.* = Channel.init(self.allocator, self);
                    const key = try self.allocator.dupe(u8, channel_name);
                    errdefer self.allocator.free(key);
                    try self.channels.put(key, channel);
                    try channel.sub(consumer);
                    log.debug("created channel {s}", .{channel_name});
                    // First channel gets all messages from the topic
                    if (self.channels.count() == 1) {
                        channel.next = self.messages.first;
                        if (self.messages.first) |msg| channel.offset = msg.sequence - 1;
                    }
                    return channel;
                }

                fn deinit(self: *Topic) void {
                    {
                        var iter = self.channels.iterator();
                        while (iter.next()) |e| {
                            const channel = e.value_ptr.*;
                            channel.deinit();
                            self.allocator.destroy(channel);
                            self.allocator.free(e.key_ptr.*);
                        }
                        self.channels.deinit();
                    }
                    self.messages.deinit(self.allocator);
                }

                pub fn publish(self: *Topic, data: []const u8) !void {
                    try self.append(data);
                    try self.notifyChannels();
                }

                pub fn multiPublish(self: *Topic, msgs: u32, data: []const u8) !void {
                    var pos: usize = 0;
                    for (0..msgs) |_| {
                        const len = mem.readInt(u32, data[pos..][0..4], .big);
                        pos += 4;
                        try self.append(data[pos..][0..len]);
                        pos += len;
                    }
                    try self.notifyChannels();
                }

                fn append(self: *Topic, data: []const u8) !void {
                    const msg = try self.allocator.create(TopicMsg);
                    const body = try self.allocator.dupe(u8, data);
                    self.sequence += 1;
                    msg.* = .{
                        .sequence = self.sequence,
                        .timestamp = timestamp(),
                        .body = body,
                    };
                    self.messages.append(msg);
                    if (self.next == null) self.next = msg;
                }

                // Notify all channels that there is pending message
                fn notifyChannels(self: *Topic) !void {
                    if (self.next) |msg| {
                        var iter = self.channels.valueIterator();
                        while (iter.next()) |ptr| try ptr.*.topicAppended(msg);
                        self.next = null;
                    }
                }

                // Channel has moved its offset to seq.
                // Messages before and including that seq can be released.
                fn fin(self: *Topic, seq: u64) void {
                    if (self.channels.count() == 1) {
                        self.messages.release(self.allocator, seq);
                        return;
                    }

                    // Find min sequence of all channels
                    var min_seq: u64 = std.math.maxInt(u64);
                    var iter = self.channels.valueIterator();
                    while (iter.next()) |ptr| {
                        const channel = ptr.*;
                        if (channel.offset < min_seq) min_seq = channel.offset;
                    }
                    self.messages.release(self.allocator, min_seq);
                }
            };
        }

        pub fn ChannelType() type {
            return struct {
                allocator: mem.Allocator,
                topic: *Topic,
                consumers: std.ArrayList(Consumer),
                // Sent but not jet acknowledged (fin) messages
                in_flight: std.AutoArrayHashMap(u64, *ChannelMsg),
                // Re-queued by consumer or timed out
                requeued: std.PriorityQueue(*ChannelMsg, void, ChannelMsg.less),
                // Sequences for which we received fin but they are out of order
                // so we can't move offset
                finished: std.PriorityQueue(u64, void, lessU64),
                // Last sequence sent and acknowledged by some consumer
                offset: u64 = 0,
                // Pointer to the next message in the topic, null if we reached
                // end of the topic
                next: ?*TopicMsg = null,
                // Round robin consumers iterator. Preserves last consumer index
                // between inits.
                iterator: ConsumersIterator = .{},
                // Message timeout in nanoseconds, 0 - disabled
                message_timeout: u64 = 0,
                stat: struct {
                    sent: usize = 0,
                    finished: usize = 0,
                    timeouted: usize = 0,
                    requeued: usize = 0,
                } = .{},

                pub fn init(allocator: mem.Allocator, topic: *Topic) Channel {
                    return .{
                        .allocator = allocator,
                        .topic = topic,
                        .consumers = std.ArrayList(Consumer).init(allocator),
                        .in_flight = std.AutoArrayHashMap(u64, *ChannelMsg).init(allocator),
                        .finished = std.PriorityQueue(u64, void, lessU64).init(allocator, {}),
                        .requeued = std.PriorityQueue(*ChannelMsg, void, ChannelMsg.less).init(allocator, {}),
                        // Channel starts at the last topic message at the moment of channel creation
                        .offset = topic.sequence,
                        .next = null,
                    };
                }

                fn topicAppended(self: *Channel, tmsg: *TopicMsg) !void {
                    if (self.next == null) {
                        self.next = tmsg;
                        try self.wakeup();
                    }
                }

                fn wakeup(self: *Channel) !void {
                    var iter = self.consumersIterator();
                    while (iter.next()) |consumer|
                        if (!try self.fillConsumer(consumer)) break;
                }

                // Returns true if there is more messages for next consumer
                // TODO error handling
                fn fillConsumer(self: *Channel, consumer: Consumer) !bool {
                    var msgs_buf: [max_msgs_send_batch_size]*ChannelMsg = undefined;
                    var msgs = msgs_buf[0..@min(consumer.ready(), msgs_buf.len)];
                    var n: usize = 0;
                    while (n < msgs.len) : (n += 1) {
                        if (self.getMsg() catch null) |msg| {
                            msg.in_flight_socket = consumer.socket;
                            try self.inFlightAppend(msg);
                            msgs[n] = msg;
                        } else break;
                    }
                    if (n == 0) return false;
                    consumer.sendMsgs(msgs[0..n]) catch |err| {
                        log.err("failed to send msg {}, re-queuing", .{err});
                        for (msgs[0..n]) |msg|
                            assert(self.req(msg.id()) catch false);
                        return false;
                    };
                    return n == msgs.len;
                }

                fn inFlightAppend(self: *Channel, msg: *ChannelMsg) !void {
                    self.stat.sent += 1;
                    //msg.timestamp = timer.timestamp();
                    try self.in_flight.put(msg.sequence(), msg);
                }

                fn inFlightTimeout(self: *Channel, ts: u64) !void {
                    var msgs = std.ArrayList(*ChannelMsg).init(self.allocator);
                    defer msgs.deinit();
                    for (self.in_flight.values()) |msg| {
                        if (msg.timeout <= ts) try msgs.append(msg);
                    }
                    for (msgs.items) |msg| {
                        try self.requeue(msg);
                    }
                    self.stat.timeouted += msgs.items.len;
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
                            if (consumer.ready() > 0) {
                                return consumer;
                            }
                            self.not_ready_count += 1;
                        }
                    }
                };

                pub fn fin(self: *Channel, msg_id: [16]u8) !bool {
                    return self.finSeq(ChannelMsg.seqFromId(msg_id));
                }

                pub fn finSeq(self: *Channel, seq: u64) !bool {
                    if (self.in_flight.fetchSwapRemove(seq)) |kv| {
                        self.stat.finished += 1;
                        self.allocator.destroy(kv.value);

                        { // Move self.sequence or add seq to finished
                            const before = self.offset;
                            if (seq == self.offset + 1) {
                                self.offset = seq;
                            } else {
                                try self.finished.add(seq);
                            }
                            while (self.finished.peek() == self.offset + 1) {
                                self.offset = self.finished.remove();
                            }
                            if (self.offset > before) {
                                self.topic.fin(self.offset);
                            }
                        }
                        return true;
                    }
                    return false;
                }

                pub fn req(self: *Channel, msg_id: [16]u8) !bool {
                    const seq = ChannelMsg.seqFromId(msg_id);
                    if (self.in_flight.get(seq)) |msg| {
                        try self.requeue(msg);
                        return true;
                    }
                    return false;
                }

                fn requeue(self: *Channel, msg: *ChannelMsg) !void {
                    msg.in_flight_socket = 0;
                    msg.incAttempts();
                    try self.requeued.add(msg);
                    assert(self.in_flight.swapRemove(msg.sequence()));
                    self.stat.requeued += 1;
                }

                fn sub(self: *Channel, consumer: Consumer) !void {
                    try self.consumers.append(consumer);
                }

                pub fn unsub(self: *Channel, consumer: Consumer) !void {
                    // Remove in_flight messages of that consumer
                    outer: while (true) {
                        for (self.in_flight.values()) |msg| {
                            if (msg.in_flight_socket == consumer.socket) {
                                _ = try self.req(msg.id());
                                continue :outer; // restart on delete from in_flight
                            }
                        }
                        break;
                    }
                    // Remove consumer
                    for (self.consumers.items, 0..) |item, i| {
                        if (item.socket == consumer.socket) {
                            _ = self.consumers.swapRemove(i);
                            return;
                        }
                    }
                }

                pub fn ready(self: *Channel, consumer: Consumer) !void {
                    if (consumer.ready() == 0) return;
                    _ = try self.fillConsumer(consumer);
                }

                fn getMsg(self: *Channel) !?*ChannelMsg {
                    // First look into re-queued messages
                    if (self.requeued.removeOrNull()) |msg| return msg;

                    // Then try to find next message in topic
                    if (self.nextTopicMsg()) |topic_msg| {
                        const msg = try self.allocator.create(ChannelMsg);
                        msg.* = topic_msg.asChannelMsg();
                        return msg;
                    }
                    return null;
                }

                fn nextTopicMsg(self: *Channel) ?*TopicMsg {
                    if (self.next) |tmsg| {
                        self.next = tmsg.next;
                        return tmsg;
                    }
                    return null;
                }

                fn deinit(self: *Channel) void {
                    self.consumers.deinit();
                    for (self.in_flight.values()) |msg| self.allocator.destroy(msg);
                    while (self.requeued.removeOrNull()) |msg| self.allocator.destroy(msg);
                    self.in_flight.deinit();
                    self.finished.deinit();
                    self.requeued.deinit();
                }
            };
        }
    };
}

test "channel consumers iterator" {
    const allocator = testing.allocator;
    const T = struct {
        _ready: usize = 1,
        fn ready(self: @This()) usize {
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
    consumer1._ready = 0;
    try testing.expectEqual(null, iter.next());

    consumer1._ready = 1;
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

test "channel fin req" {
    const allocator = testing.allocator;
    const T = struct {
        socket: socket_t = 0,
        ready_count: usize = 0,
        last_seq: usize = 0,
        last_id: [16]u8 = undefined,

        const Self = @This();
        fn ready(self: *Self) usize {
            return self.ready_count;
        }
        fn sendMsgs(self: *Self, msgs: []*ChannelMsg) !void {
            const msg = msgs[0];
            self.last_seq = msg.sequence();
            self.last_id = msg.id();
            self.ready_count = 0;
        }
    };
    const topic_name = "topic";
    const channel_name = "channel";

    var server = ServerType(*T).init(allocator);
    defer server.deinit();

    var consumer: T = .{};
    var channel = try server.sub(&consumer, topic_name, channel_name);
    const topic = channel.topic;

    try server.publish(topic_name, "1");
    try server.publish(topic_name, "2");
    try server.publish(topic_name, "3");

    { // 3 messages in topic, 0 take by channel
        try testing.expectEqual(3, topic.messages.count());
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.requeued.count());
    }
    { // wakeup without ready consumers
        try channel.wakeup();
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.requeued.count());
    }

    { // consumer is ready
        consumer.ready_count = 1;
        try channel.ready(&consumer);
        try testing.expectEqual(1, consumer.last_seq);
        // 1 is in flight
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(0, channel.requeued.count());
        try testing.expectEqual(0, channel.offset);
        try testing.expectEqual(2, channel.next.?.sequence);
    }
    { // consumer sends fin, 0 in flight after that
        try testing.expect(try channel.fin(consumer.last_id)); // fin 1
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.requeued.count());
        try testing.expectEqual(1, channel.offset);
        try testing.expectEqual(0, channel.finished.count());
        try testing.expectEqual(2, topic.messages.count());
    }
    { // send seq 2, 1 msg in flight
        consumer.ready_count = 1;
        try channel.wakeup();
        try testing.expectEqual(2, consumer.last_seq);

        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(0, channel.requeued.count());
        try testing.expectEqual(1, channel.offset);
        try testing.expectEqual(3, channel.next.?.sequence);
    }
    { // send seq 3, 2 msgs in flight
        consumer.ready_count = 1;
        try channel.wakeup();
        try testing.expectEqual(3, consumer.last_seq);

        try testing.expectEqual(2, channel.in_flight.count());
        try testing.expectEqual(0, channel.requeued.count());
        try testing.expectEqual(1, channel.offset);
        try testing.expect(channel.next == null);
    }
    { // 2 is re-queued
        try testing.expect(try channel.req(ChannelMsg.idFromSeq(2))); // req 2
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(1, channel.requeued.count());
        try testing.expectEqual(1, channel.offset);
    }
    { // out of order fin, fin seq 3 while 2 is still in flight
        try testing.expect(try channel.fin(ChannelMsg.idFromSeq(3))); // fin 3
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(1, channel.requeued.count());
        try testing.expectEqual(1, channel.offset);
        try testing.expectEqual(1, channel.finished.count());
    }
    { // re send 2
        consumer.ready_count = 1;
        try channel.wakeup();
        try testing.expectEqual(2, consumer.last_seq);

        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(0, channel.requeued.count());
        try testing.expectEqual(1, channel.offset);
        try testing.expectEqual(1, channel.finished.count());
    }
    { // fin seq 2, advances window start from sequence 1 to 3
        try testing.expectEqual(2, topic.messages.count());
        try testing.expect(try channel.fin(ChannelMsg.idFromSeq(2))); // fin 2
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.requeued.count());
        try testing.expectEqual(3, channel.offset);
        try testing.expectEqual(0, channel.finished.count());
        try testing.expectEqual(0, topic.messages.count());
    }
}

const TestConsumer = struct {
    const Self = @This();

    socket: socket_t = 0,
    channel: ?*ServerType(*Self).Channel = null,
    sequences: std.ArrayList(u64),

    fn init(alloc: mem.Allocator) Self {
        return .{ .sequences = std.ArrayList(u64).init(alloc) };
    }

    fn deinit(self: *Self) void {
        self.sequences.deinit();
    }

    fn ready(_: *Self) usize {
        return 1;
    }

    fn sendMsgs(self: *Self, msgs: []*ChannelMsg) !void {
        assert(msgs.len == 1);
        const msg = msgs[0];
        try self.sequences.append(msg.sequence());
    }

    // send fin for the last received message
    fn fin(self: *Self) !void {
        const seq = ChannelMsg.idFromSeq(self.sequences.items[self.sequences.items.len - 1]);
        assert(try self.channel.?.fin(seq));
    }

    // pull message from channel
    fn pull(self: *Self) !void {
        try self.channel.?.ready(self);
    }
};

test "multiple channels" {
    const allocator = testing.allocator;

    const Server = ServerType(*TestConsumer);
    const topic_name = "topic";
    const channel_name1 = "channel1";
    const channel_name2 = "channel2";

    var server = Server.init(allocator);
    defer server.deinit();

    var c1 = TestConsumer.init(allocator);
    defer c1.deinit();
    c1.channel = try server.sub(&c1, topic_name, channel_name1);

    const no = 1024;

    { // single channel, single consumer
        for (0..no) |_|
            try server.publish(topic_name, "message body");

        try testing.expectEqual(no, c1.sequences.items.len);
        var expected: u64 = 1;
        for (c1.sequences.items) |seq| {
            try testing.expectEqual(expected, seq);
            expected += 1;
        }
    }

    var c2 = TestConsumer.init(allocator);
    defer c2.deinit();
    c2.channel = try server.sub(&c2, topic_name, channel_name2);

    { // two channels on the same topic
        for (0..no) |_|
            try server.publish(topic_name, "another message body");

        try testing.expectEqual(no * 2, c1.sequences.items.len);
        try testing.expectEqual(no, c2.sequences.items.len);
    }

    var c3 = TestConsumer.init(allocator);
    defer c3.deinit();
    c3.channel = try server.sub(&c3, topic_name, channel_name2);

    { // two channels, one has single consumer another has two consumers
        for (0..no) |_|
            try server.publish(topic_name, "yet another message body");

        try testing.expectEqual(no * 3, c1.sequences.items.len);
        // Two consumers on the same channel are all getting some messages
        try testing.expectEqual(no * 2, c3.sequences.items.len + c2.sequences.items.len);
        // But all of them are delivered in order and can be found in one or another consumer
        var idx3: usize = 0;
        var idx2: usize = no;
        for (no * 2 + 1..no * 3 + 1) |seq| {
            if (c3.sequences.items[idx3] == seq) {
                idx3 += 1;
                continue;
            }
            if (c2.sequences.items[idx2] == seq) {
                idx2 += 1;
                continue;
            }
            unreachable;
        }
    }
}

test "first channel gets all messages accumulated in topic" {
    const allocator = testing.allocator;
    const Server = ServerType(*TestConsumer);
    const topic_name = "topic";
    const channel_name = "channel1";
    const no = 16;

    var server = Server.init(allocator);
    defer server.deinit();
    // publish messages to the topic which don't have channels created
    for (0..no) |_|
        try server.publish(topic_name, "message body");

    const topic = try server.getTopic(topic_name);
    try testing.expectEqual(no, topic.messages.count());

    // subscribe creates channel
    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();
    const channel = try server.sub(&consumer, topic_name, channel_name);
    consumer.channel = channel;

    try testing.expect(channel.next != null);

    for (0..no) |i| {
        try consumer.pull();
        try consumer.fin();
        try testing.expectEqual(i + 1, channel.offset);
    }

    try testing.expectEqual(no, channel.offset);
    try testing.expectEqual(no, consumer.sequences.items.len);
    try testing.expectEqual(0, topic.messages.count());
}

test "timeout messages" {
    const allocator = testing.allocator;
    const Server = ServerType(*TestConsumer);
    const topic_name = "topic";
    const channel_name = "channel1";
    const no = 4;

    var server = Server.init(allocator);
    defer server.deinit();

    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();
    const channel = try server.sub(&consumer, topic_name, channel_name);
    consumer.channel = channel;

    // publish messages to the topic which don't have channels created
    for (0..no) |_| {
        try server.publish(topic_name, "message body");
        try consumer.pull();
    }
    try testing.expectEqual(4, channel.in_flight.count());
    try testing.expectEqual(4, channel.stat.sent);

    // set timestamps
    for (channel.in_flight.values(), 1..) |msg, i| {
        try testing.expectEqual(1, msg.attempts());
        msg.timeout = i;
    }
    try channel.inFlightTimeout(1);
    try testing.expectEqual(1, channel.stat.requeued);
    try testing.expectEqual(3, channel.in_flight.count());
    try testing.expectEqual(1, channel.requeued.count());
    try testing.expectEqual(1, channel.stat.timeouted);
    try channel.inFlightTimeout(3);
    try testing.expectEqual(3, channel.stat.requeued);
    try testing.expectEqual(1, channel.in_flight.count());
    try testing.expectEqual(3, channel.requeued.count());
    try testing.expectEqual(3, channel.stat.timeouted);
    try testing.expectEqual(0, channel.stat.finished);
    try consumer.fin();
    try testing.expectEqual(1, channel.stat.finished);
    try testing.expectEqual(0, channel.in_flight.count());
    try consumer.pull();
    try consumer.pull();
    try testing.expectEqual(6, channel.stat.sent);
    try testing.expectEqual(2, channel.in_flight.count());
    try testing.expectEqual(1, channel.requeued.count());
    for (channel.in_flight.values()) |msg| {
        try testing.expectEqual(2, msg.attempts());
    }
}

// Linked list of topic messages
const TopicMsgList = struct {
    first: ?*TopicMsg = null,
    last: ?*TopicMsg = null,

    const Self = @This();

    fn append(self: *Self, msg: *TopicMsg) void {
        if (self.last == null) {
            assert(self.first == null);
            self.first = msg;
            self.last = msg;
            return;
        }
        assert(self.first != null);
        assert(self.last.?.sequence + 1 == msg.sequence);
        self.last.?.next = msg;
        self.last = msg;
    }

    // Release all messages with sequence smaller or equal to seq.
    fn release(self: *Self, allocator: mem.Allocator, seq: u64) void {
        while (self.first != null and self.first.?.sequence <= seq) {
            const msg = self.first.?;
            self.first = msg.next;
            allocator.free(msg.body);
            allocator.destroy(msg);
        }
        if (self.first == null) self.last = null;
    }

    // Release all messages.
    fn deinit(self: *Self, allocator: mem.Allocator) void {
        while (self.first) |msg| {
            self.first = msg.next;
            allocator.free(msg.body);
            allocator.destroy(msg);
        }
        self.last = null;
    }

    pub fn count(self: *Self) u64 {
        if (self.last == null) return 0;
        return self.last.?.sequence - self.first.?.sequence + 1;
    }

    pub fn size(self: *Self) u64 {
        if (self.last == null) return 0;
        var s: u64 = 0;
        var node = self.first.?;
        while (true) {
            s += node.body.len + @sizeOf(TopicMsg);
            if (node.next) |n| node = n else break;
        }
        return s;
    }

    test TopicMsgList {
        const allocator = testing.allocator;
        var list: TopicMsgList = .{};
        const body = "foo";
        const m1 = try allocator.create(TopicMsg);
        const m2 = try allocator.create(TopicMsg);
        const m3 = try allocator.create(TopicMsg);
        const m4 = try allocator.create(TopicMsg);
        m1.* = TopicMsg{ .sequence = 1, .timestamp = 0, .body = try allocator.dupe(u8, body) };
        m2.* = TopicMsg{ .sequence = 2, .timestamp = 0, .body = try allocator.dupe(u8, body) };
        m3.* = TopicMsg{ .sequence = 3, .timestamp = 0, .body = try allocator.dupe(u8, body) };
        m4.* = TopicMsg{ .sequence = 4, .timestamp = 0, .body = try allocator.dupe(u8, body) };

        list.append(m1);
        list.append(m2);
        list.append(m3);
        list.append(m4);
        try testing.expect(list.first.? == m1);
        try testing.expect(list.last.? == m4);

        list.release(allocator, 0);
        try testing.expect(list.first.? == m1);
        try testing.expect(list.last.? == m4);

        list.release(allocator, 2);
        try testing.expect(list.first.? == m3);
        try testing.expect(list.last.? == m4);

        list.deinit(allocator);
        try testing.expect(list.first == null);
        try testing.expect(list.last == null);
    }
};
