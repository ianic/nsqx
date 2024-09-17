const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const posix = std.posix;
const math = std.math;
const testing = std.testing;
const socket_t = posix.socket_t;

const log = std.log.scoped(.server);
const Error = @import("io.zig").Error;

const ns_per_s = std.time.ns_per_s;
const ns_per_ms = std.time.ns_per_ms;

fn nsFromMs(ms: u32) u64 {
    return @as(u64, @intCast(ms)) * ns_per_ms;
}

// iovlen in msghdr is limited by IOV_MAX in <limits.h>. On modern Linux
// systems, the limit is 1024. Each message has header and body: 2 iovecs that
// limits number of messages in a batch to 512.
// ref: https://man7.org/linux/man-pages/man2/readv.2.html
pub const max_msgs_send_batch_size = 512;

const TopicMsg = struct {
    sequence: u64, // TODO: no use for this remove
    created_at: u64,
    body: []const u8,
    // Defer message delivery until timestamp is reached.
    defer_until: u64 = 0,
    next: ?*TopicMsg = null,
    // Reference counting
    allocator: mem.Allocator,
    rc: usize = 0,

    pub fn release(self: *TopicMsg) void {
        assert(self.rc > 0);
        const next = self.next;
        self.rc -= 1;
        if (self.rc == 0) {
            self.allocator.free(self.body);
            self.allocator.destroy(self);
            if (next) |n| n.release();
        }
    }

    // If there is next message it will acquire and return, null if there is no next.
    pub fn nextAcquire(self: *TopicMsg) ?*TopicMsg {
        if (self.next) |n| return n.acquire();
        return null;
    }

    pub fn acquire(self: *TopicMsg) *TopicMsg {
        self.rc += 1;
        return self;
    }

    fn asChannelMsg(self: *TopicMsg) ChannelMsg {
        const frame_type = @intFromEnum(@import("protocol.zig").FrameType.message);

        var header: [34]u8 = .{0} ** 34;
        mem.writeInt(u32, header[0..4], @intCast(self.body.len + 30), .big); // size
        mem.writeInt(u32, header[4..8], frame_type, .big); // frame type
        mem.writeInt(u64, header[8..16], self.created_at, .big); // timestamp
        mem.writeInt(u16, header[16..18], 1, .big); // attempts
        mem.writeInt(u64, header[26..34], self.sequence, .big); // message id
        return .{
            .msg = self,
            .header = header,
            .timestamp = self.defer_until,
        };
    }
};

pub const ChannelMsg = struct {
    msg: *TopicMsg,
    header: [34]u8,
    in_flight_socket: socket_t = 0,
    // Timestamp in nanoseconds. When in flight and timestamp is reached message should
    // be re-queued. If deferred message should not be delivered until timestamp
    // is reached.
    timestamp: u64 = 0,

    fn deinit(self: *ChannelMsg, allocator: mem.Allocator) void {
        self.msg.release(); // release inner topic message
        allocator.destroy(self); // destroy this channel message
    }

    pub fn body(self: *ChannelMsg) []const u8 {
        return self.msg.body;
    }

    fn incAttempts(self: *ChannelMsg) void {
        const buf = self.header[16..18];
        const v = mem.readInt(u16, buf, .big) +% 1;
        mem.writeInt(u16, buf, v, .big);
    }

    fn sequence(self: ChannelMsg) u64 {
        return self.msg.sequence;
    }

    fn attempts(self: ChannelMsg) u16 {
        const buf = self.header[16..18];
        return mem.readInt(u16, buf, .big);
    }

    fn id(self: ChannelMsg) [16]u8 {
        return self.header[18..34].*;
    }

    fn less(_: void, a: *ChannelMsg, b: *ChannelMsg) math.Order {
        if (a.timestamp != b.timestamp)
            return math.order(a.timestamp, b.timestamp);
        return math.order(a.sequence(), b.sequence());
    }

    test incAttempts {
        var t = TopicMsg{
            .allocator = testing.allocator,
            .sequence = 0x01020304050607,
            .created_at = 0x08090a0b0c0d0e0f,
            .body = &.{ 0xaa, 0xbb, 0xcc, 0xcc },
            .rc = 0,
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

fn lessU64(_: void, a: u64, b: u64) math.Order {
    return math.order(a, b);
}

test {
    _ = ChannelMsg;
}

pub fn ServerType(Consumer: type, Io: type, Notifier: type) type {
    return struct {
        const Server = @This();
        const Topic = TopicType();
        pub const Channel = ChannelType();

        allocator: mem.Allocator,
        topics: std.StringHashMap(*Topic),
        started_at: u64,
        io: *Io,
        notifier: *Notifier,

        pub fn init(allocator: mem.Allocator, io: *Io, notifier: *Notifier) Server {
            return .{
                .allocator = allocator,
                .topics = std.StringHashMap(*Topic).init(allocator),
                .started_at = io.now(),
                .io = io,
                .notifier = notifier,
            };
        }

        pub fn sub(self: *Server, consumer: *Consumer, topic: []const u8, channel: []const u8) !*Channel {
            const t = try self.getOrCreateTopic(topic);
            return try t.sub(consumer, channel);
        }

        pub fn deleteChannel(self: *Server, topic_name: []const u8, name: []const u8) !void {
            const topic = self.topics.get(topic_name) orelse return error.NotFound;
            try topic.deleteChannel(name);
            log.debug("deleted channel {s} on topic {s}", .{ name, topic_name });
        }

        pub fn pauseChannel(self: *Server, topic_name: []const u8, name: []const u8, paused: bool) !void {
            const topic = self.topics.get(topic_name) orelse return error.NotFound;
            try topic.pauseChannel(name, paused);
            log.debug("paused channel {s} on topic {s}", .{ name, topic_name });
        }

        pub fn deleteTopic(self: *Server, name: []const u8) !void {
            const kv = self.topics.fetchRemove(name) orelse return error.NotFound;
            self.deinitTopic(kv.value);
            log.debug("deleted topic {s}", .{name});
        }

        pub fn pauseTopic(self: *Server, name: []const u8, paused: bool) !void {
            const topic = self.topics.get(name) orelse return error.NotFound;
            try topic.pause(paused);
            log.debug("paused topic {s}", .{name});
        }

        pub fn emptyTopic(self: *Server, name: []const u8) !void {
            const topic = self.topics.get(name) orelse return error.NotFound;
            try topic.empty();
            log.debug("empty topic {s}", .{name});
        }

        fn getOrCreateTopic(self: *Server, name: []const u8) !*Topic {
            if (self.topics.get(name)) |t| return t;

            const topic = try self.allocator.create(Topic);
            errdefer self.allocator.destroy(topic);
            const key = try self.allocator.dupe(u8, name);
            errdefer self.allocator.free(key);

            topic.* = Topic.init(self.allocator, self, key);
            try self.topics.put(key, topic);
            try self.notifier.topicCreated(key);
            log.debug("created topic {s}", .{name});
            return topic;
        }

        pub fn publish(self: *Server, topic_name: []const u8, data: []const u8) !void {
            const topic = try self.getOrCreateTopic(topic_name);
            try topic.publish(data);
        }

        pub fn multiPublish(self: *Server, topic_name: []const u8, msgs: u32, data: []const u8) !void {
            const topic = try self.getOrCreateTopic(topic_name);
            try topic.multiPublish(msgs, data);
        }

        pub fn deferredPublish(self: *Server, topic_name: []const u8, data: []const u8, delay: u32) !void {
            const topic = try self.getOrCreateTopic(topic_name);
            try topic.deferredPublish(data, delay);
        }

        pub fn deinit(self: *Server) void {
            var iter = self.topics.iterator();
            while (iter.next()) |e| self.deinitTopic(e.value_ptr.*);
            self.topics.deinit();
        }

        fn deinitTopic(self: *Server, topic: *Topic) void {
            const key = topic.name;
            topic.deinit();
            self.notifier.topicDeleted(key) catch {};
            self.allocator.free(key);
            self.allocator.destroy(topic);
        }

        pub fn stopTimers(self: *Server) !void {
            var ti = self.topics.valueIterator();
            while (ti.next()) |topic| {
                var ci = topic.*.channels.valueIterator();
                while (ci.next()) |channel| {
                    try channel.*.timer.close();
                }
            }
        }

        fn TopicType() type {
            return struct {
                allocator: mem.Allocator,
                name: []const u8,
                server: *Server,
                channels: std.StringHashMap(*Channel),
                sequence: u64 = 0,
                first: ?*TopicMsg = null, // starting point for first channel
                last: ?*TopicMsg = null, // linked list of topic messages
                depth: u64 = 0,
                fin_count: usize = 0, // fin's in channels
                paused: bool = false,

                pub fn init(allocator: mem.Allocator, server: *Server, name: []const u8) Topic {
                    return .{
                        .allocator = allocator,
                        .name = name,
                        .channels = std.StringHashMap(*Channel).init(allocator),
                        .server = server,
                    };
                }

                fn sub(self: *Topic, consumer: *Consumer, name: []const u8) !*Channel {
                    if (self.channels.get(name)) |channel| {
                        try channel.sub(consumer);
                        return channel;
                    }
                    var channel = try self.createChannel(name);
                    try channel.sub(consumer);

                    // First channel gets all messages from the topic
                    if (self.channels.count() == 1) {
                        channel.next = self.first;
                        channel.depth = self.depth;
                        self.first = null;
                        self.depth = 0;
                    }
                    return channel;
                }

                fn createChannel(self: *Topic, name: []const u8) !*Channel {
                    const channel = try self.allocator.create(Channel);
                    errdefer self.allocator.destroy(channel);
                    const key = try self.allocator.dupe(u8, name);
                    errdefer self.allocator.free(key);

                    channel.* = Channel.init(self.allocator, self, key);
                    channel.initTimer(self.server.io);
                    try self.server.notifier.channelCreated(self.name, channel.name);
                    try self.channels.put(key, channel);

                    log.debug("topic '{s}' channel '{s}' created", .{ self.name, key });
                    return channel;
                }

                fn deinit(self: *Topic) void {
                    var iter = self.channels.iterator();
                    while (iter.next()) |e| self.deinitChannel(e.value_ptr.*);
                    self.channels.deinit();
                    if (self.last) |l| l.release();
                }

                fn deinitChannel(self: *Topic, channel: *Channel) void {
                    const key = channel.name;
                    self.server.notifier.channelDeleted(self.name, channel.name) catch {};
                    channel.deinit();
                    self.allocator.free(key);
                    self.allocator.destroy(channel);
                }

                pub fn publish(self: *Topic, data: []const u8) !void {
                    const msg = try self.append(data);
                    try self.notifyChannels(msg, 1);
                }

                pub fn deferredPublish(self: *Topic, data: []const u8, delay: u32) !void {
                    var msg = try self.append(data);
                    msg.defer_until = self.server.io.now() + nsFromMs(delay);
                    try self.notifyChannels(msg, 1);
                }

                pub fn multiPublish(self: *Topic, msgs: u32, data: []const u8) !void {
                    var pos: usize = 0;
                    var first: ?*TopicMsg = null;
                    for (0..msgs) |_| {
                        const len = mem.readInt(u32, data[pos..][0..4], .big);
                        pos += 4;
                        const msg = try self.append(data[pos..][0..len]);
                        if (first == null) first = msg;
                        pos += len;
                    }
                    try self.notifyChannels(first.?, msgs);
                }

                fn append(self: *Topic, data: []const u8) !*TopicMsg {
                    // Allocate message and body
                    const msg = try self.allocator.create(TopicMsg);
                    errdefer self.allocator.destroy(msg);
                    const body = try self.allocator.dupe(u8, data);
                    errdefer self.allocator.free(body);

                    // Init message
                    self.sequence += 1;
                    msg.* = .{
                        .allocator = self.allocator,
                        .sequence = self.sequence,
                        .created_at = self.server.io.now(),
                        .body = body,
                        .rc = 0,
                    };

                    // Update last pointer
                    if (self.last) |prev| {
                        assert(prev.sequence + 1 == msg.sequence);
                        self.last = msg.acquire();
                        prev.next = msg.acquire(); // Previous points to new
                        prev.release(); // Decrement previous reference count
                    } else {
                        self.last = msg.acquire();
                    }

                    // If there is no channels messages are accumulated in
                    // topic. We need to hold pointer to the first message (to
                    // prevent release).
                    if (self.channels.count() == 0 and self.first == null) self.first = msg.acquire();
                    if (self.first != null) self.depth += 1;

                    return msg;
                }

                // Notify all channels that there is pending messages
                fn notifyChannels(self: *Topic, next: *TopicMsg, msgs: u32) !void {
                    var iter = self.channels.valueIterator();
                    while (iter.next()) |ptr| {
                        const channel = ptr.*;
                        try channel.topicAppended(next);
                        channel.depth += msgs;
                    }
                }

                fn deleteChannel(self: *Topic, name: []const u8) !void {
                    const kv = self.channels.fetchRemove(name) orelse return error.NotFound;
                    self.deinitChannel(kv.value);
                }

                fn pause(self: *Topic, paused: bool) !void {
                    if (self.paused == paused) return;
                    self.paused = paused;
                    if (!self.paused) { // wake-up all channels
                        var iter = self.channels.valueIterator();
                        while (iter.next()) |ptr| try ptr.*.wakeup();
                    }
                }

                fn empty(self: *Topic) !void {
                    if (self.first) |first| {
                        first.release();
                        self.first = null;
                        self.depth = 0;
                    }
                }

                fn pauseChannel(self: *Topic, name: []const u8, paused: bool) !void {
                    const channel = self.channels.get(name) orelse return error.NotFound;
                    try channel.pause(paused);
                }

                fn emptyChannel(self: *Topic, name: []const u8) !void {
                    const channel = self.channels.get(name) orelse return error.NotFound;
                    try channel.empty();
                }
            };
        }

        pub fn ChannelType() type {
            return struct {
                allocator: mem.Allocator,
                name: []const u8,
                topic: *Topic,
                timer: Io.Timer = undefined,
                consumers: std.ArrayList(*Consumer),
                // Sent but not jet acknowledged (fin) messages.
                in_flight: std.AutoArrayHashMap(u64, *ChannelMsg),
                // Re-queued by consumer, timed out or defer published messages.
                deferred: std.PriorityQueue(*ChannelMsg, void, ChannelMsg.less),
                // Pointer to the next message in the topic, null if we reached
                // end of the topic.
                next: ?*TopicMsg = null,
                // Number of messages to be delivered.
                depth: u64 = 0,
                // Round robin consumers iterator. Preserves last consumer index
                // between init's.
                iterator: ConsumersIterator = .{},
                stat: struct {
                    // number of messages
                    pull: usize = 0, // pulled from topic
                    send: usize = 0, // sent to the client
                    finish: usize = 0, // fin by the client
                    timeout: usize = 0, // time outed while in flight
                    requeue: usize = 0, // req by the client
                } = .{},
                paused: bool = false,

                fn init(allocator: mem.Allocator, topic: *Topic, name: []const u8) Channel {
                    return .{
                        .allocator = allocator,
                        .name = name,
                        .topic = topic,
                        .consumers = std.ArrayList(*Consumer).init(allocator),
                        .in_flight = std.AutoArrayHashMap(u64, *ChannelMsg).init(allocator),
                        .deferred = std.PriorityQueue(*ChannelMsg, void, ChannelMsg.less).init(allocator, {}),
                        // Channel starts at the last topic message at the moment of channel creation
                        .next = null,
                    };
                }

                fn initTimer(self: *Channel, io: *Io) void {
                    self.timer = io.initTimer(self, timerTimeout);
                }

                fn topicAppended(self: *Channel, msg: *TopicMsg) !void {
                    if (self.next == null) {
                        self.next = msg.acquire();
                        try self.wakeup();
                    }
                }

                fn wakeup(self: *Channel) !void {
                    var iter = self.consumersIterator();
                    while (iter.next()) |consumer|
                        if (!try self.fillConsumer(consumer)) break;
                }

                // Returns true if there is more messages for next consumer
                fn fillConsumer(self: *Channel, consumer: *Consumer) !bool {
                    var msgs_buf: [max_msgs_send_batch_size]*ChannelMsg = undefined;
                    var msgs = msgs_buf[0..@min(consumer.ready(), msgs_buf.len)];
                    var n: usize = 0;
                    while (n < msgs.len) : (n += 1) {
                        if (self.getMsg() catch null) |msg| {
                            msg.in_flight_socket = consumer.socket;
                            try self.inFlightAppend(msg, nsFromMs(consumer.msgTimeout()));
                            msgs[n] = msg;
                        } else break;
                    }
                    if (n == 0) return false;
                    consumer.sendMsgs(msgs[0..n]) catch |err| {
                        log.err("failed to send msg {}, re-queuing", .{err});
                        for (msgs[0..n]) |msg|
                            assert(self.req(msg.id(), 0) catch false);
                        return false;
                    };
                    return n == msgs.len;
                }

                fn inFlightAppend(self: *Channel, msg: *ChannelMsg, msg_timeout: u64) !void {
                    self.stat.send += 1;
                    msg.timestamp = self.timer.now() + msg_timeout;
                    try self.setTimeout(msg.timestamp);
                    try self.in_flight.put(msg.sequence(), msg);
                }

                /// Sets next timer timeout
                fn setTimeout(self: *Channel, next_timeout: u64) !void {
                    try self.timer.set(next_timeout);
                }

                /// Callback when timer timeout if fired
                fn timerTimeout(self: *Channel) Error!void {
                    const now = self.timer.now();
                    const next_timeout = @min(
                        try self.inFlightTimeout(now),
                        try self.deferredTimeout(now),
                    );
                    log.debug("timerTimeout next: {}", .{next_timeout});
                    try self.setTimeout(next_timeout);
                }

                /// Returns next timeout of deferred messages
                fn deferredTimeout(self: *Channel, now: u64) !u64 {
                    var timeout: u64 = Io.Timer.no_timeout;
                    if (self.deferred.count() > 0) {
                        try self.wakeup();
                        if (self.deferred.peek()) |msg| {
                            if (msg.timestamp > now and msg.timestamp < timeout) timeout = msg.timestamp;
                        }
                    }
                    return timeout;
                }

                /// Finds time-outed in flight messages and move them to the
                /// deferred queue.
                /// Returns next timeout for in flight messages.
                fn inFlightTimeout(self: *Channel, now: u64) !u64 {
                    var msgs = std.ArrayList(*ChannelMsg).init(self.allocator);
                    defer msgs.deinit();
                    var timeout: u64 = Io.Timer.no_timeout;
                    for (self.in_flight.values()) |msg| {
                        if (msg.timestamp <= now) {
                            try msgs.append(msg);
                        } else {
                            if (timeout > msg.timestamp) timeout = msg.timestamp;
                        }
                    }
                    for (msgs.items) |msg| {
                        log.debug("{} message timeout {}", .{ msg.in_flight_socket, msg.sequence() });
                        try self.requeue(msg, 0);
                    }
                    self.stat.timeout += msgs.items.len;
                    return timeout;
                }

                // Iterates over ready consumers. Returns null when there is no
                // ready consumers.
                fn consumersIterator(self: *Channel) *ConsumersIterator {
                    self.iterator.init(self.consumers.items);
                    return &self.iterator;
                }

                const ConsumersIterator = struct {
                    consumers: []*Consumer = undefined,
                    not_ready_count: usize = 0,
                    idx: usize = 0,

                    fn init(self: *ConsumersIterator, consumers: []*Consumer) void {
                        self.consumers = consumers;
                        self.not_ready_count = 0;
                    }

                    fn next(self: *ConsumersIterator) ?*Consumer {
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
                        self.stat.finish += 1;
                        self.depth -= 1;
                        kv.value.deinit(self.allocator);
                        return true;
                    }
                    return false;
                }

                /// Extend message timeout for interval (nanoseconds).
                pub fn touch(self: *Channel, msg_id: [16]u8, interval: u64) !bool {
                    const seq = ChannelMsg.seqFromId(msg_id);
                    if (self.in_flight.get(seq)) |msg| {
                        msg.timestamp += interval;
                        return true;
                    }
                    return false;
                }

                pub fn req(self: *Channel, msg_id: [16]u8, delay: u32) !bool {
                    const seq = ChannelMsg.seqFromId(msg_id);
                    if (self.in_flight.get(seq)) |msg| {
                        try self.requeue(msg, delay);
                        self.stat.requeue += 1;
                        return true;
                    }
                    return false;
                }

                fn requeue(self: *Channel, msg: *ChannelMsg, delay: u32) !void {
                    msg.in_flight_socket = 0;
                    msg.incAttempts();
                    msg.timestamp = if (delay == 0) 0 else self.timer.now() + nsFromMs(delay);
                    if (msg.timestamp > 0)
                        try self.setTimeout(msg.timestamp);
                    try self.deferred.add(msg);
                    assert(self.in_flight.swapRemove(msg.sequence()));
                }

                fn sub(self: *Channel, consumer: *Consumer) !void {
                    try self.consumers.append(consumer);
                }

                pub fn unsub(self: *Channel, consumer: *Consumer) !void {
                    // Remove in_flight messages of that consumer
                    outer: while (true) {
                        for (self.in_flight.values()) |msg| {
                            if (msg.in_flight_socket == consumer.socket) {
                                _ = try self.req(msg.id(), 0);
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

                pub fn ready(self: *Channel, consumer: *Consumer) !void {
                    if (consumer.ready() == 0) return;
                    _ = try self.fillConsumer(consumer);
                }

                fn getMsg(self: *Channel) !?*ChannelMsg {
                    const now = self.timer.now();

                    // First look into deferred messages
                    if (self.deferred.peek()) |msg| {
                        if (msg.timestamp <= now) {
                            return self.deferred.remove();
                        }
                    }

                    // Then try to find next message in topic
                    while (self.nextTopicMsg()) |topic_msg| {
                        const msg = try self.allocator.create(ChannelMsg);
                        msg.* = topic_msg.asChannelMsg();
                        self.stat.pull += 1;
                        if (msg.timestamp > now) {
                            try self.deferred.add(msg);
                            try self.setTimeout(msg.timestamp);
                            continue;
                        }
                        return msg;
                    }
                    return null;
                }

                fn nextTopicMsg(self: *Channel) ?*TopicMsg {
                    if (self.paused or self.topic.paused) return null;
                    if (self.next) |msg| {
                        self.next = msg.nextAcquire();
                        return msg;
                    }
                    return null;
                }

                fn pause(self: *Channel, paused: bool) !void {
                    if (self.paused == paused) return;
                    self.paused = paused;
                    if (!self.paused) try self.wakeup();
                }

                fn empty(self: *Channel) !void {
                    for (self.in_flight.values()) |cm| cm.deinit(self.allocator);
                    self.in_flight.clearAndFree();
                    while (self.deferred.removeOrNull()) |cm| cm.deinit(self.allocator);
                    self.depth = 0;
                    if (self.next) |n| n.release();
                    self.next = null;
                }

                fn deinit(self: *Channel) void {
                    self.timer.close() catch {};
                    for (self.consumers.items) |consumer| consumer.channelClosed();
                    for (self.in_flight.values()) |cm| cm.deinit(self.allocator);
                    while (self.deferred.removeOrNull()) |cm| cm.deinit(self.allocator);
                    self.in_flight.deinit();
                    self.deferred.deinit();
                    self.consumers.deinit();
                    if (self.next) |n| n.release();
                    self.next = null;
                }
            };
        }
    };
}

test "channel consumers iterator" {
    const allocator = testing.allocator;

    var io = TestIo{};
    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &io, &notifier);
    defer server.deinit();

    var consumer1 = TestConsumer.init(allocator);
    var channel = try server.sub(&consumer1, "topic", "channel");

    var iter = channel.consumersIterator();
    try testing.expectEqual(&consumer1, iter.next().?);
    try testing.expectEqual(&consumer1, iter.next().?);
    consumer1.ready_count = 0;
    try testing.expectEqual(null, iter.next());

    consumer1.ready_count = 1;
    var consumer2 = TestConsumer.init(allocator);
    var consumer3 = TestConsumer.init(allocator);
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
    const topic_name = "topic";
    const channel_name = "channel";

    var io = TestIo{};
    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &io, &notifier);
    defer server.deinit();

    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();
    consumer.ready_count = 0;
    var channel = try server.sub(&consumer, topic_name, channel_name);

    try server.publish(topic_name, "1");
    try server.publish(topic_name, "2");
    try server.publish(topic_name, "3");

    { // 3 messages in topic, 0 taken by channel
        try testing.expectEqual(3, channel.depth);
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
    }
    { // wakeup without ready consumers
        try channel.wakeup();
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
    }

    { // consumer is ready
        consumer.ready_count = 1;
        try channel.ready(&consumer);
        try testing.expectEqual(1, consumer.lastSeq());
        // 1 is in flight
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(2, channel.next.?.sequence);
    }
    { // consumer sends fin, 0 in flight after that
        try testing.expect(try channel.fin(consumer.lastId())); // fin 1
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(2, channel.depth);
    }
    { // send seq 2, 1 msg in flight
        consumer.ready_count = 1;
        try channel.wakeup();
        try testing.expectEqual(2, consumer.lastSeq());
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(3, channel.next.?.sequence);
    }
    { // send seq 3, 2 msgs in flight
        consumer.ready_count = 1;
        try channel.wakeup();
        try testing.expectEqual(3, consumer.lastSeq());
        try testing.expectEqual(2, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expect(channel.next == null);
    }
    { // 2 is re-queued
        try testing.expect(try channel.req(ChannelMsg.idFromSeq(2), 0)); // req 2
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(1, channel.deferred.count());
    }
    { // out of order fin, fin seq 3 while 2 is still in flight
        try testing.expect(try channel.fin(ChannelMsg.idFromSeq(3))); // fin 3
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(1, channel.deferred.count());
    }
    { // re send 2
        consumer.ready_count = 1;
        try channel.wakeup();
        try testing.expectEqual(2, consumer.lastSeq());
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
    }
    { // fin seq 2, advances window start from sequence 1 to 3
        try testing.expectEqual(1, channel.depth);
        try testing.expect(try channel.fin(ChannelMsg.idFromSeq(2))); // fin 2
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(0, channel.depth);
    }
}

const TestConsumer = struct {
    const Self = @This();

    socket: socket_t = 0,
    channel: ?*TestServer.Channel = null,
    sequences: std.ArrayList(u64) = undefined,
    ready_count: usize = 1,

    fn init(alloc: mem.Allocator) Self {
        return .{ .sequences = std.ArrayList(u64).init(alloc) };
    }

    fn deinit(self: *Self) void {
        self.sequences.deinit();
    }

    fn ready(self: *Self) usize {
        return self.ready_count;
    }

    fn sendMsgs(self: *Self, msgs: []*ChannelMsg) !void {
        for (msgs) |msg| {
            try self.sequences.append(msg.sequence());
            self.ready_count -= 1;
        }
    }

    // send fin for the last received message
    fn fin(self: *Self) !void {
        assert(try self.channel.?.fin(self.lastId()));
    }

    // pull message from channel
    fn pull(self: *Self) !void {
        self.ready_count = 1;
        try self.channel.?.ready(self);
    }

    fn lastSeq(self: *Self) u64 {
        return self.sequences.items[self.sequences.items.len - 1];
    }

    fn lastId(self: *Self) [16]u8 {
        return ChannelMsg.idFromSeq(self.lastSeq());
    }

    // in milliseconds
    fn msgTimeout(_: *Self) u32 {
        return 60000;
    }

    fn channelClosed(self: *Self) void {
        self.channel = null;
    }
};

const TestIo = struct {
    timestamp: u64 = 0,
    const Self = @This();
    pub fn now(self: Self) u64 {
        return self.timestamp;
    }
    pub fn initTimer(
        self: *Self,
        context: anytype,
        comptime _: fn (@TypeOf(context)) Error!void,
    ) TestTimer {
        return .{
            .io = self,
            .context = @intFromPtr(context),
            .callback = undefined,
        };
    }
    pub const Timer = TestTimer;
};

const TestTimer = struct {
    io: *TestIo,
    fire_at: u64 = 0,
    user_data: u64 = 0,

    context: usize = 0,
    callback: *const fn (*TestTimer) Error!void = undefined,

    fn now(self: *TestTimer) u64 {
        return self.io.timestamp;
    }

    fn set(self: *TestTimer, fire_at: u64) !void {
        self.fire_at = fire_at;
    }

    fn close(_: *TestTimer) !void {}

    const no_timeout: u64 = @import("io.zig").Io.Timer.no_timeout;
};

const NoopNotifier = struct {
    const Self = @This();
    fn topicCreated(_: *Self, _: []const u8) !void {}
    fn channelCreated(_: *Self, _: []const u8, _: []const u8) !void {}
    fn topicDeleted(_: *Self, _: []const u8) !void {}
    fn channelDeleted(_: *Self, _: []const u8, _: []const u8) !void {}
};

const TestServer = ServerType(TestConsumer, TestIo, NoopNotifier);

test "multiple channels" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name1 = "channel1";
    const channel_name2 = "channel2";
    const no = 1024;

    var io = TestIo{};
    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &io, &notifier);
    defer server.deinit();

    var c1 = TestConsumer.init(allocator);
    defer c1.deinit();
    c1.channel = try server.sub(&c1, topic_name, channel_name1);
    c1.ready_count = no * 3;

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
    c2.ready_count = no * 2;

    { // two channels on the same topic
        for (0..no) |_|
            try server.publish(topic_name, "another message body");

        try testing.expectEqual(no * 2, c1.sequences.items.len);
        try testing.expectEqual(no, c2.sequences.items.len);
    }

    var c3 = TestConsumer.init(allocator);
    defer c3.deinit();
    c3.channel = try server.sub(&c3, topic_name, channel_name2);
    c3.ready_count = no;

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
            if (c3.sequences.items.len > idx3 and c3.sequences.items[idx3] == seq) {
                idx3 += 1;
                continue;
            }
            if (c2.sequences.items.len > idx2 and c2.sequences.items[idx2] == seq) {
                idx2 += 1;
                continue;
            }
            unreachable;
        }
    }
}

test "first channel gets all messages accumulated in topic" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name = "channel1";
    const no = 16;

    var io = TestIo{};
    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &io, &notifier);
    defer server.deinit();
    // publish messages to the topic which don't have channels created
    for (0..no) |_|
        try server.publish(topic_name, "message body");

    const topic = try server.getOrCreateTopic(topic_name);
    try testing.expectEqual(no, topic.depth);

    // subscribe creates channel
    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();
    const channel = try server.sub(&consumer, topic_name, channel_name);
    consumer.channel = channel;
    try testing.expect(channel.next != null);
    try testing.expectEqual(0, topic.depth);
    try testing.expectEqual(no, channel.depth);

    for (0..no) |i| {
        try testing.expectEqual(no - i, channel.depth);
        try consumer.pull();
        try consumer.fin();
    }
    try testing.expectEqual(0, channel.depth);
}

test "timeout messages" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name = "channel";
    const no = 4;

    var io = TestIo{};
    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &io, &notifier);
    defer server.deinit();

    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();
    const channel = try server.sub(&consumer, topic_name, channel_name);
    consumer.channel = channel;

    for (0..no) |i| {
        io.timestamp = i + 1;
        try server.publish(topic_name, "message body");
        try consumer.pull();
    }
    try testing.expectEqual(4, channel.in_flight.count());
    try testing.expectEqual(4, channel.stat.send);

    { // check expire_at for in flight messages
        for (channel.in_flight.values()) |msg| {
            try testing.expectEqual(1, msg.attempts());
            try testing.expect(msg.timestamp > nsFromMs(consumer.msgTimeout()) and
                msg.timestamp <= nsFromMs(consumer.msgTimeout()) + no);
        }
    }
    const msg_timeout: u64 = 60 * ns_per_s;
    { // expire one message
        const expire_at = try channel.inFlightTimeout(msg_timeout + 1);
        try testing.expectEqual(msg_timeout + 2, expire_at);
        try testing.expectEqual(0, channel.stat.requeue);
        try testing.expectEqual(1, channel.stat.timeout);
        try testing.expectEqual(3, channel.in_flight.count());
        try testing.expectEqual(1, channel.deferred.count());
    }
    { // expire two more
        const expire_at = try channel.inFlightTimeout(msg_timeout + 3);
        try testing.expectEqual(msg_timeout + 4, expire_at);
        try testing.expectEqual(0, channel.stat.requeue);
        try testing.expectEqual(3, channel.stat.timeout);
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(3, channel.deferred.count());
        try testing.expectEqual(0, channel.stat.finish);
    }
    { // fin last one
        try consumer.fin();
        try testing.expectEqual(1, channel.stat.finish);
        try testing.expectEqual(0, channel.in_flight.count());
    }
    { // resend two
        try consumer.pull();
        try consumer.pull();
        try testing.expectEqual(6, channel.stat.send);
        try testing.expectEqual(2, channel.in_flight.count());
        try testing.expectEqual(1, channel.deferred.count());
        for (channel.in_flight.values()) |msg| {
            try testing.expectEqual(2, msg.attempts());
        }
    }
}

test "deferred messages" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name = "channel";

    var io = TestIo{};
    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &io, &notifier);
    defer server.deinit();

    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();
    const channel = try server.sub(&consumer, topic_name, channel_name);
    consumer.channel = channel;

    { // publish two deferred messages, channel puts them into deferred queue
        try server.deferredPublish(topic_name, "message body", 2);
        try server.deferredPublish(topic_name, "message body", 1);
        try testing.expectEqual(2, channel.depth);
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(2, channel.deferred.count());
    }

    { // move now, one is in flight after wakeup
        io.timestamp = ns_per_ms;
        try channel.wakeup();
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(1, channel.deferred.count());
    }
    { // re-queue
        assert(try channel.req(ChannelMsg.idFromSeq(2), 2));
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(2, channel.deferred.count());
    }

    { // move now to deliver both
        io.timestamp = 3 * ns_per_ms;
        consumer.ready_count = 2;
        try channel.wakeup();
        try testing.expectEqual(2, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
    }
}

test "topic pause" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name = "channel";

    var io = TestIo{};
    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &io, &notifier);
    defer server.deinit();
    const topic = try server.getOrCreateTopic(topic_name);

    {
        try server.publish(topic_name, "message 1");
        try server.publish(topic_name, "message 2");
        try testing.expectEqual(2, topic.depth);
    }

    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();
    const channel = try server.sub(&consumer, topic_name, channel_name);
    consumer.channel = channel;

    { // while channel is paused topic messages are not delivered to the channel
        try testing.expect(!channel.paused);
        try server.pauseChannel(topic_name, channel_name, true);
        try testing.expect(channel.paused);

        try consumer.pull();
        try testing.expectEqual(0, channel.in_flight.count());

        // unpause will pull message
        try server.pauseChannel(topic_name, channel_name, false);
        try testing.expectEqual(1, channel.in_flight.count());
    }

    { // same while topic is paused
        try testing.expect(!topic.paused);
        try server.pauseTopic(topic_name, true);
        try testing.expect(topic.paused);

        try consumer.pull();
        try testing.expectEqual(1, channel.in_flight.count());

        // unpause
        try server.pauseTopic(topic_name, false);
        try testing.expectEqual(2, channel.in_flight.count());
    }

    try server.deleteTopic(topic_name);
    try testing.expect(consumer.channel == null);
}

const TestNotifier = struct {
    const Self = @This();
    const Registration = union(enum) { topic: []const u8, channel: struct {
        topic_name: []const u8,
        name: []const u8,
    } };

    registrations: std.ArrayList(Registration),
    fn init(allocator: std.mem.Allocator) Self {
        return .{
            .registrations = std.ArrayList(Registration).init(allocator),
        };
    }
    fn deinit(self: *Self) void {
        self.registrations.deinit();
    }
    fn topicCreated(self: *Self, name: []const u8) !void {
        try self.registrations.append(.{ .topic = name });
    }
    fn channelCreated(self: *Self, topic_name: []const u8, name: []const u8) !void {
        try self.registrations.append(.{ .channel = .{ .topic_name = topic_name, .name = name } });
    }
    fn topicDeleted(self: *Self, name: []const u8) !void {
        for (self.registrations.items, 0..) |reg, i| {
            if (reg == .topic) {
                if (reg.topic.ptr == name.ptr) {
                    _ = self.registrations.swapRemove(i);
                    return;
                }
            }
        }
    }
    fn channelDeleted(self: *Self, topic_name: []const u8, name: []const u8) !void {
        for (self.registrations.items, 0..) |reg, i| {
            if (reg == .channel) {
                if (reg.channel.topic_name.ptr == topic_name.ptr and
                    reg.channel.name.ptr == name.ptr)
                {
                    _ = self.registrations.swapRemove(i);
                    return;
                }
            }
        }
    }
};

test "notifier" {
    const allocator = testing.allocator;

    var io = TestIo{};
    var notifier = TestNotifier.init(allocator);
    defer notifier.deinit();
    const Server = ServerType(TestConsumer, TestIo, TestNotifier);
    var server = Server.init(allocator, &io, &notifier);

    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();
    _ = try server.sub(&consumer, "topic1", "channel1");
    try testing.expectEqual(2, notifier.registrations.items.len);

    _ = try server.sub(&consumer, "topic1", "channel2");
    try testing.expectEqual(3, notifier.registrations.items.len);

    _ = try server.sub(&consumer, "topic2", "channel2");
    try testing.expectEqual(5, notifier.registrations.items.len);

    // test deletes
    server.deinit();
    try testing.expectEqual(0, notifier.registrations.items.len);
}
