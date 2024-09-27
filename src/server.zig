const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const posix = std.posix;
const math = std.math;
const socket_t = posix.socket_t;
const testing = std.testing;

const log = std.log.scoped(.server);
const Error = @import("io.zig").Error;
const validateName = @import("protocol.zig").validateName;

const ns_per_ms = std.time.ns_per_ms;
fn nsFromMs(ms: u32) u64 {
    return @as(u64, @intCast(ms)) * ns_per_ms;
}

pub fn ServerType(Consumer: type, Io: type, Notifier: type) type {
    return struct {
        const Server = @This();
        const Topic = TopicType();
        pub const Channel = ChannelType();

        allocator: mem.Allocator,
        topics: std.StringHashMap(*Topic),
        io: *Io,
        notifier: *Notifier,
        started_at: u64,
        metric: Topic.Metric = .{},
        metric_prev: Topic.Metric = .{},

        // Init/deinit -----------------

        pub fn init(allocator: mem.Allocator, io: *Io, notifier: *Notifier) Server {
            return .{
                .allocator = allocator,
                .topics = std.StringHashMap(*Topic).init(allocator),
                .io = io,
                .notifier = notifier,
                .started_at = io.now(),
            };
        }

        pub fn deinit(self: *Server) void {
            var iter = self.topics.iterator();
            while (iter.next()) |e| self.deinitTopic(e.value_ptr.*);
            self.topics.deinit();
        }

        fn deinitTopic(self: *Server, topic: *Topic) void {
            const key = topic.name;
            topic.deinit();
            self.notifier.topicDeleted(key);
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

        // Publish/subscribe -----------------

        fn getOrCreateTopic(self: *Server, name: []const u8) !*Topic {
            if (self.topics.get(name)) |t| return t;

            const topic = try self.allocator.create(Topic);
            errdefer self.allocator.destroy(topic);
            const key = try self.allocator.dupe(u8, name);
            errdefer self.allocator.free(key);
            try self.topics.ensureUnusedCapacity(1);

            topic.* = Topic.init(self, key);
            self.topics.putAssumeCapacityNoClobber(key, topic);
            self.notifier.topicCreated(key);
            log.debug("created topic {s}", .{name});
            return topic;
        }

        pub fn subscribe(self: *Server, consumer: *Consumer, topic_name: []const u8, channel_name: []const u8) !*Channel {
            const topic = try self.getOrCreateTopic(topic_name);
            return try topic.subscribe(consumer, channel_name);
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

        // Http interface actions -----------------

        pub fn createTopic(self: *Server, name: []const u8) !void {
            _ = try self.getOrCreateTopic(try validateName(name));
        }

        pub fn deleteTopic(self: *Server, name: []const u8) !void {
            const kv = self.topics.fetchRemove(name) orelse return error.NotFound;
            self.deinitTopic(kv.value);
            log.debug("deleted topic {s}", .{name});
        }

        pub fn createChannel(self: *Server, topic_name: []const u8, name: []const u8) !void {
            const topic = try self.getOrCreateTopic(try validateName(topic_name));
            _ = try topic.getOrCreateChannel(try validateName(name));
        }

        pub fn deleteChannel(self: *Server, topic_name: []const u8, name: []const u8) !void {
            const topic = self.topics.get(topic_name) orelse return error.NotFound;
            try topic.deleteChannel(name);
            log.debug("deleted channel {s} on topic {s}", .{ name, topic_name });
        }

        pub fn pauseTopic(self: *Server, name: []const u8) !void {
            const topic = self.topics.get(name) orelse return error.NotFound;
            topic.pause();
            log.debug("paused topic {s}", .{name});
        }

        pub fn unpauseTopic(self: *Server, name: []const u8) !void {
            const topic = self.topics.get(name) orelse return error.NotFound;
            try topic.unpause();
            log.debug("un-paused topic {s}", .{name});
        }

        pub fn pauseChannel(self: *Server, topic_name: []const u8, name: []const u8) !void {
            const topic = self.topics.get(topic_name) orelse return error.NotFound;
            try topic.pauseChannel(name);
            log.debug("paused channel {s} on topic {s}", .{ name, topic_name });
        }

        pub fn unpauseChannel(self: *Server, topic_name: []const u8, name: []const u8) !void {
            const topic = self.topics.get(topic_name) orelse return error.NotFound;
            try topic.unpauseChannel(name);
            log.debug("paused channel {s} on topic {s}", .{ name, topic_name });
        }

        pub fn emptyTopic(self: *Server, name: []const u8) !void {
            const topic = self.topics.get(name) orelse return error.NotFound;
            try topic.empty();
            log.debug("empty topic {s}", .{name});
        }

        pub fn emptyChannel(self: *Server, topic_name: []const u8, name: []const u8) !void {
            const topic = self.topics.get(topic_name) orelse return error.NotFound;
            try topic.emptyChannel(name);
            log.debug("empty channel {s} on topic {s}", .{ name, topic_name });
        }

        // Lookup registrations -----------------

        /// Iterate over topic and channel names.
        pub fn iterateNames(self: *Server, writer: anytype) !void {
            var ti = self.topics.valueIterator();
            while (ti.next()) |topic| {
                try writer.topic(topic.*.name);
                var ci = topic.*.channels.valueIterator();
                while (ci.next()) |channel| {
                    try writer.channel(topic.*.name, channel.*.name);
                }
            }
        }

        pub fn writeMetrics(self: *Server, writer: anytype) !void {
            var ti = self.topics.valueIterator();
            while (ti.next()) |topic_ptr| {
                const topic = topic_ptr.*;
                { // Topic metrics
                    const cur = topic.metric;
                    const prev = topic.metric_prev;
                    const prefix = try std.fmt.allocPrint(self.allocator, "topic.{s}", .{topic.name});
                    defer self.allocator.free(prefix);
                    try writer.gauge(prefix, "depth", cur.depth);
                    try writer.gauge(prefix, "depth_bytes", cur.depth_bytes);
                    try writer.counter(prefix, "message_count", cur.total, prev.total);
                    try writer.counter(prefix, "message_bytes", cur.total_bytes, prev.total_bytes);
                }
                var ci = topic.channels.valueIterator();
                while (ci.next()) |channel_ptr| {
                    // Channel metrics
                    const channel = channel_ptr.*;
                    const cur = channel.metric;
                    const prev = channel.metric_prev;
                    const prefix = try std.fmt.allocPrint(self.allocator, "topic.{s}.channel.{s}", .{ topic.name, channel.name });
                    defer self.allocator.free(prefix);
                    try writer.gauge(prefix, "clients", channel.consumers.items.len);
                    try writer.gauge(prefix, "deferred_count", channel.deferred.count());
                    try writer.gauge(prefix, "in_flight_count", channel.in_flight.count());
                    try writer.gauge(prefix, "depth", cur.depth);
                    try writer.counter(prefix, "message_count", cur.pull, prev.pull);
                    try writer.counter(prefix, "finish_count", cur.finish, prev.finish);
                    try writer.counter(prefix, "timeout_count", cur.timeout, prev.timeout);
                    try writer.counter(prefix, "requeue_count", cur.requeue, prev.requeue);
                    channel.metric_prev = cur;
                }
                topic.metric_prev = topic.metric;
            }
            { // Server metrics (sum of all topics)
                const cur = self.metric;
                const prev = self.metric_prev;
                const prefix = "server";
                try writer.gauge(prefix, "depth", cur.depth);
                try writer.gauge(prefix, "depth_bytes", cur.depth_bytes);
                try writer.counter(prefix, "message_count", cur.total, prev.total);
                try writer.counter(prefix, "message_bytes", cur.total_bytes, prev.total_bytes);
                self.metric_prev = self.metric;
            }
        }

        fn TopicType() type {
            return struct {
                const Msg = struct {
                    sequence: u64,
                    created_at: u64,
                    body: []const u8,
                    // Defer message delivery until timestamp is reached.
                    defer_until: u64 = 0,
                    // Linked list next node pointer
                    next: ?*Msg = null,
                    // Reference counting
                    topic: *Topic,
                    rc: usize = 0,

                    // Decrease reference counter and free
                    pub fn release(self: *Msg) void {
                        assert(self.rc > 0);
                        self.rc -= 1;
                        if (self.rc == 0) {
                            const next = self.next;
                            const topic = self.topic;
                            topic.destroyMessage(self);
                            if (next) |n| {
                                n.release();
                            } else {
                                topic.last = null;
                            }
                        }
                    }

                    // If there is next message it will acquire and return, null if there is no next.
                    pub fn nextAcquire(self: *Msg) ?*Msg {
                        if (self.next) |n| return n.acquire();
                        return null;
                    }

                    // Increase reference counter
                    pub fn acquire(self: *Msg) *Msg {
                        self.rc += 1;
                        return self;
                    }

                    // Convert Topic.Msg to Channel.Msg
                    fn asChannelMsg(self: *Msg) Channel.Msg {
                        var header: [34]u8 = .{0} ** 34;
                        { // Write to header
                            const frame_type = @intFromEnum(@import("protocol.zig").FrameType.message);
                            mem.writeInt(u32, header[0..4], @intCast(self.body.len + 30), .big); // size
                            mem.writeInt(u32, header[4..8], frame_type, .big); // frame type
                            mem.writeInt(u64, header[8..16], self.created_at, .big); // timestamp
                            mem.writeInt(u16, header[16..18], 1, .big); // attempts
                            mem.writeInt(u64, header[26..34], self.sequence, .big); // message id
                        }
                        return .{
                            .msg = self, // Channel.Msg has pointer to Topic.Msg
                            .header = header,
                            .timestamp = self.defer_until,
                        };
                    }
                };

                allocator: mem.Allocator,
                name: []const u8,
                server: *Server,
                channels: std.StringHashMap(*Channel),
                // Topic message sequence, used for message id.
                sequence: u64 = 0,
                // Pointer to the first message when topic has no channels. Null
                // if topic has channels.
                first: ?*Msg = null,
                // Week pointer to the end of linked list of topic messages
                last: ?*Msg = null,
                paused: bool = false,
                metric: Metric = .{},
                metric_prev: Metric = .{},

                const Metric = struct {
                    // Current number of messages in the topic linked list.
                    depth: usize = 0,
                    // Size in bytes of the current messages.
                    depth_bytes: usize = 0,
                    // Total number of messages.
                    total: usize = 0,
                    // Total size of all messages.
                    total_bytes: usize = 0,

                    fn inc(self: *Metric, bytes: usize) void {
                        self.depth +%= 1;
                        self.total +%= 1;
                        self.depth_bytes +%= bytes;
                        self.total_bytes +%= bytes;
                    }
                    fn dec(self: *Metric, bytes: usize) void {
                        self.depth -= 1;
                        self.depth_bytes -|= bytes;
                    }
                };

                pub fn init(server: *Server, name: []const u8) Topic {
                    const allocator = server.allocator;
                    return .{
                        .allocator = allocator,
                        .name = name,
                        .channels = std.StringHashMap(*Channel).init(allocator),
                        .server = server,
                    };
                }

                fn deinit(self: *Topic) void {
                    var iter = self.channels.iterator();
                    while (iter.next()) |e| self.deinitChannel(e.value_ptr.*);
                    self.channels.deinit();
                }

                fn deinitChannel(self: *Topic, channel: *Channel) void {
                    const key = channel.name;
                    self.server.notifier.channelDeleted(self.name, channel.name);
                    channel.deinit();
                    self.allocator.free(key);
                    self.allocator.destroy(channel);
                }

                fn subscribe(self: *Topic, consumer: *Consumer, name: []const u8) !*Channel {
                    const is_first = self.channels.count() == 0;
                    const channel = try self.getOrCreateChannel(name);
                    try channel.subscribe(consumer);
                    if (is_first) { // First channel gets all messages from the topic
                        channel.next = self.first;
                        channel.metric.depth = self.metric.depth;
                        self.first = null;
                    }
                    return channel;
                }

                fn getOrCreateChannel(self: *Topic, name: []const u8) !*Channel {
                    if (self.channels.get(name)) |channel| return channel;

                    const channel = try self.allocator.create(Channel);
                    errdefer self.allocator.destroy(channel);
                    const key = try self.allocator.dupe(u8, name);
                    errdefer self.allocator.free(key);
                    try self.channels.ensureUnusedCapacity(1);

                    channel.* = Channel.init(self, key);
                    channel.initTimer(self.server.io);
                    errdefer channel.deinit();
                    self.channels.putAssumeCapacityNoClobber(key, channel);

                    log.debug("topic '{s}' channel '{s}' created", .{ self.name, key });
                    self.server.notifier.channelCreated(self.name, channel.name);
                    return channel;
                }

                fn publish(self: *Topic, data: []const u8) !void {
                    const msg = try self.append(data);
                    self.notifyChannels(msg, 1);
                }

                fn deferredPublish(self: *Topic, data: []const u8, delay: u32) !void {
                    var msg = try self.append(data);
                    msg.defer_until = self.server.io.now() + nsFromMs(delay);
                    self.notifyChannels(msg, 1);
                }

                fn multiPublish(self: *Topic, msgs: u32, data: []const u8) !void {
                    if (msgs == 0) return;
                    var pos: usize = 0;
                    var first: *Msg = undefined;
                    for (0..msgs) |i| {
                        const len = mem.readInt(u32, data[pos..][0..4], .big);
                        pos += 4;
                        const msg = try self.append(data[pos..][0..len]);
                        if (i == 0) first = msg.acquire();
                        pos += len;
                    }
                    self.notifyChannels(first, msgs);
                    first.release();
                }

                // Create Topic.Msg add it to the linked list or topic messages
                fn append(self: *Topic, data: []const u8) !*Msg {
                    var msg = brk: {
                        // Allocate message and body
                        const msg = try self.allocator.create(Msg);
                        errdefer self.allocator.destroy(msg);
                        const body = try self.allocator.dupe(u8, data);
                        errdefer self.allocator.free(body);

                        // Init message
                        self.sequence += 1;
                        msg.* = .{
                            .topic = self,
                            .sequence = self.sequence,
                            .created_at = self.server.io.now(),
                            .body = body,
                            .rc = 0,
                        };
                        break :brk msg;
                    };
                    self.metric.inc(data.len);
                    self.server.metric.inc(data.len);
                    { // Update last pointer
                        if (self.last) |prev| {
                            assert(prev.sequence + 1 == msg.sequence);
                            prev.next = msg.acquire(); // Previous points to new
                        }
                        self.last = msg;
                    }
                    // If there is no channels messages are accumulated in
                    // topic. We need to hold pointer to the first message (to
                    // prevent release).
                    if (self.channels.count() == 0 and self.first == null) self.first = msg.acquire();

                    return msg;
                }

                fn destroyMessage(self: *Topic, msg: *Msg) void {
                    self.metric.dec(msg.body.len);
                    self.server.metric.dec(msg.body.len);
                    self.allocator.free(msg.body);
                    self.allocator.destroy(msg);
                }

                // Notify all channels that there is pending messages
                fn notifyChannels(self: *Topic, next: *Msg, msgs: u32) void {
                    var iter = self.channels.valueIterator();
                    while (iter.next()) |ptr| {
                        const channel = ptr.*;
                        channel.topicAppended(next);
                        channel.metric.depth += msgs;
                    }
                }

                // Http interface actions -----------------

                fn deleteChannel(self: *Topic, name: []const u8) !void {
                    const kv = self.channels.fetchRemove(name) orelse return error.NotFound;
                    self.deinitChannel(kv.value);
                }

                fn pause(self: *Topic) void {
                    self.paused = true;
                }

                fn unpause(self: *Topic) !void {
                    self.paused = false;
                    // wake-up all channels
                    var iter = self.channels.valueIterator();
                    while (iter.next()) |ptr| try ptr.*.wakeup();
                }

                fn empty(self: *Topic) !void {
                    if (self.first) |first| {
                        first.release();
                        self.first = null;
                        self.metric.depth = 0;
                        self.metric.depth_bytes = 0;
                    }
                }

                fn pauseChannel(self: *Topic, name: []const u8) !void {
                    const channel = self.channels.get(name) orelse return error.NotFound;
                    channel.pause();
                }

                fn unpauseChannel(self: *Topic, name: []const u8) !void {
                    const channel = self.channels.get(name) orelse return error.NotFound;
                    try channel.unpause();
                }

                fn emptyChannel(self: *Topic, name: []const u8) !void {
                    const channel = self.channels.get(name) orelse return error.NotFound;
                    try channel.empty();
                }

                // -----------------
            };
        }

        pub fn ChannelType() type {
            return struct {
                pub const Msg = struct {
                    msg: *Topic.Msg,
                    header: [34]u8,
                    sent_at: u64 = 0,
                    in_flight_socket: socket_t = 0,
                    // Timestamp in nanoseconds. When in flight and timestamp is reached message should
                    // be re-queued. If deferred message should not be delivered until timestamp
                    // is reached.
                    timestamp: u64 = 0,

                    fn destroy(self: *Msg, allocator: mem.Allocator) void {
                        self.msg.release(); // release inner topic message
                        allocator.destroy(self); // destroy this channel message
                    }

                    pub fn body(self: *Msg) []const u8 {
                        return self.msg.body;
                    }

                    fn incAttempts(self: *Msg) void {
                        const buf = self.header[16..18];
                        const v = mem.readInt(u16, buf, .big) +% 1;
                        mem.writeInt(u16, buf, v, .big);
                    }

                    fn sequence(self: Msg) u64 {
                        return self.msg.sequence;
                    }

                    fn attempts(self: Msg) u16 {
                        const buf = self.header[16..18];
                        return mem.readInt(u16, buf, .big);
                    }

                    fn id(self: Msg) [16]u8 {
                        return self.header[18..34].*;
                    }

                    fn less(_: void, a: *Msg, b: *Msg) math.Order {
                        if (a.timestamp != b.timestamp)
                            return math.order(a.timestamp, b.timestamp);
                        return math.order(a.sequence(), b.sequence());
                    }

                    test incAttempts {
                        var t = Topic.Msg{
                            .topic = undefined,
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

                allocator: mem.Allocator,
                name: []const u8,
                topic: *Topic,
                timer: Io.Timer = undefined,
                consumers: std.ArrayList(*Consumer),
                // Sent but not jet acknowledged (fin) messages.
                in_flight: std.AutoArrayHashMap(u64, *Msg),
                // Re-queued by consumer, timed-out or defer published messages.
                deferred: std.PriorityQueue(*Msg, void, Msg.less),
                // Pointer to the next message in the topic, null if we reached
                // end of the topic.
                next: ?*Topic.Msg = null,
                // Round robin consumers iterator. Preserves last consumer index
                // between init's.
                iterator: ConsumersIterator = .{},
                metric: Metric = .{},
                metric_prev: Metric = .{},
                paused: bool = false,

                const Metric = struct {
                    // Total number of messages ...
                    // ... pulled from topic
                    pull: usize = 0, //  counter
                    // ... delivered to the client(s)
                    finish: usize = 0, // counter
                    // ... time-outed while in flight.
                    timeout: usize = 0, // counter
                    // ... re-queued by the client.
                    requeue: usize = 0, //  counter
                    // Current number of messages published to the topic but not processed
                    // by this channel.
                    depth: usize = 0, // gauge
                };

                // Init/deinit -----------------

                fn init(topic: *Topic, name: []const u8) Channel {
                    const allocator = topic.allocator;
                    return .{
                        .allocator = allocator,
                        .name = name,
                        .topic = topic,
                        .consumers = std.ArrayList(*Consumer).init(allocator),
                        .in_flight = std.AutoArrayHashMap(u64, *Msg).init(allocator),
                        .deferred = std.PriorityQueue(*Msg, void, Msg.less).init(allocator, {}),
                        // Channel starts at the last topic message at the moment of channel creation
                        .next = null,
                    };
                }

                // Need stable self pointer for this part of the init.
                fn initTimer(self: *Channel, io: *Io) void {
                    self.timer = io.initTimer(self, timerTimeout);
                }

                fn deinit(self: *Channel) void {
                    self.timer.close() catch {};
                    for (self.consumers.items) |consumer| consumer.channelClosed();
                    for (self.in_flight.values()) |cm| cm.destroy(self.allocator);
                    while (self.deferred.removeOrNull()) |cm| cm.destroy(self.allocator);
                    self.in_flight.deinit();
                    self.deferred.deinit();
                    self.consumers.deinit();
                    if (self.next) |n| n.release();
                    self.next = null;
                }

                fn subscribe(self: *Channel, consumer: *Consumer) !void {
                    try self.consumers.append(consumer);
                }

                // -----------------

                // Called from topic when new topic message is created.
                fn topicAppended(self: *Channel, msg: *Topic.Msg) void {
                    if (self.next == null) {
                        self.next = msg.acquire();
                        self.wakeup() catch |err| {
                            log.err("fail to wakeup channel {s}: {}", .{ self.name, err });
                        };
                    }
                }

                // Try to push messages to the consumers.
                fn wakeup(self: *Channel) !void {
                    const cnt = self.consumers.items.len;
                    if (cnt == 0) return;
                    if (cnt == 1) {
                        _ = try self.fillConsumer(self.consumers.items[0]);
                        return;
                    }
                    var iter = self.consumersIterator();
                    while (iter.next()) |consumer|
                        if (!try self.fillConsumer(consumer)) break;
                }

                // Returns true if there are, probably, more ready messages for next consumer
                fn fillConsumer(self: *Channel, consumer: *Consumer) !bool {
                    const max = consumer.ready();
                    if (max == 0) return true;
                    var n: u32 = 0;
                    while (n < max) : (n += 1) {
                        var msg = (self.nextMsg(consumer.msgTimeout()) catch null) orelse break;
                        msg.sent_at = self.timer.now();
                        msg.in_flight_socket = consumer.socket;
                        try consumer.prepareSend(&msg.header, msg.msg.body, n);
                    }
                    if (n == 0) return false;
                    try consumer.sendPrepared(n);
                    return n == max;
                }

                // Get next message to push to some consumer
                // msg_timeout - consumer message timeout in ms
                fn nextMsg(self: *Channel, msg_timeout: u32) !?*Msg {
                    if (self.paused) return null;
                    const now = self.timer.now();

                    // First look into deferred messages
                    if (self.deferred.peek()) |msg| if (msg.timestamp <= now) {
                        try self.inFlightAppend(msg, msg_timeout);
                        _ = self.deferred.remove();
                        return msg;
                    };

                    if (self.topic.paused) return null;

                    // Then try to find next message in topic
                    while (true) {
                        if (self.next) |topic_msg| {
                            // Allocate and convert Topic.Msg to Channel.Msg
                            const msg = try self.allocator.create(Msg);
                            errdefer self.allocator.destroy(msg);
                            msg.* = topic_msg.asChannelMsg();

                            const deferred = msg.timestamp > now;
                            if (deferred) {
                                try self.setTimeout(msg.timestamp);
                                try self.deferred.add(msg);
                            } else {
                                try self.inFlightAppend(msg, msg_timeout);
                            }
                            // Move topic pointer when nothing can fail any more
                            self.next = topic_msg.nextAcquire();
                            self.metric.pull += 1;
                            if (!deferred) return msg;
                        }
                        return null;
                    }
                }

                fn inFlightAppend(self: *Channel, msg: *Msg, msg_timeout: u32) !void {
                    msg.timestamp = self.timer.now() + nsFromMs(msg_timeout);
                    try self.setTimeout(msg.timestamp);
                    try self.in_flight.put(msg.sequence(), msg);
                }

                // Move message from in_flight to the deferred
                fn deffer(self: *Channel, msg: *Msg, delay: u32) !void {
                    msg.in_flight_socket = 0;
                    msg.incAttempts();
                    msg.timestamp = if (delay == 0) 0 else self.timer.now() + nsFromMs(delay);
                    if (msg.timestamp > 0)
                        try self.setTimeout(msg.timestamp);
                    try self.deferred.add(msg);
                    assert(self.in_flight.swapRemove(msg.sequence()));
                }

                // Sets next timer timeout
                fn setTimeout(self: *Channel, next_timeout: u64) !void {
                    try self.timer.set(next_timeout);
                }

                // Callback when timer timeout if fired
                fn timerTimeout(self: *Channel) Error!void {
                    const now = self.timer.now();
                    const next_timeout = @min(
                        try self.inFlightTimeout(now),
                        try self.deferredTimeout(now),
                    );
                    log.debug("timerTimeout next: {}", .{next_timeout});
                    try self.setTimeout(next_timeout);
                }

                // Returns next timeout of deferred messages
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

                // Finds time-outed in flight messages and move them to the
                // deferred queue.
                // Returns next timeout for in flight messages.
                fn inFlightTimeout(self: *Channel, now: u64) !u64 {
                    var msgs = std.ArrayList(*Msg).init(self.allocator);
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
                        try self.deffer(msg, 0);
                    }
                    self.metric.timeout += msgs.items.len;
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

                // Consumer interface actions -----------------

                pub fn finish(self: *Channel, msg_id: [16]u8) !bool {
                    const seq = Msg.seqFromId(msg_id);
                    if (self.in_flight.fetchSwapRemove(seq)) |kv| {
                        self.metric.finish += 1;
                        self.metric.depth -= 1;
                        kv.value.destroy(self.allocator);
                        return true;
                    }
                    return false;
                }

                /// Extend message timeout for interval (nanoseconds).
                pub fn touch(self: *Channel, msg_id: [16]u8, interval: u64) !bool {
                    const seq = Msg.seqFromId(msg_id);
                    if (self.in_flight.get(seq)) |msg| {
                        msg.timestamp += interval;
                        return true;
                    }
                    return false;
                }

                pub fn requeue(self: *Channel, msg_id: [16]u8, delay: u32) !bool {
                    const seq = Msg.seqFromId(msg_id);
                    if (self.in_flight.get(seq)) |msg| {
                        try self.deffer(msg, delay);
                        self.metric.requeue += 1;
                        return true;
                    }
                    return false;
                }

                pub fn unsubscribe(self: *Channel, consumer: *Consumer) !void {
                    { // Remove in_flight messages of that consumer
                        var msgs = std.ArrayList(*Msg).init(self.allocator);
                        defer msgs.deinit();
                        for (self.in_flight.values()) |msg| {
                            if (msg.in_flight_socket == consumer.socket)
                                try msgs.append(msg);
                        }
                        for (msgs.items) |msg| try self.deffer(msg, 0);
                        self.metric.requeue += msgs.items.len;
                    }

                    // Remove consumer
                    for (self.consumers.items, 0..) |item, i| {
                        if (item == consumer) {
                            _ = self.consumers.swapRemove(i);
                            return;
                        }
                    }
                }

                /// Consumer calls this when ready to send more messages.
                pub fn ready(self: *Channel, consumer: *Consumer) !void {
                    _ = try self.fillConsumer(consumer);
                }

                // Http admin interface  -----------------

                fn pause(self: *Channel) void {
                    self.paused = true;
                }

                fn unpause(self: *Channel) !void {
                    self.paused = false;
                    try self.wakeup();
                }

                fn empty(self: *Channel) !void {
                    { // Remove in-flight messages that are not currently in the kernel.
                        var msgs = std.ArrayList(*Msg).init(self.allocator);
                        defer msgs.deinit();
                        for (self.consumers.items) |consumer| { // For each consumer
                            for (self.in_flight.values()) |cm| {
                                if (cm.in_flight_socket == consumer.socket and // If that messages is in-flight on that consumer
                                    cm.sent_at < consumer.sent_at) // And send is returned from kernel
                                    try msgs.append(cm);
                            }
                        }
                        if (msgs.items.len == self.in_flight.count()) { // Hopefully there is no messages in the kernel
                            // Remove all
                            for (self.in_flight.values()) |cm| cm.destroy(self.allocator);
                            self.in_flight.clearAndFree();
                        } else {
                            // Selectively remove
                            for (msgs.items) |cm| {
                                assert(self.in_flight.swapRemove(cm.sequence()));
                                cm.destroy(self.allocator);
                            }
                        }
                    }
                    // Remove all deferred messages.
                    while (self.deferred.removeOrNull()) |cm| cm.destroy(self.allocator);
                    // Position to the end of the topic.
                    if (self.next) |n| n.release();
                    self.next = null;
                    self.metric.depth = self.in_flight.count();
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
    var channel = try server.subscribe(&consumer1, "topic", "channel");

    var iter = channel.consumersIterator();
    try testing.expectEqual(&consumer1, iter.next().?);
    try testing.expectEqual(&consumer1, iter.next().?);
    consumer1.ready_count = 0;
    try testing.expectEqual(null, iter.next());

    consumer1.ready_count = 1;
    var consumer2 = TestConsumer.init(allocator);
    var consumer3 = TestConsumer.init(allocator);
    channel = try server.subscribe(&consumer2, "topic", "channel");
    channel = try server.subscribe(&consumer3, "topic", "channel");
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
    const idFromSeq = TestServer.Channel.Msg.idFromSeq;
    defer server.deinit();

    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();
    consumer.ready_count = 0;
    var channel = try server.subscribe(&consumer, topic_name, channel_name);

    try server.publish(topic_name, "1");
    try server.publish(topic_name, "2");
    try server.publish(topic_name, "3");

    { // 3 messages in topic, 0 taken by channel
        try testing.expectEqual(3, channel.metric.depth);
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
        try testing.expect(try channel.finish(consumer.lastId())); // fin 1
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(2, channel.metric.depth);
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
        try testing.expect(try channel.requeue(idFromSeq(2), 0)); // req 2
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(1, channel.deferred.count());
    }
    { // out of order fin, fin seq 3 while 2 is still in flight
        try testing.expect(try channel.finish(idFromSeq(3))); // fin 3
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
    { // fin seq 2
        try testing.expectEqual(1, channel.metric.depth);
        try testing.expect(try channel.finish(idFromSeq(2))); // fin 2
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(0, channel.metric.depth);
    }
    { // consumer unsubscribe re-queues in-flight messages
        try server.publish(topic_name, "4");
        consumer.ready_count = 1;
        try channel.wakeup();
        try testing.expectEqual(1, channel.in_flight.count());
        try channel.unsubscribe(&consumer);
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(1, channel.deferred.count());
        try testing.expectEqual(0, channel.consumers.items.len);
    }
}

const TestConsumer = struct {
    const Self = @This();

    socket: socket_t = 42,
    channel: ?*TestServer.Channel = null,
    sequences: std.ArrayList(u64) = undefined,
    ready_count: usize = 1,
    sent_at: u64 = 0,

    fn init(alloc: mem.Allocator) Self {
        return .{ .sequences = std.ArrayList(u64).init(alloc) };
    }

    fn deinit(self: *Self) void {
        self.sequences.deinit();
    }

    fn ready(self: *Self) usize {
        return self.ready_count;
    }

    fn prepareSend(self: *Self, header: []const u8, body: []const u8, msg_no: u32) !void {
        const sequence = mem.readInt(u64, header[26..34], .big);
        self.sequences.append(sequence) catch unreachable;
        _ = msg_no;
        _ = body;
    }

    fn sendPrepared(self: *Self, msgs: u32) !void {
        self.ready_count -= msgs;
    }

    // send fin for the last received message
    fn fin(self: *Self) !void {
        assert(try self.channel.?.finish(self.lastId()));
    }

    // pull message from channel
    fn pull(self: *Self) !void {
        self.ready_count = 1;
        try self.channel.?.ready(self);
    }

    fn pullAndFin(self: *Self) !void {
        try self.pull();
        try self.fin();
    }

    fn lastSeq(self: *Self) u64 {
        return self.sequences.items[self.sequences.items.len - 1];
    }

    fn lastId(self: *Self) [16]u8 {
        return TestServer.Channel.Msg.idFromSeq(self.lastSeq());
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
    call_count: usize = 0,
    const Self = @This();
    fn topicCreated(self: *Self, _: []const u8) void {
        self.call_count += 1;
    }
    fn channelCreated(self: *Self, _: []const u8, _: []const u8) void {
        self.call_count += 1;
    }
    fn topicDeleted(self: *Self, _: []const u8) void {
        self.call_count += 1;
    }
    fn channelDeleted(self: *Self, _: []const u8, _: []const u8) void {
        self.call_count += 1;
    }
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
    c1.channel = try server.subscribe(&c1, topic_name, channel_name1);
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
    c2.channel = try server.subscribe(&c2, topic_name, channel_name2);
    c2.ready_count = no * 2;

    { // two channels on the same topic
        for (0..no) |_|
            try server.publish(topic_name, "another message body");

        try testing.expectEqual(no * 2, c1.sequences.items.len);
        try testing.expectEqual(no, c2.sequences.items.len);
    }

    var c3 = TestConsumer.init(allocator);
    defer c3.deinit();
    c3.channel = try server.subscribe(&c3, topic_name, channel_name2);
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
    try testing.expectEqual(no, topic.metric.depth);

    // subscribe creates channel
    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();
    const channel = try server.subscribe(&consumer, topic_name, channel_name);
    consumer.channel = channel;
    try testing.expect(channel.next != null);
    try testing.expectEqual(no, topic.metric.depth);
    try testing.expectEqual(no, channel.metric.depth);

    for (0..no) |i| {
        try testing.expectEqual(no - i, channel.metric.depth);
        try consumer.pull();
        try consumer.fin();
    }
    try testing.expectEqual(0, channel.metric.depth);
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
    const channel = try server.subscribe(&consumer, topic_name, channel_name);
    consumer.channel = channel;

    for (0..no) |i| {
        io.timestamp = i + 1;
        try server.publish(topic_name, "message body");
        try consumer.pull();
    }
    try testing.expectEqual(4, channel.in_flight.count());

    { // check expire_at for in flight messages
        for (channel.in_flight.values()) |msg| {
            try testing.expectEqual(1, msg.attempts());
            try testing.expect(msg.timestamp > nsFromMs(consumer.msgTimeout()) and
                msg.timestamp <= nsFromMs(consumer.msgTimeout()) + no);
        }
    }
    const ns_per_s = std.time.ns_per_s;
    const msg_timeout: u64 = 60 * ns_per_s;
    { // expire one message
        const expire_at = try channel.inFlightTimeout(msg_timeout + 1);
        try testing.expectEqual(msg_timeout + 2, expire_at);
        try testing.expectEqual(0, channel.metric.requeue);
        try testing.expectEqual(1, channel.metric.timeout);
        try testing.expectEqual(3, channel.in_flight.count());
        try testing.expectEqual(1, channel.deferred.count());
    }
    { // expire two more
        const expire_at = try channel.inFlightTimeout(msg_timeout + 3);
        try testing.expectEqual(msg_timeout + 4, expire_at);
        try testing.expectEqual(0, channel.metric.requeue);
        try testing.expectEqual(3, channel.metric.timeout);
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(3, channel.deferred.count());
        try testing.expectEqual(0, channel.metric.finish);
    }
    { // fin last one
        try consumer.fin();
        try testing.expectEqual(1, channel.metric.finish);
        try testing.expectEqual(0, channel.in_flight.count());
    }
    { // resend two
        try consumer.pull();
        try consumer.pull();
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
    const channel = try server.subscribe(&consumer, topic_name, channel_name);
    consumer.channel = channel;

    { // publish two deferred messages, channel puts them into deferred queue
        try server.deferredPublish(topic_name, "message body", 2);
        try server.deferredPublish(topic_name, "message body", 1);
        try testing.expectEqual(2, channel.metric.depth);
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
        assert(try channel.requeue(TestServer.Channel.Msg.idFromSeq(2), 2));
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
        try testing.expectEqual(2, topic.metric.depth);
    }

    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();
    const channel = try server.subscribe(&consumer, topic_name, channel_name);
    consumer.channel = channel;

    { // while channel is paused topic messages are not delivered to the channel
        try testing.expect(!channel.paused);
        try server.pauseChannel(topic_name, channel_name);
        try testing.expect(channel.paused);

        try consumer.pull();
        try testing.expectEqual(0, channel.in_flight.count());

        // unpause will pull message
        try server.unpauseChannel(topic_name, channel_name);
        try testing.expectEqual(1, channel.in_flight.count());
    }

    { // same while topic is paused
        try testing.expect(!topic.paused);
        try server.pauseTopic(topic_name);
        try testing.expect(topic.paused);

        try consumer.pull();
        try testing.expectEqual(1, channel.in_flight.count());

        // unpause
        try server.unpauseTopic(topic_name);
        try testing.expectEqual(2, channel.in_flight.count());
    }

    try server.deleteTopic(topic_name);
    try testing.expect(consumer.channel == null);
}

test "channel empty" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name = "channel";

    var io = TestIo{};
    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &io, &notifier);
    defer server.deinit();

    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();

    const channel = try server.subscribe(&consumer, topic_name, channel_name);
    consumer.channel = channel;

    io.timestamp = 1;
    try server.publish(topic_name, "message 1");
    try server.publish(topic_name, "message 2");

    io.timestamp = 2;
    try testing.expectEqual(2, channel.timer.now());
    try consumer.pull();
    try testing.expectEqual(2, channel.in_flight.count());
    try channel.empty();
    try testing.expectEqual(2, channel.in_flight.count());

    consumer.sent_at = 2;
    try channel.empty();
    try testing.expectEqual(1, channel.in_flight.count());

    consumer.sent_at = 3;
    try channel.empty();
    try testing.expectEqual(0, channel.in_flight.count());
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
    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &io, &notifier);

    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();
    _ = try server.subscribe(&consumer, "topic1", "channel1");
    try testing.expectEqual(2, notifier.call_count);

    _ = try server.subscribe(&consumer, "topic1", "channel2");
    try testing.expectEqual(3, notifier.call_count);

    _ = try server.subscribe(&consumer, "topic2", "channel2");
    try testing.expectEqual(5, notifier.call_count);

    {
        var writer = @import("lookup.zig").RegistrationsWriter.init(testing.allocator);
        try server.iterateNames(&writer);
        const buf = try writer.toOwned();
        defer testing.allocator.free(buf);

        try testing.expectEqualStrings(
            \\REGISTER topic2
            \\REGISTER topic2 channel2
            \\REGISTER topic1
            \\REGISTER topic1 channel2
            \\REGISTER topic1 channel1
            \\
        , buf);
    }
    // test deletes
    server.deinit();
    try testing.expectEqual(10, notifier.call_count);
}

test "depth" {
    const allocator = testing.allocator;
    const topic_name = "topic";

    var io = TestIo{};
    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &io, &notifier);
    defer server.deinit();

    var consumer1 = TestConsumer.init(allocator);
    defer consumer1.deinit();
    consumer1.channel = try server.subscribe(&consumer1, topic_name, "channel1");

    var consumer2 = TestConsumer.init(allocator);
    defer consumer2.deinit();
    consumer2.channel = try server.subscribe(&consumer2, topic_name, "channel2");

    const channel1 = consumer1.channel.?;
    const channel2 = consumer2.channel.?;
    const topic = channel1.topic;

    consumer1.ready_count = 0;
    consumer2.ready_count = 0;
    try server.publish(topic_name, "message 1");
    try server.publish(topic_name, "message 2");

    try testing.expectEqual(2, topic.metric.depth);
    try testing.expectEqual(2, channel1.metric.depth);
    try testing.expectEqual(2, channel2.metric.depth);

    try testing.expectEqual(1, channel1.next.?.sequence);
    try testing.expectEqual(1, channel2.next.?.sequence);
    try consumer1.pullAndFin();
    try testing.expectEqual(2, channel1.next.?.sequence);

    try testing.expectEqual(2, topic.metric.depth);
    try testing.expectEqual(1, channel1.metric.depth);
    try testing.expectEqual(2, channel2.metric.depth);

    try consumer2.pullAndFin();
    try testing.expectEqual(1, topic.metric.depth);
    try testing.expectEqual(1, channel1.metric.depth);
    try testing.expectEqual(1, channel2.metric.depth);

    try consumer2.pullAndFin();
    try testing.expectEqual(1, topic.metric.depth);
    try testing.expectEqual(1, channel1.metric.depth);
    try testing.expectEqual(0, channel2.metric.depth);

    try server.publish(topic_name, "message 3");

    try testing.expectEqual(2, topic.metric.depth);
    try testing.expectEqual(2, channel1.metric.depth);
    try testing.expectEqual(1, channel2.metric.depth);

    { // When all pulled: depth to 0, topic.last is null
        try testing.expectEqual(3, topic.last.?.sequence);
        try consumer2.pullAndFin();
        try consumer1.pullAndFin();
        try consumer1.pullAndFin();
        try testing.expectEqual(0, channel1.metric.depth);
        try testing.expectEqual(0, channel2.metric.depth);
        try testing.expectEqual(0, topic.metric.depth);
        try testing.expect(topic.last == null);
    }
}
