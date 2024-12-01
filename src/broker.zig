const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const math = std.math;
const builtin = @import("builtin");
const testing = std.testing;

const log = std.log.scoped(.broker);
const protocol = @import("protocol.zig");
const Options = @import("Options.zig").Broker;
const store = @import("store.zig");
const timer = @import("timer.zig");

pub const MsgId = struct {
    pub fn parse(msg_id: [16]u8) u64 {
        return mem.readInt(u64, msg_id[8..], .big);
    }

    fn from(sequence: u64) [16]u8 {
        var buf: [16]u8 = undefined;
        encode(&buf, sequence);
        return buf;
    }

    fn encode(buf: *[16]u8, sequence: u64) void {
        mem.writeInt(u64, buf[0..8], 0, .big); // 8 bytes, unused
        mem.writeInt(u64, buf[8..16], sequence, .big); // 8 bytes, sequence
    }
};

fn nsFromMs(ms: u32) u64 {
    return @as(u64, @intCast(ms)) * std.time.ns_per_ms;
}
var time: struct {
    now: u64 = 0,

    fn after(self: @This(), delay_ms: u32) u64 {
        return self.now + nsFromMs(delay_ms);
    }
} = .{};

pub fn BrokerType(Consumer: type, Notifier: type) type {
    return struct {
        const Broker = @This();
        const Topic = TopicType();
        pub const Channel = ChannelType();

        allocator: mem.Allocator,
        topics: std.StringHashMap(*Topic),
        notifier: *Notifier,
        options: Options,

        started_at: u64,
        metric: store.Metric = .{},
        metric_prev: store.Metric = .{},

        timer_queue: timer.Queue,

        // Init/deinit -----------------

        pub fn init(
            allocator: mem.Allocator,
            notifier: *Notifier,
            now: u64,
            options: Options,
        ) Broker {
            return .{
                .allocator = allocator,
                .topics = std.StringHashMap(*Topic).init(allocator),
                .notifier = notifier,
                .started_at = now,
                .options = options,
                .metric = .{ .max_mem = options.max_mem },
                .timer_queue = timer.Queue.init(allocator),
            };
        }

        pub fn deinit(self: *Broker) void {
            var iter = self.topics.iterator();
            while (iter.next()) |e| self.deinitTopic(e.value_ptr.*);
            self.topics.deinit();
            self.timer_queue.deinit();
        }

        fn deinitTopic(self: *Broker, topic: *Topic) void {
            const key = topic.name;
            topic.deinit();
            self.allocator.free(key);
            self.allocator.destroy(topic);
        }

        pub fn tick(self: *Broker, ts: u64) !u64 {
            time.now = ts;
            return try self.timer_queue.tick(ts);
        }

        // Publish/subscribe -----------------

        fn getOrCreateTopic(self: *Broker, name: []const u8) !*Topic {
            if (self.topics.get(name)) |t| return t;

            try self.notifier.ensureCapacity(name, "");
            const topic = try self.allocator.create(Topic);
            errdefer self.allocator.destroy(topic);
            const key = try self.allocator.dupe(u8, name);
            errdefer self.allocator.free(key);
            try self.topics.ensureUnusedCapacity(1);

            topic.* = Topic.init(self, key);
            topic.timer_op.init(&self.timer_queue, topic, Topic.onTimer);

            self.topics.putAssumeCapacityNoClobber(key, topic);
            self.notifier.topicCreated(key);
            log.debug("created topic {s}", .{name});
            return topic;
        }

        pub fn subscribe(self: *Broker, consumer: *Consumer, topic_name: []const u8, channel_name: []const u8) !void {
            const topic = try self.getOrCreateTopic(topic_name);
            try topic.subscribe(consumer, channel_name);
        }

        pub fn publish(self: *Broker, topic_name: []const u8, data: []const u8) !void {
            const topic = try self.getOrCreateTopic(topic_name);
            try topic.publish(data);
        }

        pub fn multiPublish(self: *Broker, topic_name: []const u8, msgs: u32, data: []const u8) !void {
            const topic = try self.getOrCreateTopic(topic_name);
            try topic.multiPublish(msgs, data);
        }

        pub fn deferredPublish(self: *Broker, topic_name: []const u8, data: []const u8, delay: u32) !void {
            const topic = try self.getOrCreateTopic(topic_name);
            try topic.deferredPublish(data, delay);
        }

        // Http interface actions -----------------

        pub fn createTopic(self: *Broker, name: []const u8) !void {
            _ = try self.getOrCreateTopic(name);
        }

        pub fn deleteTopic(self: *Broker, name: []const u8) !void {
            try self.notifier.ensureCapacity(name, "");
            const kv = self.topics.fetchRemove(name) orelse return error.NotFound;
            const topic = kv.value;
            topic.delete();
            self.deinitTopic(topic);
            self.notifier.topicDeleted(name);
            log.debug("deleted topic {s}", .{name});
        }

        pub fn createChannel(self: *Broker, topic_name: []const u8, name: []const u8) !void {
            const topic = try self.getOrCreateTopic(topic_name);
            _ = try topic.getOrCreateChannel(name);
        }

        pub fn deleteChannel(self: *Broker, topic_name: []const u8, name: []const u8) !void {
            const topic = self.topics.get(topic_name) orelse return error.NotFound;
            try self.notifier.ensureCapacity(topic_name, name);
            try topic.deleteChannel(name);
            self.notifier.channelDeleted(topic_name, name);
            log.debug("deleted channel {s} on topic {s}", .{ name, topic_name });
        }

        pub fn pauseTopic(self: *Broker, name: []const u8) !void {
            const topic = self.topics.get(name) orelse return error.NotFound;
            topic.pause();
            log.debug("paused topic {s}", .{name});
        }

        pub fn unpauseTopic(self: *Broker, name: []const u8) !void {
            const topic = self.topics.get(name) orelse return error.NotFound;
            topic.unpause();
            log.debug("un-paused topic {s}", .{name});
        }

        pub fn pauseChannel(self: *Broker, topic_name: []const u8, name: []const u8) !void {
            const topic = self.topics.get(topic_name) orelse return error.NotFound;
            try topic.pauseChannel(name);
            log.debug("paused channel {s} on topic {s}", .{ name, topic_name });
        }

        pub fn unpauseChannel(self: *Broker, topic_name: []const u8, name: []const u8) !void {
            const topic = self.topics.get(topic_name) orelse return error.NotFound;
            try topic.unpauseChannel(name);
            log.debug("paused channel {s} on topic {s}", .{ name, topic_name });
        }

        pub fn emptyTopic(self: *Broker, name: []const u8) !void {
            const topic = self.topics.get(name) orelse return error.NotFound;
            topic.empty();
            log.debug("empty topic {s}", .{name});
        }

        pub fn emptyChannel(self: *Broker, topic_name: []const u8, name: []const u8) !void {
            const topic = self.topics.get(topic_name) orelse return error.NotFound;
            try topic.emptyChannel(name);
            log.debug("empty channel {s} on topic {s}", .{ name, topic_name });
        }

        // Metrics, dump, restore -----------------

        pub fn writeMetrics(self: *Broker, writer: anytype) !void {
            var ti = self.topics.valueIterator();
            while (ti.next()) |topic_ptr| {
                const topic = topic_ptr.*;
                { // Topic metrics
                    const cur = topic.stream.metric;
                    const prev = topic.metric_prev;
                    const prefix = try std.fmt.allocPrint(self.allocator, "topic.{s}", .{topic.name});
                    defer self.allocator.free(prefix);
                    // nsqd compatibility
                    const depth = if (topic.channels.count() == 0) cur.msgs else 0;
                    try writer.gauge(prefix, "depth", depth);
                    try writer.counter(prefix, "message_count", cur.total_msgs, prev.total_msgs);
                    try writer.counter(prefix, "message_bytes", cur.total_bytes, prev.total_bytes);
                    // nsql specific
                    try writer.gauge(prefix, "msgs", cur.msgs);
                    try writer.gauge(prefix, "bytes", cur.bytes);
                    try writer.gauge(prefix, "capacity", cur.capacity);
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
                topic.metric_prev = topic.stream.metric;
            }
            { // Broker metrics (sum of all topics)
                const cur = self.metric;
                const prev = self.metric_prev;
                const prefix = "broker";
                try writer.gauge(prefix, "msgs", cur.msgs);
                try writer.gauge(prefix, "bytes", cur.bytes);
                try writer.gauge(prefix, "capacity", cur.capacity);
                try writer.counter(prefix, "total_msgs", cur.total_msgs, prev.total_msgs);
                try writer.counter(prefix, "total_bytes", cur.total_bytes, prev.total_bytes);
                self.metric_prev = self.metric;
            }
        }

        const metadata_file_name = "nsql.dump";

        // metadata paused, topic, channel
        pub fn dump(self: *Broker, dir: std.fs.Dir) !void {
            var meta_file = try dir.createFile(metadata_file_name, .{});
            defer meta_file.close();
            var meta_buf_writer = std.io.bufferedWriter(meta_file.writer());
            var meta = meta_buf_writer.writer();
            // Meta file header:
            // | version (1) | topics count (4) |
            try meta.writeByte(0); // version placeholder
            try meta.writeInt(u32, self.topics.count(), .little);

            var ti = self.topics.valueIterator();
            while (ti.next()) |topic_ptr| {
                const topic = topic_ptr.*;
                try topic.publishDeferred();

                var topic_file = try dir.createFile(topic.name, .{});
                defer topic_file.close();
                try topic.stream.dump(topic_file);

                { // Topic meta:
                    // | name_len (1) | name (name_len) | paused (1)  | channels count (4) |
                    try meta.writeByte(@intCast(topic.name.len));
                    try meta.writeAll(topic.name);
                    try meta.writeByte(if (topic.paused) 1 else 0);
                    try meta.writeInt(u32, topic.channels.count(), .little);

                    // For each channel:
                    // | name_len (1) | name (name_len) | paused (1) | messages count (4) | sequence (8) |
                    // then for each message in channel:
                    // | sequence (8) | timestamp (8) | attempts (2)
                    var ci = topic.channels.valueIterator();
                    while (ci.next()) |channel_ptr| {
                        const channel = channel_ptr.*;
                        try meta.writeByte(@intCast(channel.name.len));
                        try meta.writeAll(channel.name);
                        try meta.writeByte(if (channel.paused) 1 else 0);
                        try meta.writeInt(u32, @intCast(channel.in_flight.count() + channel.deferred.count()), .little);
                        try meta.writeInt(u64, channel.sequence, .little);

                        var ifi = channel.in_flight.iterator();
                        while (ifi.next()) |e| {
                            const sequence = e.key_ptr.*;
                            const ifm = e.value_ptr;
                            try meta.writeInt(u64, sequence, .little);
                            try meta.writeInt(u64, 0, .little);
                            try meta.writeInt(u16, ifm.attempts, .little);
                        }
                        var dmi = channel.deferred.iterator();
                        while (dmi.next()) |dm| {
                            try meta.writeInt(u64, dm.sequence, .little);
                            try meta.writeInt(u64, dm.defer_until, .little);
                            try meta.writeInt(u16, dm.attempts, .little);
                        }
                    }
                }
                log.debug("dump topic {s}, channels {} ", .{ topic.name, topic.channels.count() });
            }
            try meta_buf_writer.flush();
        }

        pub fn restore(self: *Broker, dir: std.fs.Dir) !void {
            var meta_file = dir.openFile(metadata_file_name, .{}) catch |err| switch (err) {
                error.FileNotFound => {
                    log.info("dump file {s} not found in data path", .{metadata_file_name});
                    return;
                },
                else => return err,
            };
            defer meta_file.close();
            var meta_buf_reader = std.io.bufferedReader(meta_file.reader());
            var meta = meta_buf_reader.reader();
            var buf: [protocol.max_name_len]u8 = undefined;

            const version = try meta.readByte();
            if (version != 0) return error.InvalidVersion;

            var topics = try meta.readInt(u32, .little);
            while (topics > 0) : (topics -= 1) {
                // Topic meta
                const name_len = try meta.readByte();
                if (name_len > protocol.max_name_len) return error.InvalidName;
                try meta.readNoEof(buf[0..name_len]);
                var topic = try self.getOrCreateTopic(try protocol.validateName(buf[0..name_len]));
                topic.paused = try meta.readByte() != 0;
                var channels = try meta.readInt(u32, .little);

                // Stream
                var topic_file = try dir.openFile(topic.name, .{});
                defer topic_file.close();
                try topic.stream.restore(topic_file);

                // Channels
                while (channels > 0) : (channels -= 1) {
                    // Channel meta
                    const n = try meta.readByte();
                    if (n > protocol.max_name_len) return error.InvalidName;
                    try meta.readNoEof(buf[0..n]);
                    var channel = try topic.getOrCreateChannel(buf[0..n]);
                    channel.paused = try meta.readByte() != 0;
                    var messages = try meta.readInt(u32, .little);
                    try channel.restore(try meta.readInt(u64, .little));

                    // Channel messages
                    while (messages > 0) : (messages -= 1) {
                        const sequence = try meta.readInt(u64, .little);
                        const defer_until = try meta.readInt(u64, .little);
                        const attempts = try meta.readInt(u16, .little);
                        try channel.restoreMsg(sequence, defer_until, attempts);
                    }
                }
            }
        }

        fn TopicType() type {
            return struct {
                allocator: mem.Allocator,
                broker: *Broker,
                name: []const u8,
                paused: bool = false,
                channels: std.StringHashMap(*Channel),
                stream: store.Stream,
                metric_prev: store.Metric = .{},

                deferred: std.PriorityQueue(DeferredPublish, void, DeferredPublish.less),
                timer_op: timer.Op = undefined,

                const DeferredPublish = struct {
                    data: []const u8,
                    defer_until: u64 = 0,

                    const Self = @This();

                    fn less(_: void, a: Self, b: Self) math.Order {
                        return math.order(a.defer_until, b.defer_until);
                    }
                };

                pub fn init(broker: *Broker, name: []const u8) Topic {
                    const allocator = broker.allocator;
                    return .{
                        .allocator = allocator,
                        .name = name,
                        .channels = std.StringHashMap(*Channel).init(allocator),
                        .broker = broker,
                        .deferred = std.PriorityQueue(DeferredPublish, void, DeferredPublish.less).init(allocator, {}),
                        .stream = store.Stream.init(
                            allocator,
                            .{
                                .ack_policy = .explicit,
                                .retention_policy = .interest,
                                .max_page_size = broker.options.max_page_size,
                                .initial_page_size = broker.options.initial_page_size,
                                .max_mem = broker.options.max_topic_mem,
                            },
                            &broker.metric,
                        ),
                    };
                }

                fn deinit(self: *Topic) void {
                    // self.empty();
                    var iter = self.channels.iterator();
                    while (iter.next()) |e| self.deinitChannel(e.value_ptr.*);
                    self.channels.deinit();
                    while (self.deferred.removeOrNull()) |dp| self.allocator.free(dp.data);
                    self.deferred.deinit();
                    self.stream.deinit();
                    self.timer_op.deinit();
                }

                fn empty(self: *Topic) void {
                    if (self.channels.count() == 0) {
                        self.stream.empty();
                    }
                }

                fn delete(self: *Topic) void {
                    // call delete on all channels
                    var iter = self.channels.iterator();
                    while (iter.next()) |e| e.value_ptr.*.delete();
                }

                fn removeChannel(self: *Topic, channel: *Channel) void {
                    assert(self.channels.remove(channel.name));
                    self.deinitChannel(channel);
                }

                fn deinitChannel(self: *Topic, channel: *Channel) void {
                    self.stream.unsubscribe(channel.sequence);
                    const key = channel.name;
                    channel.deinit();
                    self.allocator.free(key);
                    self.allocator.destroy(channel);
                    self.empty();
                }

                fn subscribe(self: *Topic, consumer: *Consumer, name: []const u8) !void {
                    const channel = try self.getOrCreateChannel(name);
                    try channel.subscribe(consumer);
                }

                fn getOrCreateChannel(self: *Topic, name: []const u8) !*Channel {
                    if (self.channels.get(name)) |channel| return channel;

                    try self.broker.notifier.ensureCapacity(self.name, name);
                    const first_channel = self.channels.count() == 0;
                    const channel = try self.allocator.create(Channel);
                    errdefer self.allocator.destroy(channel);
                    const key = try self.allocator.dupe(u8, name);
                    errdefer self.allocator.free(key);
                    try self.channels.ensureUnusedCapacity(1);
                    try channel.init(self, key);
                    errdefer channel.deinit();
                    self.channels.putAssumeCapacityNoClobber(key, channel);

                    channel.timer_op.init(&self.broker.timer_queue, channel, Channel.onTimer);
                    channel.sequence = self.stream.subscribe(if (first_channel) .all else .new);
                    channel.metric.depth = if (first_channel) self.stream.metric.msgs else 0;
                    self.broker.notifier.channelCreated(self.name, channel.name);
                    log.debug("topic '{s}' channel '{s}' created", .{ self.name, channel.name });
                    return channel;
                }

                fn appendStream(self: *Topic, data: []const u8) !void {
                    if (data.len > self.broker.options.max_msg_size) {
                        log.err(
                            "{s} publish failed, message length of {} bytes over limit of {} bytes ",
                            .{ self.name, data.len, self.broker.options.max_msg_size },
                        );
                        return error.MessageSizeOverflow;
                    }
                    const res = try self.stream.alloc(@intCast(data.len + 34));
                    const header = res.data[0..34];
                    const body = res.data[34..];
                    {
                        mem.writeInt(u32, header[0..4], @intCast(data.len + 30), .big); // size (without 4 bytes size field)
                        mem.writeInt(u32, header[4..8], @intFromEnum(protocol.FrameType.message), .big); // frame type
                        mem.writeInt(u64, header[8..16], time.now, .big); // timestamp
                        mem.writeInt(u16, header[16..18], 1, .big); // attempts
                        MsgId.encode(header[18..34], res.sequence); // msg id
                    }
                    @memcpy(body, data);
                }

                fn publish(self: *Topic, data: []const u8) !void {
                    try self.appendStream(data);
                    self.notifyChannels(1);
                }

                fn deferredPublish(self: *Topic, data: []const u8, delay: u32) !void {
                    if (data.len > self.broker.options.max_msg_size)
                        return error.MessageSizeOverflow;
                    const data_dupe = try self.allocator.dupe(u8, data);
                    errdefer self.allocator.free(data_dupe);
                    const defer_until = time.after(delay);
                    if (defer_until < self.timer_op.ts)
                        try self.timer_op.update(defer_until);
                    try self.deferred.add(.{
                        .data = data_dupe,
                        .defer_until = defer_until,
                    });
                }

                fn onTimer(self: *Topic, now: u64) !u64 {
                    while (self.deferred.peek()) |dp| {
                        if (dp.defer_until <= now) {
                            try self.publish(dp.data);
                            self.allocator.free(dp.data);
                            _ = self.deferred.remove();
                            continue;
                        }
                        return dp.defer_until;
                    }
                    return timer.infinite;
                }

                fn publishDeferred(self: *Topic) !void {
                    while (self.deferred.peek()) |dp| {
                        try self.publish(dp.data);
                        self.allocator.free(dp.data);
                        _ = self.deferred.remove();
                    }
                }

                fn multiPublish(self: *Topic, msgs: u32, data: []const u8) !void {
                    if (msgs == 0) return;
                    var pos: usize = 0;
                    for (0..msgs) |_| {
                        const len = mem.readInt(u32, data[pos..][0..4], .big);
                        pos += 4;
                        try self.appendStream(data[pos..][0..len]);
                        pos += len;
                    }
                    self.notifyChannels(msgs);
                }

                // Notify all channels that there is pending messages
                fn notifyChannels(self: *Topic, msgs: u32) void {
                    var iter = self.channels.valueIterator();
                    while (iter.next()) |ptr| {
                        const channel = ptr.*;
                        channel.onTopicAppend(msgs);
                    }
                }

                // Http interface actions -----------------

                fn deleteChannel(self: *Topic, name: []const u8) !void {
                    const kv = self.channels.fetchRemove(name) orelse return error.NotFound;
                    const channel = kv.value;
                    channel.delete();
                    self.deinitChannel(channel);
                }

                fn pause(self: *Topic) void {
                    self.paused = true;
                }

                fn unpause(self: *Topic) void {
                    self.paused = false;
                    // wake-up all channels
                    var iter = self.channels.valueIterator();
                    while (iter.next()) |ptr| ptr.*.wakeup();
                }

                fn pauseChannel(self: *Topic, name: []const u8) !void {
                    const channel = self.channels.get(name) orelse return error.NotFound;
                    channel.pause();
                }

                fn unpauseChannel(self: *Topic, name: []const u8) !void {
                    const channel = self.channels.get(name) orelse return error.NotFound;
                    channel.unpause();
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
                pub const InFlightMsg = struct {
                    timeout_at: u64 = 0,
                    consumer_id: u32 = 0,
                    attempts: u16 = 1,

                    const Self = @This();

                    fn setAttempts(self: Self, payload: []u8) void {
                        const buf = payload[16..18];
                        mem.writeInt(u16, buf, self.attempts, .big);
                    }
                };

                const DeferredMsg = struct {
                    sequence: u64,
                    defer_until: u64,
                    attempts: u16,

                    const Self = @This();

                    fn less(_: void, a: Self, b: Self) math.Order {
                        if (a.defer_until != b.defer_until)
                            return math.order(a.defer_until, b.defer_until);
                        return math.order(a.sequence, b.sequence);
                    }
                };

                allocator: mem.Allocator,
                name: []const u8,
                topic: *Topic,
                consumers: std.ArrayList(*Consumer),
                // Round robin consumers iterator.
                consumers_iterator: ConsumersIterator,

                // Sent but not jet acknowledged (fin) messages.
                in_flight: std.AutoHashMap(u64, InFlightMsg),
                // Re-queued by consumer, timed-out or defer published messages.
                deferred: std.PriorityQueue(DeferredMsg, void, DeferredMsg.less),
                timer_op: timer.Op = undefined,

                metric: Metric = .{},
                metric_prev: Metric = .{},
                paused: bool = false,
                ephemeral: bool = false,

                // Last stream sequence consumed by this channel
                sequence: u64 = 0,

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

                fn init(self: *Channel, topic: *Topic, name: []const u8) !void {
                    const allocator = topic.allocator;
                    self.* = .{
                        .allocator = allocator,
                        .name = name,
                        .topic = topic,
                        .consumers = std.ArrayList(*Consumer).init(allocator),
                        .consumers_iterator = .{ .channel = self },
                        .in_flight = std.AutoHashMap(u64, InFlightMsg).init(allocator),
                        .deferred = std.PriorityQueue(DeferredMsg, void, DeferredMsg.less).init(allocator, {}),
                        .ephemeral = protocol.isEphemeral(name),
                    };
                }

                fn delete(self: *Channel) void {
                    for (self.consumers.items) |consumer| {
                        consumer.channel = null;
                        consumer.shutdown();
                    }
                    self.consumers.clearAndFree();
                }

                fn deinit(self: *Channel) void {
                    self.in_flight.deinit();
                    self.deferred.deinit();
                    self.consumers.deinit();
                    self.timer_op.deinit();
                }

                fn subscribe(self: *Channel, consumer: *Consumer) !void {
                    assert(consumer.channel == null);
                    try self.consumers.append(consumer);
                    consumer.channel = self;
                }

                // -----------------

                // Called from topic when new topic message is created.
                fn onTopicAppend(self: *Channel, msgs: u32) void {
                    self.metric.depth += msgs;
                    if (!self.needWakeup()) return;
                    self.wakeup();
                }

                fn needWakeup(self: *Channel) bool {
                    return !self.topic.paused and
                        !self.paused and
                        self.topic.stream.hasMore(self.sequence);
                }

                // Notify consumers that there are messages to pull.
                fn wakeup(self: *Channel) void {
                    while (self.consumers_iterator.next()) |consumer| {
                        consumer.wakeup();
                        if (!self.needWakeup()) break;
                    }
                }

                fn restore(self: *Channel, sequence: u64) !void {
                    if (sequence == self.sequence) return;
                    self.topic.stream.unsubscribe(self.sequence);
                    self.sequence = self.topic.stream.subscribe(.{ .from_sequence = sequence });
                    self.metric.depth = self.topic.stream.last_sequence - sequence;
                }

                fn restoreMsg(self: *Channel, sequence: u64, defer_until: u64, attempts: u16) !void {
                    const dm = DeferredMsg{
                        .sequence = sequence,
                        .defer_until = if (defer_until > time.now) defer_until else 0,
                        .attempts = attempts + 1,
                    };
                    try self.updateTimer(dm.defer_until);
                    try self.deferred.add(dm);
                    self.topic.stream.acquire(sequence);
                    self.metric.depth += 1;
                }

                // Defer in flight message.
                // Moves message from in-flight to the deferred.
                fn deferInFlight(self: *Channel, sequence: u64, ifm: InFlightMsg, delay: u32) !void {
                    try self.deferred.ensureUnusedCapacity(1);
                    const dm = DeferredMsg{
                        .sequence = sequence,
                        .defer_until = if (delay == 0) 0 else time.after(delay),
                        .attempts = ifm.attempts + 1,
                    };
                    try self.updateTimer(dm.defer_until);
                    self.deferred.add(dm) catch unreachable; // capacity is ensured
                    assert(self.in_flight.remove(sequence));
                }

                // Find deferred message, make copy of that message, set
                // increased attempts to message payload, put it in-flight and
                // return payload.
                fn popDeferred(self: *Channel, consumer_id: u32, msg_timeout: u32) !?PullChunk {
                    if (self.deferred.peek()) |dm| if (dm.defer_until <= time.now) {
                        try self.in_flight.ensureUnusedCapacity(1);
                        const payload = try self.allocator.dupe(u8, self.topic.stream.message(dm.sequence));
                        errdefer self.allocator.free(payload);

                        const ifm = InFlightMsg{
                            .timeout_at = time.after(msg_timeout),
                            .consumer_id = consumer_id,
                            .attempts = dm.attempts,
                        };
                        ifm.setAttempts(payload);
                        try self.updateTimer(ifm.timeout_at);
                        self.in_flight.putAssumeCapacityNoClobber(dm.sequence, ifm);
                        _ = self.deferred.remove();

                        return .{
                            .data = payload,
                            .count = 1,
                            .allocator = self.allocator,
                        };
                    };
                    return null;
                }

                fn updateTimer(self: *Channel, next_ts: u64) !void {
                    if (next_ts == 0 or self.timer_op.ts <= next_ts) return;
                    try self.timer_op.update(next_ts);
                }

                // Callback when timer timeout if fired
                fn onTimer(self: *Channel, now: u64) !u64 {
                    return @min(
                        try self.inFlightTimeout(now),
                        self.deferredTimeout(now),
                    );
                }

                // Returns next timeout of deferred messages
                fn deferredTimeout(self: *Channel, now: u64) u64 {
                    var ts: u64 = timer.infinite;
                    if (self.deferred.count() > 0) {
                        self.wakeup();
                        if (self.deferred.peek()) |dm| {
                            if (dm.defer_until > now and dm.defer_until < ts) ts = dm.defer_until;
                        }
                    }
                    return ts;
                }

                // Finds time-outed in flight messages and moves them to the
                // deferred queue.
                // Returns next timeout for in flight messages.
                fn inFlightTimeout(self: *Channel, now: u64) !u64 {
                    var next_timeout: u64 = timer.infinite;

                    // iterate over all in_flight messages
                    // find expired and next timeout
                    var expired = std.ArrayList(u64).init(self.allocator);
                    defer expired.deinit();
                    var iter = self.in_flight.iterator();
                    while (iter.next()) |e| {
                        const timeout_at = e.value_ptr.timeout_at;
                        if (timeout_at <= now) {
                            const sequence = e.key_ptr.*;
                            try expired.append(sequence);
                        } else {
                            if (next_timeout > timeout_at)
                                next_timeout = timeout_at;
                        }
                    }

                    // defer expired messages
                    for (expired.items) |sequence| {
                        const msg = self.in_flight.get(sequence).?;
                        try self.deferInFlight(sequence, msg, 0);
                        self.metric.timeout += 1;
                        if (!builtin.is_test)
                            log.warn(
                                "{s}/{s} message timeout consumer: {}, sequence: {}",
                                .{ self.topic.name, self.name, msg.consumer_id, sequence },
                            );
                    }

                    return next_timeout;
                }

                const ConsumersIterator = struct {
                    channel: *Channel,
                    idx: usize = 0,

                    // Iterates over ready consumers. Returns null when there is no
                    // ready consumers. Ensures round robin consumer selection.
                    fn next(self: *ConsumersIterator) ?*Consumer {
                        const consumers = self.channel.consumers.items;
                        const count = consumers.len;
                        if (count == 0) return null;
                        for (0..count) |_| {
                            self.idx += 1;
                            if (self.idx >= count) self.idx = 0;
                            const consumer = consumers[self.idx];
                            if (consumer.ready()) return consumer;
                        }
                        return null;
                    }
                };

                // Consumer interface actions -----------------

                // Chunk of data to send to the consumer
                pub const PullChunk = struct {
                    data: []const u8,
                    count: u32 = 1,

                    stream: ?*store.Stream = null,
                    sequence: u64 = 0,
                    allocator: ?mem.Allocator = null,

                    // Consumer should call this when data is no more in use by
                    // underlying network interface; when io_uring send
                    // operation is completed. This will release stream reference
                    // or deallocate buffer (in case of copied message;
                    // deferred).
                    pub fn done(self: PullChunk) void {
                        if (self.stream) |stream|
                            stream.release(self.sequence);
                        if (self.allocator) |allocator|
                            allocator.free(self.data);
                    }
                };

                pub fn pull(self: *Channel, consumer_id: u32, msg_timeout: u32, ready_count: u32) !?PullChunk {
                    assert(ready_count > 0);
                    assert(consumer_id > 0);
                    if (self.paused) return null;

                    // if there is deferred message return one
                    if (try self.popDeferred(consumer_id, msg_timeout)) |sc| return sc;

                    // else find next chunk in stream
                    if (self.topic.paused) return null;
                    if (self.topic.stream.pull(self.sequence, ready_count)) |res| {
                        errdefer res.revert(&self.topic.stream);

                        { // add all sequence to in_flight
                            try self.in_flight.ensureUnusedCapacity(res.count);
                            const timeout_at = time.after(msg_timeout);
                            try self.updateTimer(timeout_at);
                            for (res.sequence.from..res.sequence.to + 1) |sequence| {
                                const ifm = InFlightMsg{
                                    .timeout_at = timeout_at,
                                    .consumer_id = consumer_id,
                                    .attempts = 1,
                                };
                                self.in_flight.putAssumeCapacityNoClobber(sequence, ifm);
                            }
                        }

                        self.metric.pull += res.count;
                        self.sequence = res.sequence.to;
                        return .{
                            .data = res.data,
                            .count = res.count,
                            .stream = &self.topic.stream,
                            .sequence = res.sequence.from,
                        };
                    }
                    return null;
                }

                fn findInFlight(self: *Channel, consumer_id: u32, msg_id: [16]u8) !struct { u64, InFlightMsg } {
                    const sequence = MsgId.parse(msg_id);
                    const ifm = self.in_flight.get(sequence) orelse return error.MessageNotInFlight;
                    if (ifm.consumer_id != consumer_id) return error.MessageNotInFlight;
                    return .{ sequence, ifm };
                }

                pub fn finish(self: *Channel, consumer_id: u32, msg_id: [16]u8) !void {
                    const sequence, _ = try self.findInFlight(consumer_id, msg_id);
                    self.topic.stream.release(sequence);
                    assert(self.in_flight.remove(sequence));
                    self.metric.finish += 1;
                    self.metric.depth -|= 1;
                }

                /// Extend message timeout for interval (milliseconds).
                pub fn touch(self: *Channel, consumer_id: u32, msg_id: [16]u8, interval: u32) !void {
                    const sequence, _ = try self.findInFlight(consumer_id, msg_id);
                    const ifm = self.in_flight.getPtr(sequence).?;
                    ifm.timeout_at += nsFromMs(interval);
                }

                pub fn requeue(self: *Channel, consumer_id: u32, msg_id: [16]u8, delay: u32) !void {
                    const sequence, const ifm = try self.findInFlight(consumer_id, msg_id);
                    try self.deferInFlight(sequence, ifm, delay);
                    self.metric.requeue += 1;
                }

                pub fn unsubscribe(self: *Channel, consumer: *Consumer) void {
                    self.requeueAll(consumer.id()) catch |err| {
                        log.warn("failed to remove in flight messages for socket {}, {}", .{ consumer.id(), err });
                    };

                    // Remove consumer
                    for (self.consumers.items, 0..) |item, i| {
                        if (item == consumer) {
                            _ = self.consumers.swapRemove(i);
                            break;
                        }
                    }
                    if (self.consumers.items.len == 0 and self.ephemeral)
                        self.topic.removeChannel(self);
                }

                // Re-queue all messages which are in-flight on some consumer.
                fn requeueAll(self: *Channel, consumer_id: u32) !void {
                    var sequences = std.ArrayList(u64).init(self.allocator);
                    defer sequences.deinit();
                    var iter = self.in_flight.iterator();
                    while (iter.next()) |e| {
                        if (e.value_ptr.consumer_id == consumer_id) {
                            const sequence = e.key_ptr.*;
                            try sequences.append(sequence);
                        }
                    }

                    for (sequences.items) |sequence| {
                        const msg = self.in_flight.get(sequence).?;
                        try self.deferInFlight(sequence, msg, 0);
                        self.metric.requeue += 1;
                        // log.debug("{} message requeue {}", .{ msg.consumer_id, sequence });
                    }
                }

                // Http admin interface  -----------------

                fn pause(self: *Channel) void {
                    self.paused = true;
                }

                fn unpause(self: *Channel) void {
                    self.paused = false;
                    self.wakeup();
                }

                fn empty(self: *Channel) !void {
                    { // release in_flight messages
                        var iter = self.in_flight.keyIterator();
                        while (iter.next()) |e| {
                            self.topic.stream.release(e.*);
                        }
                        self.in_flight.clearAndFree();
                    }
                    { // release deferred messages
                        var iter = self.deferred.iterator();
                        while (iter.next()) |dm| {
                            self.topic.stream.release(dm.sequence);
                        }
                        self.deferred.shrinkAndFree(0);
                    }
                    { // move stream pointer
                        self.topic.stream.unsubscribe(self.sequence);
                        self.sequence = self.topic.stream.subscribe(.new);
                        self.metric.depth = 0;
                    }
                }
            };
        }
    };
}

test "channel consumers iterator" {
    const allocator = testing.allocator;

    var notifier = NoopNotifier{};
    var broker = TestBroker.init(allocator, &notifier, 0, .{});
    defer broker.deinit();

    var consumer1 = TestConsumer.init(allocator);
    try broker.subscribe(&consumer1, "topic", "channel");
    const channel = consumer1.channel.?;
    consumer1.ready_count = 1;

    var iter = channel.consumers_iterator;
    try testing.expectEqual(&consumer1, iter.next().?);
    try testing.expectEqual(&consumer1, iter.next().?);
    consumer1.ready_count = 0;
    try testing.expectEqual(null, iter.next());

    consumer1.ready_count = 1;
    var consumer2 = TestConsumer.init(allocator);
    var consumer3 = TestConsumer.init(allocator);
    try broker.subscribe(&consumer2, "topic", "channel");
    try broker.subscribe(&consumer3, "topic", "channel");
    try testing.expectEqual(3, channel.consumers.items.len);
    consumer2.ready_count = 1;
    consumer3.ready_count = 1;

    try testing.expectEqual(3, channel.consumers.items.len);
    try testing.expectEqual(&consumer2, iter.next().?);
    try testing.expectEqual(&consumer3, iter.next().?);
    try testing.expectEqual(&consumer1, iter.next().?);
    try testing.expectEqual(&consumer2, iter.next().?);

    try testing.expectEqual(&consumer3, iter.next().?);
    try testing.expectEqual(&consumer1, iter.next().?);
    try testing.expectEqual(&consumer2, iter.next().?);
    consumer3.ready_count = 0;
    try testing.expectEqual(&consumer1, iter.next().?);
    try testing.expectEqual(&consumer2, iter.next().?);
    try testing.expectEqual(&consumer1, iter.next().?);
    consumer2.ready_count = 0;
    try testing.expectEqual(&consumer1, iter.next().?);
    try testing.expectEqual(&consumer1, iter.next().?);
    consumer1.ready_count = 0;
    try testing.expect(iter.next() == null);
}

test "channel fin req" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name = "channel";

    var broker = TestBroker.init(allocator, &noop_notifier, 0, .{});
    defer broker.deinit();

    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();

    try broker.subscribe(&consumer, topic_name, channel_name);
    const channel = consumer.channel.?;

    try broker.publish(topic_name, "1");
    try broker.publish(topic_name, "2");
    try broker.publish(topic_name, "3");

    { // 3 messages in topic, 0 taken by channel
        try testing.expectEqual(3, channel.metric.depth);
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
    }
    { // wakeup without ready consumers
        channel.wakeup();
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
    }
    { // consumer is ready
        try consumer.pull();
        try testing.expectEqual(1, consumer.lastSequence());
        // 1 is in flight
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(1, channel.sequence);
    }
    { // consumer sends fin, 0 in flight after that
        try consumer.finish(1);
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(2, channel.metric.depth);
        try testing.expectEqual(1, channel.sequence);
    }
    { // send seq 2, 1 msg in flight
        try consumer.pull();
        try testing.expectEqual(2, consumer.lastSequence());
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(2, channel.sequence);
    }
    { // send seq 3, 2 msgs in flight
        try consumer.pull();
        try testing.expectEqual(3, consumer.lastSequence());
        try testing.expectEqual(2, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(3, channel.sequence);
    }
    { // 2 is re-queued
        try consumer.requeue(2, 0);
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(1, channel.deferred.count());
    }
    { // out of order fin, fin seq 3 while 2 is still in flight
        try consumer.finish(3);
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(1, channel.deferred.count());
    }
    { // re send 2
        try testing.expectEqual(3, consumer.sequences.items.len);
        try testing.expectEqual(1, channel.deferred.count());
        try consumer.pull();
        try testing.expectEqual(4, consumer.sequences.items.len);
        try testing.expectEqual(2, consumer.lastSequence());
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
    }
    { // fin seq 2
        try testing.expectEqual(1, channel.metric.depth);
        try consumer.finish(2);
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(0, channel.metric.depth);
    }
    { // consumer unsubscribe re-queues in-flight messages
        try broker.publish(topic_name, "4");
        try consumer.pull();
        try testing.expectEqual(1, channel.in_flight.count());
        channel.unsubscribe(&consumer);
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(1, channel.deferred.count());
        try testing.expectEqual(0, channel.consumers.items.len);
    }
}

const TestConsumer = struct {
    const Self = @This();

    allocator: mem.Allocator,
    channel: ?*TestBroker.Channel = null,
    sequences: std.ArrayList(u64) = undefined,
    ready_count: u32 = 0,
    _id: u32 = 1,

    fn init(allocator: mem.Allocator) Self {
        return .{
            .allocator = allocator,
            .sequences = std.ArrayList(u64).init(allocator),
        };
    }

    pub fn id(self: Self) u32 {
        return self._id;
    }

    pub fn msgTimeout(_: *Self) u32 {
        return 60000;
    }

    fn deinit(self: *Self) void {
        self.sequences.deinit();
    }

    fn wakeup(self: *Self) void {
        self.wakeup_() catch {};
    }

    fn wakeup_(self: *Self) !void {
        {
            // Making this method fallible in check all allocations
            const buf = try self.allocator.alloc(u8, 8);
            defer self.allocator.free(buf);
        }
        while (self.ready_count > 0) {
            const sc = try self.channel.?.pull(self.id(), self.msgTimeout(), self.ready_count) orelse break;
            var pos: usize = 0;
            while (pos < sc.data.len) {
                const header = sc.data[pos .. pos + 34];
                const size = mem.readInt(u32, header[0..4], .big);
                const page = mem.readInt(u32, header[22..26], .big);
                const sequence = mem.readInt(u64, header[26..34], .big);
                pos += 4 + size;
                _ = page;
                try self.sequences.append(sequence);
            }
            self.ready_count -= sc.count;
            sc.done();
        }
    }

    fn ready(self: *Self) bool {
        return self.ready_count > 0;
    }

    fn finish(self: *Self, sequence: u64) !void {
        try self.channel.?.finish(self.id(), MsgId.from(sequence));
    }

    fn requeue(self: *Self, sequence: u64, delay: u32) !void {
        try self.channel.?.requeue(self.id(), MsgId.from(sequence), delay);
    }

    fn pull(self: *Self) !void {
        self.ready_count = 1;
        try self.wakeup_();
    }

    fn pullFinish(self: *Self) !void {
        try self.pull();
        try self.finish(self.lastSequence());
    }

    fn lastSequence(self: *Self) u64 {
        return self.sequences.items[self.sequences.items.len - 1];
    }

    fn shutdown(_: *Self) void {}
};

pub const NoopNotifier = struct {
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
    fn ensureCapacity(_: *Self, _: []const u8, _: []const u8) !void {}
};

var noop_notifier = NoopNotifier{};
const TestBroker = BrokerType(TestConsumer, NoopNotifier);

test "multiple channels" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name1 = "channel1";
    const channel_name2 = "channel2";
    const no = 1024;

    var broker = TestBroker.init(allocator, &noop_notifier, 0, .{});
    defer broker.deinit();

    var c1 = TestConsumer.init(allocator);
    defer c1.deinit();
    try broker.subscribe(&c1, topic_name, channel_name1);
    c1.ready_count = no * 3;

    { // single channel, single consumer
        for (0..no) |_|
            try broker.publish(topic_name, "message body");

        try testing.expectEqual(no, c1.sequences.items.len);
        var expected: u64 = 1;
        for (c1.sequences.items) |seq| {
            try testing.expectEqual(expected, seq);
            expected += 1;
        }
    }

    var c2 = TestConsumer.init(allocator);
    defer c2.deinit();
    try broker.subscribe(&c2, topic_name, channel_name2);
    c2.ready_count = no * 2;

    { // two channels on the same topic
        for (0..no) |_|
            try broker.publish(topic_name, "another message body");

        try testing.expectEqual(no * 2, c1.sequences.items.len);
        try testing.expectEqual(no, c2.sequences.items.len);
    }

    var c3 = TestConsumer.init(allocator);
    defer c3.deinit();
    try broker.subscribe(&c3, topic_name, channel_name2);
    c3.ready_count = no;

    { // two channels, one has single consumer another has two consumers
        for (0..no) |_|
            try broker.publish(topic_name, "yet another message body");

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

    var broker = TestBroker.init(allocator, &noop_notifier, 0, .{});
    defer broker.deinit();
    // publish messages to the topic which don't have channels created
    for (0..no) |_|
        try broker.publish(topic_name, "message body"); // 1-16

    const topic = try broker.getOrCreateTopic(topic_name);
    try testing.expectEqual(no, topic.stream.metric.msgs);

    // subscribe creates channel
    // channel gets all messages
    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();
    try broker.subscribe(&consumer, topic_name, channel_name);
    var channel = consumer.channel.?;
    try testing.expectEqual(0, channel.sequence);
    try testing.expectEqual(no, channel.metric.depth);

    for (0..no) |i| {
        try testing.expectEqual(no - i, channel.metric.depth);
        try consumer.pullFinish();
    }
    try testing.expectEqual(0, channel.metric.depth);
    try testing.expectEqual(no, topic.stream.metric.msgs);

    try broker.publish(topic_name, "message body"); // 17
    try testing.expectEqual(1, channel.metric.depth);
    try testing.expectEqual(17, topic.stream.metric.msgs);
    try testing.expectEqual(17, topic.stream.last_sequence);

    try broker.deleteChannel(topic_name, channel_name);
    try testing.expectEqual(0, topic.stream.metric.msgs);
    try broker.publish(topic_name, "message body"); // 18
    try testing.expectEqual(1, topic.stream.metric.msgs);

    var consumer2 = TestConsumer.init(allocator);
    defer consumer2.deinit();
    try broker.subscribe(&consumer2, topic_name, channel_name);
    channel = consumer2.channel.?;
    try testing.expectEqual(17, channel.sequence);
    try testing.expectEqual(1, topic.stream.metric.msgs);
    try testing.expectEqual(1, channel.metric.depth);
}

test "timeout messages" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name = "channel";

    var broker = TestBroker.init(allocator, &noop_notifier, 0, .{});
    defer broker.deinit();

    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();
    try broker.subscribe(&consumer, topic_name, channel_name);
    const channel = consumer.channel.?;

    for (0..4) |i| {
        time.now = i + 1;
        try broker.publish(topic_name, "message body");
        try consumer.pull();
    }
    try testing.expectEqual(4, channel.in_flight.count());

    { // check expire_at for in flight messages
        var iter = channel.in_flight.valueIterator();
        while (iter.next()) |msg| {
            try testing.expectEqual(1, msg.attempts);
            try testing.expect(msg.timeout_at > nsFromMs(consumer.msgTimeout()) and
                msg.timeout_at <= nsFromMs(consumer.msgTimeout()) + 4);
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
        try consumer.finish(consumer.lastSequence());
        try testing.expectEqual(1, channel.metric.finish);
        try testing.expectEqual(0, channel.in_flight.count());
    }
    { // resend two
        try consumer.pull();
        try consumer.pull();
        try testing.expectEqual(2, channel.in_flight.count());
        try testing.expectEqual(1, channel.deferred.count());
        var iter = channel.in_flight.valueIterator();
        while (iter.next()) |msg| {
            try testing.expectEqual(2, msg.attempts);
        }
    }
    time.now = 0;
}

test "deferred messages" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name = "channel";

    var broker = TestBroker.init(allocator, &noop_notifier, 0, .{});
    defer broker.deinit();

    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();
    try broker.subscribe(&consumer, topic_name, channel_name);
    const channel = consumer.channel.?;
    const topic = channel.topic;
    consumer.ready_count = 3;

    { // publish two deferred messages, topic puts them into deferred queue
        try broker.deferredPublish(topic_name, "message body", 2);
        try broker.deferredPublish(topic_name, "message body", 1);
        try testing.expectEqual(0, channel.metric.depth);
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(2, topic.deferred.count());
        try testing.expectEqual(0, topic.stream.last_sequence);
    }

    { // move now, one is in flight after publish from topic.onTimer
        _ = try broker.tick(nsFromMs(1));
        try testing.expectEqual(1, topic.stream.last_sequence);
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(1, topic.deferred.count());
    }
    { // re-queue
        try consumer.requeue(1, 2);
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(1, channel.deferred.count());
    }

    { // move now to deliver both
        time.now = nsFromMs(3);
        channel.wakeup();
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        _ = try topic.onTimer(time.now);
        try testing.expectEqual(2, topic.stream.last_sequence);
        try testing.expectEqual(0, topic.deferred.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(2, channel.in_flight.count());
    }
}

test "topic pause" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name = "channel";

    var broker = TestBroker.init(allocator, &noop_notifier, 0, .{});
    defer broker.deinit();
    const topic = try broker.getOrCreateTopic(topic_name);

    {
        try broker.publish(topic_name, "message 1");
        try broker.publish(topic_name, "message 2");
        try testing.expectEqual(2, topic.stream.metric.msgs);
    }

    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();
    try broker.subscribe(&consumer, topic_name, channel_name);
    const channel = consumer.channel.?;

    { // while channel is paused topic messages are not delivered to the channel
        try testing.expect(!channel.paused);
        try broker.pauseChannel(topic_name, channel_name);
        try testing.expect(channel.paused);

        try consumer.pull();
        try testing.expectEqual(0, channel.in_flight.count());

        // unpause will pull message
        try broker.unpauseChannel(topic_name, channel_name);
        try testing.expectEqual(1, channel.in_flight.count());
    }

    { // same while topic is paused
        try testing.expect(!topic.paused);
        try broker.pauseTopic(topic_name);
        try testing.expect(topic.paused);

        try consumer.pull();
        try testing.expectEqual(1, channel.in_flight.count());

        // unpause
        try broker.unpauseTopic(topic_name);
        try testing.expectEqual(2, channel.in_flight.count());
    }

    try broker.deleteTopic(topic_name);
    try testing.expect(consumer.channel == null);
}

test "channel empty" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name = "channel";

    var broker = TestBroker.init(allocator, &noop_notifier, 0, .{});
    defer broker.deinit();

    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();

    try broker.subscribe(&consumer, topic_name, channel_name);
    const channel = consumer.channel.?;

    time.now = 1;
    try broker.publish(topic_name, "message 1");
    try broker.publish(topic_name, "message 2");

    time.now = 2;
    try consumer.pull();
    try consumer.pull();
    try testing.expectEqual(2, channel.in_flight.count());
    try channel.empty();
    try testing.expectEqual(0, channel.in_flight.count());

    time.now = 0;
}

test "ephemeral channel" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name = "channel#ephemeral";

    var broker = TestBroker.init(allocator, &noop_notifier, 0, .{});
    defer broker.deinit();

    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();

    try broker.subscribe(&consumer, topic_name, channel_name);
    const channel = consumer.channel.?;
    const topic = consumer.channel.?.topic;
    try testing.expect(channel.ephemeral);

    try testing.expectEqual(1, topic.channels.count());
    channel.unsubscribe(&consumer);
    try testing.expectEqual(0, topic.channels.count());
}

test noop_notifier {
    const allocator = testing.allocator;

    var notifier = NoopNotifier{};
    var broker = TestBroker.init(allocator, &notifier, 0, .{});
    defer broker.deinit();

    var consumer1 = TestConsumer.init(allocator);
    defer consumer1.deinit();
    _ = try broker.subscribe(&consumer1, "topic1", "channel1");
    try testing.expectEqual(2, notifier.call_count);

    var consumer2 = TestConsumer.init(allocator);
    defer consumer2.deinit();
    _ = try broker.subscribe(&consumer2, "topic1", "channel2");
    try testing.expectEqual(3, notifier.call_count);

    var consumer3 = TestConsumer.init(allocator);
    defer consumer3.deinit();
    _ = try broker.subscribe(&consumer3, "topic2", "channel2");
    try testing.expectEqual(5, notifier.call_count);

    // test deletes
    try testing.expectEqual(5, notifier.call_count);
    try broker.deleteTopic("topic1");
    try testing.expectEqual(6, notifier.call_count);
    try broker.deleteChannel("topic2", "channel2");
    try testing.expectEqual(7, notifier.call_count);
}

test "depth" {
    const allocator = testing.allocator;
    const topic_name = "topic";

    var broker = TestBroker.init(allocator, &noop_notifier, 0, .{});
    defer broker.deinit();

    var consumer1 = TestConsumer.init(allocator);
    defer consumer1.deinit();
    try broker.subscribe(&consumer1, topic_name, "channel1");

    var consumer2 = TestConsumer.init(allocator);
    defer consumer2.deinit();
    try broker.subscribe(&consumer2, topic_name, "channel2");

    const channel1 = consumer1.channel.?;
    const channel2 = consumer2.channel.?;
    const topic = channel1.topic;

    try broker.publish(topic_name, "message 1");
    try broker.publish(topic_name, "message 2");

    try testing.expectEqual(2, topic.stream.metric.msgs);
    try testing.expectEqual(2, channel1.metric.depth);
    try testing.expectEqual(2, channel2.metric.depth);

    try testing.expectEqual(0, channel1.sequence);
    try testing.expectEqual(0, channel2.sequence);
    try consumer1.pullFinish();
    try testing.expectEqual(1, channel1.sequence);

    try testing.expectEqual(1, channel1.metric.depth);
    try testing.expectEqual(2, channel2.metric.depth);

    try consumer2.pullFinish();
    try testing.expectEqual(1, channel1.metric.depth);
    try testing.expectEqual(1, channel2.metric.depth);

    try consumer2.pullFinish();
    try testing.expectEqual(1, channel1.metric.depth);
    try testing.expectEqual(0, channel2.metric.depth);

    try broker.publish(topic_name, "message 3");

    try testing.expectEqual(2, channel1.metric.depth);
    try testing.expectEqual(1, channel2.metric.depth);
}

test "check allocations" {
    const allocator = testing.allocator;
    try publishFinish(allocator);

    try testing.checkAllAllocationFailures(allocator, publishFinish, .{});
}

fn publishFinish(allocator: mem.Allocator) !void {
    const topic_name = "topic";

    var broker = TestBroker.init(allocator, &noop_notifier, 0, .{});
    defer broker.deinit();

    // Create 2 channels
    const channel1_name = "channel1";
    const channel2_name = "channel2";
    try broker.createChannel(topic_name, channel1_name);
    try broker.createChannel(topic_name, channel2_name);
    const topic = broker.getOrCreateTopic(topic_name) catch unreachable;

    // Publish some messages
    try broker.publish(topic_name, "message 1");
    try broker.publish(topic_name, "message 2");
    try testing.expectEqual(2, topic.stream.metric.msgs);
    try testing.expectEqual(2, topic.stream.last_sequence);

    // 1 consumer for channel 1 and 2 consumers for channel 2
    var channel1_consumer = TestConsumer.init(allocator);
    defer channel1_consumer.deinit();
    try broker.subscribe(&channel1_consumer, topic_name, channel1_name);

    var channel2_consumer1 = TestConsumer.init(allocator);
    defer channel2_consumer1.deinit();
    try broker.subscribe(&channel2_consumer1, topic_name, channel2_name);
    var channel2_consumer2 = TestConsumer.init(allocator);
    defer channel2_consumer2.deinit();
    channel2_consumer2._id = 2;
    try broker.subscribe(&channel2_consumer2, topic_name, channel2_name);

    try testing.expectEqual(0, channel1_consumer.channel.?.sequence);
    try testing.expectEqual(0, channel2_consumer1.channel.?.sequence);

    // Consume messages
    try channel1_consumer.pullFinish();
    try channel1_consumer.pullFinish();
    try channel2_consumer1.pullFinish();
    try channel2_consumer2.pullFinish();
    try testing.expectEqual(2, topic.stream.metric.msgs);
    try testing.expectEqual(2, channel1_consumer.sequences.items.len);
    try testing.expectEqual(1, channel2_consumer1.sequences.items.len);
    try testing.expectEqual(1, channel2_consumer2.sequences.items.len);

    // Publish some more
    try broker.publish(topic_name, "message 3");
    try broker.publish(topic_name, "message 4");
    try broker.deleteChannel(topic_name, channel1_name);

    // Re-queue from one consumer 1 to consumer 2
    const channel2 = channel2_consumer2.channel.?;
    try channel2_consumer1.pull();
    try testing.expectEqual(3, channel2_consumer1.lastSequence());
    try channel2_consumer1.requeue(3, 0);
    try testing.expectEqual(1, channel2.deferred.count());
    try channel2_consumer2.pull();
    try testing.expectEqual(3, channel2_consumer2.lastSequence());
    // Unsubscribe consumer2
    try testing.expectEqual(1, channel2.in_flight.count());
    try channel2.requeueAll(channel2_consumer2.id());
    channel2.unsubscribe(&channel2_consumer2);
    try testing.expectEqual(0, channel2.in_flight.count());
    try testing.expectEqual(1, channel2.deferred.count());
    try channel2_consumer1.pull();
    try testing.expectEqual(3, channel2_consumer1.lastSequence());

    try channel2.empty();
    try testing.expectEqual(0, channel2.in_flight.count());
    try broker.deleteTopic(topic_name);
}

test "dump/restore" {
    const ns_per_s = std.time.ns_per_s;
    const now = 1730221264 * ns_per_s;
    const allocator = testing.allocator;
    const topic_name = "some.valid-topic_name.01234";
    const channel1_name = [_]u8{'a'} ** 64;
    const channel2_name = [_]u8{'b'} ** 64;

    const dir_name = "dump";
    try std.fs.cwd().makePath(dir_name);
    var dir = try std.fs.cwd().openDir(dir_name, .{ .iterate = true });
    defer dir.close();

    { // dump
        var broker = TestBroker.init(allocator, &noop_notifier, 0, .{});
        defer broker.deinit();
        const topic = try broker.getOrCreateTopic(topic_name);

        var consumer1 = TestConsumer.init(allocator);
        defer consumer1.deinit();
        var consumer2 = TestConsumer.init(allocator);
        defer consumer2.deinit();
        try broker.subscribe(&consumer1, topic_name, &channel1_name);
        var channel1 = consumer1.channel.?;
        try broker.subscribe(&consumer2, topic_name, &channel2_name);
        var channel2 = consumer2.channel.?;

        { // add 3 messages to the topic
            topic.stream.last_sequence = 100;
            time.now = now;
            try broker.publish(topic_name, "Iso "); // msg 1, sequence: 101
            time.now += ns_per_s;
            try broker.publish(topic_name, "medo u ducan "); // msg 2, sequence: 102
            time.now += ns_per_s;
            try broker.publish(topic_name, "nije reko dobar dan."); // msg 3, sequence: 103
        }

        // channel 1: 1-finished, 2-in flight, 3-next
        try consumer1.pullFinish();
        try consumer1.pull();
        try testing.expectEqual(0, channel1.deferred.count());
        try testing.expectEqual(1, channel1.in_flight.count());
        try testing.expectEqual(102, channel1.sequence);

        // channel 2: 1-in flight, 2-finished, 3-in flight, next null
        try consumer2.pull();
        try consumer2.pull();
        try consumer2.pull();
        try testing.expectEqual(3, channel2.in_flight.count());
        try consumer2.finish(102);
        try testing.expectEqual(2, channel2.in_flight.count());
        try testing.expectEqual(103, channel2.sequence);
        channel1.pause();

        try testing.expectEqual(2, channel1.metric.depth);
        try testing.expectEqual(2, channel2.metric.depth);

        try testing.expectEqual(5, topic.stream.pages.items[0].ref_count);
        try broker.dump(dir);
    }

    { // restore in another instance
        var broker = TestBroker.init(allocator, &noop_notifier, 0, .{});
        defer broker.deinit();
        try broker.restore(dir);

        const topic = try broker.getOrCreateTopic(topic_name);
        try testing.expectEqual(103, topic.stream.last_sequence);
        const page = topic.stream.pages.items[0];
        try testing.expectEqual(3, page.offsets.items.len);
        try testing.expectEqual(5, page.ref_count);

        try testing.expectEqualStrings(topic.stream.message(101)[34..], "Iso ");
        try testing.expectEqualStrings(topic.stream.message(102)[34..], "medo u ducan ");
        try testing.expectEqualStrings(topic.stream.message(103)[34..], "nije reko dobar dan.");

        try testing.expectEqual(2, topic.channels.count());
        var channel1 = try topic.getOrCreateChannel(&channel1_name);
        var channel2 = try topic.getOrCreateChannel(&channel2_name);
        try testing.expectEqual(1, channel1.deferred.count());
        try testing.expectEqual(2, channel2.deferred.count());

        try testing.expectEqual(102, channel1.sequence);
        try testing.expectEqual(103, channel2.sequence);

        try testing.expect(channel1.paused);
        try testing.expect(!channel2.paused);

        try testing.expectEqual(2, channel1.metric.depth);
        try testing.expectEqual(2, channel2.metric.depth);
    }

    try std.fs.cwd().deleteTree(dir_name);
    time.now = 0;
}
