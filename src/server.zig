const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const math = std.math;
const builtin = @import("builtin");
const testing = std.testing;

const log = std.log.scoped(.server);
const Error = @import("io.zig").Error;
const protocol = @import("protocol.zig");
const Limits = @import("Options.zig").Limits;
const Store = @import("store.zig").Store;

fn nsFromMs(ms: u32) u64 {
    return @as(u64, @intCast(ms)) * std.time.ns_per_ms;
}

const empty_payload: []u8 = &.{};

pub fn ServerType(Consumer: type, Notifier: type) type {
    return struct {
        const Server = @This();
        const Topic = TopicType();
        pub const Channel = ChannelType();

        allocator: mem.Allocator,
        // channel_msg_pool: std.heap.MemoryPool(Channel.Msg),
        // topic_msg_pool: std.heap.MemoryPool(Topic.Msg),
        topics: std.StringHashMap(*Topic),
        notifier: *Notifier,
        limits: Limits,

        started_at: u64,
        metric: Topic.Metric = .{},
        metric_prev: Topic.Metric = .{},

        // Current timestamp, set in tick().
        now: u64 = 0,
        // Channels waiting for wake up at specific timestamp.
        timers: TimerQueue(Channel),

        // Init/deinit -----------------

        pub fn init(allocator: mem.Allocator, notifier: *Notifier, now: u64, limits: Limits) Server {
            return .{
                .allocator = allocator,
                //.channel_msg_pool = std.heap.MemoryPool(Channel.Msg).init(allocator),
                // .topic_msg_pool = std.heap.MemoryPool(Topic.Msg).init(allocator),
                .topics = std.StringHashMap(*Topic).init(allocator),
                .notifier = notifier,
                .started_at = now,
                .now = now,
                .timers = TimerQueue(Channel).init(allocator),
                .limits = limits,
            };
        }

        pub fn deinit(self: *Server) void {
            var iter = self.topics.iterator();
            while (iter.next()) |e| self.deinitTopic(e.value_ptr.*);
            self.topics.deinit();
            self.timers.deinit();
            // self.channel_msg_pool.deinit();
            // self.topic_msg_pool.deinit();
        }

        fn deinitTopic(self: *Server, topic: *Topic) void {
            const key = topic.name;
            topic.deinit();
            self.allocator.free(key);
            self.allocator.destroy(topic);
        }

        pub fn tick(self: *Server, ts: u64) u64 {
            self.now = ts;
            return self.timers.tick(ts);
        }

        fn tsFromDelay(self: *Server, delay_ms: u32) u64 {
            return self.now + nsFromMs(delay_ms);
        }

        fn channelCreated(self: *Server, topic_name: []const u8, name: []const u8) void {
            self.notifier.channelCreated(topic_name, name);
            log.debug("topic '{s}' channel '{s}' created", .{ topic_name, name });
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
            try self.checkLimits(topic_name, data.len);
            const topic = try self.getOrCreateTopic(topic_name);
            try topic.publish(data);
        }

        pub fn multiPublish(self: *Server, topic_name: []const u8, msgs: u32, data: []const u8) !void {
            try self.checkLimits(topic_name, data.len);
            const topic = try self.getOrCreateTopic(topic_name);
            try topic.multiPublish(msgs, data);
        }

        pub fn deferredPublish(self: *Server, topic_name: []const u8, data: []const u8, delay: u32) !void {
            try self.checkLimits(topic_name, data.len);
            const topic = try self.getOrCreateTopic(topic_name);
            try topic.deferredPublish(data, delay);
        }

        fn checkLimits(self: *Server, topic_name: []const u8, len: usize) !void {
            if (self.metric.depth_bytes + len > self.limits.max_mem) {
                log.err(
                    "{s} publish failed, server memory limit of {} bytes reached",
                    .{ topic_name, self.limits.max_mem },
                );
                return error.ServerMemoryOverflow;
            }
        }

        // Http interface actions -----------------

        pub fn createTopic(self: *Server, name: []const u8) !void {
            _ = try self.getOrCreateTopic(try protocol.validateName(name));
        }

        pub fn deleteTopic(self: *Server, name: []const u8) !void {
            const kv = self.topics.fetchRemove(name) orelse return error.NotFound;
            const topic = kv.value;
            topic.delete();
            self.deinitTopic(topic);
            self.notifier.topicDeleted(name);
            log.debug("deleted topic {s}", .{name});
        }

        pub fn createChannel(self: *Server, topic_name: []const u8, name: []const u8) !void {
            const topic = try self.getOrCreateTopic(try protocol.validateName(topic_name));
            _ = try topic.getOrCreateChannel(try protocol.validateName(name));
        }

        pub fn deleteChannel(self: *Server, topic_name: []const u8, name: []const u8) !void {
            const topic = self.topics.get(topic_name) orelse return error.NotFound;
            try topic.deleteChannel(name);
            self.notifier.channelDeleted(topic_name, name);
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
            topic.empty();
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

        const metadata_file_name = "nsql.dump";

        // metadata paused, topic, channel
        pub fn dump(self: *Server, dir: std.fs.Dir) !void {
            _ = self;
            _ = dir;
            // var meta_file = try dir.createFile(metadata_file_name, .{});
            // defer meta_file.close();
            // var meta_buf_writer = std.io.bufferedWriter(meta_file.writer());
            // var meta = meta_buf_writer.writer();
            // // Meta file header:
            // // | version (1) | topics count (4) |
            // try meta.writeByte(0); // version placeholder
            // try meta.writeInt(u32, self.topics.count(), .little);

            // var ti = self.topics.valueIterator();
            // while (ti.next()) |topic_ptr| {
            //     const topic = topic_ptr.*;
            //     var messages: usize = 0;

            //     // If there are messages in topic
            //     if (topic.first != null) {
            //         // Topic messages to the separate file named by topic name
            //         // For each message in topic:
            //         // | sequence (8) | created_at (8) | defer_until (8) | body_len (4) | body (body_len) |
            //         var topic_file = try dir.createFile(topic.name, .{});
            //         defer topic_file.close();

            //         var next = topic.first;
            //         while (next) |msg| : ({
            //             next = msg.next;
            //             messages += 1;
            //         }) {
            //             var header: [28]u8 = undefined;
            //             mem.writeInt(u64, header[0..8], msg.sequence, .little);
            //             mem.writeInt(u64, header[8..16], msg.created_at, .little);
            //             mem.writeInt(u64, header[16..24], msg.defer_until, .little);
            //             mem.writeInt(u32, header[24..28], @intCast(msg.body.len), .little);
            //             try topic_file.writeAll(&header);
            //             try topic_file.writeAll(msg.body);
            //         }
            //     }
            //     { // Topic meta:
            //         // | name_len (4) | name (name_len) | paused (1) | messages count (4) | channels count (4) |
            //         try meta.writeByte(@intCast(topic.name.len));
            //         try meta.writeAll(topic.name);
            //         try meta.writeByte(if (topic.paused) 1 else 0);
            //         try meta.writeInt(u32, @intCast(messages), .little);
            //         try meta.writeInt(u32, topic.channels.count(), .little);

            //         // For each channel:
            //         // | name_len (4) | name (name_len) | paused (1) | messages count (4) | next sequence (8) |
            //         // then for each message in channel:
            //         // | sequence (8) | timestamp (8) |
            //         var ci = topic.channels.valueIterator();
            //         while (ci.next()) |channel_ptr| {
            //             const channel = channel_ptr.*;
            //             try meta.writeByte(@intCast(channel.name.len));
            //             try meta.writeAll(channel.name);
            //             try meta.writeByte(if (channel.paused) 1 else 0);
            //             try meta.writeInt(u32, @intCast(channel.in_flight.count() + channel.deferred.count()), .little);
            //             try meta.writeInt(u64, if (channel.next) |n| n.sequence else 0, .little);
            //             for (channel.in_flight.values()) |msg| {
            //                 try meta.writeInt(u64, msg.sequence(), .little);
            //                 try meta.writeInt(u64, 0, .little);
            //             }
            //             var iter = channel.deferred.iterator();
            //             while (iter.next()) |msg| {
            //                 try meta.writeInt(u64, msg.sequence(), .little);
            //                 try meta.writeInt(u64, msg.timestamp, .little);
            //             }
            //         }
            //     }
            //     log.debug("dump topic {s}, channels {}, messages: {} ", .{ topic.name, topic.channels.count(), messages });
            // }
            // try meta_buf_writer.flush();
        }

        pub fn restore(self: *Server, dir: std.fs.Dir) !void {
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
                const messages = try meta.readInt(u32, .little);
                const channels = try meta.readInt(u32, .little);

                if (messages > 0) {
                    // Topic messages
                    var topic_file = try dir.openFile(topic.name, .{});
                    defer topic_file.close();
                    const rdr = topic_file.reader();
                    var i: usize = 0;
                    while (i < messages) : (i += 1) {
                        const header = buf[0..28];
                        try rdr.readNoEof(header);
                        const sequence = mem.readInt(u64, header[0..8], .little);
                        const created_at = mem.readInt(u64, header[8..16], .little);
                        const defer_until = mem.readInt(u64, header[16..24], .little);
                        const body_len = mem.readInt(u32, header[24..28], .little);
                        const body = try self.allocator.alloc(u8, body_len);
                        try rdr.readNoEof(body);
                        try topic.restore(sequence, created_at, defer_until, body);
                    }
                }

                // Channels
                var i: usize = 0;
                while (i < channels) : (i += 1) {
                    // Channel meta
                    const n = try meta.readByte();
                    if (n > protocol.max_name_len) return error.InvalidName;
                    try meta.readNoEof(buf[0..n]);
                    var channel = try topic.getOrCreateChannel(buf[0..n]);
                    channel.paused = try meta.readByte() != 0;
                    var channel_messages = try meta.readInt(u32, .little);
                    const next_sequence = try meta.readInt(u64, .little);
                    if (next_sequence > 0) try channel.restoreNext(next_sequence);
                    // Channel messages
                    while (channel_messages > 0) : (channel_messages -= 1) {
                        const sequence = try meta.readInt(u64, .little);
                        const timestamp = try meta.readInt(u64, .little);
                        try channel.restoreMsg(sequence, timestamp);
                    }
                }
                // Release reference created during restore.
                // It is now transferred to the channels.
                // if (topic.channels.count() > 0) if (topic.first) |first| first.release();

                log.debug("restored topic {s}, channels: {}, messages: {}", .{ topic.name, channels, messages });
            }
        }

        fn TopicType() type {
            return struct {
                // const Msg = struct {
                //     sequence: u64,
                //     created_at: u64,
                //     body: []const u8,
                //     // Defer message delivery until timestamp is reached.
                //     defer_until: u64 = 0,
                //     // Linked list next node pointer
                //     next: ?*Msg = null,
                //     // Reference counting
                //     topic: *Topic,
                //     rc: usize = 0,

                //     // Decrease reference counter and free
                //     pub fn release(self: *Msg) void {
                //         assert(self.rc > 0);
                //         self.rc -= 1;
                //         if (self.rc == 0) {
                //             const next = self.next;
                //             const topic = self.topic;
                //             topic.destroyMessage(self);
                //             if (next) |n| {
                //                 n.release();
                //             } else {
                //                 topic.last = null;
                //             }
                //         }
                //     }

                //     // If there is next message it will acquire and return, null if there is no next.
                //     pub fn nextAcquire(self: *Msg) ?*Msg {
                //         if (self.next) |n| return n.acquire();
                //         return null;
                //     }

                //     // Increase reference counter
                //     pub fn acquire(self: *Msg) *Msg {
                //         self.rc += 1;
                //         return self;
                //     }

                //     // Convert Topic.Msg to Channel.Msg
                //     fn asChannelMsg(self: *Msg) Channel.Msg {
                //         var header: [34]u8 = .{0} ** 34;
                //         { // Write to header
                //             const frame_type = @intFromEnum(@import("protocol.zig").FrameType.message);
                //             mem.writeInt(u32, header[0..4], @intCast(self.body.len + 30), .big); // size
                //             mem.writeInt(u32, header[4..8], frame_type, .big); // frame type
                //             mem.writeInt(u64, header[8..16], self.created_at, .big); // timestamp
                //             mem.writeInt(u16, header[16..18], 1, .big); // attempts
                //             mem.writeInt(u64, header[26..34], self.sequence, .big); // message id
                //         }
                //         return .{
                //             .msg = self, // Channel.Msg has pointer to Topic.Msg
                //             .header = header,
                //             .timestamp = self.defer_until,
                //         };
                //     }
                // };

                allocator: mem.Allocator,
                name: []const u8,
                server: *Server,
                channels: std.StringHashMap(*Channel),
                // Topic message sequence, used for message id.
                sequence: u64 = 0,
                // Hard pointer to the first message when topic has no channels.
                // Weak pointer if topic has channels.
                // first: ?*Msg = null,
                // Weak pointer to the end of linked list of topic messages
                // last: ?*Msg = null,
                paused: bool = false,
                metric: Metric = .{},
                metric_prev: Metric = .{},
                // msg_pool: *std.heap.MemoryPool(Msg),

                store: Store,

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
                        // .msg_pool = &server.topic_msg_pool,
                        .store = Store.init(allocator, .{
                            .ack_policy = .explicit,
                            .deliver_policy = .all,
                            .retention_policy = .all,
                            .page_size = 1024 * 1024,
                        }),
                    };
                }

                fn deinit(self: *Topic) void {
                    // self.empty();
                    var iter = self.channels.iterator();
                    while (iter.next()) |e| self.deinitChannel(e.value_ptr.*);
                    self.channels.deinit();
                    self.store.deinit();
                }

                fn empty(self: *Topic) void {
                    // If first is hard pointer release it.
                    // if (self.channels.count() == 0) if (self.first) |n| n.release();
                    _ = self;
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
                    const key = channel.name;
                    channel.deinit();
                    self.allocator.free(key);
                    self.allocator.destroy(channel);
                }

                fn subscribe(self: *Topic, consumer: *Consumer, name: []const u8) !*Channel {
                    const first_channel = self.channels.count() == 0;
                    const channel = try self.getOrCreateChannel(name);
                    try channel.subscribe(consumer);

                    if (first_channel) {
                        // First channel gets all messages from the topic
                        channel.metric.depth = self.metric.depth;
                        self.store.options.retention_policy = .interest;
                        self.store.options.deliver_policy = .new;
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
                    try channel.init(self, key);
                    errdefer channel.deinit();

                    self.channels.putAssumeCapacityNoClobber(key, channel);
                    self.server.channelCreated(self.name, channel.name);

                    channel.sequence = self.store.subscribe();
                    return channel;
                }

                fn checkLimits(self: Topic, msgs: u32, bytes: usize) !void {
                    if (self.metric.depth_bytes + bytes > self.server.limits.topic_max_mem) {
                        log.err(
                            "{s} publish failed, topic memory limit of {} bytes reached",
                            .{ self.name, self.server.limits.topic_max_mem },
                        );
                        return error.TopicMemoryOverflow;
                    }
                    if (self.metric.depth + msgs > self.server.limits.topic_max_msgs) {
                        log.err(
                            "{s} publish failed, topic max number of messages limit of {} reached",
                            .{ self.name, self.server.limits.topic_max_msgs },
                        );
                        return error.TopicMessagesOverflow;
                    }
                }

                fn storeAppend(self: *Topic, data: []const u8) !void {
                    const res = try self.store.alloc(@intCast(data.len + 34));
                    const header = res.data[0..34];
                    const body = res.data[34..];
                    {
                        mem.writeInt(u32, header[0..4], @intCast(data.len + 30), .big); // size
                        mem.writeInt(u32, header[4..8], @intFromEnum(protocol.FrameType.message), .big); // frame type
                        mem.writeInt(u64, header[8..16], self.server.now, .big); // timestamp
                        mem.writeInt(u16, header[16..18], 1, .big); // attempts
                        mem.writeInt(u64, header[18..26], 0, .big); // message id, first 8 bytes, unused
                        mem.writeInt(u64, header[26..34], res.sequence, .big); // message id, second 8 bytes
                    }
                    log.debug("topic {s} store append {} {}", .{ self.name, res.sequence, data.len });
                    @memcpy(body, data);
                }

                fn publish(self: *Topic, data: []const u8) !void {
                    try self.checkLimits(1, data.len);
                    try self.storeAppend(data);
                    self.notifyChannels(1);
                }

                fn deferredPublish(self: *Topic, data: []const u8, delay: u32) !void {
                    // TODO
                    try self.publish(data);
                    _ = delay;
                    //try self.checkLimits(1, data.len);
                    // var msg = try self.append(data);
                    // msg.defer_until = self.server.tsFromDelay(delay);
                    // self.notifyChannels(msg, 1);
                }

                fn multiPublish(self: *Topic, msgs: u32, data: []const u8) !void {
                    if (msgs == 0) return;
                    try self.checkLimits(msgs, data.len);
                    var pos: usize = 0;
                    for (0..msgs) |_| {
                        const len = mem.readInt(u32, data[pos..][0..4], .big);
                        pos += 4;
                        try self.storeAppend(data[pos..][0..len]);
                        pos += len;
                    }
                    self.notifyChannels(msgs);
                }

                fn restore(self: *Topic, sequence: u64, created_at: u64, defer_until: u64, body: []const u8) !void {
                    // TODO
                    _ = self;
                    _ = sequence;
                    _ = created_at;
                    _ = defer_until;
                    _ = body;
                    // const msg = try self.msg_pool.create();
                    // errdefer self.msg_pool.destroy(msg);
                    // assert(self.sequence == 0 or self.sequence + 1 == sequence);
                    // msg.* = .{
                    //     .topic = self,
                    //     .sequence = sequence,
                    //     .created_at = created_at,
                    //     .defer_until = defer_until,
                    //     .body = body,
                    //     .rc = 0,
                    // };
                    // try self.appendMsg(msg);
                    // self.sequence = sequence;
                }

                // // Create Topic.Msg add it to the linked list or topic messages
                // fn append(self: *Topic, data: []const u8) !*Msg {
                //     try self.storeAppend(data);

                //     // Allocate message and body
                //     const msg = try self.msg_pool.create();
                //     errdefer self.msg_pool.destroy(msg);
                //     const body = try self.allocator.dupe(u8, data);
                //     errdefer self.allocator.free(body);

                //     // Init message
                //     self.sequence += 1;
                //     msg.* = .{
                //         .topic = self,
                //         .sequence = self.sequence,
                //         .created_at = self.server.now,
                //         .body = body,
                //         .rc = 0,
                //     };

                //     try self.appendMsg(msg);
                //     return msg;
                // }

                // fn appendMsg(self: *Topic, msg: *Msg) !void {
                //     const bytes = msg.body.len;
                //     self.metric.inc(bytes);
                //     self.server.metric.inc(bytes);
                //     { // Update last pointer
                //         if (self.last) |prev| {
                //             assert(prev.sequence + 1 == msg.sequence);
                //             prev.next = msg.acquire(); // Previous points to new
                //         }
                //         self.last = msg;
                //     }
                //     // If there is no channels messages are accumulated in
                //     // topic. We hold hard pointer to the first message (to
                //     // prevent release).
                //     if (self.first == null)
                //         self.first = if (self.channels.count() == 0) msg.acquire() else msg;
                // }

                // fn destroyMessage(self: *Topic, msg: *Msg) void {
                //     assert(self.first.? == msg); // must be in order
                //     self.first = msg.next;

                //     self.metric.dec(msg.body.len);
                //     self.server.metric.dec(msg.body.len);
                //     self.allocator.free(msg.body);
                //     self.msg_pool.destroy(msg);
                // }

                // Notify all channels that there is pending messages
                fn notifyChannels(self: *Topic, msgs: u32) void {
                    var iter = self.channels.valueIterator();
                    while (iter.next()) |ptr| {
                        const channel = ptr.*;
                        channel.topicAppended(msgs);
                    }
                }

                // fn findMsg(self: *Topic, sequence: u64) ?*Msg {
                //     var next = self.first;
                //     while (next) |msg| : (next = msg.next) {
                //         if (msg.sequence == sequence) return msg;
                //     }
                //     return null;
                // }

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

                fn unpause(self: *Topic) !void {
                    self.paused = false;
                    // wake-up all channels
                    var iter = self.channels.valueIterator();
                    while (iter.next()) |ptr| try ptr.*.wakeup();
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
                pub const InFlightMsg = struct {
                    timeout_at: u64 = 0,
                    consumer_id: u32 = 0,
                    attempts: u16 = 1,

                    const Self = @This();

                    fn setAttempts(self: Self, payload: []u8) void {
                        const buf = payload[16..18];
                        mem.writeInt(u16, buf, self.attempts, .big);
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

                const DeferredMsg = struct {
                    sequence: u64,
                    defer_until: u64 = 0,
                    attempts: u16 = 1,

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
                // Timestamp when next onTimer will be called
                timer_ts: u64,
                timers: *TimerQueue(Channel),
                now: *u64,
                // Sent but not jet acknowledged (fin) messages.
                in_flight: std.AutoArrayHashMap(u64, InFlightMsg),
                // Re-queued by consumer, timed-out or defer published messages.
                deferred: std.PriorityQueue(DeferredMsg, void, DeferredMsg.less),

                metric: Metric = .{},
                metric_prev: Metric = .{},
                paused: bool = false,
                ephemeral: bool = false,

                // Last store sequence consumed by this channel
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
                        .in_flight = std.AutoArrayHashMap(u64, InFlightMsg).init(allocator),
                        .deferred = std.PriorityQueue(DeferredMsg, void, DeferredMsg.less).init(allocator, {}),
                        .timers = &topic.server.timers,
                        .now = &topic.server.now,
                        .timer_ts = infinite,
                        .ephemeral = protocol.isEphemeral(name),
                    };
                }

                fn delete(self: *Channel) void {
                    for (self.consumers.items) |consumer| consumer.channelClosed();
                }

                fn deinit(self: *Channel) void {
                    self.timers.remove(self);
                    self.in_flight.deinit();
                    self.deferred.deinit();
                    self.consumers.deinit();
                }

                fn subscribe(self: *Channel, consumer: *Consumer) !void {
                    try self.consumers.append(consumer);
                }

                // -----------------

                // Called from topic when new topic message is created.
                fn topicAppended(self: *Channel, msgs: u32) void {
                    self.metric.depth += msgs;
                    self.wakeup() catch |err| {
                        if (!builtin.is_test)
                            log.err("fail to wakeup channel {s}: {}", .{ self.name, err });
                    };
                }

                // Notify consumers that there are messages to pull.
                fn wakeup(self: *Channel) !void {
                    while (self.consumers_iterator.next()) |consumer| {
                        if (!self.topic.store.hasNext(self.sequence)) break;
                        try consumer.wakeup();
                    }
                }

                fn restoreNext(self: *Channel, sequence: u64) !void {
                    _ = self;
                    _ = sequence;
                    // var topic_msg = self.topic.findMsg(sequence) orelse return error.InvalidSequence;
                    // self.next = topic_msg.acquire();
                    // self.metric.depth += self.topic.last.?.sequence - topic_msg.sequence + 1;
                }

                fn restoreMsg(self: *Channel, sequence: u64, timestamp: u64) !void {
                    _ = self;
                    _ = sequence;
                    _ = timestamp;
                    // var topic_msg = self.topic.findMsg(sequence) orelse return error.InvalidSequence;
                    // const msg = try self.msg_pool.create();
                    // errdefer self.msg_pool.destroy(msg);
                    // msg.* = topic_msg.asChannelMsg();
                    // msg.timestamp = timestamp;
                    // try self.deferred.add(msg);
                    // _ = topic_msg.acquire();
                    // self.metric.depth += 1;
                }

                fn tsFromDelay(self: *Channel, delay_ms: u32) u64 {
                    return self.topic.server.tsFromDelay(delay_ms);
                }

                // Add many in flight messages with sequences from - to.
                fn addInFlight(
                    self: *Channel,
                    from_sequence: u64,
                    to_sequence: u64,
                    consumer_id: u32,
                    msg_timeout: u32,
                ) !void {
                    const msgs_count: usize = @intCast(to_sequence - from_sequence + 1);
                    try self.in_flight.ensureUnusedCapacity(msgs_count);

                    const timeout_at = self.tsFromDelay(msg_timeout);
                    for (from_sequence..to_sequence + 1) |sequence| {
                        const ifm = InFlightMsg{
                            .timeout_at = timeout_at,
                            .consumer_id = consumer_id,
                            .attempts = 1,
                        };
                        self.in_flight.putAssumeCapacityNoClobber(sequence, ifm);
                    }
                    self.updateTimer(timeout_at);
                }

                // Defer in flight message.
                // Moves message from in-flight to the deferred.
                fn deferInFlight(self: *Channel, sequence: u64, ifm: InFlightMsg, delay: u32) !void {
                    try self.deferred.ensureUnusedCapacity(1);
                    const dm = DeferredMsg{
                        .sequence = sequence,
                        .defer_until = if (delay == 0) 0 else self.tsFromDelay(delay),
                        .attempts = ifm.attempts + 1,
                    };
                    if (dm.defer_until > 0) self.updateTimer(dm.defer_until);
                    self.deferred.add(dm) catch unreachable; // capacity is ensured
                    assert(self.in_flight.swapRemove(sequence));
                }

                // Find deferred message, make copy of that message, set
                // increased attempts to message payload, put it in-flight and
                // return payload.
                fn popDeferred(self: *Channel, consumer_id: u32, msg_timeout: u32) ?[]const u8 {
                    if (self.deferred.peek()) |dm| if (dm.defer_until <= self.now.*) {
                        const payload = self.allocator.dupe(u8, self.topic.store.message(dm.sequence)) catch |err| {
                            log.err("failed to duplicate message {}", .{err});
                            return null;
                        };
                        self.in_flight.ensureUnusedCapacity(1) catch |err| {
                            log.err("failed to add in flight message {}", .{err});
                            return null;
                        };
                        const ifm = InFlightMsg{
                            .timeout_at = self.tsFromDelay(msg_timeout),
                            .consumer_id = consumer_id,
                            .attempts = dm.attempts,
                        };
                        ifm.setAttempts(payload);
                        self.updateTimer(ifm.timeout_at);
                        self.in_flight.putAssumeCapacityNoClobber(dm.sequence, ifm);
                        _ = self.deferred.remove();
                        return payload;
                    };
                    return null;
                }

                fn updateTimer(self: *Channel, next_ts: u64) void {
                    if (self.timer_ts <= next_ts) return;
                    self.timer_ts = next_ts;
                    self.timers.update(self) catch {};
                }

                // Callback when timer timeout if fired
                fn onTimer(self: *Channel, now: u64) void {
                    self.timer_ts = @min(
                        self.inFlightTimeout(now) catch infinite,
                        self.deferredTimeout(now) catch infinite,
                    );
                }

                // Returns next timeout of deferred messages
                fn deferredTimeout(self: *Channel, now: u64) !u64 {
                    var ts: u64 = infinite;
                    if (self.deferred.count() > 0) {
                        try self.wakeup();
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
                    var ts: u64 = infinite;

                    const keys = self.in_flight.keys();
                    const values = self.in_flight.values();
                    var i = values.len;
                    while (i > 0) {
                        i -= 1;
                        const msg = values[i];
                        if (msg.timeout_at <= now) {
                            const sequence = keys[i];
                            try self.deferInFlight(sequence, msg, 0);
                            self.metric.timeout += 1;
                            log.warn("{} message timeout {}", .{ msg.consumer_id, sequence });
                        } else {
                            if (ts > msg.timeout_at) ts = msg.timeout_at;
                        }
                    }

                    return ts;
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
                pub const SendChunk = struct {
                    data: []const u8,
                    msgs_count: u32 = 1,

                    store: ?*Store = null,
                    sequence: u64 = 0,
                    allocator: ?mem.Allocator = null,

                    // Consumer should call this when data is no more in use by
                    // underlying network interface; when io_uring send
                    // operation is completed. This will release store reference
                    // or deallocate buffer (in case op copied message;
                    // deferred).
                    pub fn done(self: SendChunk) void {
                        if (self.store) |store| {
                            store.fin(self.sequence);
                        }
                        if (self.allocator) |allocator| {
                            allocator.free(self.data);
                        }
                    }
                };

                pub fn popMsgs(self: *Channel, consumer_id: u32, msg_timeout: u32, ready_count: u32) ?SendChunk {
                    assert(ready_count > 0);
                    assert(consumer_id > 0);

                    if (self.popDeferred(consumer_id, msg_timeout)) |data| {
                        assert(data.len >= 34);
                        return .{
                            .data = data,
                            .msgs_count = 1,
                            .allocator = self.allocator,
                        };
                    }

                    if (self.topic.store.next(self.sequence, ready_count)) |res| {
                        const msgs_count: u32 = @intCast(res.sequence.to - res.sequence.from + 1);
                        self.addInFlight(res.sequence.from, res.sequence.to, consumer_id, msg_timeout) catch |err| {
                            // release store references
                            self.topic.store.fin(res.sequence.from);
                            for (res.sequence.from..res.sequence.to + 1) |sequence| {
                                self.topic.store.fin(sequence);
                            }
                            log.err("failed to add {} messages to in flight {}", .{ msgs_count, err });
                            return null;
                        };

                        self.metric.pull += msgs_count;
                        self.sequence = res.sequence.to;
                        return .{
                            .data = res.data,
                            .msgs_count = msgs_count,
                            .store = &self.topic.store,
                            .sequence = res.sequence.from,
                        };
                    }
                    return null;
                }

                pub fn finish(self: *Channel, consumer_id: u32, msg_id: [16]u8) !void {
                    const sequence = InFlightMsg.seqFromId(msg_id);
                    const ifm = self.in_flight.get(sequence) orelse return error.MessageNotInFlight;
                    if (ifm.consumer_id != consumer_id) return error.MessageNotInFlight;

                    self.topic.store.fin(sequence);
                    assert(self.in_flight.swapRemove(sequence));
                    self.metric.finish += 1;
                    self.metric.depth -|= 1;
                }

                /// Extend message timeout for interval (milliseconds).
                pub fn touch(self: *Channel, consumer_id: u32, msg_id: [16]u8, interval: u32) !void {
                    const sequence = InFlightMsg.seqFromId(msg_id);
                    const ifm = self.in_flight.getPtr(sequence) orelse return error.MessageNotInFlight;
                    if (ifm.consumer_id != consumer_id) return error.MessageNotInFlight;

                    ifm.timeout_at += nsFromMs(interval);
                }

                pub fn requeue(self: *Channel, consumer_id: u32, msg_id: [16]u8, delay: u32) !void {
                    const sequence = InFlightMsg.seqFromId(msg_id);
                    const ifm = self.in_flight.get(sequence) orelse return error.MessageNotInFlight;
                    if (ifm.consumer_id != consumer_id) return error.MessageNotInFlight;

                    try self.deferInFlight(sequence, ifm, delay);
                    self.metric.requeue += 1;
                }

                pub fn unsubscribe(self: *Channel, consumer: *Consumer) void {
                    self.requeueAll(consumer) catch |err| {
                        log.warn("failed to remove in flight messages for socket {}, {}", .{ consumer.socket, err });
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
                fn requeueAll(self: *Channel, consumer: *Consumer) !void {
                    const keys = self.in_flight.keys();
                    const values = self.in_flight.values();
                    var i = values.len;
                    while (i > 0) {
                        i -= 1;
                        const msg = values[i];
                        if (msg.consumer_id == consumer.id()) {
                            const sequence = keys[i];
                            try self.deferInFlight(sequence, msg, 0);
                            self.metric.requeue += 1;
                        }
                    }
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
                    _ = self;
                    // { // Remove in-flight messages that are not currently in the kernel.
                    //     var msgs = std.ArrayList(*Msg).init(self.allocator);
                    //     defer msgs.deinit();
                    //     for (self.consumers.items) |consumer| { // For each consumer
                    //         for (self.in_flight.values()) |cm| {
                    //             if (cm.in_flight_socket == consumer.socket and // If that messages is in-flight on that consumer
                    //                 cm.sent_at < consumer.sent_at) // And send is returned from kernel
                    //                 try msgs.append(cm);
                    //         }
                    //     }
                    //     if (msgs.items.len == self.in_flight.count()) { // Hopefully there is no messages in the kernel
                    //         // Remove all
                    //         for (self.in_flight.values()) |cm| cm.destroy(self.msg_pool);
                    //         self.in_flight.clearAndFree();
                    //     } else {
                    //         // Selectively remove
                    //         for (msgs.items) |cm| {
                    //             assert(self.in_flight.swapRemove(cm.sequence()));
                    //             cm.destroy(self.msg_pool);
                    //         }
                    //     }
                    // }
                    // // Remove all deferred messages.
                    // while (self.deferred.removeOrNull()) |cm| cm.destroy(self.msg_pool);
                    // // Position to the end of the topic.
                    // if (self.next) |n| n.release();
                    // self.next = null;
                    // self.metric.depth = self.in_flight.count();
                }
            };
        }
    };
}

test "channel consumers iterator" {
    const allocator = testing.allocator;

    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &notifier, 0, .{});
    defer server.deinit();

    var consumer1 = TestConsumer.init(allocator);
    var channel = try server.subscribe(&consumer1, "topic", "channel");

    var iter = channel.consumers_iterator;
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

    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &notifier, 0, .{});
    const idFromSeq = TestServer.Channel.Msg.idFromSeq;
    defer server.deinit();

    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();
    consumer.ready_count = 0;
    var channel = try server.subscribe(&consumer, topic_name, channel_name);
    consumer.channel = channel;
    const topic = server.topics.get(topic_name).?;

    try server.publish(topic_name, "1");
    try server.publish(topic_name, "2");
    try server.publish(topic_name, "3");
    try testing.expect(topic.first.?.sequence == 1);

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
        try channel.wakeup();
        try testing.expectEqual(1, consumer.lastSeq());
        // 1 is in flight
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(2, channel.next.?.sequence);
    }
    { // consumer sends fin, 0 in flight after that
        try testing.expect(channel.finish(consumer.lastId())); // fin 1
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(2, channel.metric.depth);
        try testing.expect(topic.first.?.sequence == 2);
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
        try testing.expect(channel.finish(idFromSeq(3))); // fin 3
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(1, channel.deferred.count());
        try testing.expect(topic.first.?.sequence == 2);
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
        try testing.expect(channel.finish(idFromSeq(2))); // fin 2
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(0, channel.metric.depth);
        try testing.expect(topic.first == null);
        try testing.expect(topic.metric.depth_bytes == 0);
        try testing.expect(topic.metric.depth == 0);
    }
    { // consumer unsubscribe re-queues in-flight messages
        try server.publish(topic_name, "4");
        consumer.ready_count = 1;
        try channel.wakeup();
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
    channel: ?*TestServer.Channel = null,
    sequences: std.ArrayList(u64) = undefined,
    ready_count: usize = 1,
    sent_at: u64 = 0,

    fn init(allocator: mem.Allocator) Self {
        return .{
            .allocator = allocator,
            .sequences = std.ArrayList(u64).init(allocator),
        };
    }

    fn deinit(self: *Self) void {
        self.sequences.deinit();
    }

    fn wakeup(self: *Self) !void {
        {
            // Making this method fallible in check all allocations
            const buf = try self.allocator.alloc(u8, 8);
            defer self.allocator.free(buf);
        }
        while (self.ready_count > 0) {
            var msg = try self.channel.?.popMsg(self) orelse break;
            try self.sequences.append(msg.sequence());
            self.ready_count -= 1;
        }
    }

    fn ready(self: *Self) bool {
        return self.ready_count > 0;
    }

    // send fin for the last received message
    fn fin(self: *Self) void {
        //if (self.sequences.items.len == 0) return;
        assert(self.channel.?.finish(self.lastId()));
    }

    fn requeue(self: *Self) !void {
        assert(try self.channel.?.requeue(self.lastId(), 0));
    }

    // pull message from channel
    fn pull(self: *Self) !void {
        self.ready_count = 1;
        try self.wakeup();
    }

    fn pullAndFin(self: *Self) !void {
        try self.pull();
        self.fin();
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
};

var noop_notifier = NoopNotifier{};
const TestServer = ServerType(TestConsumer, NoopNotifier);

test "multiple channels" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name1 = "channel1";
    const channel_name2 = "channel2";
    const no = 1024;

    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &notifier, 0, .{});
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

    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &notifier, 0, .{});
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
        consumer.fin();
    }
    try testing.expectEqual(0, channel.metric.depth);
}

test "timeout messages" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name = "channel";
    const no = 4;

    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &notifier, 0, .{});
    defer server.deinit();

    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();
    const channel = try server.subscribe(&consumer, topic_name, channel_name);
    consumer.channel = channel;

    for (0..no) |i| {
        server.now = i + 1;
        try server.publish(topic_name, "message body");
        try consumer.pull();
    }
    try testing.expectEqual(4, channel.in_flight.count());

    { // check expire_at for in flight messages
        for (channel.in_flight.values()) |msg| {
            try testing.expectEqual(1, msg.attempts());
            try testing.expect(msg.timeout > nsFromMs(consumer.msgTimeout()) and
                msg.timeout <= nsFromMs(consumer.msgTimeout()) + no);
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
        consumer.fin();
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

    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &notifier, 0, .{});
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
        server.now = nsFromMs(1);
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
        server.now = nsFromMs(3);
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

    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &notifier, 0, .{});
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

    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &notifier, 0, .{});
    defer server.deinit();

    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();

    const channel = try server.subscribe(&consumer, topic_name, channel_name);
    consumer.channel = channel;

    server.now = 1;
    try server.publish(topic_name, "message 1");
    try server.publish(topic_name, "message 2");

    server.now = 2;
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

test "ephemeral channel" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name = "channel#ephemeral";

    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &notifier, 0, .{});
    defer server.deinit();

    var consumer = TestConsumer.init(allocator);
    defer consumer.deinit();

    const channel = try server.subscribe(&consumer, topic_name, channel_name);
    consumer.channel = channel;
    const topic = consumer.channel.?.topic;
    try testing.expect(channel.ephemeral);

    try testing.expectEqual(1, topic.channels.count());
    channel.unsubscribe(&consumer);
    try testing.expectEqual(0, topic.channels.count());
}

test noop_notifier {
    const allocator = testing.allocator;

    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &notifier, 0, .{});
    defer server.deinit();

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
    try testing.expectEqual(5, notifier.call_count);
    try server.deleteTopic("topic1");
    try testing.expectEqual(6, notifier.call_count);
    try server.deleteChannel("topic2", "channel2");
    try testing.expectEqual(7, notifier.call_count);
}

test "depth" {
    const allocator = testing.allocator;
    const topic_name = "topic";

    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &notifier, 0, .{});
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

test "check allocations" {
    const allocator = testing.allocator;
    try publishFinish(allocator);

    try testing.checkAllAllocationFailures(allocator, publishFinish, .{});
}

fn publishFinish(allocator: mem.Allocator) !void {
    const topic_name = "topic";
    var notifier = NoopNotifier{};
    var server = TestServer.init(allocator, &notifier, 0, .{});
    defer server.deinit();

    // Create 2 channels
    const channel1_name = "channel1";
    const channel2_name = "channel2";
    try server.createChannel(topic_name, channel1_name);
    try server.createChannel(topic_name, channel2_name);

    // Publish some messages
    try server.publish(topic_name, "message 1");
    try server.publish(topic_name, "message 2");
    const topic = server.getOrCreateTopic(topic_name) catch unreachable;
    try testing.expectEqual(2, topic.metric.depth);

    // 1 consumer for channel 1 and 2 consumers for channel 2
    var channel1_consumer = TestConsumer.init(allocator);
    defer channel1_consumer.deinit();
    channel1_consumer.channel = try server.subscribe(&channel1_consumer, topic_name, channel1_name);

    var channel2_consumer1 = TestConsumer.init(allocator);
    defer channel2_consumer1.deinit();
    channel2_consumer1.channel = try server.subscribe(&channel2_consumer1, topic_name, channel2_name);
    var channel2_consumer2 = TestConsumer.init(allocator);
    defer channel2_consumer2.deinit();
    channel2_consumer2.channel = try server.subscribe(&channel2_consumer1, topic_name, channel2_name);

    // Consume messages
    try channel1_consumer.pullAndFin();
    try channel1_consumer.pullAndFin();
    try channel2_consumer1.pullAndFin();
    try channel2_consumer2.pullAndFin();
    try testing.expectEqual(0, topic.metric.depth);
    try testing.expectEqual(2, channel1_consumer.sequences.items.len);
    try testing.expectEqual(1, channel2_consumer1.sequences.items.len);
    try testing.expectEqual(1, channel2_consumer2.sequences.items.len);

    // Publish some more
    try server.publish(topic_name, "message 3");
    try server.publish(topic_name, "message 4");
    try server.deleteChannel(topic_name, channel1_name);

    // Re-queue from one consumer 1 to consumer 2
    const channel2 = channel2_consumer2.channel.?;
    try channel2_consumer1.pull();
    try testing.expectEqual(3, channel2_consumer1.lastSeq());
    try channel2_consumer1.requeue();
    try testing.expectEqual(1, channel2.deferred.count());
    try channel2_consumer2.pull();
    try testing.expectEqual(3, channel2_consumer2.lastSeq());
    // Unsubscribe consumer2
    try testing.expectEqual(1, channel2.in_flight.count());
    try channel2.requeueAll(&channel2_consumer2);
    channel2.unsubscribe(&channel2_consumer2);
    try testing.expectEqual(0, channel2.in_flight.count());
    try testing.expectEqual(1, channel2.deferred.count());
    try channel2_consumer1.pull();
    try testing.expectEqual(3, channel2_consumer1.lastSeq());

    try channel2.empty();
    try testing.expectEqual(1, channel2.in_flight.count());
    try server.deleteTopic(topic_name);
}

pub const infinite: u64 = std.math.maxInt(u64);

pub fn TimerQueue(comptime T: type) type {
    return struct {
        pq: PQ,

        const PQ = std.PriorityQueue(*T, void, less);
        const Self = @This();

        fn less(_: void, a: *T, b: *T) math.Order {
            return math.order(a.timer_ts, b.timer_ts);
        }

        pub fn init(allocator: mem.Allocator) Self {
            return .{
                .pq = PQ.init(allocator, {}),
            };
        }

        pub fn deinit(self: *Self) void {
            self.pq.deinit();
        }

        pub fn add(self: *Self, elem: *T) !void {
            try self.pq.add(elem);
        }

        pub fn remove(self: *Self, elem: *T) void {
            const index = blk: {
                var idx: usize = 0;
                while (idx < self.pq.items.len) : (idx += 1) {
                    const item = self.pq.items[idx];
                    if (item == elem) break :blk idx;
                }
                return;
            };
            _ = self.pq.removeIndex(index);
        }

        pub fn update(self: *Self, elem: *T) !void {
            self.remove(elem);
            if (elem.timer_ts == infinite) return;
            try self.add(elem);
        }

        pub fn tick(self: *Self, ts: u64) u64 {
            while (self.pq.peek()) |elem| {
                if (elem.timer_ts > ts) return elem.timer_ts;
                _ = self.pq.remove();
                elem.onTimer(ts);
                if (elem.timer_ts > ts and elem.timer_ts < infinite)
                    self.add(elem) catch {};
            }
            return infinite;
        }

        pub fn next(self: *Self) u64 {
            if (self.pq.peek()) |elem| return elem.timer_ts;
            return infinite;
        }
    };
}

test "timer queue" {
    const allocator = testing.allocator;
    const C = struct {
        timer_ts: u64 = 0,
        count: usize = 0,
        pub fn onTimer(self: *Self, ts: u64) void {
            assert(ts <= self.timer_ts);
            self.count += 1;
        }
        const Self = @This();
    };

    var pq = TimerQueue(C).init(allocator);
    defer pq.deinit();

    var c1 = C{ .timer_ts = 5 };
    try pq.add(&c1);
    try testing.expectEqual(5, pq.next());

    var c2 = C{ .timer_ts = 3 };
    try pq.add(&c2);
    try testing.expectEqual(3, pq.next());

    try testing.expectEqual(3, pq.tick(2));
    _ = pq.tick(3);
    try testing.expectEqual(1, c2.count);
    try testing.expectEqual(0, c1.count);
    try testing.expectEqual(5, pq.next());

    c2.timer_ts = 4;
    try pq.add(&c2);
    try testing.expectEqual(4, pq.next());

    c2.timer_ts = 10;
    try pq.update(&c2);
    try testing.expectEqual(5, pq.next());

    c1.timer_ts = 20;
    try pq.update(&c1);
    try testing.expectEqual(10, pq.next());

    pq.remove(&c2);
    try testing.expectEqual(20, pq.tick(11));
    pq.remove(&c1);
    try testing.expectEqual(infinite, pq.tick(0));
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
        var server = TestServer.init(allocator, &noop_notifier, 0, .{});
        defer server.deinit();
        var topic = try server.getOrCreateTopic(topic_name);

        var dummy = TestConsumer.init(allocator); // used only for subscribe
        defer dummy.deinit();
        dummy.ready_count = 0;
        var channel1 = try server.subscribe(&dummy, topic_name, &channel1_name);
        var channel2 = try server.subscribe(&dummy, topic_name, &channel2_name);

        { // add 3 messages to the topic
            topic.sequence = 100;
            server.now = now;
            try server.publish(topic_name, "Iso "); // msg 1, sequence: 101
            server.now += ns_per_s;
            try server.publish(topic_name, "medo u ducan "); // msg 2, sequence: 102
            server.now += ns_per_s;
            try server.deferredPublish(topic_name, "nije reko dobar dan.", 987); // msg 3, sequence: 103
        }

        // channel 1: 1-finished, 2-in flight, 3-next
        const msg1 = try channel1.nextMsg(0);
        _ = try channel1.nextMsg(0);
        try testing.expect(channel1.finish(msg1.?.id()));
        try testing.expectEqual(0, channel1.deferred.count());
        try testing.expectEqual(1, channel1.in_flight.count());

        // channel 2: 1-in flight, 2-finished, 3-in flight, next null
        _ = try channel2.nextMsg(0);
        const msg2 = try channel2.nextMsg(0);
        try testing.expect(try channel2.nextMsg(0) == null);
        try testing.expect(channel2.finish(msg2.?.id()));
        try testing.expectEqual(1, channel2.deferred.count());
        try testing.expectEqual(1, channel2.in_flight.count());
        channel1.pause();
        try testing.expectEqual(2, channel1.metric.depth);
        try testing.expectEqual(2, channel2.metric.depth);

        try testing.expectEqual(3, topic.metric.depth);
        try testing.expectEqual(37, topic.metric.depth_bytes);
        try testing.expectEqual(1, topic.first.?.rc);
        try testing.expectEqual(2, topic.first.?.next.?.rc);
        try testing.expectEqual(3, topic.first.?.next.?.next.?.rc);

        try server.dump(dir);
    }
    { // restore in another instance
        var server = TestServer.init(allocator, &noop_notifier, 0, .{});
        defer server.deinit();
        try server.restore(dir);

        const topic = try server.getOrCreateTopic(topic_name);
        var msg = topic.first.?;
        try testing.expectEqual(101, msg.sequence);
        try testing.expectEqual(now, msg.created_at);
        try testing.expectEqual(0, msg.defer_until);
        try testing.expectEqualStrings(msg.body, "Iso ");
        msg = msg.next.?;
        try testing.expectEqual(102, msg.sequence);
        try testing.expectEqual(now + ns_per_s, msg.created_at);
        try testing.expectEqual(0, msg.defer_until);
        try testing.expectEqualStrings(msg.body, "medo u ducan ");
        msg = msg.next.?;
        try testing.expectEqual(103, msg.sequence);
        try testing.expectEqual(now + 2 * ns_per_s, msg.created_at);
        try testing.expectEqual(msg.created_at + 987 * std.time.ns_per_ms, msg.defer_until);
        try testing.expectEqualStrings(msg.body, "nije reko dobar dan.");
        try testing.expect(msg.next == null);
        try testing.expectEqual(103, topic.sequence);

        try testing.expectEqual(2, topic.channels.count());
        var channel1 = try topic.getOrCreateChannel(&channel1_name);
        var channel2 = try topic.getOrCreateChannel(&channel2_name);
        try testing.expectEqual(1, channel1.deferred.count());
        try testing.expectEqual(2, channel2.deferred.count());
        try testing.expect(channel2.next == null);
        try testing.expectEqual(103, channel1.next.?.sequence);

        try testing.expect(channel1.paused);
        try testing.expect(!channel2.paused);
        try testing.expectEqual(2, channel1.metric.depth);
        try testing.expectEqual(2, channel2.metric.depth);

        try testing.expectEqual(3, topic.metric.depth);
        try testing.expectEqual(37, topic.metric.depth_bytes);
        try testing.expectEqual(1, topic.first.?.rc);
        try testing.expectEqual(2, topic.first.?.next.?.rc);
        try testing.expectEqual(3, topic.first.?.next.?.next.?.rc);
    }

    try std.fs.cwd().deleteTree(dir_name);
}

test "hash map" {
    var m = std.AutoArrayHashMap(usize, usize).init(testing.allocator);
    defer m.deinit();

    for (0..128) |i| {
        try m.put(i, i);
    }
    const keys = m.keys();
    assert(keys.len == 128);
    var i = keys.len;
    while (i > 0) {
        i -= 1;
        //for (m.keys()) |k| {
        const k = keys[i];
        if (k % 2 == 0) {
            std.debug.print("removing k={}\n", .{k});
            assert(m.swapRemove(k));
        }
    }
    for (m.keys()) |k| {
        std.debug.print("k={}\n", .{k});
    }
    assert(m.keys().len == 64);
}

test "sizeOf" {
    std.debug.print("{}\n", .{@sizeOf(TestServer.Channel.InFlightMsg)});
    std.debug.print("{}\n", .{@sizeOf(TestServer.Channel.DeferredMsg)});

    var x: u64 = 0;
    x -|= 1;
    std.debug.print("x={}\n", .{x});
}
