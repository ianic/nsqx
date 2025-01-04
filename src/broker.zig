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

const Counter = @import("statsd.zig").Counter;
const Gauge = @import("statsd.zig").Gauge;

fn nsFromMs(ms: u32) u64 {
    return @as(u64, @intCast(ms)) * std.time.ns_per_ms;
}
var time: struct {
    now: u64 = 0,

    fn after(self: @This(), delay_ms: u32) u64 {
        return self.now + nsFromMs(delay_ms);
    }
} = .{};

pub fn BrokerType(Client: type) type {
    return struct {
        const Broker = @This();
        const Topic = TopicType();
        const Channel = ChannelType();

        pub const Consumer = struct {
            // set by client
            ready_count: u32 = 0,
            msg_timeout: u32 = 0,
            // set in subscribe
            client: *Client = undefined,
            channel: *Channel = undefined,
            //
            in_flight_count: u32 = 0,
            in_kernel_buffers: u32 = 0,

            metric: struct {
                connected_at: u64 = 0,
                // Total number of
                send: usize = 0,
                finish: usize = 0,
                requeue: usize = 0,
            } = .{},

            const Self = @This();

            /// Is client ready to send
            fn ready(self: Self) bool {
                return self.in_flight_count < self.ready_count;
            }

            fn readyCount(self: Self) u32 {
                return if (self.ready()) self.ready_count - self.in_flight_count else 0;
            }

            // Client api -----------------

            pub fn finish(self: *Self, msg_id: [16]u8) !void {
                self.in_flight_count -|= 1;
                try self.channel.finish(self, msg_id);
                self.metric.finish += 1;
            }

            pub fn requeue(self: *Self, msg_id: [16]u8, delay: u32) !void {
                self.in_flight_count -|= 1;
                try self.channel.requeue(self, msg_id, delay);
                self.metric.requeue += 1;
            }

            pub fn touch(self: *Self, msg_id: [16]u8) !void {
                try self.channel.touch(self, msg_id);
            }

            pub fn unsubscribe(self: *Self) void {
                self.channel.unsubscribe(self);
            }

            /// Pull data from channel and send to client
            pub fn pull(self: *Self) !void {
                while (self.ready()) {
                    const count, const data = try self.channel.pull(self) orelse return;
                    self.metric.send +%= count;
                    self.in_flight_count += count;
                    self.in_kernel_buffers += 1;
                    try self.client.send(data);
                }
            }

            /// Notification that client has finished sending data is no more in
            /// kernel and can be released now.
            pub fn onSend(self: *Self, data: []const u8) void {
                self.in_kernel_buffers -= 1;
                const msg = protocol.parseMessageFrame(data) catch unreachable;
                if (msg.attempts == 1) return;
                // release data allocated in channel
                self.channel.allocator.free(data);
            }

            // Client api -----------------

            fn close(self: *Self) void {
                self.client.close();
            }
        };

        const Registrations = struct {
            const Self = @This();
            stream: store.Stream,

            ptr: ?*anyopaque = null,
            onRegister: *const fn (ptr: *anyopaque) void = undefined,

            fn init(allocator: mem.Allocator) Self {
                return .{
                    .stream = store.Stream.init(
                        allocator,
                        .{
                            .initial_page_size = 64 * 1024,
                            .max_page_size = 1024 * 1024,
                            .ack_policy = .none,
                            .retention_policy = .all,
                        },
                        null,
                    ),
                };
            }

            fn deinit(self: *Self) void {
                self.stream.deinit();
            }

            pub fn topicCreated(self: *Self, name: []const u8) void {
                self.append("REGISTER {s}\n", .{name});
            }

            pub fn channelCreated(self: *Self, topic_name: []const u8, name: []const u8) void {
                self.append("REGISTER {s} {s}\n", .{ topic_name, name });
            }

            pub fn topicDeleted(self: *Self, name: []const u8) void {
                self.append("UNREGISTER {s}\n", .{name});
            }

            pub fn channelDeleted(self: *Self, topic_name: []const u8, name: []const u8) void {
                self.append("UNREGISTER {s} {s}\n", .{ topic_name, name });
            }

            pub fn ensureCapacity(self: *Self, topic_name: []const u8, channel_name: []const u8) !void {
                try self.stream.ensureUnusedCapacity(@intCast(10 + 1 + topic_name.len + 1 + channel_name.len + 1));
            }

            fn append(self: *Self, comptime fmt: []const u8, args: anytype) void {
                self.appendStream(fmt, args) catch |err| {
                    log.err("add registration failed {}", .{err});
                };
            }

            fn appendStream(self: *Self, comptime fmt: []const u8, args: anytype) !void {
                const bytes_count = std.fmt.count(fmt, args);
                const res = try self.stream.alloc(@intCast(bytes_count));
                _ = try std.fmt.bufPrint(res.data, fmt, args);
                if (self.ptr) |ptr|
                    self.onRegister(ptr);
            }
        };

        allocator: mem.Allocator,
        topics: std.StringHashMap(*Topic),
        registrations: Registrations,
        options: Options,

        started_at: u64,
        pub_err: ?anyerror = null,
        metric: store.Metric = .{},

        timer_queue: timer.Queue,

        // Init/deinit -----------------

        pub fn init(
            allocator: mem.Allocator,
            now: u64,
            options: Options,
        ) Broker {
            return .{
                .allocator = allocator,
                .topics = std.StringHashMap(*Topic).init(allocator),
                .started_at = now,
                .options = options,
                .metric = .{ .max_mem = options.max_mem },
                .timer_queue = timer.Queue.init(allocator),
                .registrations = Registrations.init(allocator),
            };
        }

        pub fn deinit(self: *Broker) void {
            var iter = self.topics.valueIterator();
            while (iter.next()) |e| e.*.deinit();
            self.topics.deinit();
            self.timer_queue.deinit();
            self.registrations.deinit();
        }

        pub fn setRegistrationsCallback(
            self: *Broker,
            ptr: *anyopaque,
            onRegister: *const fn (ptr: *anyopaque) void,
        ) void {
            self.registrations.ptr = ptr;
            self.registrations.onRegister = onRegister;
        }

        pub fn tick(self: *Broker, ts: u64) !u64 {
            time.now = ts;
            return try self.timer_queue.tick(ts);
        }

        // Publish/subscribe -----------------

        fn getOrCreateTopic(self: *Broker, name: []const u8) !*Topic {
            if (self.topics.get(name)) |t| return t;

            try self.registrations.ensureCapacity(name, "");
            try self.topics.ensureUnusedCapacity(1);

            const topic = try self.allocator.create(Topic);
            errdefer self.allocator.destroy(topic);
            const key = try self.allocator.dupe(u8, name);
            errdefer self.allocator.free(key);

            topic.* = Topic.init(self, key);
            topic.timer_op.init(&self.timer_queue, topic, Topic.onTimer);

            self.topics.putAssumeCapacityNoClobber(key, topic);
            self.registrations.topicCreated(key);
            log.debug("created topic {s}", .{name});
            return topic;
        }

        pub fn subscribe(self: *Broker, client: *Client, topic_name: []const u8, channel_name: []const u8) !void {
            const topic = try self.getOrCreateTopic(topic_name);
            try topic.subscribe(client, channel_name);
        }

        pub fn publish(self: *Broker, topic_name: []const u8, data: []const u8) !void {
            const topic = try self.getOrCreateTopic(topic_name);
            try self.setPubErr(topic.publish(data));
        }

        pub fn multiPublish(self: *Broker, topic_name: []const u8, msgs: u32, data: []const u8) !void {
            const topic = try self.getOrCreateTopic(topic_name);
            try self.setPubErr(topic.multiPublish(msgs, data));
        }

        pub fn deferredPublish(self: *Broker, topic_name: []const u8, data: []const u8, delay: u32) !void {
            const topic = try self.getOrCreateTopic(topic_name);
            try self.setPubErr(topic.deferredPublish(data, delay));
        }

        pub fn setPubErr(self: *Broker, error_union: anytype) !void {
            if (error_union) |_| {
                self.pub_err = null;
            } else |err| {
                self.pub_err = err;
                return err;
            }
        }

        // Http interface actions -----------------

        pub fn createTopic(self: *Broker, name: []const u8) !void {
            _ = try self.getOrCreateTopic(name);
        }

        pub fn deleteTopic(self: *Broker, name: []const u8) !void {
            try self.registrations.ensureCapacity(name, "");
            const kv = self.topics.fetchRemove(name) orelse return error.NotFound;
            errdefer self.topics.putAssumeCapacityNoClobber(kv.key, kv.value);
            const topic = kv.value;
            try topic.delete();
            self.registrations.topicDeleted(name);
            log.debug("deleted topic {s}", .{name});
        }

        pub fn createChannel(self: *Broker, topic_name: []const u8, name: []const u8) !void {
            const topic = try self.getOrCreateTopic(topic_name);
            _ = try topic.getOrCreateChannel(name);
        }

        pub fn deleteChannel(self: *Broker, topic_name: []const u8, name: []const u8) !void {
            const topic = self.topics.get(topic_name) orelse return error.NotFound;
            try self.registrations.ensureCapacity(topic_name, name);
            try topic.deleteChannel(name);
            self.registrations.channelDeleted(topic_name, name);
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

        pub fn timeNow(_: *Broker) u64 {
            return time.now;
        }

        pub fn writeMetrics(self: *Broker, writer: anytype) !void {
            var buf: [6 + protocol.max_name_len + 9 + protocol.max_name_len]u8 = undefined; // to fit: "topic.{s}.channel.{s}"
            var ti = self.topics.valueIterator();
            while (ti.next()) |topic_ptr| {
                const topic = topic_ptr.*;
                { // Topic metrics
                    var m = &topic.stream.metric;
                    const prefix = try std.fmt.bufPrint(&buf, "topic.{s}", .{topic.name});
                    // nsqd compatibility
                    const depth = if (topic.channels.count() == 0) m.msgs.value else 0;
                    try writer.gauge(prefix, "depth", depth);
                    try writer.add(prefix, "message_count", &m.total_msgs);
                    try writer.add(prefix, "message_bytes", &m.total_bytes);
                    // nsql specific
                    try writer.add(prefix, "msgs", m.msgs);
                    try writer.add(prefix, "bytes", m.bytes);
                    try writer.add(prefix, "capacity", m.capacity);
                }
                var ci = topic.channels.valueIterator();
                while (ci.next()) |channel_ptr| {
                    // Channel metrics
                    const channel = channel_ptr.*;
                    var m = &channel.metric;
                    const prefix = try std.fmt.bufPrint(&buf, "topic.{s}.channel.{s}", .{ topic.name, channel.name });
                    try writer.gauge(prefix, "clients", channel.consumers.count());
                    try writer.gauge(prefix, "deferred_count", channel.deferred.count());
                    try writer.gauge(prefix, "in_flight_count", channel.in_flight.count());
                    try writer.add(prefix, "depth", m.depth);
                    try writer.add(prefix, "message_count", &m.pull);
                    try writer.add(prefix, "finish_count", &m.finish);
                    try writer.add(prefix, "timeout_count", &m.timeout);
                    try writer.add(prefix, "requeue_count", &m.requeue);
                }
            }
            { // Broker metrics (sum of all topics)
                const prefix = "broker";
                var m = &self.metric;
                try writer.add(prefix, "msgs", m.msgs);
                try writer.add(prefix, "bytes", m.bytes);
                try writer.add(prefix, "capacity", m.capacity);
                try writer.add(prefix, "total_msgs", &m.total_msgs);
                try writer.add(prefix, "total_bytes", &m.total_bytes);
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
                    try meta.writeByte(@intFromEnum(topic.state));
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
                        try meta.writeByte(@intFromEnum(channel.state));
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
                topic.state = @enumFromInt(try meta.readByte());
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
                    channel.state = @enumFromInt(try meta.readByte());
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
                channels: std.StringHashMap(*Channel),
                stream: store.Stream,
                state: State = .active,

                deferred: std.PriorityQueue(DeferredPublish, void, DeferredPublish.less),
                timer_op: timer.Op = undefined,

                const State = enum {
                    active,
                    paused,
                    deleting,
                };

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
                    var iter = self.channels.valueIterator();
                    while (iter.next()) |e| e.*.deinit();
                    self.channels.deinit();
                    while (self.deferred.removeOrNull()) |dp| self.allocator.free(dp.data);
                    self.deferred.deinit();
                    self.stream.deinit();
                    self.timer_op.deinit();
                    self.allocator.free(self.name);
                    self.allocator.destroy(self);
                }

                fn empty(self: *Topic) void {
                    if (self.channels.count() == 0) {
                        self.stream.empty();
                    }
                }

                fn delete(self: *Topic) !void {
                    const allocator = self.allocator; // topic can be deinited, make copy
                    var channels = try allocator.alloc(*Channel, self.channels.count());
                    defer allocator.free(channels);

                    if (self.state != .deleting) {
                        self.state = .deleting;
                        if (self.channels.count() > 0) {
                            // map can be invalidated during iteration
                            // make channels list copy
                            var iter = self.channels.valueIterator();
                            var i: usize = 0;
                            while (iter.next()) |e| {
                                channels[i] = e.*;
                                i += 1;
                            }
                            // start deleting channels
                            for (channels) |channel| channel.delete();
                            return;
                        }
                    }
                    if (self.channels.count() == 0) {
                        //self.channels.clearAndFree();
                        self.deinit();
                    }
                }

                fn subscribe(self: *Topic, client: *Client, name: []const u8) !void {
                    const channel = try self.getOrCreateChannel(name);
                    try channel.subscribe(client);
                }

                fn getOrCreateChannel(self: *Topic, name: []const u8) !*Channel {
                    if (self.channels.get(name)) |channel| {
                        if (channel.state == .deleting) return error.Deleting;
                        return channel;
                    }

                    try self.broker.registrations.ensureCapacity(self.name, name);
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
                    channel.metric.depth.set(if (first_channel) self.stream.metric.msgs.value else 0);
                    self.broker.registrations.channelCreated(self.name, channel.name);
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
                    const res = self.stream.alloc(@intCast(protocol.header_len + data.len)) catch |err| brk: switch (err) {
                        error.StreamMemoryLimit => {
                            if (self.cleanupStream())
                                break :brk try self.stream.alloc(@intCast(protocol.header_len + data.len));
                            return err;
                        },
                        else => return err,
                    };
                    protocol.writeHeader(res.data, @intCast(data.len), time.now, res.sequence);
                    const body = res.data[protocol.header_len..];
                    @memcpy(body, data);
                }

                fn cleanupStream(self: *Topic) bool {
                    // Find channel which is holding oldest reference in store.
                    const last_channel = brk: {
                        var last_channel: ?*Channel = null;
                        var min_sequence: u64 = math.maxInt(u64);
                        var iter = self.channels.valueIterator();
                        while (iter.next()) |e| {
                            const channel = e.*;
                            if (channel.minSequence() < min_sequence) {
                                min_sequence = channel.minSequence();
                                last_channel = channel;
                            }
                        }
                        break :brk last_channel;
                    };
                    // If that channel is ephemeral, remove it.
                    if (last_channel) |lc| if (lc.ephemeral) {
                        _ = self.channels.fetchRemove(lc.name);
                        lc.delete();
                        self.stream.empty();
                        // There is probably some space cleared in store.
                        return true;
                    };
                    // No cleanup.
                    return false;
                }

                fn publish(self: *Topic, data: []const u8) !void {
                    try self.appendStream(data);
                    self.onPublish(1);
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
                    if (data.len > self.broker.options.max_body_size) {
                        log.err(
                            "{s} multi publish failed, messages length of {} bytes over limit of {} bytes ",
                            .{ self.name, data.len, self.broker.options.max_body_size },
                        );
                        return error.MessageSizeOverflow;
                    }
                    if (msgs == 0) return;
                    var pos: usize = 0;
                    for (0..msgs) |_| {
                        const len = mem.readInt(u32, data[pos..][0..4], .big);
                        pos += 4;
                        try self.appendStream(data[pos..][0..len]);
                        pos += len;
                    }
                    self.onPublish(msgs);
                }

                // Notify all channels that there are pending messages
                fn onPublish(self: *Topic, msgs: u32) void {
                    var iter = self.channels.valueIterator();
                    while (iter.next()) |e| {
                        const channel = e.*;
                        channel.onPublish(msgs);
                    }
                }

                // Called from channel when finished deleting channel
                fn channelDeleted(self: *Topic, channel: *Channel) void {
                    assert(self.channels.remove(channel.name));
                    self.stream.unsubscribe(channel.sequence);
                    if (self.state == .deleting and self.channels.count() == 0)
                        return self.deinit();
                    self.empty();
                }

                fn getChannel(self: *Topic, name: []const u8) !*Channel {
                    const channel = self.channels.get(name) orelse return error.NotFound;
                    if (channel.state == .deleting) return error.NotFound;
                    return channel;
                }

                // Http interface actions -----------------

                fn deleteChannel(self: *Topic, name: []const u8) !void {
                    const channel = try self.getChannel(name);
                    channel.delete();
                }

                fn pause(self: *Topic) void {
                    self.state = .paused;
                }

                fn unpause(self: *Topic) void {
                    if (self.state == .paused) {
                        self.state = .active;
                        // wake-up all channels
                        var iter = self.channels.valueIterator();
                        while (iter.next()) |ptr| ptr.*.wakeup();
                    }
                }

                fn pauseChannel(self: *Topic, name: []const u8) !void {
                    const channel = try self.getChannel(name);
                    channel.pause();
                }

                fn unpauseChannel(self: *Topic, name: []const u8) !void {
                    const channel = try self.getChannel(name);
                    channel.unpause();
                }

                fn emptyChannel(self: *Topic, name: []const u8) !void {
                    const channel = try self.getChannel(name);
                    channel.empty();
                }

                // -----------------
            };
        }

        pub fn ChannelType() type {
            return struct {
                const InFlightMsg = struct {
                    timeout_at: u64 = 0,
                    consumer: *Consumer = undefined,
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

                const State = enum {
                    active,
                    paused,
                    deleting,
                };

                allocator: mem.Allocator,
                name: []const u8,
                ephemeral: bool = false,
                topic: *Topic,
                consumers: Consumers,
                state: State = .active,

                // Sent but not jet acknowledged (fin) messages.
                in_flight: std.AutoHashMap(u64, InFlightMsg),
                // Re-queued by consumer, timed-out or defer published messages.
                deferred: std.PriorityQueue(DeferredMsg, void, DeferredMsg.less),
                timer_op: timer.Op = undefined,

                metric: Metric = .{},

                // Last stream sequence consumed by this channel
                sequence: u64 = 0,

                const Metric = struct {
                    // Total number of messages ...
                    // ... pulled from topic
                    pull: Counter = .{},
                    // ... delivered to the client(s)
                    finish: Counter = .{},
                    // ... time-outed while in flight.
                    timeout: Counter = .{},
                    // ... re-queued by the client.
                    requeue: Counter = .{},
                    // Current number of messages published to the topic but not processed
                    // by this channel.
                    depth: Gauge = .{},
                };

                const Consumers = struct {
                    list: std.ArrayList(*Consumer),
                    last_idx: usize = 0,
                    fn init(allocator: mem.Allocator) Consumers {
                        return .{ .list = std.ArrayList(*Consumer).init(allocator) };
                    }
                    fn deinit(self: *Consumers) void {
                        self.list.deinit();
                    }
                    fn append(self: *Consumers, consumer: *Consumer) !void {
                        try self.list.append(consumer);
                    }
                    fn close(self: *Consumers) void {
                        // reverse iteration allows remove to be called from consumer.close
                        var i = self.list.items.len;
                        while (i > 0) {
                            i -= 1;
                            const consumer = self.list.items[i];
                            consumer.close();
                        }
                    }
                    fn remove(self: *Consumers, consumer: *Consumer) void {
                        for (self.list.items, 0..) |ptr, i| {
                            if (ptr == consumer) {
                                _ = self.list.swapRemove(i);
                                return;
                            }
                        }
                        unreachable;
                    }
                    pub fn count(self: *Consumers) usize {
                        return self.list.items.len;
                    }
                    fn iter(self: *Consumers) Iterator {
                        return .{
                            .parent = self,
                            .idx = self.last_idx,
                            .len = self.list.items.len,
                        };
                    }

                    const Iterator = struct {
                        parent: *Consumers,
                        idx: usize,
                        len: usize,

                        fn next(self: *Iterator) ?*Consumer {
                            while (self.len > 0) {
                                self.len -= 1;
                                const items = self.parent.list.items;
                                self.idx = if (self.idx + 1 >= items.len) 0 else self.idx + 1;
                                const consumer = items[self.idx];
                                if (consumer.ready()) {
                                    self.parent.last_idx = self.idx;
                                    return consumer;
                                }
                            }
                            return null;
                        }
                    };
                };

                // Init/deinit -----------------

                fn init(self: *Channel, topic: *Topic, name: []const u8) !void {
                    const allocator = topic.allocator;
                    self.* = .{
                        .allocator = allocator,
                        .name = name,
                        .topic = topic,
                        .consumers = Consumers.init(allocator),
                        .in_flight = std.AutoHashMap(u64, InFlightMsg).init(allocator),
                        .deferred = std.PriorityQueue(DeferredMsg, void, DeferredMsg.less).init(allocator, {}),
                        .ephemeral = protocol.isEphemeral(name),
                    };
                }

                fn delete(self: *Channel) void {
                    if (self.state != .deleting) {
                        self.state = .deleting;
                        if (self.consumers.count() > 0) {
                            self.consumers.close();
                            return;
                        }
                    }
                    if (self.consumers.count() == 0) {
                        self.releasePending();
                        self.topic.channelDeleted(self);
                        self.deinit();
                    }
                }

                fn deinit(self: *Channel) void {
                    self.in_flight.deinit();
                    self.deferred.deinit();
                    self.consumers.deinit();
                    self.timer_op.deinit();
                    self.allocator.free(self.name);
                    self.allocator.destroy(self);
                }

                fn subscribe(self: *Channel, client: *Client) !void {
                    client.consumer = .{
                        .client = client,
                        .channel = self,
                        .metric = .{ .connected_at = time.now },
                    };
                    try self.consumers.append(&client.consumer.?);
                }

                // -----------------

                // Called from topic when message is published to the topic.
                fn onPublish(self: *Channel, msgs: u32) void {
                    self.metric.depth.inc(msgs);
                    if (!self.needWakeup()) return;
                    self.wakeup();
                }

                fn needWakeup(self: *Channel) bool {
                    return self.topic.state == .active and
                        self.state == .active and
                        self.topic.stream.hasMore(self.sequence);
                }

                // Notify consumers that there are messages to pull.
                fn wakeup(self: *Channel) void {
                    var iter = self.consumers.iter();
                    while (iter.next()) |consumer| {
                        consumer.pull() catch {};
                        if (!self.needWakeup()) break;
                    }
                }

                fn restore(self: *Channel, sequence: u64) !void {
                    if (sequence == self.sequence) return;
                    self.topic.stream.unsubscribe(self.sequence);
                    self.sequence = self.topic.stream.subscribe(.{ .from_sequence = sequence });
                    self.metric.depth.set(self.topic.stream.last_sequence - sequence);
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
                    self.metric.depth.inc(1);
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
                fn popDeferred(self: *Channel, consumer: *Consumer) !?[]const u8 {
                    if (self.deferred.peek()) |dm| if (dm.defer_until <= time.now) {
                        try self.in_flight.ensureUnusedCapacity(1);
                        const data = try self.allocator.dupe(u8, self.topic.stream.message(dm.sequence));
                        errdefer self.allocator.free(data);

                        const ifm = InFlightMsg{
                            .timeout_at = time.after(consumer.msg_timeout),
                            .consumer = consumer,
                            .attempts = dm.attempts,
                        };
                        ifm.setAttempts(data);
                        try self.updateTimer(ifm.timeout_at);
                        self.in_flight.putAssumeCapacityNoClobber(dm.sequence, ifm);
                        _ = self.deferred.remove();

                        return data;
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
                        self.metric.timeout.inc(1);
                        if (!builtin.is_test)
                            log.warn(
                                "{s}/{s} message timeout sequence: {}",
                                .{ self.topic.name, self.name, sequence },
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

                pub fn pull(self: *Channel, consumer: *Consumer) !?struct { u32, []const u8 } {
                    if (!consumer.ready()) return null;
                    if (self.state != .active) return null;

                    // if there is deferred message return one
                    if (try self.popDeferred(consumer)) |data| {
                        return .{ 1, data };
                    }

                    // else find next chunk in stream
                    if (self.topic.state != .active) return null;
                    if (self.topic.stream.pull(self.sequence, consumer.readyCount())) |res| {
                        errdefer res.revert(&self.topic.stream);

                        { // add all sequence to in_flight
                            try self.in_flight.ensureUnusedCapacity(res.count);
                            const timeout_at = time.after(consumer.msg_timeout);
                            try self.updateTimer(timeout_at);
                            for (res.sequence.from..res.sequence.to + 1) |sequence| {
                                const ifm = InFlightMsg{
                                    .timeout_at = timeout_at,
                                    .consumer = consumer,
                                    .attempts = 1,
                                };
                                self.in_flight.putAssumeCapacityNoClobber(sequence, ifm);
                            }
                        }

                        self.metric.pull.inc(res.count);
                        self.sequence = res.sequence.to;
                        return .{ res.count, res.data };
                    }
                    return null;
                }

                fn findInFlight(self: *Channel, consumer: *Consumer, msg_id: [16]u8) !struct { u64, InFlightMsg } {
                    const sequence = protocol.msg_id.decode(msg_id);
                    const ifm = self.in_flight.get(sequence) orelse return error.MessageNotInFlight;
                    if (ifm.consumer != consumer) return error.MessageNotInFlight;
                    return .{ sequence, ifm };
                }

                pub fn finish(self: *Channel, consumer: *Consumer, msg_id: [16]u8) !void {
                    const sequence, _ = try self.findInFlight(consumer, msg_id);
                    self.topic.stream.release(sequence);
                    assert(self.in_flight.remove(sequence));
                    self.metric.finish.inc(1);
                    self.metric.depth.dec(1);
                }

                /// Extend message timeout for interval (milliseconds).
                pub fn touch(self: *Channel, consumer: *Consumer, msg_id: [16]u8) !void {
                    const sequence, _ = try self.findInFlight(consumer, msg_id);
                    const ifm = self.in_flight.getPtr(sequence).?;
                    ifm.timeout_at += nsFromMs(consumer.msg_timeout);
                }

                pub fn requeue(self: *Channel, consumer: *Consumer, msg_id: [16]u8, delay: u32) !void {
                    const sequence, const ifm = try self.findInFlight(consumer, msg_id);
                    try self.deferInFlight(sequence, ifm, delay);
                    self.metric.requeue.inc(1);
                }

                pub fn unsubscribe(self: *Channel, consumer: *Consumer) void {
                    self.requeueAll(consumer) catch |err| {
                        log.warn("failed to remove in flight messages {}", .{err});
                    };
                    self.consumers.remove(consumer);
                    if ((self.consumers.count() == 0 and self.ephemeral) or
                        (self.state == .deleting))
                    {
                        self.delete();
                    }
                }

                // Re-queue all messages which are in-flight for some consumer.
                fn requeueAll(self: *Channel, consumer: *Consumer) !void {
                    var sequences = std.ArrayList(u64).init(self.allocator);
                    defer sequences.deinit();

                    var iter = self.in_flight.iterator();
                    while (iter.next()) |e| {
                        if (e.value_ptr.consumer == consumer)
                            try sequences.append(e.key_ptr.*);
                    }

                    for (sequences.items) |sequence| {
                        const msg = self.in_flight.get(sequence).?;
                        try self.deferInFlight(sequence, msg, 0);
                        self.metric.requeue.inc(1);
                    }
                    consumer.in_flight_count = 0;
                }

                // Http admin interface  -----------------

                fn pause(self: *Channel) void {
                    self.state = .paused;
                }

                fn unpause(self: *Channel) void {
                    if (self.state == .paused) {
                        self.state = .active;
                        self.wakeup();
                    }
                }

                fn empty(self: *Channel) void {
                    self.releasePending();
                    { // move stream pointer
                        const last = self.topic.stream.subscribe(.new);
                        self.topic.stream.unsubscribe(self.sequence);
                        self.sequence = last;
                        self.metric.depth.set(0);
                    }
                }

                fn releasePending(self: *Channel) void {
                    { // release in_flight messages
                        var iter = self.in_flight.keyIterator();
                        while (iter.next()) |e| {
                            self.topic.stream.release(e.*);
                        }
                        self.in_flight.clearAndFree();
                    }
                    { // release deferred messages
                        while (self.deferred.removeOrNull()) |dm|
                            self.topic.stream.release(dm.sequence);
                    }
                }

                fn minSequence(self: *Channel) u64 {
                    if (self.in_flight.count() == 0 and self.deferred.count() == 0)
                        return self.sequence;

                    var min_sequence: u64 = self.sequence;
                    {
                        var iter = self.in_flight.keyIterator();
                        while (iter.next()) |e| {
                            if (e.* < min_sequence) min_sequence = e.*;
                        }
                    }
                    {
                        var iter = self.deferred.iterator();
                        while (iter.next()) |dm| {
                            if (dm.sequence < min_sequence) min_sequence = dm.sequence;
                        }
                    }
                    return min_sequence;
                }
            };
        }
    };
}

const TestBroker = BrokerType(TestClient);

const TestClient = struct {
    const Self = @This();

    consumer: ?TestBroker.Consumer = .{},
    sequences: std.ArrayList(u64) = undefined,
    in_kernel_data: std.ArrayList([]const u8) = undefined,

    fn init(allocator: mem.Allocator) Self {
        return .{
            .sequences = std.ArrayList(u64).init(allocator),
            .in_kernel_data = std.ArrayList([]const u8).init(allocator),
        };
    }

    fn deinit(self: *Self) void {
        self.sequences.deinit();
        self.in_kernel_data.deinit();
    }

    fn ready(_: Self) bool {
        return true;
    }

    fn send(self: *Self, data: []const u8) !void {
        const consumer = &self.consumer.?;
        errdefer consumer.onSend(data);

        try self.in_kernel_data.append(data);
        var pos: usize = 0;
        while (pos < data.len) {
            const m = protocol.parseMessageFrame(data[pos..]) catch unreachable;
            pos += m.size;
            try self.sequences.append(m.sequence);
        }
    }

    fn close(self: *Self) void {
        const consumer = &self.consumer.?;
        if (consumer.in_kernel_buffers == 0)
            consumer.unsubscribe();
    }

    fn pull(self: *Self) !void {
        const consumer = &self.consumer.?;
        consumer.ready_count = consumer.in_flight_count + 1;
        defer consumer.ready_count -= 1;
        try consumer.pull();
        self.sendDone();
    }

    fn pullHoldInKernel(self: *Self) !void {
        const consumer = &self.consumer.?;
        consumer.ready_count = consumer.in_flight_count + 1;
        defer consumer.ready_count -= 1;
        try consumer.pull();
    }

    fn sendDone(self: *Self) void {
        const consumer = &self.consumer.?;
        for (self.in_kernel_data.items) |buf|
            consumer.onSend(buf);
        self.in_kernel_data.clearAndFree();
    }

    fn finish(self: *Self, sequence: u64) !void {
        const consumer = &self.consumer.?;
        try consumer.finish(protocol.msg_id.encode(sequence));
    }

    fn unsubscribe(self: *Self) void {
        const consumer = &self.consumer.?;
        consumer.unsubscribe();
    }

    fn pullFinish(self: *Self) !void {
        try self.pull();
        try self.finish(self.lastSequence());
    }

    fn requeue(self: *Self, sequence: u64, delay: u32) !void {
        const consumer = &self.consumer.?;
        try consumer.requeue(protocol.msg_id.encode(sequence), delay);
    }

    fn lastSequence(self: *Self) u64 {
        return self.sequences.items[self.sequences.items.len - 1];
    }
};

test "consumers" {
    const allocator = testing.allocator;
    const Consumers = TestBroker.Channel.Consumers;
    var cs = Consumers.init(allocator);
    defer cs.deinit();

    var c1: TestBroker.Consumer = .{};
    try cs.append(&c1);

    // c1 is not ready
    var iter = cs.iter();
    try testing.expect(iter.next() == null);

    // iterator on single consumer
    c1.ready_count = 1;
    iter = cs.iter();
    try testing.expectEqual(&c1, iter.next().?);
    try testing.expect(iter.next() == null);

    // append another
    var c2: TestBroker.Consumer = .{};
    try cs.append(&c2);
    c2.ready_count = 1;
    // iterator on 2 consumers
    iter = cs.iter();
    try testing.expectEqual(&c2, iter.next().?);
    try testing.expectEqual(&c1, iter.next().?);
    try testing.expect(iter.next() == null);
    // starts from next
    iter = cs.iter();
    try testing.expectEqual(&c2, iter.next().?);
    try testing.expectEqual(&c1, iter.next().?);
    try testing.expect(iter.next() == null);

    // append another
    var c3: TestBroker.Consumer = .{};
    try cs.append(&c3);
    c3.ready_count = 1;
    // iterator on 3 consumers
    iter = cs.iter();
    try testing.expectEqual(&c2, iter.next().?);
    try testing.expectEqual(&c3, iter.next().?);
    try testing.expectEqual(&c1, iter.next().?);
    try testing.expect(iter.next() == null);
    // partial iteration
    iter = cs.iter();
    try testing.expectEqual(&c2, iter.next().?);
    try testing.expectEqual(&c3, iter.next().?);
    // continues on next consumer
    iter = cs.iter();
    try testing.expectEqual(&c1, iter.next().?);
    try testing.expectEqual(&c2, iter.next().?);
    try testing.expectEqual(&c3, iter.next().?);
    try testing.expect(iter.next() == null);
    // skips not ready consumers
    c2.ready_count = 0;
    iter = cs.iter();
    try testing.expectEqual(&c1, iter.next().?);
    try testing.expectEqual(&c3, iter.next().?);
    try testing.expect(iter.next() == null);
    // remove
    cs.remove(&c3);
    iter = cs.iter();
    try testing.expectEqual(&c1, iter.next().?);
    try testing.expect(iter.next() == null);
}

test "channel fin req" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name = "channel";

    var broker = TestBroker.init(allocator, 0, .{});
    defer broker.deinit();

    var client = TestClient.init(allocator);
    defer client.deinit();

    try broker.subscribe(&client, topic_name, channel_name);
    const channel = client.consumer.?.channel;

    try broker.publish(topic_name, "1");
    try broker.publish(topic_name, "2");
    try broker.publish(topic_name, "3");

    { // 3 messages in topic, 0 taken by channel
        try testing.expectEqual(3, channel.metric.depth.value);
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
    }
    { // wakeup without ready consumers
        channel.wakeup();
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
    }
    { // consumer is ready
        try client.pull();
        try testing.expectEqual(1, client.lastSequence());
        // 1 is in flight
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(1, channel.sequence);
    }
    { // consumer sends fin, 0 in flight after that
        try client.finish(1);
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(2, channel.metric.depth.value);
        try testing.expectEqual(1, channel.sequence);
    }
    { // send seq 2, 1 msg in flight
        try client.pull();
        try testing.expectEqual(2, client.lastSequence());
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(2, channel.sequence);
    }
    { // send seq 3, 2 msgs in flight
        try client.pull();
        try testing.expectEqual(3, client.lastSequence());
        try testing.expectEqual(2, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(3, channel.sequence);
    }
    { // 2 is re-queued
        try client.requeue(2, 0);
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(1, channel.deferred.count());
    }
    { // out of order fin, fin seq 3 while 2 is still in flight
        try client.finish(3);
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(1, channel.deferred.count());
    }
    { // re send 2
        try testing.expectEqual(3, client.sequences.items.len);
        try testing.expectEqual(1, channel.deferred.count());
        try client.pull();
        try testing.expectEqual(4, client.sequences.items.len);
        try testing.expectEqual(2, client.lastSequence());
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
    }
    { // fin seq 2
        try testing.expectEqual(1, channel.metric.depth.value);
        try client.finish(2);
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(0, channel.deferred.count());
        try testing.expectEqual(0, channel.metric.depth.value);
    }
    { // consumer unsubscribe re-queues in-flight messages
        try broker.publish(topic_name, "4");
        try client.pull();
        try testing.expectEqual(1, channel.in_flight.count());
        client.unsubscribe();
        try testing.expectEqual(0, channel.in_flight.count());
        try testing.expectEqual(1, channel.deferred.count());
        try testing.expectEqual(0, channel.consumers.count());
    }
}

test "multiple channels" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name1 = "channel1";
    const channel_name2 = "channel2";
    const no = 1024;

    var broker = TestBroker.init(allocator, 0, .{});
    defer broker.deinit();

    var c1 = TestClient.init(allocator);
    defer c1.deinit();
    try broker.subscribe(&c1, topic_name, channel_name1);
    c1.consumer.?.ready_count = no * 3;

    { // single channel, single consumer
        for (0..no) |_|
            try broker.publish(topic_name, "message body");

        try testing.expectEqual(no, c1.sequences.items.len);
        try testing.expectEqual(no, c1.consumer.?.in_flight_count);
        try testing.expectEqual(no * 2, c1.consumer.?.readyCount());
        var expected: u64 = 1;
        for (c1.sequences.items) |seq| {
            try testing.expectEqual(expected, seq);
            expected += 1;
        }
    }

    var c2 = TestClient.init(allocator);
    defer c2.deinit();
    try broker.subscribe(&c2, topic_name, channel_name2);
    c2.consumer.?.ready_count = no * 2;

    { // two channels on the same topic
        for (0..no) |_|
            try broker.publish(topic_name, "another message body");

        try testing.expectEqual(no * 2, c1.sequences.items.len);
        try testing.expectEqual(no, c2.sequences.items.len);
    }

    var c3 = TestClient.init(allocator);
    defer c3.deinit();
    try broker.subscribe(&c3, topic_name, channel_name2);
    c3.consumer.?.ready_count = no;

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

    var broker = TestBroker.init(allocator, 0, .{});
    defer broker.deinit();
    // publish messages to the topic which don't have channels created
    for (0..no) |_|
        try broker.publish(topic_name, "message body"); // 1-16

    const topic = try broker.getOrCreateTopic(topic_name);
    try testing.expectEqual(no, topic.stream.metric.msgs.value);

    // subscribe creates channel
    // channel gets all messages
    var client = TestClient.init(allocator);
    defer client.deinit();
    try broker.subscribe(&client, topic_name, channel_name);
    var channel = client.consumer.?.channel;
    try testing.expectEqual(0, channel.sequence);
    try testing.expectEqual(no, channel.metric.depth.value);

    for (0..no) |i| {
        try testing.expectEqual(no - i, channel.metric.depth.value);
        try client.pullFinish();
    }
    try testing.expectEqual(0, channel.metric.depth.value);
    try testing.expectEqual(no, topic.stream.metric.msgs.value);

    try broker.publish(topic_name, "message body"); // 17
    try testing.expectEqual(1, channel.metric.depth.value);
    try testing.expectEqual(17, topic.stream.metric.msgs.value);
    try testing.expectEqual(17, topic.stream.last_sequence);

    try broker.deleteChannel(topic_name, channel_name);
    try testing.expectEqual(0, topic.stream.metric.msgs.value);
    try broker.publish(topic_name, "message body"); // 18
    try testing.expectEqual(1, topic.stream.metric.msgs.value);

    var clinet2 = TestClient.init(allocator);
    defer clinet2.deinit();
    try broker.subscribe(&clinet2, topic_name, channel_name);
    channel = clinet2.consumer.?.channel;
    try testing.expectEqual(17, channel.sequence);
    try testing.expectEqual(1, topic.stream.metric.msgs.value);
    try testing.expectEqual(1, channel.metric.depth.value);
}

test "timeout messages" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name = "channel";

    var broker = TestBroker.init(allocator, 0, .{});
    defer broker.deinit();

    var client = TestClient.init(allocator);
    defer client.deinit();
    try broker.subscribe(&client, topic_name, channel_name);
    const channel = client.consumer.?.channel;
    client.consumer.?.msg_timeout = 60000;

    for (0..4) |i| {
        time.now = i + 1;
        try broker.publish(topic_name, "message body");
        try client.pull();
    }
    try testing.expectEqual(4, channel.in_flight.count());

    { // check expire_at for in flight messages
        var iter = channel.in_flight.valueIterator();
        while (iter.next()) |msg| {
            try testing.expectEqual(1, msg.attempts);
            try testing.expect(msg.timeout_at > nsFromMs(client.consumer.?.msg_timeout) and
                msg.timeout_at <= nsFromMs(client.consumer.?.msg_timeout) + 4);
        }
    }
    const ns_per_s = std.time.ns_per_s;
    const msg_timeout: u64 = 60 * ns_per_s;
    { // expire one message
        const expire_at = try channel.inFlightTimeout(msg_timeout + 1);
        try testing.expectEqual(msg_timeout + 2, expire_at);
        try testing.expectEqual(0, channel.metric.requeue.value);
        try testing.expectEqual(1, channel.metric.timeout.value);
        try testing.expectEqual(3, channel.in_flight.count());
        try testing.expectEqual(1, channel.deferred.count());
    }
    { // expire two more
        const expire_at = try channel.inFlightTimeout(msg_timeout + 3);
        try testing.expectEqual(msg_timeout + 4, expire_at);
        try testing.expectEqual(0, channel.metric.requeue.value);
        try testing.expectEqual(3, channel.metric.timeout.value);
        try testing.expectEqual(1, channel.in_flight.count());
        try testing.expectEqual(3, channel.deferred.count());
        try testing.expectEqual(0, channel.metric.finish.value);
    }
    { // fin last one
        try client.finish(client.lastSequence());
        try testing.expectEqual(1, channel.metric.finish.value);
        try testing.expectEqual(0, channel.in_flight.count());
    }
    { // resend two
        try client.pull();
        try client.pull();
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

    var broker = TestBroker.init(allocator, 0, .{});
    defer broker.deinit();

    var client = TestClient.init(allocator);
    defer client.deinit();
    try broker.subscribe(&client, topic_name, channel_name);
    const channel = client.consumer.?.channel;

    const topic = channel.topic;
    client.consumer.?.ready_count = 3;
    client.consumer.?.msg_timeout = 100;

    { // publish two deferred messages, topic puts them into deferred queue
        try broker.deferredPublish(topic_name, "message body 2", 2);
        try broker.deferredPublish(topic_name, "message body 1", 1);
        try testing.expectEqual(0, channel.metric.depth.value);
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
        try client.requeue(1, 2);
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
        client.sendDone();
    }
}

test "topic pause" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name = "channel";

    var broker = TestBroker.init(allocator, 0, .{});
    defer broker.deinit();
    const topic = try broker.getOrCreateTopic(topic_name);

    {
        try broker.publish(topic_name, "message 1");
        try broker.publish(topic_name, "message 2");
        try testing.expectEqual(2, topic.stream.metric.msgs.value);
    }

    var client = TestClient.init(allocator);
    defer client.deinit();
    try broker.subscribe(&client, topic_name, channel_name);
    const channel = client.consumer.?.channel;

    { // while channel is paused topic messages are not delivered to the channel
        try testing.expect(channel.state == .active);
        try broker.pauseChannel(topic_name, channel_name);
        try testing.expect(channel.state == .paused);

        try client.pull();
        try testing.expectEqual(0, channel.in_flight.count());

        // unpause will pull message
        client.consumer.?.ready_count += 1;
        try broker.unpauseChannel(topic_name, channel_name);
        try testing.expectEqual(1, channel.in_flight.count());
    }

    { // same while topic is paused
        try testing.expect(topic.state == .active);
        try broker.pauseTopic(topic_name);
        try testing.expect(topic.state == .paused);

        try client.pull();
        try testing.expectEqual(1, channel.in_flight.count());

        // unpause
        client.consumer.?.ready_count += 1;
        try broker.unpauseTopic(topic_name);
        try testing.expectEqual(2, channel.in_flight.count());
    }
    client.sendDone();
    try broker.deleteTopic(topic_name);
}

test "channel empty" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name = "channel";

    var broker = TestBroker.init(allocator, 0, .{});
    defer broker.deinit();

    var client = TestClient.init(allocator);
    defer client.deinit();

    try broker.subscribe(&client, topic_name, channel_name);
    const channel = client.consumer.?.channel;

    time.now = 1;
    try broker.publish(topic_name, "message 1");
    try broker.publish(topic_name, "message 2");
    try broker.publish(topic_name, "message 3");
    try broker.publish(topic_name, "message 4");

    time.now = 2;
    try client.pull();
    try client.pull();
    try client.pull();
    try client.requeue(1, 0);
    try testing.expectEqual(2, channel.in_flight.count());
    try testing.expectEqual(1, channel.deferred.count());
    try testing.expectEqual(3, channel.sequence);

    channel.empty();
    try testing.expectEqual(0, channel.in_flight.count());
    try testing.expectEqual(0, channel.deferred.count());
    try testing.expectEqual(4, channel.sequence);

    time.now = 0;
}

test "ephemeral channel" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name = "channel#ephemeral";

    var broker = TestBroker.init(allocator, 0, .{});
    defer broker.deinit();

    var client = TestClient.init(allocator);
    defer client.deinit();

    try broker.subscribe(&client, topic_name, channel_name);
    const channel = client.consumer.?.channel;

    const topic = channel.topic;
    try testing.expect(channel.ephemeral);

    try testing.expectEqual(1, topic.channels.count());
    client.unsubscribe();
    try testing.expectEqual(0, topic.channels.count());
}

test "channel delete page references" {
    const allocator = testing.allocator;
    const topic_name = "topic";
    const channel_name = "channel1";

    var broker = TestBroker.init(allocator, 0, .{});
    defer broker.deinit();

    var client1 = TestClient.init(allocator);
    defer client1.deinit();
    try broker.subscribe(&client1, topic_name, channel_name);

    try broker.publish(topic_name, "message 1");
    try broker.publish(topic_name, "message 2");

    const channel = client1.consumer.?.channel;
    const topic = channel.topic;
    const page = &topic.stream.pages.items[0];
    try testing.expectEqual(1, page.ref_count);

    try testing.expectEqual(0, channel.sequence);
    try client1.pullHoldInKernel();
    try testing.expectEqual(1, channel.sequence);
    try testing.expectEqual(1, channel.in_flight.count());
    try testing.expectEqual(1, client1.consumer.?.in_kernel_buffers);
    // first reference is channel, second in flight message
    try testing.expectEqual(2, page.ref_count);

    try broker.deleteChannel(topic_name, channel_name);
    try testing.expectEqual(2, page.ref_count);
    try testing.expectEqual(1, client1.consumer.?.in_kernel_buffers);
    try testing.expectEqual(1, topic.channels.count());
    try testing.expect(channel.state == .deleting);
    client1.sendDone();
    client1.close();
    try testing.expectEqual(0, client1.consumer.?.in_kernel_buffers);
    try testing.expectEqual(0, topic.stream.pages.items.len);
    try testing.expectEqual(0, topic.channels.count());
}

test "depth" {
    const allocator = testing.allocator;
    const topic_name = "topic";

    var broker = TestBroker.init(allocator, 0, .{});
    defer broker.deinit();

    var client1 = TestClient.init(allocator);
    defer client1.deinit();
    try broker.subscribe(&client1, topic_name, "channel1");

    var client2 = TestClient.init(allocator);
    defer client2.deinit();
    try broker.subscribe(&client2, topic_name, "channel2");

    const channel1 = client1.consumer.?.channel;
    const channel2 = client2.consumer.?.channel;
    const topic = channel1.topic;

    try broker.publish(topic_name, "message 1");
    try broker.publish(topic_name, "message 2");

    try testing.expectEqual(2, topic.stream.metric.msgs.value);
    try testing.expectEqual(2, channel1.metric.depth.value);
    try testing.expectEqual(2, channel2.metric.depth.value);

    try testing.expectEqual(0, channel1.sequence);
    try testing.expectEqual(0, channel2.sequence);
    try client1.pullFinish();
    try testing.expectEqual(1, channel1.sequence);

    try testing.expectEqual(1, channel1.metric.depth.value);
    try testing.expectEqual(2, channel2.metric.depth.value);

    try client2.pullFinish();
    try testing.expectEqual(1, channel1.metric.depth.value);
    try testing.expectEqual(1, channel2.metric.depth.value);

    try client2.pullFinish();
    try testing.expectEqual(1, channel1.metric.depth.value);
    try testing.expectEqual(0, channel2.metric.depth.value);

    try broker.publish(topic_name, "message 3");

    try testing.expectEqual(2, channel1.metric.depth.value);
    try testing.expectEqual(1, channel2.metric.depth.value);
}

test "check allocations" {
    const allocator = testing.allocator;
    try publishFinish(allocator);

    try testing.checkAllAllocationFailures(allocator, publishFinish, .{});
}

fn publishFinish(allocator: mem.Allocator) !void {
    const topic_name = "topic";

    var broker = TestBroker.init(allocator, 0, .{});
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
    try testing.expectEqual(2, topic.stream.metric.msgs.value);
    try testing.expectEqual(2, topic.stream.last_sequence);

    // 1 consumer for channel 1 and 2 consumers for channel 2
    var channel_client = TestClient.init(allocator);
    defer channel_client.deinit();
    try broker.subscribe(&channel_client, topic_name, channel1_name);

    var channel2_client1 = TestClient.init(allocator);
    defer channel2_client1.deinit();
    try broker.subscribe(&channel2_client1, topic_name, channel2_name);
    var channel2_client2 = TestClient.init(allocator);
    defer channel2_client2.deinit();
    //channel2_client2._id = 2;
    try broker.subscribe(&channel2_client2, topic_name, channel2_name);

    try testing.expectEqual(0, channel_client.consumer.?.channel.sequence);
    try testing.expectEqual(0, channel2_client1.consumer.?.channel.sequence);

    // Consume messages
    try channel_client.pullFinish();
    try channel_client.pullFinish();
    try channel2_client1.pullFinish();
    try channel2_client2.pullFinish();
    try testing.expectEqual(2, topic.stream.metric.msgs.value);
    try testing.expectEqual(2, channel_client.sequences.items.len);
    try testing.expectEqual(1, channel2_client1.sequences.items.len);
    try testing.expectEqual(1, channel2_client2.sequences.items.len);

    // Publish some more
    try broker.publish(topic_name, "message 3");
    try broker.publish(topic_name, "message 4");
    try broker.deleteChannel(topic_name, channel1_name);

    // Re-queue from client 1 to client 2
    const channel2 = channel2_client2.consumer.?.channel;
    try channel2_client1.pull();
    try testing.expectEqual(3, channel2_client1.lastSequence());
    try channel2_client1.requeue(3, 0);
    try testing.expectEqual(1, channel2.deferred.count());
    try channel2_client2.pull();
    try testing.expectEqual(3, channel2_client2.lastSequence());
    // Unsubscribe consumer2
    try testing.expectEqual(1, channel2.in_flight.count());
    try channel2.requeueAll(&(channel2_client2.consumer.?));
    channel2_client2.unsubscribe();
    try testing.expectEqual(0, channel2.in_flight.count());
    try testing.expectEqual(1, channel2.deferred.count());
    try channel2_client1.pull();
    try testing.expectEqual(3, channel2_client1.lastSequence());

    channel2.empty();
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
        var broker = TestBroker.init(allocator, 0, .{});
        defer broker.deinit();
        const topic = try broker.getOrCreateTopic(topic_name);

        var consumer1 = TestClient.init(allocator);
        defer consumer1.deinit();
        var consumer2 = TestClient.init(allocator);
        defer consumer2.deinit();
        try broker.subscribe(&consumer1, topic_name, &channel1_name);
        var channel1 = consumer1.consumer.?.channel;
        try broker.subscribe(&consumer2, topic_name, &channel2_name);
        var channel2 = consumer2.consumer.?.channel;

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

        try testing.expectEqual(2, channel1.metric.depth.value);
        try testing.expectEqual(2, channel2.metric.depth.value);

        try testing.expectEqual(5, topic.stream.pages.items[0].ref_count);
        try broker.dump(dir);
    }

    { // restore in another instance
        var broker = TestBroker.init(allocator, 0, .{});
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

        try testing.expect(channel1.state == .paused);
        try testing.expect(channel2.state == .active);

        try testing.expectEqual(2, channel1.metric.depth.value);
        try testing.expectEqual(2, channel2.metric.depth.value);
    }

    try std.fs.cwd().deleteTree(dir_name);
    time.now = 0;
}

test "registrations" {
    const allocator = testing.allocator;
    var broker = TestBroker.init(allocator, 0, .{});
    defer broker.deinit();

    const T = struct {
        call_count: usize = 0,
        fn onRegister(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
        }
    };
    var t: T = .{};
    broker.setRegistrationsCallback(&t, T.onRegister);

    try broker.createChannel("foo", "bar");
    try testing.expectEqual(2, t.call_count);
    try broker.createChannel("jozo", "bozo");
    try broker.deleteChannel("jozo", "bozo");
    try broker.deleteTopic("jozo");
    try testing.expectEqual(6, t.call_count);

    const expected =
        \\REGISTER foo
        \\REGISTER foo bar
        \\REGISTER jozo
        \\REGISTER jozo bozo
        \\UNREGISTER jozo bozo
        \\UNREGISTER jozo
        \\
    ;

    const res = broker.registrations.stream.pull(0, 100).?;
    try testing.expectEqualStrings(expected, res.data);
}
