const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const posix = std.posix;
const socket_t = std.posix.socket_t;
const fd_t = std.posix.fd_t;

const validateName = @import("protocol.zig").validateName;
const Options = @import("Options.zig");
const Io = @import("io.zig").Io;
const Op = @import("io.zig").Op;
const SendOp = @import("io.zig").SendOp;
const Error = @import("io.zig").Error;
const Broker = @import("main.zig").Broker;
const store = @import("store.zig");
pub const Listener = @import("tcp.zig").ListenerType(Conn);

const log = std.log.scoped(.http);

pub const Conn = struct {
    gpa: mem.Allocator,
    listener: *Listener,
    io: *Io,
    socket: socket_t = 0,
    addr: std.net.Address,

    recv_op: Op = .{},
    send_op: SendOp = .{},
    close_op: Op = .{},

    rsp_arena: ?std.heap.ArenaAllocator = null, // arena allocator for response
    timer_ts: u64 = 0, // not used here but required by listener

    state: State = .connected,
    const State = enum {
        connected,
        closing,
    };

    pub fn init(self: *Conn, listener: *Listener, socket: socket_t, addr: std.net.Address) !void {
        const allocator = listener.allocator;
        self.* = .{
            .gpa = allocator,
            .listener = listener,
            .io = listener.io,
            .socket = socket,
            .addr = addr,
        };
        errdefer self.deinit();
        try self.send_op.init(allocator, socket, self, onSend, onSendFail);

        self.recv_op = Op.recv(self.socket, self, onRecv, onRecvFail);
        self.io.submit(&self.recv_op);
    }

    pub fn deinit(self: *Conn) void {
        self.deinitResponse();
        self.send_op.deinit(self.gpa);
    }

    fn onRecv(self: *Conn, bytes: []const u8) Error!void {
        if (self.send_op.active()) return self.shutdown();
        assert(self.rsp_arena == null);

        self.rsp_arena = std.heap.ArenaAllocator.init(self.gpa);
        errdefer self.deinitResponse();
        const allocator = self.rsp_arena.?.allocator();

        var body_bytes = std.ArrayList(u8).init(allocator);
        defer body_bytes.deinit();
        const writer = body_bytes.writer().any();

        if (self.handle(writer, bytes)) {
            const body = try body_bytes.toOwnedSlice();
            const header = try std.fmt.allocPrint(allocator, header_template, .{ body.len, "application/json" });
            self.send_op.prep(header);
            self.send_op.prep(body);
        } else |err| {
            log.warn("request failed {}", .{err});
            const header = switch (err) {
                error.NotFound => not_found,
                else => bad_request,
            };
            self.send_op.prep(header);
        }
        self.send_op.send(self.io);
        if (self.recv_op.active()) self.shutdown();
    }

    fn handle(self: *Conn, writer: std.io.AnyWriter, bytes: []const u8) !void {
        var hp: std.http.HeadParser = .{};
        const head_end = hp.feed(bytes);
        if (hp.state != .finished) return error.BadRequest;
        const head = try std.http.Server.Request.Head.parse(bytes[0..head_end]);
        const content_length = if (head.content_length) |l| l else 0;
        if (content_length != bytes[head_end..].len) {
            log.err(
                "{} receive buffer overflow method: {}, target: {s}, content length: {}",
                .{ self.socket, head.method, head.target, content_length },
            );
            return error.BadRequest;
        }

        log.debug(
            "{} method: {}, target: {s}, content length: {}",
            .{ self.socket, head.method, head.target, content_length },
        );

        const broker = self.listener.broker;
        const cmd = parse(head.target) catch return error.NotFound;
        switch (cmd) {
            .stats => |args| try jsonStat(self.gpa, args, writer, broker),
            .info => try jsonInfo(writer, broker, self.listener.options),

            .topic_create => |name| try broker.createTopic(name),
            .topic_delete => |name| try broker.deleteTopic(name),
            .topic_empty => |name| try broker.emptyTopic(name),
            .topic_pause => |name| try broker.pauseTopic(name),
            .topic_unpause => |name| try broker.unpauseTopic(name),

            .channel_create => |arg| try broker.createChannel(arg.topic_name, arg.name),
            .channel_delete => |arg| try broker.deleteChannel(arg.topic_name, arg.name),
            .channel_empty => |arg| try broker.emptyChannel(arg.topic_name, arg.name),
            .channel_pause => |arg| try broker.pauseChannel(arg.topic_name, arg.name),
            .channel_unpause => |arg| try broker.unpauseChannel(arg.topic_name, arg.name),
        }
    }

    fn deinitResponse(self: *Conn) void {
        if (self.rsp_arena) |arena| {
            arena.deinit();
            self.rsp_arena = null;
        }
    }
    fn onSend(self: *Conn) Error!void {
        self.deinitResponse();
    }

    fn onSendFail(self: *Conn, err: anyerror) Error!void {
        self.deinitResponse();
        switch (err) {
            error.BrokenPipe, error.ConnectionResetByPeer => {},
            else => log.err("{} send failed {}", .{ self.socket, err }),
        }
        self.shutdown();
    }

    fn onRecvFail(self: *Conn, err: anyerror) Error!void {
        switch (err) {
            error.EndOfFile, error.OperationCanceled, error.ConnectionResetByPeer => {},
            else => log.err("{} recv failed {}", .{ self.socket, err }),
        }
        self.shutdown();
    }

    fn onClose(self: *Conn, _: ?anyerror) void {
        self.shutdown();
    }

    pub fn shutdown(self: *Conn) void {
        // log.debug("{} shutdown state: {s}", .{ self.socket, @tagName(self.state) });
        switch (self.state) {
            .connected => {
                self.close_op = Op.shutdown(self.socket, self, onClose);
                self.io.submit(&self.close_op);
                self.state = .closing;
            },
            .closing => {
                if (self.recv_op.active() or
                    self.send_op.active() or
                    self.close_op.active())
                    return;

                log.debug("{} closed", .{self.socket});
                self.deinit();
                self.listener.remove(self);
            },
        }
    }
};

const header_template =
    "HTTP/1.1 200 OK\r\n" ++
    "Content-Length: {d}\r\n" ++
    "Content-Type: {s}\r\n\r\n";

const not_found =
    "HTTP/1.1 404 Not Found\r\n" ++
    "content-length: 9\r\n" ++
    "content-type: text/plain\r\n\r\n" ++
    "Not Found";

const bad_request =
    "HTTP/1.1 404 Bad Request\r\n" ++
    "content-length: 11\r\n" ++
    "content-type: text/plain\r\n\r\n" ++
    "Bad Request";

const Info = struct {
    version: []const u8 = "V2",
    broadcast_address: []const u8,
    hostname: []const u8,
    http_port: u16,
    tcp_port: u16,
    start_time: u64,
    max_heartbeat_interval: u32,
};

fn jsonInfo(writer: anytype, broker: *Broker, options: Options) !void {
    var hostname_buf: [std.posix.HOST_NAME_MAX]u8 = undefined;
    const hostname = try std.posix.gethostname(&hostname_buf);
    const start_time = broker.started_at / std.time.ns_per_s;

    const info = Info{
        .broadcast_address = "localhost",
        .hostname = hostname,
        .http_port = options.http_address.getPort(),
        .tcp_port = options.tcp_address.getPort(),
        .start_time = start_time,
        .max_heartbeat_interval = options.max_heartbeat_interval,
    };

    try std.json.stringify(info, .{}, writer);
}

const Stat = struct {
    const Topic = struct {
        topic_name: []const u8,
        depth: usize = 0,
        message_count: usize,
        message_bytes: usize,
        paused: bool,
        channels: []Channel,
        e2e_processing_latency: struct { count: usize = 0 } = .{},
        stream: Stream,
    };
    const Channel = struct {
        channel_name: []const u8,
        depth: usize,
        in_flight_count: usize,
        deferred_count: usize,
        message_count: usize,
        requeue_count: usize,
        timeout_count: usize,
        client_count: usize,
        clients: []Client,
        paused: bool,
        sequence: u64,
        e2e_processing_latency: struct { count: usize = 0 } = .{},
    };
    const Client = struct {
        client_id: []const u8,
        hostname: []const u8,
        socket: u32,
        user_agent: []const u8,
        version: []const u8 = "V2",
        remote_address: []const u8,
        state: u8 = 3,
        ready_count: usize,
        in_flight_count: usize,
        message_count: usize,
        finish_count: usize,
        requeue_count: usize,
        connect_ts: usize,
        msg_timeout: usize,
        pub_counts: []PubCount = &.{},
    };
    const PubCount = struct {
        topic: []const u8,
        count: usize,
    };
    const Stream = struct {
        last_sequence: u64,
        page_size: usize,
        pages_count: usize,
        metric: StreamMetric,
        pages: []Page,
    };
    const StreamMetric = struct {
        msgs: usize,
        bytes: usize,
        capacity: usize,
        total_msgs: usize,
        total_bytes: usize,
    };
    const Page = struct {
        first_sequence: u64,
        msgs: usize,
        bytes: usize,
        capacity: usize,
        ref_count: u32,
    };

    version: []const u8 = "0.1.0",
    health: []const u8 = "OK",
    start_time: u64,
    topics: []Topic,
    metric: StreamMetric,
    producers: []Client,
};

// gauge   - current value, can go up or down
// counter - summary value since start/creation, only increases
fn jsonStat(gpa: std.mem.Allocator, args: Command.Stats, writer: anytype, broker: *Broker) !void {
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const allocator = arena.allocator();

    const topics_capacity = if (args.topic.len == 0) broker.topics.count() else 1;
    var topics = try std.ArrayList(Stat.Topic).initCapacity(allocator, topics_capacity);
    var topics_iter = broker.topics.valueIterator();
    while (topics_iter.next()) |topic_elem| {
        const topic = topic_elem.*;

        if (args.topic.len > 0 and !mem.eql(u8, topic.name, args.topic))
            continue;

        const channels_capacity = if (args.channel.len == 0) topic.channels.count() else 1;
        var channels = try std.ArrayList(Stat.Channel).initCapacity(allocator, channels_capacity);
        var channels_iter = topic.channels.valueIterator();
        while (channels_iter.next()) |channel_elem| {
            const channel = channel_elem.*;

            if (args.channel.len > 0 and !mem.eql(u8, channel.name, args.channel))
                continue;

            const client_count = channel.consumers.items.len;
            const clients: []Stat.Client = if (args.includeClients()) brk: {
                var clients = try allocator.alloc(Stat.Client, client_count);
                for (channel.consumers.items, 0..) |consumer, i| {
                    const remote_address = try std.fmt.allocPrint(allocator, "{}", .{consumer.addr});
                    clients[i] = Stat.Client{
                        .client_id = consumer.identify.client_id,
                        .socket = @intCast(consumer.socket),
                        .hostname = consumer.identify.hostname,
                        .user_agent = consumer.identify.user_agent,
                        .remote_address = remote_address,
                        .ready_count = consumer.ready_count,
                        .in_flight_count = consumer.in_flight,
                        .message_count = consumer.metric.send,
                        .finish_count = consumer.metric.finish,
                        .requeue_count = consumer.metric.requeue,
                        .connect_ts = consumer.metric.connected_at / std.time.ns_per_s,
                        .msg_timeout = consumer.identify.msg_timeout,
                    };
                }
                break :brk clients;
            } else &.{};

            try channels.append(Stat.Channel{
                .channel_name = channel.name,
                // Current number of messages published to the topic but not
                // processed by this channel.
                .depth = channel.metric.depth.value, // gauge
                // Current number of in-flight messages, sent to the client but
                // not fin jet.
                .in_flight_count = channel.in_flight.count(), // gauge
                // Current number of messages in the deferred queue. Message can be
                // deferred by the client (re-queue action), by timeout while
                // in-flight, or by producer (defer publish).
                .deferred_count = channel.deferred.count(), // gauge
                // Total number of messages finished by the clinet(s).
                .message_count = channel.metric.finish.value, // counter
                // Total number of messages re-queued by the client(s).
                .requeue_count = channel.metric.requeue.value, // counter
                // Total number of messages timed-out while in-flight.
                .timeout_count = channel.metric.timeout.value, // counter
                // Current number of connected clients.
                .client_count = client_count, // gauge
                .paused = channel.paused,
                .sequence = channel.sequence,
                .clients = clients,
            });
        }

        var pages = try std.ArrayList(Stat.Page).initCapacity(allocator, topic.stream.pages.items.len);
        var message_count: usize = 0;
        var message_bytes: usize = 0;
        for (topic.stream.pages.items) |page| {
            if (args.includePages()) {
                try pages.append(.{
                    .first_sequence = page.first_sequence,
                    .msgs = page.messagesCount(),
                    .bytes = page.bytesSize(),
                    .capacity = page.capacity(),
                    .ref_count = page.ref_count,
                });
            }
            message_count += page.messagesCount();
            message_bytes += page.bytesSize();
        }
        const stream = Stat.Stream{
            .last_sequence = topic.stream.last_sequence,
            .page_size = topic.stream.page_size,
            .metric = .{
                .msgs = topic.stream.metric.msgs.value,
                .bytes = topic.stream.metric.bytes.value,
                .capacity = topic.stream.metric.capacity.value,
                .total_msgs = topic.stream.metric.total_msgs.value,
                .total_bytes = topic.stream.metric.total_bytes.value,
            },
            .pages_count = topic.stream.pages.items.len,
            .pages = pages.items,
        };

        try topics.append(.{
            .topic_name = topic.name,
            .depth = if (topic.channels.count() == 0) topic.stream.metric.msgs.value else 0,
            .message_count = topic.stream.metric.total_msgs.value,
            .message_bytes = topic.stream.metric.total_bytes.value,
            .paused = topic.paused,
            .channels = channels.items,
            .stream = stream,
        });
    }

    const stat = Stat{
        .start_time = broker.started_at / std.time.ns_per_s,
        .topics = topics.items,
        .metric = .{
            .msgs = broker.metric.msgs.value,
            .bytes = broker.metric.bytes.value,
            .capacity = broker.metric.capacity.value,
            .total_msgs = broker.metric.total_msgs.value,
            .total_bytes = broker.metric.total_bytes.value,
        },
        .producers = &.{},
    };

    try std.json.stringify(stat, .{}, writer);
    return;
}

const Command = union(enum) {
    topic_create: []const u8,
    topic_delete: []const u8,
    topic_empty: []const u8,
    topic_pause: []const u8,
    topic_unpause: []const u8,

    channel_create: Channel,
    channel_delete: Channel,
    channel_empty: Channel,
    channel_pause: Channel,
    channel_unpause: Channel,

    stats: Stats,
    info: void,

    const Channel = struct {
        topic_name: []const u8,
        name: []const u8,
    };

    const Stats = struct {
        topic: []const u8 = &.{},
        channel: []const u8 = &.{},
        format: []const u8 = &.{},
        include_clients: []const u8 = &.{},
        include_pages: []const u8 = &.{},

        fn includeClients(self: Stats) bool {
            if (mem.eql(u8, self.include_clients, "false") or
                mem.eql(u8, self.include_clients, "0")) return false;
            return true;
        }

        fn includePages(self: Stats) bool {
            if (mem.eql(u8, self.include_pages, "true") or
                mem.eql(u8, self.include_pages, "1")) return true;
            return false;
        }
    };
};

fn parse(target: []const u8) !Command {
    if (mem.startsWith(u8, target, "/stats")) {
        if (target.len <= 7) return .{ .stats = .{} };
        const qs = target[7..];
        return .{ .stats = .{
            .topic = try nameOrEmpty(queryStringValue(qs, "topic")),
            .channel = try nameOrEmpty(queryStringValue(qs, "channel")),
            .format = queryStringValue(qs, "format"),
            .include_clients = queryStringValue(qs, "include_clients"),
            .include_pages = queryStringValue(qs, "include_pages"),
        } };
    }
    if (mem.startsWith(u8, target, "/info")) return .{ .info = {} };

    if (mem.startsWith(u8, target, "/topic")) {
        if (mem.startsWith(u8, target[6..], "/create?topic="))
            return .{ .topic_create = try validateName(target[20..]) };
        if (mem.startsWith(u8, target[6..], "/delete?topic="))
            return .{ .topic_delete = try validateName(target[20..]) };
        if (mem.startsWith(u8, target[6..], "/empty?topic="))
            return .{ .topic_empty = try validateName(target[19..]) };
        if (mem.startsWith(u8, target[6..], "/pause?topic="))
            return .{ .topic_pause = try validateName(target[19..]) };
        if (mem.startsWith(u8, target[6..], "/unpause?topic="))
            return .{ .topic_unpause = try validateName(target[21..]) };
    }
    if (mem.startsWith(u8, target, "/channel")) {
        if (mem.startsWith(u8, target[8..], "/create?"))
            return .{ .channel_create = .{
                .topic_name = try findName(target[16..], "topic"),
                .name = try findName(target[16..], "channel"),
            } };
        if (mem.startsWith(u8, target[8..], "/delete?"))
            return .{ .channel_delete = .{
                .topic_name = try findName(target[16..], "topic"),
                .name = try findName(target[16..], "channel"),
            } };
        if (mem.startsWith(u8, target[8..], "/empty?"))
            return .{ .channel_empty = .{
                .topic_name = try findName(target[15..], "topic"),
                .name = try findName(target[15..], "channel"),
            } };
        if (mem.startsWith(u8, target[8..], "/pause?"))
            return .{ .channel_pause = .{
                .topic_name = try findName(target[15..], "topic"),
                .name = try findName(target[15..], "channel"),
            } };
        if (mem.startsWith(u8, target[8..], "/unpause?"))
            return .{ .channel_unpause = .{
                .topic_name = try findName(target[17..], "topic"),
                .name = try findName(target[17..], "channel"),
            } };
    }
    if (mem.startsWith(u8, target, "/create")) {
        if (mem.startsWith(u8, target[7..], "_topic?topic="))
            return .{ .topic_create = target[20..] };
        if (mem.startsWith(u8, target[7..], "_channel?"))
            return .{ .channel_create = .{
                .topic_name = try findName(target[16..], "topic"),
                .name = try findName(target[16..], "channel"),
            } };
    }

    return error.UnknownCommand;
}

fn nameOrEmpty(name: []const u8) ![]const u8 {
    if (name.len == 0) return name;
    return try validateName(name);
}

fn findName(query: []const u8, key: []const u8) ![]const u8 {
    return try validateName(try findValue(query, key));
}

fn findValue(query: []const u8, key: []const u8) ![]const u8 {
    var it = std.mem.splitScalar(u8, query, '&');
    while (it.next()) |pair| {
        if (mem.indexOfScalarPos(u8, pair, 0, '=')) |sep| {
            if (mem.eql(u8, key, pair[0..sep])) return pair[sep + 1 ..];
        }
    }
    return error.KeyNotFound;
}

fn queryStringValue(query: []const u8, key: []const u8) []const u8 {
    var it = std.mem.splitScalar(u8, query, '&');
    while (it.next()) |pair| {
        if (mem.indexOfScalarPos(u8, pair, 0, '=')) |sep| {
            if (mem.eql(u8, key, pair[0..sep])) return pair[sep + 1 ..];
        }
    }
    return &.{};
}

const testing = std.testing;

test parse {
    try testing.expectEqualStrings("topic-001", (try parse("/topic/empty?topic=topic-001")).topic_empty);
    try testing.expectEqualStrings("topic-002", (try parse("/topic/pause?topic=topic-002")).topic_pause);
    try testing.expectEqualStrings("topic-003", (try parse("/topic/delete?topic=topic-003")).topic_delete);
    try testing.expectEqualStrings("topic-004", (try parse("/topic/unpause?topic=topic-004")).topic_unpause);
    try testing.expectEqualStrings("topic-005", (try parse("/topic/create?topic=topic-005")).topic_create);
    try testing.expectEqualStrings("topic-006", (try parse("/create_topic?topic=topic-006")).topic_create);

    var cmd = try parse("/channel/delete?topic=topic-004&channel=005");
    try testing.expectEqualStrings("topic-004", cmd.channel_delete.topic_name);
    try testing.expectEqualStrings("005", cmd.channel_delete.name);

    cmd = try parse("/channel/empty?channel=009&topic=topic-008");
    try testing.expectEqualStrings("topic-008", cmd.channel_empty.topic_name);
    try testing.expectEqualStrings("009", cmd.channel_empty.name);

    cmd = try parse("/channel/pause?topic=topic-006&channel=007");
    try testing.expectEqualStrings("topic-006", cmd.channel_pause.topic_name);
    try testing.expectEqualStrings("007", cmd.channel_pause.name);

    cmd = try parse("/channel/unpause?topic=topic-008&channel=009");
    try testing.expectEqualStrings("topic-008", cmd.channel_unpause.topic_name);
    try testing.expectEqualStrings("009", cmd.channel_unpause.name);

    cmd = try parse("/channel/create?topic=topic-010&channel=011");
    try testing.expectEqualStrings("topic-010", cmd.channel_create.topic_name);
    try testing.expectEqualStrings("011", cmd.channel_create.name);

    cmd = try parse("/create_channel?topic=topic-012&channel=013");
    try testing.expectEqualStrings("topic-012", cmd.channel_create.topic_name);
    try testing.expectEqualStrings("013", cmd.channel_create.name);

    cmd = try parse("/stats?format=json&topic=topic-014&channel=channel-014&include_clients=false");
    try testing.expectEqualStrings("topic-014", cmd.stats.topic);
    try testing.expectEqualStrings("channel-014", cmd.stats.channel);
    try testing.expectEqualStrings("false", cmd.stats.include_clients);
    try testing.expect(!cmd.stats.includeClients());
    try testing.expectEqualStrings("json", cmd.stats.format);

    try testing.expectError(error.InvalidNameCharacter, parse("/topic/empty?topic=topic-000%"));
    try testing.expectError(error.InvalidName, parse("/topic/empty?topic="));
    try testing.expectError(error.InvalidName, parse("/topic/empty?topic=01234567890012345678900123456789001234567890012345678900123456789001234567890"));
}

test findValue {
    try testing.expectEqualStrings("topic-001", try findValue("topic=topic-001&channel=001", "topic"));
    try testing.expectEqualStrings("001", try findValue("topic=topic-001&channel=001", "channel"));
    try testing.expectEqualStrings("", try findValue("topic=topic-001&channel=", "channel"));
    try testing.expectError(error.KeyNotFound, findValue("topic=topic-001&channel=001", "pero"));
}
