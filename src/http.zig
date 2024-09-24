const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const posix = std.posix;
const socket_t = std.posix.socket_t;
const fd_t = std.posix.fd_t;

const protocol = @import("protocol.zig");
const Options = @import("Options.zig");
const Io = @import("io.zig").Io;
const Op = @import("io.zig").Op;
const Error = @import("io.zig").Error;
const Timer = @import("io.zig").Timer;
const Server = @import("tcp.zig").Server;
pub const Listener = @import("tcp.zig").ListenerType(Conn);

const log = std.log.scoped(.http);

pub const Conn = struct {
    gpa: mem.Allocator,
    listener: *Listener,
    io: *Io,
    socket: socket_t = 0,
    addr: std.net.Address,

    recv_op: ?*Op = null,
    send_op: ?*Op = null,
    arena: ?std.heap.ArenaAllocator = null,

    send_vec: [2]posix.iovec_const = undefined, // header and body
    send_msghdr: posix.msghdr_const = .{ .iov = undefined, .iovlen = undefined, .name = null, .namelen = 0, .control = null, .controllen = 0, .flags = 0 },

    pub fn init(listener: *Listener, socket: socket_t, addr: std.net.Address) Conn {
        return .{
            .gpa = listener.allocator,
            .listener = listener,
            .io = listener.io,
            .socket = socket,
            .addr = addr,
        };
    }

    pub fn recv(self: *Conn) !void {
        self.recv_op = try self.io.recv(self.socket, self, received, recvFailed);
    }

    fn received(self: *Conn, bytes: []const u8) Error!void {
        if (self.send_op != null)
            return try self.close();
        assert(self.arena == null);

        self.arena = std.heap.ArenaAllocator.init(self.gpa);
        errdefer self.sendDeinit();
        const allocator = self.arena.?.allocator();

        var body_list = std.ArrayList(u8).init(allocator);
        defer body_list.deinit();
        const writer = body_list.writer().any();

        self.handle(writer, bytes) catch |err| {
            log.warn("request failed {}", .{err});
            const rsp = switch (err) {
                error.NotFound => not_found,
                else => bad_request,
            };
            self.send_vec[0] = .{ .base = rsp.ptr, .len = rsp.len };
            self.send_vec[1].len = 0;
            return self.send();
        };

        const body = try body_list.toOwnedSlice();
        const header = try std.fmt.allocPrint(allocator, header_template, .{ body.len, "text/plain" });
        self.send_vec[0] = .{ .base = header.ptr, .len = header.len };
        self.send_vec[1] = .{ .base = body.ptr, .len = body.len };
        try self.send();
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

        const server = self.listener.server;
        const cmd = parse(head.target) catch return error.NotFound;
        switch (cmd) {
            .stats => return try jsonStat(self.gpa, writer, server),
            .info => return try jsonInfo(writer, server, self.listener.options),
            .channel_delete => |arg| try server.deleteChannel(arg.topic_name, arg.name),
            .topic_delete => |name| try server.deleteTopic(name),
            .channel_pause => |arg| try server.pauseChannel(arg.topic_name, arg.name),
            .channel_unpause => |arg| try server.unpauseChannel(arg.topic_name, arg.name),
            .topic_pause => |name| try server.pauseTopic(name),
            .topic_unpause => |name| try server.unpauseTopic(name),
            .topic_empty => |name| try server.emptyTopic(name),
            .channel_empty => |arg| try server.emptyChannel(arg.topic_name, arg.name),
        }
    }

    fn send(self: *Conn) !void {
        assert(self.send_op == null);
        self.send_msghdr.iov = &self.send_vec;
        self.send_msghdr.iovlen = if (self.send_vec[1].len > 0) 2 else 1;
        self.send_op = try self.io.sendv(self.socket, &self.send_msghdr, self, sent, sendFailed);
    }

    fn sent(self: *Conn) Error!void {
        self.sendDeinit();
    }

    fn sendDeinit(self: *Conn) void {
        self.send_op = null;
        if (self.arena) |arena| {
            arena.deinit();
            self.arena = null;
        }
    }

    fn sendFailed(self: *Conn, err: anyerror) Error!void {
        self.sendDeinit();
        switch (err) {
            error.Canceled, error.BrokenPipe, error.ConnectionResetByPeer => {},
            else => log.err("{} send failed {}", .{ self.socket, err }),
        }
        try self.close();
    }

    fn recvFailed(self: *Conn, err: anyerror) Error!void {
        self.recv_op = null;
        switch (err) {
            error.EndOfFile => {},
            error.ConnectionResetByPeer => {},
            else => log.err("{} recv failed {}", .{ self.socket, err }),
        }
        try self.close();
    }

    pub fn close(self: *Conn) !void {
        if (self.send_op) |op| {
            try op.cancel();
            return;
        }
        log.debug("{} close", .{self.socket});
        if (self.recv_op) |op| {
            try op.cancel();
            op.unsubscribe(self);
        }

        try self.io.close(self.socket);
        self.listener.remove(self);
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

fn jsonInfo(writer: anytype, server: *Server, options: Options) !void {
    var hostname_buf: [std.posix.HOST_NAME_MAX]u8 = undefined;
    const hostname = try std.posix.gethostname(&hostname_buf);
    const start_time = server.started_at / std.time.ns_per_s;

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
        depth: u64 = 0,
        message_count: u64 = 0,
        message_bytes: u64 = 0,
        paused: bool,
        channels: []Channel,
        e2e_processing_latency: struct { count: u64 = 0 } = .{},
    };
    const Channel = struct {
        channel_name: []const u8,
        depth: u64,
        in_flight_count: u64,
        deferred_count: u64,
        message_count: u64,
        requeue_count: u64,
        timeout_count: u64,
        client_count: u64,
        clients: []Client,
        paused: bool,
        e2e_processing_latency: struct { count: u64 = 0 } = .{},
    };
    const Client = struct {
        client_id: []const u8,
        hostname: []const u8,
        user_agent: []const u8,
        version: []const u8 = "V2",
        remote_address: []const u8,
        state: u8 = 3,
        ready_count: u64,
        in_flight_count: u64,
        message_count: u64,
        finish_count: u64,
        requeue_count: u64,
        connect_ts: u64,
        msg_timeout: u64,
        pub_counts: []PubCount = &.{},
    };
    const PubCount = struct {
        topic: []const u8,
        count: u64,
    };

    version: []const u8 = "0.1.0",
    health: []const u8 = "OK",
    start_time: u64,
    topics: []Topic,
    producers: []Client,
};

fn jsonStat(gpa: std.mem.Allocator, writer: anytype, server: *Server) !void {
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const allocator = arena.allocator();

    var topics = try allocator.alloc(Stat.Topic, server.topics.count());
    var topics_iter = server.topics.iterator();
    var topic_idx: usize = 0;
    while (topics_iter.next()) |topic_elem| : (topic_idx += 1) {
        const topic_name = topic_elem.key_ptr.*;
        const topic = topic_elem.value_ptr.*;

        var channels = try allocator.alloc(Stat.Channel, topic.channels.count());
        var channels_iter = topic.channels.iterator();
        var channel_idx: usize = 0;
        while (channels_iter.next()) |channel_elem| : (channel_idx += 1) {
            const channel_name = channel_elem.key_ptr.*;
            const channel = channel_elem.value_ptr.*;

            const client_count = channel.consumers.items.len;
            var clients = try allocator.alloc(Stat.Client, client_count);

            for (channel.consumers.items, 0..) |consumer, i| {
                const remote_address = try std.fmt.allocPrint(allocator, "{}", .{consumer.addr});
                clients[i] = Stat.Client{
                    .client_id = consumer.identify.client_id,
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

            // gauge   - current value, can go up or down
            // counter - summary value since start/creation, only increases

            channels[channel_idx] = Stat.Channel{
                .channel_name = channel_name,

                // Current number of messages published to the topic but not
                // processed by this channel.
                .depth = channel.metric.depth, // gauge

                // Current number of in-flight messages, sent to the client but
                // not fin jet.
                .in_flight_count = channel.in_flight.count(), // gauge

                // Current number of messages in the deferred queue. Message can be
                // deferred by the client (re-queue action), by timeout while
                // in-flight, or by producer (defer publish).
                .deferred_count = channel.deferred.count(), // gauge

                // Total number of messages finished by the clinet(s).
                .message_count = channel.metric.finish, // counter

                // Total number of messages re-queued by the client(s).
                .requeue_count = channel.metric.requeue, // counter

                // Total number of messages timed-out while in-flight.
                .timeout_count = channel.metric.timeout, // counter

                // Current number of connected clients.
                .client_count = client_count, // gauge

                .clients = clients,
                .paused = channel.paused,
            };
        }

        topics[topic_idx] = Stat.Topic{
            .topic_name = topic_name,
            .depth = topic.metric.depth,
            .message_count = topic.metric.total,
            .message_bytes = topic.metric.total_bytes,
            .paused = topic.paused,
            .channels = channels,
        };
    }

    const stat = Stat{
        .start_time = server.started_at / std.time.ns_per_s,
        .topics = topics,
        .producers = &.{},
    };

    try std.json.stringify(stat, .{}, writer);
    return;
}

const Command = union(enum) {
    topic_empty: []const u8,
    topic_pause: []const u8,
    topic_unpause: []const u8,
    topic_delete: []const u8,
    channel_empty: Channel,
    channel_pause: Channel,
    channel_unpause: Channel,
    channel_delete: Channel,
    stats: void,
    info: void,

    const Channel = struct {
        topic_name: []const u8,
        name: []const u8,
    };
};

fn parse(target: []const u8) !Command {
    if (mem.startsWith(u8, target, "/stats")) return .{ .stats = {} };
    if (mem.startsWith(u8, target, "/info")) return .{ .info = {} };

    if (mem.startsWith(u8, target, "/topic")) {
        if (mem.startsWith(u8, target[6..], "/empty?topic="))
            return .{ .topic_empty = target[19..] };
        if (mem.startsWith(u8, target[6..], "/pause?topic="))
            return .{ .topic_pause = target[19..] };
        if (mem.startsWith(u8, target[6..], "/unpause?topic="))
            return .{ .topic_unpause = target[21..] };
        if (mem.startsWith(u8, target[6..], "/delete?topic="))
            return .{ .topic_delete = target[20..] };
    }
    if (mem.startsWith(u8, target, "/channel")) {
        if (mem.startsWith(u8, target[8..], "/empty?"))
            return .{ .channel_empty = .{
                .topic_name = try findValue(target[15..], "topic"),
                .name = try findValue(target[15..], "channel"),
            } };
        if (mem.startsWith(u8, target[8..], "/pause?"))
            return .{ .channel_pause = .{
                .topic_name = try findValue(target[15..], "topic"),
                .name = try findValue(target[15..], "channel"),
            } };
        if (mem.startsWith(u8, target[8..], "/unpause?"))
            return .{ .channel_unpause = .{
                .topic_name = try findValue(target[17..], "topic"),
                .name = try findValue(target[17..], "channel"),
            } };
        if (mem.startsWith(u8, target[8..], "/delete?"))
            return .{ .channel_delete = .{
                .topic_name = try findValue(target[16..], "topic"),
                .name = try findValue(target[16..], "channel"),
            } };
    }
    return error.UnknownCommand;
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

const testing = std.testing;

// parse also these params for stats requests
// target: /stats?format=json&topic=topic-000&include_clients=false, content length: 0
// target: /stats?format=json&topic=topic-000&channel=000, content length: 0
// target: /stats?format=json&include_clients=false, content length: 0

test parse {
    try testing.expectEqualStrings("topic-001", (try parse("/topic/empty?topic=topic-001")).topic_empty);
    try testing.expectEqualStrings("topic-002", (try parse("/topic/pause?topic=topic-002")).topic_pause);
    try testing.expectEqualStrings("topic-003", (try parse("/topic/delete?topic=topic-003")).topic_delete);
    try testing.expectEqualStrings("topic-004", (try parse("/topic/unpause?topic=topic-004")).topic_unpause);

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
}

test findValue {
    try testing.expectEqualStrings("topic-001", try findValue("topic=topic-001&channel=001", "topic"));
    try testing.expectEqualStrings("001", try findValue("topic=topic-001&channel=001", "channel"));
    try testing.expectEqualStrings("", try findValue("topic=topic-001&channel=", "channel"));
    try testing.expectError(error.KeyNotFound, findValue("topic=topic-001&channel=001", "pero"));
}
