const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const posix = std.posix;
const socket_t = std.posix.socket_t;
const fd_t = std.posix.fd_t;

const protocol = @import("protocol.zig");
const Options = protocol.Options;
const Io = @import("io.zig").Io;
const Op = @import("io.zig").Op;
const Error = @import("io.zig").Error;
const Timer = @import("io.zig").Timer;
const Server = @import("tcp.zig").Server;

const log = std.log.scoped(.http);

pub const Listener = struct {
    allocator: mem.Allocator,
    server: *Server,
    options: Options,
    io: *Io,
    op: ?*Op = null,
    stat: struct {
        accepted: u64 = 0,
        completed: u64 = 0,
    } = .{},

    pub fn init(allocator: mem.Allocator, io: *Io, server: *Server, options: Options) !Listener {
        return .{
            .allocator = allocator,
            .server = server,
            .options = options,
            .io = io,
        };
    }

    pub fn deinit(self: *Listener) void {
        _ = self;
    }

    pub fn accept(self: *Listener, socket: socket_t) !void {
        self.op = try self.io.accept(socket, self, accepted, failed);
    }

    fn accepted(self: *Listener, socket: socket_t, addr: std.net.Address) Error!void {
        var conn = try self.allocator.create(Conn);
        conn.* = Conn{
            .gpa = self.allocator,
            .socket = socket,
            .addr = addr,
            .listener = self,
            .io = self.io,
        };
        try conn.init();
        self.stat.accepted +%= 1;
    }

    fn failed(self: *Listener, err: anyerror) Error!void {
        self.op = null;
        switch (err) {
            error.OperationCanceled => {},
            else => log.err("accept failed {}", .{err}),
        }
    }

    pub fn close(self: *Listener) !void {
        if (self.op) |op|
            try op.cancel();
    }

    fn release(self: *Listener, conn: *Conn) void {
        self.allocator.destroy(conn);
        self.stat.completed +%= 1;
    }
};

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
    send_msghdr: posix.msghdr_const = .{
        .iov = undefined,
        .iovlen = undefined,
        .name = null,
        .namelen = 0,
        .control = null,
        .controllen = 0,
        .flags = 0,
    },

    fn init(self: *Conn) !void {
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
            log.err("{} receive buffer overflow method: {}, target: {s}, content length: {}", .{ self.socket, head.method, head.target, content_length });
            return error.BadRequest;
        }

        log.debug("{} method: {}, target: {s}, content length: {}", .{ self.socket, head.method, head.target, content_length });

        if (std.mem.startsWith(u8, head.target, "/stats"))
            return try jsonStat(self.gpa, writer, self.listener.server);
        if (std.mem.startsWith(u8, head.target, "/info"))
            return try jsonInfo(writer, self.listener.server, self.listener.options);
        return error.NotFound;
    }

    fn send(self: *Conn) !void {
        assert(self.send_op == null);
        self.send_msghdr.iov = &self.send_vec;
        self.send_msghdr.iovlen = if (self.send_vec[1].len > 0) 2 else 1;
        self.send_op = try self.io.sendv(self.socket, &self.send_msghdr, self, sent, sendFailed);
    }

    fn sent(self: *Conn, _: usize) Error!void {
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

    fn close(self: *Conn) !void {
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
        self.listener.release(self);
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
        .broadcast_address = "",
        .hostname = hostname,
        .http_port = options.http_port,
        .tcp_port = options.tcp_port,
        .start_time = start_time,
        .max_heartbeat_interval = options.heartbeat_interval,
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
        paused: bool = false,
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
                    // TODO
                    .message_count = 0,
                    .finish_count = 0,
                    .requeue_count = 0,
                    .connect_ts = consumer.connected_at / std.time.ns_per_s,
                    .msg_timeout = consumer.identify.msg_timeout,
                };
            }

            channels[channel_idx] = Stat.Channel{
                .channel_name = channel_name,
                .depth = channel.in_flight.count() + channel.deferred.count(),
                .in_flight_count = channel.in_flight.count(),
                .deferred_count = channel.deferred.count(),
                .message_count = channel.stat.pull,
                .requeue_count = channel.stat.requeue,
                .timeout_count = channel.stat.timeout,
                .client_count = client_count,
                .clients = clients,
                .paused = false,
            };
        }

        topics[topic_idx] = Stat.Topic{
            .topic_name = topic_name,
            .depth = topic.messages.count(),
            .message_count = topic.sequence,
            .message_bytes = 0, // TODO
            .paused = false,
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
