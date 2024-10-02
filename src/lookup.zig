const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const net = std.net;
const posix = std.posix;
const socket_t = std.posix.socket_t;
const testing = std.testing;

const Io = @import("io.zig").Io;
const Op = @import("io.zig").Op;
const Error = @import("io.zig").Error;
const Topic = @import("simple_topic.zig").SimpleTopic([]const u8, Conn, Conn.pullTopic);
const RecvBuf = @import("tcp.zig").RecvBuf;
const Server = @import("tcp.zig").Server;

const log = std.log.scoped(.lookup);

pub const Connector = struct {
    const Self = @This();

    allocator: mem.Allocator,
    io: *Io,
    server: *Server,
    connections: std.ArrayList(*Conn),
    identify: []const u8,
    topic: Topic,
    state: State,
    const State = enum {
        active,
        closing,
    };

    pub fn init(self: *Self, allocator: mem.Allocator, io: *Io, server: *Server, lookup_tcp_addresses: []net.Address) !void {
        // TODO: create identify from command line arguments, options
        const identify = identifyMessage(allocator, "127.0.0.1", "hydra", 4151, 4150, "0.1.0") catch "";
        self.* = .{
            .allocator = allocator,
            .io = io,
            .server = server,
            .connections = std.ArrayList(*Conn).init(allocator),
            .topic = Topic.init(allocator),
            .identify = identify,
            .state = .active,
        };
        errdefer self.deinit();
        try self.connections.ensureUnusedCapacity(lookup_tcp_addresses.len);
        for (lookup_tcp_addresses) |addr| try self.addLookupd(addr);
    }

    fn addLookupd(self: *Self, address: std.net.Address) !void {
        const conn = try self.allocator.create(Conn);
        errdefer self.allocator.destroy(conn);
        try self.connections.ensureUnusedCapacity(1);
        try conn.init(self, address);
        self.connections.appendAssumeCapacity(conn);
    }

    pub fn close(self: *Self) !void {
        self.state = .closing;
        for (self.connections.items) |conn| try conn.close();
    }

    pub fn deinit(self: *Self) void {
        for (self.connections.items) |conn| {
            conn.deinit();
            self.allocator.destroy(conn);
        }
        self.connections.deinit();
        self.allocator.free(self.identify);
        self.topic.deinit();
    }

    pub fn topicCreated(self: *Self, name: []const u8) void {
        self.register("REGISTER {s}\n", .{name});
    }

    pub fn channelCreated(self: *Self, topic_name: []const u8, name: []const u8) void {
        self.register("REGISTER {s} {s}\n", .{ topic_name, name });
    }

    pub fn topicDeleted(self: *Self, name: []const u8) void {
        self.register("UNREGISTER {s}\n", .{name});
    }

    pub fn channelDeleted(self: *Self, topic_name: []const u8, name: []const u8) void {
        self.register("UNREGISTER {s} {s}\n", .{ topic_name, name });
    }

    fn register(self: *Self, comptime fmt: []const u8, args: anytype) void {
        if (self.state != .active or !self.topic.hasConsumers()) return;
        self.registerFailable(fmt, args) catch |err| {
            // Disconnect all connections. On next connect they will get fresh
            // current state.
            for (self.connections.items) |conn| conn.disconnect();
            log.err("add registration failed {}", .{err});
        };
    }

    fn registerFailable(self: *Self, comptime fmt: []const u8, args: anytype) !void {
        const buf = try std.fmt.allocPrint(self.allocator, fmt, args);
        errdefer self.allocator.free(buf);
        assert(try self.topic.append(buf));
    }

    fn connect(self: *Self, conn: *Conn) !void {
        if (self.server.topics.count() > 0) {
            try self.topic.setFirst(self.registrations() catch return error.OutOfMemory);
        }
        try self.topic.subscribe(conn);
    }

    fn disconnect(self: *Self, conn: *Conn) void {
        self.topic.unsubscribe(conn);
    }

    // Current state for newly connected lookupd
    fn registrations(self: *Self) ![]const u8 {
        if (self.server.topics.count() == 0) return "";

        var writer = RegistrationsWriter.init(self.allocator);
        try self.server.iterateNames(&writer);
        return try writer.toOwned();
    }
};

pub const RegistrationsWriter = struct {
    list: std.ArrayList(u8),

    const Self = @This();
    pub fn init(allocator: mem.Allocator) Self {
        return .{
            .list = std.ArrayList(u8).init(allocator),
        };
    }

    pub fn topic(self: *Self, name: []const u8) !void {
        const writer = self.list.writer().any();
        try writer.print("REGISTER {s}\n", .{name});
    }
    pub fn channel(self: *Self, topic_name: []const u8, name: []const u8) !void {
        const writer = self.list.writer().any();
        try writer.print("REGISTER {s} {s}\n", .{ topic_name, name });
    }

    pub fn toOwned(self: *Self) ![]const u8 {
        const buf = try self.list.toOwnedSlice();
        self.list.deinit();
        return buf;
    }
};

const Conn = struct {
    connector: *Connector,
    recv_buf: RecvBuf,
    io: *Io,
    address: std.net.Address,
    socket: socket_t = 0,
    connect_op: ?*Op = null,
    send_op: ?*Op = null,
    recv_op: ?*Op = null,
    ticker_op: ?*Op = null,
    state: State = .connecting,

    const ping_msg = "PING\n";
    const ping_interval = 15 * 1000; // in milliseconds

    const State = enum {
        connecting,
        connected,
        disconnected,
        closed,
    };

    const Self = @This();

    pub fn init(self: *Self, connector: *Connector, address: std.net.Address) !void {
        const allocator = connector.allocator;
        self.* = .{
            .connector = connector,
            .io = connector.io,
            .address = address,
            .recv_buf = RecvBuf.init(allocator),
        };
        errdefer self.deinit();
        try self.io.ticker(ping_interval, self, tick, tickerFailed, &self.ticker_op);
        errdefer Op.cancel(self.ticker_op) catch {};
        try self.reconnect();
    }

    fn deinit(self: *Self) void {
        self.recv_buf.free();
    }

    fn pullTopic(self: *Self) !void {
        if (self.send_op != null) return;
        if (self.connector.topic.next(self)) |buf| {
            try self.send(buf);
        }
    }

    fn reconnect(self: *Self) Error!void {
        Op.unsubscribe(self.send_op);
        Op.unsubscribe(self.recv_op);
        if (self.socket != 0) {
            try self.io.close(self.socket);
            self.socket = 0;
        }
        try self.io.socketCreate(
            self.address.any.family,
            posix.SOCK.STREAM | posix.SOCK.CLOEXEC,
            0,
            self,
            socketCreated,
            socketFailed,
            &self.connect_op,
        );
        self.state = .connecting;
    }

    fn tick(self: *Self) Error!void {
        switch (self.state) {
            .disconnected => try self.reconnect(),
            .connected => try self.ping(),
            else => {},
        }
    }

    fn tickerFailed(self: *Self, err: anyerror) Error!void {
        switch (err) {
            error.OperationCanceled => {},
            else => log.err("{} ticker failed {}", .{ self.address, err }),
        }
    }

    fn socketCreated(self: *Self, socket: socket_t) Error!void {
        self.socket = socket;
        self.state = .connecting;
        try self.io.connect(self.socket, &self.address, self, connected, connectFailed, &self.connect_op);
    }

    fn socketFailed(self: *Self, err: anyerror) Error!void {
        self.disconnect();
        log.err("{} socket create failed {}", .{ self.address, err });
    }

    fn connectFailed(self: *Self, err: anyerror) Error!void {
        self.disconnect();
        log.info("{} connect failed {}", .{ self.address, err });
    }

    fn connected(self: *Self) Error!void {
        try self.send(self.connector.identify);
        try self.recv();
        try self.connector.connect(self);
        self.state = .connected;
        log.debug("{} connected", .{self.address});
    }

    fn disconnect(self: *Self) void {
        self.state = .disconnected;
        self.connector.disconnect(self);
        log.info("{} disconnected", .{self.address});
    }

    fn send(self: *Self, buf: []const u8) !void {
        assert(self.send_op == null);
        assert(buf.len > 0);
        try self.io.send(self.socket, buf, self, sent, sendFailed, &self.send_op);
    }

    fn ping(self: *Self) Error!void {
        if (self.send_op == null) try self.send(ping_msg);
    }

    fn sent(self: *Self) Error!void {
        try self.pullTopic();
    }

    fn sendFailed(self: *Self, err: anyerror) Error!void {
        switch (err) {
            error.BrokenPipe, error.ConnectionResetByPeer => {},
            else => log.err("{} send failed {}", .{ self.address, err }),
        }
        if (self.state == .connected) self.disconnect();
    }

    fn recv(self: *Self) !void {
        try self.io.recv(self.socket, self, received, recvFailed, &self.recv_op);
    }

    fn received(self: *Self, bytes: []const u8) Error!void {
        var buf = try self.recv_buf.append(bytes);

        while (true) {
            if (buf.len < 4) break;
            const n = mem.readInt(u32, buf[0..4], .big);
            const msg_buf = buf[4..];
            if (msg_buf.len < n) break;
            const msg = msg_buf[0..n];
            buf = buf[4 + msg.len ..];

            if (msg.len == 2 and msg[0] == 'O' and msg[1] == 'K') {
                // OK most common case
                continue;
            }
            if (msg[0] == '{' and msg[msg.len - 1] == '}') {
                // identify response
                log.debug("{} identify: {s}", .{ self.address, msg });
                continue;
            }
            // error
            log.warn("{} {s}", .{ self.socket, msg });
        }

        if (buf.len > 0)
            try self.recv_buf.set(buf)
        else
            self.recv_buf.free();
    }

    fn recvFailed(self: *Self, err: anyerror) Error!void {
        switch (err) {
            error.EndOfFile, error.ConnectionResetByPeer => {},
            else => log.err("{} recv failed {}", .{ self.address, err }),
        }
        if (self.state == .connected) self.disconnect();
    }

    pub fn close(self: *Self) !void {
        try Op.cancel(self.connect_op);
        try Op.cancel(self.send_op);
        try Op.cancel(self.recv_op);
        try Op.cancel(self.ticker_op);
        if (self.socket > 0)
            try self.io.close(self.socket);
        self.state = .closed;
    }
};

// Create lookupd version and identify message
fn identifyMessage(
    allocator: mem.Allocator,
    broadcast_address: []const u8,
    hostname: []const u8,
    http_port: u16,
    tcp_port: u16,
    version: []const u8,
) ![]const u8 {
    const msg = try std.fmt.allocPrint(
        allocator,
        "  V1IDENTIFY\n\x00\x00\x00\x63{{\"broadcast_address\":\"{s}\",\"hostname\":\"{s}\",\"http_port\":{d},\"tcp_port\":{d},\"version\":\"{s}\"}}",
        .{ broadcast_address, hostname, http_port, tcp_port, version },
    );
    std.mem.writeInt(u32, msg[13..17], @intCast(msg[17..].len), .big);
    return msg;
}

test "identify message" {
    const im = try identifyMessage(
        testing.allocator,
        "hydra",
        "hydra",
        4151,
        4150,
        "1.3.0",
    );
    defer testing.allocator.free(im);
    const expected = "  V1IDENTIFY\n\x00\x00\x00\x63{\"broadcast_address\":\"hydra\",\"hostname\":\"hydra\",\"http_port\":4151,\"tcp_port\":4150,\"version\":\"1.3.0\"}";
    try testing.expectEqualStrings(expected, im);
}
