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
    ticker_op: ?*Op = null,
    topic: Topic,
    state: State,
    const State = enum {
        connected,
        closing,
    };
    const ping_interval = 15 * 1000; // in milliseconds

    pub fn init(self: *Self, allocator: mem.Allocator, io: *Io, server: *Server, lookup_tcp_addresses: []net.Address) !void {
        // TODO: create identify from command line arguments, options
        const identify = try identifyMessage(allocator, "127.0.0.1", "hydra", 4151, 4150, "0.1.0");
        self.* = .{
            .allocator = allocator,
            .io = io,
            .server = server,
            .connections = std.ArrayList(*Conn).init(allocator),
            .topic = Topic.init(allocator),
            .identify = identify,
            .state = .connected,
        };
        errdefer self.deinit();
        try self.connections.ensureUnusedCapacity(lookup_tcp_addresses.len);
        for (lookup_tcp_addresses) |addr| try self.addLookupd(addr);
        try self.io.ticker(ping_interval, self, tick, tickerFailed, &self.ticker_op);
    }

    fn addLookupd(self: *Self, address: std.net.Address) !void {
        const conn = try self.allocator.create(Conn);
        errdefer self.allocator.destroy(conn);
        try self.connections.ensureUnusedCapacity(1);
        try conn.init(self, address);
        self.connections.appendAssumeCapacity(conn);
    }

    pub fn close(self: *Self) void {
        self.state = .closing;
        for (self.connections.items) |conn| conn.shutdown();
        if (self.ticker_op) |op| op.detach();
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

    fn tick(self: *Self) Error!void {
        for (self.connections.items) |conn| {
            conn.tick();
        }
    }

    fn tickerFailed(self: *Self, err: anyerror) Error!void {
        switch (err) {
            error.OperationCanceled => {},
            else => log.err("ticker failed {}", .{err}),
        }
        if (self.state == .connected)
            try self.io.ticker(ping_interval, self, tick, tickerFailed, &self.ticker_op);
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
        if (self.state != .connected or !self.topic.hasConsumers()) return;
        self.tryRegister(fmt, args) catch |err| {
            // Disconnect all connections. On next connect they will get fresh
            // current state.
            for (self.connections.items) |conn| conn.shutdown();
            log.err("add registration failed {}", .{err});
        };
    }

    fn tryRegister(self: *Self, comptime fmt: []const u8, args: anytype) !void {
        const buf = try std.fmt.allocPrint(self.allocator, fmt, args);
        errdefer self.allocator.free(buf);
        assert(try self.topic.append(buf));
    }

    fn connect(self: *Self, conn: *Conn) !void {
        if (self.server.topics.count() > 0) {
            try self.topic.setFirst(try self.registrations());
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
    close_op: ?*Op = null,
    state: State = .closed,

    const ping_msg = "PING\n";

    const State = enum {
        closed,
        connecting,
        connected,
        closing,
    };

    const Self = @This();

    fn init(self: *Self, connector: *Connector, address: std.net.Address) !void {
        const allocator = connector.allocator;
        self.* = .{
            .connector = connector,
            .io = connector.io,
            .address = address,
            .recv_buf = RecvBuf.init(allocator),
        };
        errdefer self.deinit();
        try self.reconnect();
    }

    fn deinit(self: *Self) void {
        self.recv_buf.free();
    }

    fn tick(self: *Self) void {
        switch (self.state) {
            .closed => self.reconnect() catch {},
            .connected => self.ping() catch {},
            else => {},
        }
    }

    fn ping(self: *Self) Error!void {
        if (self.send_op == null) try self.send(ping_msg);
    }

    fn reconnect(self: *Self) Error!void {
        assert(self.state == .closed);
        assert(self.connect_op == null);
        assert(self.send_op == null);
        assert(self.recv_op == null);
        assert(self.socket == 0);

        try self.io.socketCreate(
            self.address.any.family,
            posix.SOCK.STREAM | posix.SOCK.CLOEXEC,
            0,
            self,
            socketCreated,
            socketCreateFailed,
            &self.connect_op,
        );
        self.state = .connecting;
    }

    fn socketCreated(self: *Self, socket: socket_t) Error!void {
        self.socket = socket;
        try self.io.connect(self.socket, &self.address, self, connected, connectFailed, &self.connect_op);
    }

    fn socketCreateFailed(self: *Self, err: anyerror) Error!void {
        log.err("{} socket create failed {}", .{ self.address, err });
        self.shutdown();
    }

    fn connectFailed(self: *Self, err: anyerror) Error!void {
        log.info("{} connect failed {}", .{ self.address, err });
        self.shutdown();
    }

    fn connected(self: *Self) Error!void {
        self.setup() catch |err| {
            log.warn("{} setup failed {}", .{ self.address, err });
            self.shutdown();
        };
    }

    fn setup(self: *Self) !void {
        try self.send(self.connector.identify);
        try self.io.recv(self.socket, self, received, receiveFailed, &self.recv_op);
        try self.connector.connect(self);
        self.state = .connected;
        log.debug("{} connected", .{self.address});
    }

    fn send(self: *Self, buf: []const u8) !void {
        assert(self.send_op == null);
        assert(buf.len > 0);
        try self.io.send(self.socket, buf, self, sent, sendFailed, &self.send_op);
    }

    fn sent(self: *Self) Error!void {
        try self.pullTopic();
    }

    fn pullTopic(self: *Self) !void {
        if (self.send_op != null or self.state != .connected) return;
        if (self.connector.topic.next(self)) |buf| {
            try self.send(buf);
        }
    }

    fn sendFailed(self: *Self, err: anyerror) Error!void {
        switch (err) {
            error.BrokenPipe, error.ConnectionResetByPeer => {},
            else => log.err("{} send failed {}", .{ self.address, err }),
        }
        self.shutdown();
    }

    fn received(self: *Self, bytes: []const u8) Error!void {
        if (self.state != .connected) return;
        self.handleResponse(bytes) catch |err| {
            log.err("{} handle reponse failed {}", .{ self.address, err });
            self.shutdown();
        };
    }

    fn handleResponse(self: *Self, bytes: []const u8) error{OutOfMemory}!void {
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

    fn receiveFailed(self: *Self, err: anyerror) Error!void {
        switch (err) {
            error.EndOfFile, error.ConnectionResetByPeer => {},
            else => log.err("{} recv failed {}", .{ self.address, err }),
        }
        self.shutdown();
    }

    fn shutdown(self: *Self) void {
        // log.debug("{} shutdown state: {s}", .{ self.address, @tagName(self.state) });
        switch (self.state) {
            .connecting, .connected => {
                self.cleanShutdown() catch |err| {
                    log.warn("{} clean shutdown failed {}", .{ self.address, err });
                    self.dirtyShutdown();
                };
                self.connector.disconnect(self);
                self.recv_buf.free();
                self.state = .closing;

                self.shutdown(); // check if all is already done
            },
            .closing => {
                if (self.connect_op != null or
                    self.recv_op != null or
                    self.send_op != null or
                    self.close_op != null)
                    return;
                self.state = .closed;
                log.debug("{} closed", .{self.address});
            },
            else => unreachable,
        }
    }

    fn cleanShutdown(self: *Self) !void {
        if (self.connect_op) |op| try op.cancel();
        if (self.socket != 0) {
            try self.io.shutdownClose(self.socket, self, shutdown, &self.close_op);
            self.socket = 0;
        }
    }

    fn dirtyShutdown(self: *Self) void {
        if (self.connect_op) |op| op.detach();
        if (self.recv_op) |op| op.detach();
        if (self.send_op) |op| op.detach();
        if (self.socket > 0) {
            self.io.close(self.socket) catch {};
            self.socket = 0;
        }
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
