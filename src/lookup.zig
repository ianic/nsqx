const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const posix = std.posix;
const socket_t = std.posix.socket_t;
const testing = std.testing;

const Io = @import("io.zig").Io;
const Op = @import("io.zig").Op;
const Error = @import("io.zig").Error;
const Topic = @import("simple_topic.zig").SimpleTopic([]const u8, Conn, Conn.pullTopic);
const RecvBuf = @import("tcp.zig").RecvBuf;

const log = std.log.scoped(.lookup);

pub const Connector = struct {
    const Self = @This();
    const Registration = union(enum) {
        topic: []const u8,
        channel: struct {
            topic_name: []const u8,
            name: []const u8,
        },

        fn print(self: Registration, allocator: mem.Allocator) ![]const u8 {
            return switch (self) {
                .topic => |t| try std.fmt.allocPrint(allocator, "REGISTER {s}\n", .{t}),
                .channel => |c| try std.fmt.allocPrint(allocator, "REGISTER {s} {s}\n", .{ c.topic_name, c.name }),
            };
        }
    };

    allocator: mem.Allocator,
    io: *Io,
    registrations: std.ArrayList(Registration),
    connections: std.ArrayList(*Conn),
    identify: []const u8,
    topic: Topic,

    pub fn init(allocator: mem.Allocator, io: *Io) Self {
        const identify = identifyMessage(allocator, "127.0.0.1", "hydra", 4151, 4150, "0.1.0") catch "";
        return .{
            .allocator = allocator,
            .io = io,
            .registrations = std.ArrayList(Registration).init(allocator),
            .connections = std.ArrayList(*Conn).init(allocator),
            .topic = Topic.init(allocator),
            .identify = identify,
        };
    }

    pub fn close(self: *Self) !void {
        for (self.connections.items) |conn| try conn.close();
    }

    pub fn deinit(self: *Self) void {
        self.registrations.deinit();
        for (self.connections.items) |conn| conn.deinit();
        self.connections.deinit();
        self.allocator.free(self.identify);
        self.topic.deinit();
    }

    pub fn topicCreated(self: *Self, name: []const u8) !void {
        try self.append(.{ .topic = name });
    }

    pub fn channelCreated(self: *Self, topic_name: []const u8, name: []const u8) !void {
        try self.append(.{ .channel = .{ .topic_name = topic_name, .name = name } });
    }

    fn append(self: *Self, reg: Registration) !void {
        try self.registrations.append(reg);
        if (self.topic.consumers.count() > 0) {
            try self.topic.append(try reg.print(self.allocator));
        }
    }

    pub fn topicDeleted(self: *Self, name: []const u8) !void {
        for (self.registrations.items, 0..) |reg, i| {
            if (reg == .topic) {
                if (reg.topic.ptr == name.ptr) {
                    _ = self.registrations.swapRemove(i);
                    return;
                }
            }
        }
    }

    pub fn channelDeleted(self: *Self, topic_name: []const u8, name: []const u8) !void {
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

    pub fn addLookupd(self: *Self, address: std.net.Address) !void {
        const conn = try self.allocator.create(Conn);
        conn.* = Conn.init(self, address);
        try self.connections.append(conn);
        try conn.connect();
    }

    fn connected(self: *Self, conn: *Conn) !void {
        const first = try self.state();
        if (first.len > 0) try self.topic.setFirst(first);
        try self.topic.subscribe(conn);
    }

    fn disconnected(self: *Self, conn: *Conn) void {
        self.topic.unsubscribe(conn);
    }

    // Current state for newly connected lookupd
    fn state(self: *Self) ![]const u8 {
        if (self.registrations.items.len == 0) return "";

        var arr = std.ArrayList(u8).init(self.allocator);
        for (self.registrations.items) |reg| {
            const data = try reg.print(self.allocator);
            defer self.allocator.free(data);
            try arr.appendSlice(data);
        }
        return try arr.toOwnedSlice();
    }
};

const Conn = struct {
    allocator: mem.Allocator,
    connector: *Connector,
    recv_buf: RecvBuf,
    err: ?anyerror = null,
    socket: socket_t = 0,
    address: std.net.Address,
    io: *Io,
    send_op: ?*Op = null,
    recv_op: ?*Op = null,
    state: State = .init,
    ticker_op: ?*Op = null,

    const ping_msg = "PING\n";
    const ping_interval = 15 * 1000; // in milliseconds

    const State = enum {
        init,
        connecting,
        connected,
        disconnected,
        closing,
        closed,
    };

    const Self = @This();

    pub fn init(connector: *Connector, address: std.net.Address) Self {
        const allocator = connector.allocator;
        return .{
            .allocator = allocator,
            .connector = connector,
            .io = connector.io,
            .address = address,
            .recv_buf = RecvBuf.init(allocator),
        };
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

    pub fn connect(self: *Self) !void {
        self.ticker_op = try self.io.ticker(ping_interval, self, tick, tickerFailed);
        try self.reconnect();
    }

    fn reconnect(self: *Self) Error!void {
        self.state = .connecting;
        _ = try self.io.socketCreate(self.address.any.family, posix.SOCK.STREAM | posix.SOCK.CLOEXEC, 0, self, socketCreated, socketFailed);
    }

    fn tick(self: *Self) Error!void {
        switch (self.state) {
            .disconnected => {
                try self.io.close(self.socket);
                self.socket = 0;
                try self.reconnect();
            },
            .connected => try self.ping(),
            else => {},
        }
    }

    fn tickerFailed(self: *Self, err: anyerror) Error!void {
        self.ticker_op = null;
        switch (err) {
            error.Canceled => {},
            else => log.err("ticker failed {}", .{err}),
        }
    }

    fn socketCreated(self: *Self, socket: socket_t) Error!void {
        self.socket = socket;
        self.state = .connecting;
        _ = try self.io.connect(self.socket, self.address, self, connected, connectFailed);
    }

    fn socketFailed(self: *Self, err: anyerror) Error!void {
        self.disconnected();
        log.err("socket create failed {}", .{err});
    }

    fn connectFailed(self: *Self, err: anyerror) Error!void {
        self.disconnected();
        log.info("{} connect failed {}", .{ self.address, err });
    }

    fn connected(self: *Self) Error!void {
        self.state = .connected;
        try self.send(self.connector.identify);
        try self.recv();
        try self.connector.connected(self);
        log.debug("{} connected", .{self.address});
    }

    fn disconnected(self: *Self) void {
        self.state = .disconnected;
        self.connector.disconnected(self);
        log.info("{} disconnected", .{self.address});
    }

    fn send(self: *Self, buf: []const u8) !void {
        assert(self.send_op == null);
        self.send_op = try self.io.send(self.socket, buf, self, sent, sendFailed);
    }

    fn ping(self: *Self) Error!void {
        if (self.send_op == null) try self.send(ping_msg);
    }

    fn sent(self: *Self, _: usize) Error!void {
        self.send_op = null;
        try self.pullTopic();
    }

    fn sendFailed(self: *Self, err: anyerror) Error!void {
        self.send_op = null;
        switch (err) {
            error.BrokenPipe, error.ConnectionResetByPeer => {},
            else => log.err("{} send failed {}", .{ self.address, err }),
        }
        if (self.state == .connected) self.disconnected();
    }

    fn recv(self: *Self) !void {
        self.recv_op = try self.io.recv(self.socket, self, received, recvFailed);
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
                log.debug("{} identify: {s}", .{ self.socket, msg });
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
        self.recv_op = null;
        switch (err) {
            error.EndOfFile => {},
            error.ConnectionResetByPeer => {},
            else => log.err("{} recv failed {}", .{ self.address, err }),
        }
        if (self.state == .connected) self.disconnected();
    }

    pub fn close(self: *Self) !void {
        self.state = .closing;
        if (self.send_op) |op| {
            try op.cancel();
            return;
        }
        if (self.recv_op) |op| {
            try op.cancel();
            op.unsubscribe(self);
        }
        if (self.ticker_op) |op| {
            try op.cancel();
            op.unsubscribe(self);
        }
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
