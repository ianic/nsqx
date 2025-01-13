const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;
const net = std.net;
const posix = std.posix;
const socket_t = std.posix.socket_t;
const testing = std.testing;

const io = @import("iox");
const Options = @import("Options.zig");
const Stream = @import("store.zig").Stream;

const log = std.log.scoped(.lookup);

pub const Connector = struct {
    const Self = @This();

    allocator: mem.Allocator,
    io_loop: *io.Loop,
    connections: std.ArrayList(*Conn),
    identify: []const u8,
    ticker_op: io.Op = .{},
    stream: *Stream = undefined,
    state: State,
    const State = enum {
        connected,
        closing,
    };
    const ping_interval = 15 * 1000; // in milliseconds

    pub fn init(
        self: *Self,
        allocator: mem.Allocator,
        io_loop: *io.Loop,
        stream: *Stream,
        lookup_tcp_addresses: []net.Address,
        options: Options,
    ) !void {
        const identify = try identifyMessage(
            allocator,
            options.broadcastAddress(),
            options.hostname,
            options.broadcast_http_port,
            options.broadcast_tcp_port,
            "0.1.0",
        );

        self.* = .{
            .allocator = allocator,
            .io_loop = io_loop,
            .connections = std.ArrayList(*Conn).init(allocator),
            .identify = identify,
            .state = .connected,
            .stream = stream,
        };
        errdefer self.deinit();
        try self.connections.ensureUnusedCapacity(lookup_tcp_addresses.len);
        for (lookup_tcp_addresses) |addr| try self.addLookupd(addr);

        // Start endless ticker
        self.ticker_op = io.Op.ticker(ping_interval, self, onTick);
        self.io_loop.submit(&self.ticker_op);
    }

    fn addLookupd(self: *Self, address: std.net.Address) !void {
        const conn = try self.allocator.create(Conn);
        errdefer self.allocator.destroy(conn);
        try self.connections.ensureUnusedCapacity(1);
        conn.init(self, address);
        self.connections.appendAssumeCapacity(conn);
    }

    pub fn close(self: *Self) void {
        self.state = .closing;
        for (self.connections.items) |conn| conn.shutdown();
    }

    pub fn deinit(self: *Self) void {
        for (self.connections.items) |conn| {
            conn.deinit();
            self.allocator.destroy(conn);
        }
        self.connections.deinit();
        self.allocator.free(self.identify);
    }

    fn onTick(self: *Self) void {
        for (self.connections.items) |conn| {
            conn.onTick();
        }
    }

    pub fn onRegister(ptr: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        for (self.connections.items) |conn| conn.pull();
    }
};

const Conn = struct {
    const Self = @This();
    const ping_msg = "PING\n";

    connector: *Connector,
    tcp: io.tcp.Client(*Conn),
    // null if not subscribed jet
    sequence: ?u64 = null,

    fn init(self: *Self, connector: *Connector, address: std.net.Address) void {
        const allocator = connector.allocator;
        self.* = .{
            .connector = connector,
            .tcp = io.tcp.Client(*Conn).init(allocator, connector.io_loop, self, address),
        };
        self.tcp.connect();
    }

    fn deinit(self: *Self) void {
        self.tcp.deinit();
    }

    fn onTick(self: *Self) void {
        switch (self.tcp.conn.state) {
            .closed => self.tcp.connect(),
            .connected => self.ping(),
            else => {},
        }
    }

    fn ping(self: *Self) void {
        self.tcp.sendZc(ping_msg) catch {
            self.tcp.close();
        };
    }

    pub fn onConnect(self: *Self) io.Error!void {
        log.debug("{} connected", .{self.tcp.address});
        try self.tcp.sendZc(self.connector.identify);
        self.sequence = self.connector.stream.subscribe(.all);
        self.pull();
    }

    fn pull(self: *Self) void {
        const sequence = self.sequence orelse return;
        if (self.connector.stream.pull(sequence, 1024)) |res| {
            self.tcp.sendZc(res.data) catch return;
            self.sequence = res.sequence.to;
        }
    }

    pub fn onSend(_: *Self, _: []const u8) void {}

    pub fn onRecv(self: *Self, bytes: []const u8) io.Error!usize {
        return self.handleResponse(bytes) catch |err| {
            log.err("{} handle reponse failed {}", .{ self.tcp.address, err });
            self.tcp.close();
            return 0;
        };
    }

    fn handleResponse(self: *Self, bytes: []const u8) io.Error!usize {
        var pos: usize = 0;
        while (true) {
            var buf = bytes[pos..];
            if (buf.len < 4) break;
            const n = mem.readInt(u32, buf[0..4], .big);
            const msg_buf = buf[4..];
            if (msg_buf.len < n) break;
            const msg = msg_buf[0..n];
            pos += 4 + msg.len;

            if (msg.len == 2 and msg[0] == 'O' and msg[1] == 'K') {
                // OK most common case
                continue;
            }
            if (msg[0] == '{' and msg[msg.len - 1] == '}') {
                // identify response
                log.debug("{} identify: {s}", .{ self.tcp.address, msg });
                continue;
            }
            // error
            log.warn("{} {s}", .{ self.tcp.address, msg });
        }
        return pos;
    }

    pub fn onClose(self: *Self) void {
        if (self.sequence) |sequence|
            self.connector.stream.unsubscribe(sequence);
        self.sequence = null;
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

pub const RecvBuf = struct {
    allocator: mem.Allocator,
    buf: []u8 = &.{},

    const Self = @This();

    pub fn init(allocator: mem.Allocator) Self {
        return .{ .allocator = allocator };
    }

    pub fn free(self: *Self) void {
        self.allocator.free(self.buf);
        self.buf = &.{};
    }

    pub fn append(self: *Self, bytes: []const u8) ![]const u8 {
        if (self.buf.len == 0) return bytes;
        const old_len = self.buf.len;
        self.buf = try self.allocator.realloc(self.buf, old_len + bytes.len);
        @memcpy(self.buf[old_len..], bytes);
        return self.buf;
    }

    pub fn set(self: *Self, bytes: []const u8) !void {
        if (bytes.len == 0) return self.free();
        if (self.buf.len == bytes.len and self.buf.ptr == bytes.ptr) return;

        const new_buf = try self.allocator.dupe(u8, bytes);
        self.free();
        self.buf = new_buf;
    }
};
