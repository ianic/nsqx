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
    clients: std.ArrayList(*Client),
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
            .clients = std.ArrayList(*Client).init(allocator),
            .identify = identify,
            .state = .connected,
            .stream = stream,
        };
        errdefer self.deinit();
        try self.clients.ensureUnusedCapacity(lookup_tcp_addresses.len);
        for (lookup_tcp_addresses) |addr| try self.addLookupd(addr);

        // Start endless ticker
        self.ticker_op = io.Op.ticker(ping_interval, self, onTick);
        self.io_loop.submit(&self.ticker_op);
    }

    fn addLookupd(self: *Self, address: std.net.Address) !void {
        const client = try self.allocator.create(Client);
        errdefer self.allocator.destroy(client);
        try self.clients.ensureUnusedCapacity(1);
        client.connect(self, address);
        self.clients.appendAssumeCapacity(client);
    }

    pub fn close(self: *Self) void {
        self.state = .closing;
        for (self.clients.items) |conn| conn.shutdown();
    }

    pub fn deinit(self: *Self) void {
        for (self.clients.items) |client| {
            client.deinit();
            self.allocator.destroy(client);
        }
        self.clients.deinit();
        self.allocator.free(self.identify);
    }

    fn onTick(self: *Self) void {
        for (self.clients.items) |client| {
            client.onTick();
        }
    }

    pub fn onRegister(ptr: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        for (self.clients.items) |client| client.pull();
    }
};

const Client = struct {
    const Self = @This();

    tcp: io.tcp.Connector(Client),
    parent: *Connector,
    conn: ?Conn = null,

    fn connect(
        self: *Self,
        parent: *Connector,
        addr: net.Address,
    ) void {
        self.* = .{
            .tcp = io.tcp.Connector(Client).init(parent.allocator, parent.io_loop, self, addr),
            .parent = parent,
            .conn = null,
        };
        self.tcp.connect();
    }

    fn deinit(self: *Self) void {
        if (self.conn) |*conn| conn.deinit();
    }

    pub fn create(self: *Self) !struct { *Conn, *Conn.Tcp } {
        self.conn = .{
            .parent = self,
            .tcp = undefined,
            .stream = self.parent.stream,
            .identify = self.parent.identify,
            .sequence = null,
        };
        const conn = &self.conn.?;
        return .{ conn, &conn.tcp };
    }

    fn onTick(self: *Self) void {
        if (self.conn) |*conn| {
            conn.ping();
        } else {
            // TODO move this check to io.tcp
            if (!self.tcp.connect_op.active() and !self.tcp.close_op.active()) {
                self.tcp.connect();
            }
        }
    }

    fn pull(self: *Self) void {
        if (self.conn) |*conn| conn.pull();
    }

    pub fn onError(self: *Self, err: anyerror) void {
        log.err("client addr: {} {}", .{ self.tcp.address, err });
    }

    pub fn onClose(_: *Self) void {}
};

const Conn = struct {
    const Self = @This();
    pub const Tcp = io.tcp.Conn(Conn);
    const ping_msg = "PING\n";

    parent: *Client,
    tcp: Tcp,
    // null if not subscribed jet
    sequence: ?u64 = null,
    identify: []const u8,
    stream: *Stream,

    fn deinit(self: *Self) void {
        self.tcp.deinit();
    }

    fn ping(self: *Self) void {
        self.tcp.sendZc(ping_msg) catch {
            self.tcp.close();
        };
    }

    pub fn onConnect(self: *Self) void {
        self.onConnect_() catch |err| {
            log.err("on connect {}", .{err});
            self.tcp.close();
        };
    }

    pub fn onError(_: *Self, err: anyerror) void {
        log.err("conn {}", .{err});
    }

    fn onConnect_(self: *Self) !void {
        log.debug("{} connected", .{self.tcp.socket});
        try self.tcp.sendZc(self.identify);
        self.sequence = self.stream.subscribe(.all);
        self.pull();
    }

    fn pull(self: *Self) void {
        const sequence = self.sequence orelse return;
        if (self.stream.pull(sequence, 1024)) |res| {
            self.tcp.sendZc(res.data) catch return;
            self.sequence = res.sequence.to;
        }
    }

    pub fn onSend(_: *Self, _: []const u8) void {}

    pub fn onRecv(self: *Self, bytes: []const u8) usize {
        return self.handleResponse(bytes) catch |err| {
            log.err("{} handle response failed {}", .{ self.tcp.socket, err });
            self.tcp.close();
            return bytes.len;
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
                log.debug("{} identify: {s}", .{ self.tcp.socket, msg });
                continue;
            }
            // error
            log.warn("{} {s}", .{ self.tcp.socket, msg });
        }
        return pos;
    }

    pub fn onClose(self: *Self) void {
        if (self.sequence) |sequence|
            self.stream.unsubscribe(sequence);
        self.sequence = null;
        self.deinit();
        self.parent.conn = null;
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
