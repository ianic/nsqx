const std = @import("std");
const mem = std.mem;
const fmt = std.fmt;
const net = std.net;
const Options = @import("Options.zig");

pub const Message = union(MessageTag) {
    version: void,
    identify: []const u8,
    subscribe: struct {
        topic: []const u8,
        channel: []const u8,
    },
    publish: struct {
        topic: []const u8,
        data: []const u8,
    },
    multi_publish: struct {
        topic: []const u8,
        msgs: u32, // number of messages in data
        data: []const u8, // [ 4-byte message #1 size ][ N-byte binary data ]...
    },
    deferred_publish: struct {
        topic: []const u8,
        data: []const u8,
        delay: u32, //  defer publish for delay milliseconds
    },
    ready: u32,
    finish: [16]u8,
    requeue: struct {
        msg_id: [16]u8,
        delay: u32, //  defer requeue for delay milliseconds
    },
    touch: [16]u8,
    close: void,
    nop: void,
    auth: []const u8,

    pub fn isPublish(self: @This()) bool {
        return switch (self) {
            .publish, .multi_publish, .deferred_publish => true,
            else => false,
        };
    }

    pub fn parseIdentify(self: @This(), allocator: std.mem.Allocator, opt: Options) !Identify {
        return try Identify.parse(self.identify, allocator, opt);
    }
};

const MessageTag = enum {
    version,
    identify,
    subscribe,
    publish, // pub is keyword, so calling it single publish
    multi_publish,
    deferred_publish,
    ready,
    finish,
    requeue,
    touch,
    close,
    nop,
    auth,
};

pub const FrameType = enum(u32) {
    response = 0,
    err = 1,
    message = 2,
};

pub const Parser = struct {
    buf: []const u8,
    pos: usize = 0,

    // null - Not enough data in the buf.
    // pos  - Tail position after successful message parsing.
    //        First byte of the next message in buf or buf.len.
    pub fn next(p: *Parser) !?Message {
        while (true) {
            const msg = p.parse() catch |err| switch (err) {
                error.SplitBuffer => return null,
                else => return err,
            };
            if (msg != .version) return msg;
        }
    }

    pub fn unparsed(p: *Parser) []const u8 {
        return p.buf[p.pos..];
    }

    fn parse(p: *Parser) !Message {
        if (p.buf[p.pos..].len < 4) return error.SplitBuffer;

        const start_pos = p.pos;
        errdefer p.pos = start_pos;
        switch (p.buf[p.pos]) {
            ' ' => {
                try p.matchString("  V2");
                return .{ .version = {} };
            },
            'I' => {
                // IDENTIFY\n[ 4-byte size in bytes ][ N-byte JSON data ]
                try p.matchString("IDENTIFY\n");
                return .{ .identify = try p.readBytes(try p.readInt()) };
            },
            'S' => {
                // SUB <topic_name> <channel_name>\n
                try p.matchString("SUB ");
                const topic = try validateName(try p.readString(' '));
                const channel = try validateName(try p.readString('\n'));
                return .{ .subscribe = .{ .topic = topic, .channel = channel } };
            },
            'P' => {
                // PUB <topic_name>\n[ 4-byte size in bytes ][ N-byte binary data ]
                try p.matchString("PUB ");
                const topic = try validateName(try p.readString('\n'));
                const data = try p.readBytes(try p.readInt());
                return .{ .publish = .{ .topic = topic, .data = data } };
            },
            'M' => {
                // MPUB <topic_name>\n[ 4-byte body size ][ 4-byte num messages ]
                // [ 4-byte message #1 size ][ N-byte binary data ]
                try p.matchString("MPUB ");
                const topic = try validateName(try p.readString('\n'));
                const size = try p.readInt();
                if (size < 4) return error.Invalid;
                const msgs = try p.readInt();
                const data = try p.readBytes(size - 4);
                // check that individual messages has [size][data]
                const data_end_pos = p.pos;
                p.pos -= data.len;
                for (0..msgs) |_| {
                    const msg_size = p.readInt() catch return error.Invalid;
                    _ = p.readBytes(msg_size) catch return error.Invalid;
                }
                if (p.pos != data_end_pos) return error.Invalid;

                return .{ .multi_publish = .{ .topic = topic, .msgs = msgs, .data = data } };
            },
            'D' => {
                // DPUB <topic_name> <defer_time>\n
                // [ 4-byte size in bytes ][ N-byte binary data ]
                try p.matchString("DPUB ");
                const topic = try validateName(try p.readString(' '));
                const delay = try p.readStringInt('\n');
                const size = try p.readInt();
                const data = try p.readBytes(size);
                return .{ .deferred_publish = .{ .topic = topic, .delay = delay, .data = data } };
            },
            'R' => {
                switch (p.buf[p.pos + 1]) {
                    'D' => {
                        // RDY <count>\n
                        try p.matchString("RDY ");
                        const count = try p.readStringInt('\n');
                        return .{ .ready = count };
                    },
                    'E' => {
                        // REQ <message_id> <timeout>\n
                        try p.matchString("REQ ");
                        const msg_id = try p.readMessageId(' ');
                        const delay = try p.readStringInt('\n');
                        return .{ .requeue = .{ .msg_id = msg_id, .delay = delay } };
                    },
                    else => return error.Invalid,
                }
            },
            'F' => {
                // FIN <message_id>\n
                try p.matchString("FIN ");
                const msg_id = try p.readMessageId('\n');
                return .{ .finish = msg_id };
            },
            'T' => {
                // TOUCH <message_id>\n
                try p.matchString("TOUCH ");
                const msg_id = try p.readMessageId('\n');
                return .{ .touch = msg_id };
            },
            'C' => { // CLS\n
                try p.matchString("CLS\n");
                return .{ .close = {} };
            },
            'N' => { // NOP\n
                try p.matchString("NOP\n");
                return .{ .nop = {} };
            },
            'A' => { // AUTH\n[ 4-byte size in bytes ][ N-byte Auth Secret ]
                try p.matchString("AUTH\n");
                const size = try p.readInt();
                const data = try p.readBytes(size);
                return .{ .auth = data };
            },
            else => return error.Invalid,
        }
    }

    fn matchString(p: *Parser, str: []const u8) !void {
        const buf = p.buf[p.pos..];
        if (buf.len < str.len) return error.SplitBuffer;
        if (!mem.eql(u8, buf[0..str.len], str)) return error.Invalid;
        p.pos += str.len;
    }

    fn readMessageId(p: *Parser, delim: u8) ![16]u8 {
        const buf = p.buf[p.pos..];
        if (buf.len < 17) return error.SplitBuffer;
        if (buf[16] != delim) return error.Invalid;
        p.pos += 17;
        return buf[0..16].*;
    }

    fn readStringInt(p: *Parser, delim: u8) !u32 {
        return fmt.parseInt(u32, try p.readString(delim), 10) catch return error.Invalid;
    }

    fn readInt(p: *Parser) !u32 {
        const buf = p.buf[p.pos..];
        if (buf.len < 4) return error.SplitBuffer;
        p.pos += 4;
        return mem.readInt(u32, buf[0..4], .big);
    }

    fn readString(p: *Parser, delim: u8) ![]const u8 {
        const buf = p.buf[p.pos..];
        const len = mem.indexOfScalar(u8, buf, delim) orelse return error.SplitBuffer;
        p.pos += len + 1;
        return buf[0..len];
    }

    fn readBytes(p: *Parser, size: u32) ![]const u8 {
        const buf = p.buf[p.pos..];
        if (buf.len < size) return error.SplitBuffer;
        p.pos += size;
        return buf[0..size];
    }
};

const testing = std.testing;

test "identify" {
    var buf = "IDENTIFY\n\x00\x00\x00\x0aHelloWorldIDENTIFY\n\x00\x00\x00\x03Foo_----";
    {
        var p = Parser{ .buf = buf };
        var m = try p.parse();
        try testing.expectEqualStrings(m.identify, "HelloWorld");
        try testing.expectEqual(23, p.pos);
        m = try p.parse();
        try testing.expectEqualStrings(m.identify, "Foo");
        try testing.expectEqual(39, p.pos);
        try testing.expectEqual('_', p.buf[p.pos]);
    }
    { // split buffer
        var p = Parser{ .buf = buf[0..22] };
        try testing.expectError(error.SplitBuffer, p.parse());
        try testing.expectEqual(0, p.pos);
    }
    { // invalid command
        var buf2 = try testing.allocator.dupe(u8, buf);
        defer testing.allocator.free(buf2);
        buf2[6] = 'T'; // IDNETIFY => IDENTITY
        var p = Parser{ .buf = buf2 };
        try testing.expectError(error.Invalid, p.parse());
        try testing.expectEqual(0, p.pos);
    }
}

test "sub" {
    const buf = "SUB pero zdero\nSUB jozo bozo\n-_____";
    {
        var p = Parser{ .buf = buf };
        var m = try p.parse();
        try testing.expectEqualStrings(m.subscribe.topic, "pero");
        try testing.expectEqualStrings(m.subscribe.channel, "zdero");
        try testing.expectEqual(15, p.pos);
        try testing.expectEqual('S', buf[15]);
        m = try p.parse();
        try testing.expectEqualStrings(m.subscribe.topic, "jozo");
        try testing.expectEqualStrings(m.subscribe.channel, "bozo");
        try testing.expectEqual(29, p.pos);
        try testing.expectEqual('-', buf[29]);
    }
    { // split buffer
        var p = Parser{ .buf = buf[0..6] };
        try testing.expectError(error.SplitBuffer, p.parse());
        try testing.expectEqual(0, p.pos);

        p = Parser{ .buf = buf[0..14] };
        try testing.expectError(error.SplitBuffer, p.parse());
        try testing.expectEqual(0, p.pos);
    }
}

test "pub" {
    const buf = "PUB pero\n\x00\x00\x00\x05zderoPUB foo\n\x00\x00\x00\x03bar-_____";
    {
        var p = Parser{ .buf = buf };
        var m = try p.parse();
        try testing.expectEqualStrings("pero", m.publish.topic);
        try testing.expectEqualStrings("zdero", m.publish.data);
        try testing.expectEqual(18, p.pos);
        try testing.expectEqual('P', buf[18]);
        m = try p.parse();
        try testing.expectEqualStrings(m.publish.topic, "foo");
        try testing.expectEqualStrings(m.publish.data, "bar");
        try testing.expectEqual(33, p.pos);
        try testing.expectEqual('-', buf[33]);
    }
    { // split buffer
        var p = Parser{ .buf = buf[0..6] };
        try testing.expectError(error.SplitBuffer, p.parse());
        try testing.expectEqual(0, p.pos);

        p = Parser{ .buf = buf[0..12] };
        try testing.expectError(error.SplitBuffer, p.parse());
        try testing.expectEqual(0, p.pos);
    }
}

test "multi_publish" {
    const buf = "MPUB pero\n" ++
        "\x00\x00\x00\x14" ++ // body size
        "\x00\x00\x00\x02" ++ // number of message
        "\x00\x00\x00\x05zdero" ++ // message 1 [size][body]
        "\x00\x00\x00\x03bar"; // message 2 [size][body]
    {
        var p = Parser{ .buf = buf };
        const m = try p.parse();
        try testing.expectEqualStrings("pero", m.multi_publish.topic);
        try testing.expectEqual(2, m.multi_publish.msgs);
        try testing.expectEqual(16, m.multi_publish.data.len);
        try testing.expectEqual(buf.len, p.pos);
    }
    { // split buffer
        var p = Parser{ .buf = buf[0..6] };
        try testing.expectError(error.SplitBuffer, p.parse());
        try testing.expectEqual(0, p.pos);

        p = Parser{ .buf = buf[0..14] };
        try testing.expectError(error.SplitBuffer, p.parse());
        try testing.expectEqual(0, p.pos);
    }
    { // invalid number of messages
        const buf2 = try testing.allocator.dupe(u8, buf);
        defer testing.allocator.free(buf2);
        var p = Parser{ .buf = buf2 };
        try testing.expectEqual(2, buf[17]);
        buf2[17] = 3; // change number of messages
        try testing.expectError(error.Invalid, p.parse());
    }
    { // invalid message 1
        const buf2 = try testing.allocator.dupe(u8, buf);
        defer testing.allocator.free(buf2);
        var p = Parser{ .buf = buf2 };
        try testing.expectEqual(5, buf[21]);
        buf2[21] = 4; // change number of bytes in message 1
        try testing.expectError(error.Invalid, p.parse());
    }
}

test "deferred_publish" {
    const buf = "DPUB pero 1234\n" ++
        "\x00\x00\x00\x03bar"; // [size][body]
    {
        var p = Parser{ .buf = buf };
        const m = try p.parse();
        try testing.expectEqualStrings("pero", m.deferred_publish.topic);
        try testing.expectEqual(1234, m.deferred_publish.delay);
        try testing.expectEqual(3, m.deferred_publish.data.len);
        try testing.expectEqual(buf.len, p.pos);
    }
}

test "ready,finish.." {
    const buf = "RDY 123\nFIN 0123456789abcdef\nTOUCH 0123401234012345\nCLS\nNOP\nREQ 5678956789567890 4567\n";
    var p = Parser{ .buf = buf };
    var m = try p.parse();
    try testing.expectEqual(123, m.ready);
    m = try p.parse();
    try testing.expectEqualStrings("0123456789abcdef", &m.finish);
    m = try p.parse();
    try testing.expectEqualStrings("0123401234012345", &m.touch);
    m = try p.parse();
    try testing.expect(m == .close);
    m = try p.parse();
    try testing.expect(m == .nop);
    m = try p.parse();
    try testing.expectEqualStrings("5678956789567890", &m.requeue.msg_id);
    try testing.expectEqual(4567, m.requeue.delay);
}

pub const Identify = struct {
    client_id: []const u8 = &.{},
    hostname: []const u8 = &.{},
    user_agent: []const u8 = &.{},
    heartbeat_interval: u32 = 0, // in milliseconds
    msg_timeout: u32 = 0, // in milliseconds

    pub fn parse(data: []const u8, allocator: std.mem.Allocator, opt: Options) !Identify {
        const parsed = try std.json.parseFromSlice(
            Identify,
            allocator,
            data,
            .{ .ignore_unknown_fields = true },
        );
        defer parsed.deinit();
        const v = parsed.value;

        const client_id = try allocator.dupe(u8, v.client_id);
        errdefer allocator.free(client_id);
        const hostname = try allocator.dupe(u8, v.hostname);
        errdefer allocator.free(hostname);
        const user_agent = try allocator.dupe(u8, v.user_agent);
        errdefer allocator.free(user_agent);

        return .{
            .client_id = client_id,
            .hostname = hostname,
            .user_agent = user_agent,
            .heartbeat_interval = if (v.heartbeat_interval == 0 or v.heartbeat_interval > opt.max_heartbeat_interval)
                opt.max_heartbeat_interval
            else
                v.heartbeat_interval,
            .msg_timeout = if (v.msg_timeout == 0)
                opt.msg_timeout
            else if (v.msg_timeout > opt.max_msg_timeout)
                opt.max_msg_timeout
            else
                v.msg_timeout,
        };
    }

    pub fn deinit(self: Identify, allocator: std.mem.Allocator) void {
        allocator.free(self.client_id);
        allocator.free(self.hostname);
        allocator.free(self.user_agent);
    }

    pub fn format(
        self: Identify,
        comptime _: []const u8,
        _: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        try writer.print(
            "client_id: {s}, hostname: {s}, heartbeat_interval: {}ms, msg_timeout: {}ms",
            .{ self.client_id, self.hostname, self.heartbeat_interval, self.msg_timeout },
        );
    }
};

test "identify parse json" {
    {
        const opt = Options{
            .hostname = "host",
        };
        const data =
            \\ {"client_id":"io","deflate":false,"deflate_level":6,"feature_negotiation":true,"heartbeat_interval":34567,"hostname":"io.local","long_id":"io","msg_timeout":12345,"output_buffer_size":16384,"output_buffer_timeout":250,"sample_rate":0,"short_id":"io","snappy":false,"tls_v1":false,"user_agent":"go-nsq/1.1.0"}
        ;
        var idf = try Identify.parse(data, testing.allocator, opt);
        defer idf.deinit(testing.allocator);

        try testing.expectEqualStrings("io", idf.client_id);
        try testing.expectEqualStrings("io.local", idf.hostname);
        try testing.expectEqual(34567, idf.heartbeat_interval);
        try testing.expectEqual(12345, idf.msg_timeout);
    }
    {
        const opt = Options{
            .hostname = "host",
            .msg_timeout = 111,
            .max_heartbeat_interval = 222,
        };
        const data =
            \\ {"client_id":"client_id","heartbeat_interval":0}
        ;
        var idf = try Identify.parse(data, testing.allocator, opt);
        defer idf.deinit(testing.allocator);

        try testing.expectEqualStrings("client_id", idf.client_id);
        try testing.expectEqual(opt.max_heartbeat_interval, idf.heartbeat_interval);
        try testing.expectEqual(opt.msg_timeout, idf.msg_timeout);
    }
}

// Valid topic and channel names are characters [.a-zA-Z0-9_-] and 1 <= length <= 64
pub fn validateName(name: []const u8) ![]const u8 {
    const ephemeral_suffix = "#ephemeral";
    const chars = if (std.mem.endsWith(u8, name, ephemeral_suffix)) name[0 .. name.len - ephemeral_suffix.len] else name;
    if (chars.len < 1 or chars.len > 64) return error.InvalidName;

    for (chars) |c| {
        switch (c) {
            '0'...'9', 'A'...'Z', 'a'...'z' => {},
            '.', '_', '-' => {},
            else => return error.InvalidNameCharacter,
        }
    }
    return name;
}

test "validate name" {
    _ = try validateName("foo.bar");
    _ = try validateName("foo.bar#ephemeral");
    try testing.expectError(error.InvalidName, validateName("#ephemeral"));
    try testing.expectError(error.InvalidName, validateName("a" ** 65));
    try testing.expectError(error.InvalidNameCharacter, validateName("foo%bar"));
}
