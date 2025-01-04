const std = @import("std");
const assert = std.debug.assert;
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

pub const Parser = struct {
    buf: []const u8,
    pos: usize = 0,

    max_msg_size: u32 = 0,
    max_body_size: u32 = 0,

    // null - Not enough data in the buf.
    // pos  - Tail position after successful message parsing.
    //        First byte of the next message in buf or buf.len.
    pub fn next(p: *Parser) !?Message {
        while (true) {
            const msg = p.parse() catch |err| switch (err) {
                error.SplitBuffer => return null,
                error.Invalid => return error.Invalid,
                error.InvalidName => return error.InvalidName,
                error.InvalidNameCharacter => return error.InvalidNameCharacter,
                error.Overflow => return error.Overflow,
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
                return .{ .identify = try p.readBytes(try p.readInt(p.max_body_size)) };
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
                const data = try p.readBytes(try p.readInt(p.max_msg_size));
                return .{ .publish = .{ .topic = topic, .data = data } };
            },
            'M' => {
                // MPUB <topic_name>\n[ 4-byte body size ][ 4-byte num messages ]
                // [ 4-byte message #1 size ][ N-byte binary data ]
                try p.matchString("MPUB ");
                const topic = try validateName(try p.readString('\n'));
                const size = try p.readInt(p.max_body_size);
                if (size < 4) return error.Invalid;
                const msgs = try p.readInt(0);
                const data = try p.readBytes(size - 4);
                // check that individual messages has [size][data]
                const data_end_pos = p.pos;
                p.pos -= data.len;
                for (0..msgs) |_| {
                    const msg_size = p.readInt(p.max_msg_size) catch return error.Invalid;
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
                const size = try p.readInt(p.max_msg_size);
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
                        const id = try p.readMessageId(' ');
                        const delay = try p.readStringInt('\n');
                        return .{ .requeue = .{ .msg_id = id, .delay = delay } };
                    },
                    else => return error.Invalid,
                }
            },
            'F' => {
                // FIN <message_id>\n
                try p.matchString("FIN ");
                const id = try p.readMessageId('\n');
                return .{ .finish = id };
            },
            'T' => {
                // TOUCH <message_id>\n
                try p.matchString("TOUCH ");
                const id = try p.readMessageId('\n');
                return .{ .touch = id };
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
                const size = try p.readInt(p.max_body_size);
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

    fn readInt(p: *Parser, max_value: u32) !u32 {
        const buf = p.buf[p.pos..];
        if (buf.len < 4) return error.SplitBuffer;
        p.pos += 4;
        const value = mem.readInt(u32, buf[0..4], .big);
        if (max_value > 0 and value > max_value)
            return error.Overflow;
        return value;
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

const identify_msg = "IDENTIFY\n\x00\x00\x00\x0aHelloWorldIDENTIFY\n\x00\x00\x00\x03Foo";

test "identify" {
    var buf = identify_msg ++ "_----";
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

const sub_msg = "SUB pero zdero\nSUB jozo bozo\n";

test "sub" {
    const buf = sub_msg ++ "-_____";
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

const pub_msg = "PUB pero\n\x00\x00\x00\x05zderoPUB foo\n\x00\x00\x00\x03bar";

test "pub" {
    const buf = pub_msg ++ "-_____";
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
    { // message size overflow
        var p = Parser{ .buf = buf, .max_msg_size = 4 };
        try testing.expectError(error.Overflow, p.parse());
    }
}

const mpub_msg = "MPUB pero\n" ++
    "\x00\x00\x00\x14" ++ // body size
    "\x00\x00\x00\x02" ++ // number of message
    "\x00\x00\x00\x05zdero" ++ // message 1 [size][body]
    "\x00\x00\x00\x03bar"; // message 2 [size][body];

test "multi_publish" {
    const buf = mpub_msg;
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

const dpub_msg = "DPUB pero 1234\n" ++
    "\x00\x00\x00\x03bar"; // [size][body]

test "deferred_publish" {
    var p = Parser{ .buf = dpub_msg };
    const m = try p.parse();
    try testing.expectEqualStrings("pero", m.deferred_publish.topic);
    try testing.expectEqual(1234, m.deferred_publish.delay);
    try testing.expectEqual(3, m.deferred_publish.data.len);
    try testing.expectEqual(dpub_msg.len, p.pos);
}

const other_msgs = "RDY 123\nFIN 0123456789abcdef\nTOUCH 0123401234012345\nCLS\nNOP\nREQ 5678956789567890 4567\n";

test "ready,finish.." {
    var p = Parser{ .buf = other_msgs };
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

pub const max_name_len = 64;
const ephemeral_suffix = "#ephemeral";

// Valid topic and channel names are characters [.a-zA-Z0-9_-] and 1 <= length <= 64
pub fn validateName(name: []const u8) ![]const u8 {
    if (name.len < 1 or name.len > max_name_len) return error.InvalidName;
    const chars = if (std.mem.endsWith(u8, name, ephemeral_suffix)) name[0 .. name.len - ephemeral_suffix.len] else name;
    if (chars.len < 1) return error.InvalidName;

    for (chars) |c| {
        switch (c) {
            '0'...'9', 'A'...'Z', 'a'...'z' => {},
            '.', '_', '-' => {},
            else => return error.InvalidNameCharacter,
        }
    }
    return name;
}

pub fn isEphemeral(name: []const u8) bool {
    _ = validateName(name) catch return false;
    return std.mem.endsWith(u8, name, ephemeral_suffix);
}

test "validate name" {
    _ = try validateName("foo.bar");
    _ = try validateName("foo.bar#ephemeral");
    try testing.expectError(error.InvalidName, validateName("#ephemeral"));
    try testing.expectError(error.InvalidName, validateName("a" ** 65));
    try testing.expectError(error.InvalidNameCharacter, validateName("foo%bar"));
}

test isEphemeral {
    try testing.expect(!isEphemeral("#ephemeral"));
    try testing.expect(isEphemeral("foo#ephemeral"));
}

pub const FrameType = enum(u32) {
    response = 0,
    err = 1,
    message = 2,
};

// Message frame:
// |  (int32) ||  (int32) || (binary)
// |  4-byte  ||  4-byte  || N-byte
// ------------------------------------...
//     size     frame type     data
//
pub fn frame(comptime data: []const u8, comptime typ: FrameType) [data.len + 8]u8 {
    var hdr: [8]u8 = undefined;
    mem.writeInt(u32, hdr[0..4], @intCast(4 + data.len), .big);
    mem.writeInt(u32, hdr[4..8], @intFromEnum(typ), .big);
    return hdr ++ data[0..].*;
}

pub fn parseFrame(data: []const u8) !struct { u32, FrameType } {
    if (data.len < 8) return error.InvalidFrame;
    var hdr = data[0..8];
    const size = mem.readInt(u32, hdr[0..4], .big);
    const typ = mem.readInt(u32, hdr[4..8], .big);
    if (typ > 2) return error.InvalidFrame;
    return .{ size, @as(FrameType, @enumFromInt(typ)) };
}

test parseFrame {
    {
        const size, const frame_type = try parseFrame(&.{ 0, 0, 0, 1, 0, 0, 0, 0, 255 });
        try testing.expectEqual(1, size);
        try testing.expectEqual(FrameType.response, frame_type);
    }
    {
        const size, const frame_type = try parseFrame(&.{ 0, 0, 0, 3, 0, 0, 0, 2, 1, 2, 3 });
        try testing.expectEqual(3, size);
        try testing.expectEqual(FrameType.message, frame_type);
    }
    try testing.expectError(error.InvalidFrame, parseFrame(&.{ 0, 0, 0, 0, 0, 0, 0, 3 }));
    try testing.expectError(error.InvalidFrame, parseFrame(&.{ 0, 0, 0, 0, 0, 0, 0 }));
}

pub const Response = enum {
    ok,
    close,
    heartbeat,
    invalid,
    pub_failed,
    bad_message,
    fin_failed,
    touch_failed,
    requeue_failed,
    bad_topic,

    pub fn body(self: Response) []const u8 {
        return switch (self) {
            .ok => &ok_frame,
            .close => &close_frame,
            .heartbeat => &heartbeat_frame,
            .pub_failed => &pub_failed_frame,
            .bad_message => &bad_message_frame,
            .invalid => &invalid_frame,
            .fin_failed => &fin_failed_frame,
            .touch_failed => &touch_failed_frame,
            .requeue_failed => &req_failed_frame,
            .bad_topic => &bad_topic_frame,
        };
    }
};

const ok_frame = frame("OK", .response);
const close_frame = frame("CLOSE_WAIT", .response);
const heartbeat_frame = frame("_heartbeat_", .response);
const pub_failed_frame = frame("E_PUB_FAILED", .err);
const bad_message_frame = frame("E_BAD_MESSAGE", .err);
const invalid_frame = frame("E_INVALID", .err);
const fin_failed_frame = frame("E_FIN_FAILED", .err);
const touch_failed_frame = frame("E_TOUCH_FAILED", .err);
const req_failed_frame = frame("E_REQ_FAILED", .err);
const bad_topic_frame = frame("E_BAD_TOPIC", .err);
// more possible error responses:
// E_BAD_CHANNEL
// E_BAD_BODY
// E_MPUB_FAILED
// E_DPUB_FAILED

test "response body is comptime" {
    const r1: Response = .heartbeat;
    const r2: Response = .heartbeat;
    try testing.expect(r1.body().ptr == r2.body().ptr);
    try testing.expectEqualStrings(
        &[_]u8{ 0, 0, 0, 15, 0, 0, 0, 0, 95, 104, 101, 97, 114, 116, 98, 101, 97, 116, 95 },
        r1.body(),
    );
}

pub const msg_id = struct {
    pub fn decode(id: [16]u8) u64 {
        return mem.readInt(u64, id[8..], .big);
    }

    pub fn encode(sequence: u64) [16]u8 {
        var buf: [16]u8 = undefined;
        write(&buf, sequence);
        return buf;
    }

    pub fn write(buf: *[16]u8, sequence: u64) void {
        mem.writeInt(u64, buf[0..8], 0, .big); // 8 bytes, unused
        mem.writeInt(u64, buf[8..16], sequence, .big); // 8 bytes, sequence
    }
};

pub const header_len = 34;
pub fn writeHeader(
    buf: []u8,
    body_size: u32,
    timestamp: u64,
    sequence: u64,
) void {
    assert(buf.len >= header_len);
    mem.writeInt(u32, buf[0..4], body_size + 30, .big); // size (without 4 bytes size field)
    mem.writeInt(u32, buf[4..8], @intFromEnum(FrameType.message), .big); // frame type
    mem.writeInt(u64, buf[8..16], timestamp, .big); // timestamp
    mem.writeInt(u16, buf[16..18], 1, .big); // attempts
    msg_id.write(buf[18..34], sequence); // sequence
}

test "fuzz parser" {
    const wrap = struct {
        fn testOne(input: []const u8) anyerror!void {
            var p = Parser{ .buf = input };
            while (true) {
                _ = p.parse() catch return;
            }
        }
    };
    try std.testing.fuzz(wrap.testOne, .{
        .corpus = &.{
            identify_msg,
            sub_msg,
            pub_msg,
            mpub_msg,
            dpub_msg,
            other_msgs,
        },
    });
}

pub const Msg = struct {
    size: u32,
    timestamp: u64,
    attempts: u16,
    sequence: u64,
};

// Parses first message from data !!!
pub fn parseMessageFrame(data: []const u8) !Msg {
    if (data.len < header_len) return error.InvalidFrame;
    const size, const frame_type = try parseFrame(data);
    if (frame_type != .message) return error.InvalidFrame;
    if (data.len < size + 4) return error.InvalidFrame;

    const hdr = data[0..header_len];
    return .{
        .size = size + 4,
        .timestamp = mem.readInt(u64, hdr[8..16], .big),
        .attempts = mem.readInt(u16, hdr[16..18], .big),
        .sequence = msg_id.decode(hdr[18..34].*),
    };
}
