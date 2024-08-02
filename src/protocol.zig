const std = @import("std");
const mem = std.mem;

const Message = union(MessageTag) {
    identify: []const u8,
    sub: struct {
        topic: []const u8,
        channel: []const u8,
    },
    spub: struct {
        topic: []const u8,
        data: []const u8,
    },
    mpub: struct {
        topic: []const u8,
        msgs: u32, // number of messages in data
        data: []const u8, // [ 4-byte message #1 size ][ N-byte binary data ]...
    },
    dpub: struct {
        topic: []const u8,
        data: []const u8,
        defer_time: u32,
    },
    rdy: u32,
    fin: [16]u8,
    req: [16]u8,
    touch: [16]u8,
    cls: void,
    nop: void,
    auth: []const u8,
};

const MessageTag = enum {
    identify,
    sub,
    spub, // pub is keyword, so calling it single publish
    mpub,
    dpub,
    rdy,
    fin,
    req,
    touch,
    cls,
    nop,
    auth,
};

const Error = error{
    Invalid,
};

const Parser = struct {
    buf: []const u8,
    pos: usize = 0,

    pub fn next(p: *Parser) !?Message {
        const msg = p.parse() catch |err| switch (err) {
            error.BufferOverflow => return null,
            else => return err,
        };
        return msg;
    }

    // null - Not enough data in the buf.
    // pos  - Tail position after successful message parsing.
    //        First byte of the next message in buf or buf.len.
    fn parse(p: *Parser) !Message {
        const start_pos = p.pos;
        errdefer p.pos = start_pos;

        switch (p.buf[p.pos]) {
            'I' => {
                // IDENTIFY\n[ 4-byte size in bytes ][ N-byte JSON data ]
                try p.matchString("IDENTIFY\n");
                return .{ .identify = try p.readBytes(try p.readInt()) };
            },
            'S' => {
                // SUB <topic_name> <channel_name>\n
                try p.matchString("SUB ");
                const topic = try p.readString(' ');
                const channel = try p.readString('\n');
                return .{ .sub = .{ .topic = topic, .channel = channel } };
            },
            'P' => {
                // PUB <topic_name>\n[ 4-byte size in bytes ][ N-byte binary data ]
                try p.matchString("PUB ");
                const topic = try p.readString('\n');
                const data = try p.readBytes(try p.readInt());
                return .{ .spub = .{ .topic = topic, .data = data } };
            },
            'M' => {
                //MPUB <topic_name>\n[ 4-byte body size ][ 4-byte num messages ]
                //[ 4-byte message #1 size ][ N-byte binary data ]
                try p.matchString("MPUB ");
                const topic = try p.readString('\n');
                const size = try p.readInt();
                const msgs = try p.readInt();
                const data = try p.readBytes(size);
                // check that individual messages has [size][data]
                const data_end_pos = p.pos;
                p.pos -= data.len;
                for (0..msgs) |_| {
                    const msg_size = p.readInt() catch return error.Invalid;
                    _ = p.readBytes(msg_size) catch return error.Invalid;
                }
                if (p.pos != data_end_pos) return error.Invalid;

                return .{ .mpub = .{ .topic = topic, .msgs = msgs, .data = data } };
            },
            else => return error.Invalid,
        }
    }

    fn matchString(p: *Parser, str: []const u8) !void {
        const buf = p.buf[p.pos..];
        if (buf.len < str.len) return error.BufferOverflow;
        if (!mem.eql(u8, buf[0..str.len], str)) return error.Invalid;
        p.pos += str.len;
    }

    fn readInt(p: *Parser) !u32 {
        const buf = p.buf[p.pos..];
        if (buf.len < 4) return error.BufferOverflow;
        p.pos += 4;
        return mem.readInt(u32, buf[0..4], .big);
    }

    fn readString(p: *Parser, delim: u8) ![]const u8 {
        const buf = p.buf[p.pos..];
        const len = mem.indexOfScalar(u8, buf, delim) orelse return error.BufferOverflow;
        p.pos += len + 1;
        return buf[0..len];
    }

    fn readBytes(p: *Parser, size: u32) ![]const u8 {
        const buf = p.buf[p.pos..];
        if (buf.len < size) return error.BufferOverflow;
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
        try testing.expectError(error.BufferOverflow, p.parse());
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
        try testing.expectEqualStrings(m.sub.topic, "pero");
        try testing.expectEqualStrings(m.sub.channel, "zdero");
        try testing.expectEqual(15, p.pos);
        try testing.expectEqual('S', buf[15]);
        m = try p.parse();
        try testing.expectEqualStrings(m.sub.topic, "jozo");
        try testing.expectEqualStrings(m.sub.channel, "bozo");
        try testing.expectEqual(29, p.pos);
        try testing.expectEqual('-', buf[29]);
    }
    { // split buffer
        var p = Parser{ .buf = buf[0..6] };
        try testing.expectError(error.BufferOverflow, p.parse());
        try testing.expectEqual(0, p.pos);

        p = Parser{ .buf = buf[0..14] };
        try testing.expectError(error.BufferOverflow, p.parse());
        try testing.expectEqual(0, p.pos);
    }
}

test "pub" {
    const buf = "PUB pero\n\x00\x00\x00\x05zderoPUB foo\n\x00\x00\x00\x03bar-_____";
    {
        var p = Parser{ .buf = buf };
        var m = try p.parse();
        try testing.expectEqualStrings("pero", m.spub.topic);
        try testing.expectEqualStrings("zdero", m.spub.data);
        try testing.expectEqual(18, p.pos);
        try testing.expectEqual('P', buf[18]);
        m = try p.parse();
        try testing.expectEqualStrings(m.spub.topic, "foo");
        try testing.expectEqualStrings(m.spub.data, "bar");
        try testing.expectEqual(33, p.pos);
        try testing.expectEqual('-', buf[33]);
    }
    { // split buffer
        var p = Parser{ .buf = buf[0..6] };
        try testing.expectError(error.BufferOverflow, p.parse());
        try testing.expectEqual(0, p.pos);

        p = Parser{ .buf = buf[0..12] };
        try testing.expectError(error.BufferOverflow, p.parse());
        try testing.expectEqual(0, p.pos);
    }
}

test "mpub" {
    const buf = "MPUB pero\n" ++
        "\x00\x00\x00\x10" ++ // body size
        "\x00\x00\x00\x02" ++ // number of message
        "\x00\x00\x00\x05zdero" ++ // message 1 [size][body]
        "\x00\x00\x00\x03bar"; // message 2 [size][body]
    {
        var p = Parser{ .buf = buf };
        const m = try p.parse();
        try testing.expectEqualStrings("pero", m.mpub.topic);
        try testing.expectEqual(2, m.mpub.msgs);
        try testing.expectEqual(16, m.mpub.data.len);
        try testing.expectEqual(buf.len, p.pos);
    }
    { // split buffer
        var p = Parser{ .buf = buf[0..6] };
        try testing.expectError(error.BufferOverflow, p.parse());
        try testing.expectEqual(0, p.pos);

        p = Parser{ .buf = buf[0..14] };
        try testing.expectError(error.BufferOverflow, p.parse());
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
