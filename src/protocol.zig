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
        data: [][]const u8,
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

    fn parse(p: *Parser) !?Message {
        const buf = p.buf[p.pos..];
        if (buf.len < 4) return null;
        switch (buf[0]) {
            'I' => {
                const cmd = "IDENTIFY\n";
                if (buf.len < cmd.len + 4) return null;
                if (!mem.eql(u8, buf[0..cmd.len], cmd)) return error.Invalid;
                const size = mem.readInt(u32, buf[cmd.len..][0..4], .big);
                if (buf.len < cmd.len + 4 + size) return null;
                p.pos += cmd.len + 4 + size;
                return .{ .identify = buf[cmd.len + 4 ..][0..size] };
            },
            else => return error.Invalid,
        }
    }
};

const testing = std.testing;

test "Parse identify" {
    var buf = "IDENTIFY\n\x00\x00\x00\x0aHelloWorldIDENTIFY\n\x00\x00\x00\x03Foo-----";
    {
        var p = Parser{ .buf = buf };
        var m = (try p.parse()).?;
        try testing.expectEqualStrings(m.identify, "HelloWorld");
        try testing.expectEqual(p.pos, 23);
        m = (try p.parse()).?;
        try testing.expectEqualStrings(m.identify, "Foo");
        try testing.expectEqual(p.pos, 39);
    }
    { // split buffer
        var p = Parser{ .buf = buf[0..22] };
        try testing.expect(try p.parse() == null);
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
