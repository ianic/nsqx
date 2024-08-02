const std = @import("std");

const Message = union(enum) {
    idnetity: []const u8,
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
    cls: .{},
    nop: .{},
    auth: []const u8,
};
