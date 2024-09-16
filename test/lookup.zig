const std = @import("std");

pub fn main() !void {
    const port = 4164;
    const address = std.net.Address.initIp4([4]u8{ 127, 0, 0, 1 }, port);
    var server = try address.listen(.{
        .reuse_address = true,
    });

    while (true) {
        const tcp = try server.accept();
        const stream = tcp.stream;
        defer stream.close();

        var buf: [4096]u8 = undefined;
        while (true) {
            const n = try stream.read(&buf);
            if (n == 0) return;
            std.debug.print("{s}", .{buf[0..n]});
        }
    }
}
