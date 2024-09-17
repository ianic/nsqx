const std = @import("std");
const posix = std.posix;
const Atomic = std.atomic.Value;

const Options = @import("protocol.zig").Options;
const Io = @import("io.zig").Io;
const tcp = @import("tcp.zig");
const http = @import("http.zig");
const lookup = @import("lookup.zig");

// pub const std_options = std.Options{
//     .log_level = .info,
// };
//
const log = std.log.scoped(.main);

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    //const allocator = std.heap.c_allocator;
    const options: Options = .{};

    const tcp_addr = std.net.Address.initIp4([4]u8{ 127, 0, 0, 1 }, options.tcp_port);
    const tcp_socket = (try tcp_addr.listen(.{ .reuse_address = true })).stream.handle;

    const http_addr = std.net.Address.initIp4([4]u8{ 127, 0, 0, 1 }, options.http_port);
    const http_socket = (try http_addr.listen(.{ .reuse_address = true })).stream.handle;

    var io = Io{ .allocator = allocator };
    try io.init(
        options.ring.entries,
        options.ring.recv_buffers,
        options.ring.recv_buffer_len,
    );
    defer io.deinit();

    var lookup_connector = lookup.Connector.init(allocator, &io);
    defer lookup_connector.deinit();
    const lookupd1 = try std.net.Address.parseIp4("127.0.0.1", 4160);
    const lookupd2 = try std.net.Address.parseIp4("127.0.0.1", 4162);
    const lookupd3 = try std.net.Address.parseIp4("127.0.0.1", 4164);
    try lookup_connector.addLookupd(lookupd1);
    try lookup_connector.addLookupd(lookupd2);
    try lookup_connector.addLookupd(lookupd3);

    var server = tcp.Server.init(allocator, &io, &lookup_connector);
    defer server.deinit();

    var tcp_listener = try tcp.Listener.init(allocator, &io, &server, options);
    defer tcp_listener.deinit();
    try tcp_listener.accept(tcp_socket);

    var http_listener = try http.Listener.init(allocator, &io, &server, options);
    defer http_listener.deinit();
    try http_listener.accept(http_socket);

    catchSignals();
    while (true) {
        try io.tick();

        const sig = signal.load(.monotonic);
        if (sig != 0) {
            signal.store(0, .release);
            switch (sig) {
                posix.SIG.USR1 => try showStat(&tcp_listener, &io, &server),
                posix.SIG.USR2 => mallocTrim(),
                posix.SIG.TERM, posix.SIG.INT => break,
                else => {},
            }
        }
    }

    log.info("draining", .{});
    try server.stopTimers();
    try lookup_connector.close();
    try http_listener.close();
    try tcp_listener.close();
    try io.drain();
}

fn mallocTrim() void {
    const c = @cImport(@cInclude("malloc.h"));
    c.malloc_stats();
    const ret = c.malloc_trim(0);
    log.info("malloc_trim: {}", .{ret});
    c.malloc_stats();
}

fn showStat(listener: *tcp.Listener, io: *Io, server: *tcp.Server) !void {
    const print = std.debug.print;
    print("listener connections:\n", .{});
    print("  active {}, accepted: {}, completed: {}\n", .{ listener.stat.accepted - listener.stat.completed, listener.stat.accepted, listener.stat.completed });

    print("io operations: loops: {}, cqes: {}, cqes/loop {}\n", .{
        io.stat.loops,
        io.stat.cqes,
        if (io.stat.loops > 0) io.stat.cqes / io.stat.loops else 0,
    });
    print("  all    {}\n", .{io.stat.all});
    print("  recv   {}\n", .{io.stat.recv});
    print("  sendv  {}\n", .{io.stat.sendv});
    print("  ticker {}\n", .{io.stat.ticker});
    print("  close  {}\n", .{io.stat.close});
    print("  accept {}\n", .{io.stat.accept});

    print(
        "  receive buffers group:\n    success: {}, no-buffs: {} {d:5.2}%\n",
        .{ io.recv_buf_grp_stat.success, io.recv_buf_grp_stat.no_bufs, io.recv_buf_grp_stat.noBufs() },
    );

    print("server topics: {}\n", .{server.topics.count()});
    var ti = server.topics.iterator();
    while (ti.next()) |te| {
        const topic_name = te.key_ptr.*;
        const topic = te.value_ptr.*;
        const size = topic.messages.size();
        print("  {s} messages: {d} bytes: {} {}Mb {}Gb, sequence: {}\n", .{
            topic_name,
            topic.messages.count(),
            size,
            size / 1024 / 1024,
            size / 1024 / 1024 / 1024,
            topic.sequence,
        });

        var ci = topic.channels.iterator();
        while (ci.next()) |ce| {
            const channel_name = ce.key_ptr.*;
            const channel = ce.value_ptr.*;
            print("  --{s} consumers: {},  in flight messages: {}, deferred: {}\n", .{
                channel_name,
                channel.consumers.items.len,
                channel.in_flight.count(),
                channel.deferred.count(),
            });
            print("    pull: {}, send: {}, finish: {}, timeout: {}, requeue: {}\n", .{
                channel.stat.pull,
                channel.stat.send,
                channel.stat.finish,
                channel.stat.timeout,
                channel.stat.requeue,
            });
        }
    }
}

var signal = Atomic(c_int).init(0);

fn catchSignals() void {
    var act = posix.Sigaction{
        .handler = .{
            .handler = struct {
                fn wrapper(sig: c_int) callconv(.C) void {
                    signal.store(sig, .release);
                }
            }.wrapper,
        },
        .mask = posix.empty_sigset,
        .flags = 0,
    };
    posix.sigaction(posix.SIG.TERM, &act, null);
    posix.sigaction(posix.SIG.INT, &act, null);
    posix.sigaction(posix.SIG.USR1, &act, null);
    posix.sigaction(posix.SIG.USR2, &act, null);
    posix.sigaction(posix.SIG.PIPE, &act, null);
}

test {
    _ = @import("server.zig");
    _ = @import("protocol.zig");
}
