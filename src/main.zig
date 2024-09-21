const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const Atomic = std.atomic.Value;

const Options = @import("protocol.zig").Options;
const Io = @import("io.zig").Io;
const tcp = @import("tcp.zig");
const http = @import("http.zig");
const lookup = @import("lookup.zig");
const statsd = @import("statsd.zig");

// pub const std_options = std.Options{
//     .log_level = .info,
// };

const log = std.log.scoped(.main);

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = if (builtin.mode == .ReleaseFast) std.heap.c_allocator else gpa.allocator();

    const options: Options = .{};

    const tcp_addr = std.net.Address.initIp4([4]u8{ 127, 0, 0, 1 }, options.tcp_port);
    const tcp_socket = (try tcp_addr.listen(.{ .reuse_address = true })).stream.handle;

    const http_addr = std.net.Address.initIp4([4]u8{ 127, 0, 0, 1 }, options.http_port);
    const http_socket = (try http_addr.listen(.{ .reuse_address = true })).stream.handle;

    const statsd_addr = std.net.Address.initIp4([4]u8{ 127, 0, 0, 1 }, 8125);

    var io = Io{ .allocator = allocator };
    try io.init(
        options.ring.entries,
        options.ring.recv_buffers,
        options.ring.recv_buffer_len,
    );
    defer io.deinit();

    var lookup_connector = lookup.Connector.init(allocator, &io);
    defer lookup_connector.deinit();

    var server = tcp.Server.init(allocator, &io, &lookup_connector);
    defer server.deinit();

    { // start lookupd connections
        // TODO: can we avoid this
        lookup_connector.server = &server;
        const lookupd1 = try std.net.Address.parseIp4("127.0.0.1", 4160);
        const lookupd2 = try std.net.Address.parseIp4("127.0.0.1", 4162);
        const lookupd3 = try std.net.Address.parseIp4("127.0.0.1", 4164);
        try lookup_connector.addLookupd(lookupd1);
        try lookup_connector.addLookupd(lookupd2);
        try lookup_connector.addLookupd(lookupd3);
    }

    var tcp_listener = try tcp.Listener.init(allocator, &io, &server, options);
    defer tcp_listener.deinit();
    try tcp_listener.accept(tcp_socket);

    var http_listener = try http.Listener.init(allocator, &io, &server, options);
    defer http_listener.deinit();
    try http_listener.accept(http_socket);

    var statsd_connector = statsd.Connector{ .io = &io, .allocator = allocator, .address = statsd_addr };
    try statsd_connector.start();

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
    try statsd_connector.close();
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
    print("  active {}, accepted: {}, completed: {}\n", .{ listener.metric.accept - listener.metric.close, listener.metric.accept, listener.metric.close });

    print("io operations: loops: {}, cqes: {}, cqes/loop {}\n", .{
        io.metric.loops,
        io.metric.cqes,
        if (io.metric.loops > 0) io.metric.cqes / io.metric.loops else 0,
    });
    print("  all    {}\n", .{io.metric.all});
    print("  recv   {}\n", .{io.metric.recv});
    print("  sendv  {}\n", .{io.metric.sendv});
    print("  ticker {}\n", .{io.metric.ticker});
    print("  close  {}\n", .{io.metric.close});
    print("  accept {}\n", .{io.metric.accept});

    print(
        "  receive buffers group:\n    success: {}, no-buffs: {} {d:5.2}%\n",
        .{ io.metric.recv_buf_grp.success, io.metric.recv_buf_grp.no_bufs, io.metric.recv_buf_grp.noBufs() },
    );

    print("server topics: {}\n", .{server.topics.count()});
    var ti = server.topics.iterator();
    while (ti.next()) |te| {
        const topic_name = te.key_ptr.*;
        const topic = te.value_ptr.*;
        print("  {s} depth: {d}  sequence: {}\n", .{
            topic_name,
            topic.metric.depth,
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
            print("    pull: {}, finish: {}, timeout: {}, requeue: {}\n", .{
                channel.metric.pull,
                channel.metric.finish,
                channel.metric.timeout,
                channel.metric.requeue,
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
