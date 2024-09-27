const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const Atomic = std.atomic.Value;
const net = std.net;
const mem = std.mem;

const Options = @import("Options.zig");
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

    var options = try Options.initFromArgs(allocator);
    defer options.deinit(allocator);

    const tcp_socket = (try options.tcp_address.listen(.{ .reuse_address = true })).stream.handle;
    const http_socket = (try options.http_address.listen(.{ .reuse_address = true })).stream.handle;

    var io = Io{ .allocator = allocator };
    try io.init(
        options.io.entries,
        options.io.recv_buffers,
        options.io.recv_buffer_len,
    );
    defer io.deinit();

    var lookup_connector = lookup.Connector.init(allocator, &io);
    defer lookup_connector.deinit();

    var server = tcp.Server.init(allocator, &io, &lookup_connector);
    defer server.deinit();

    { // start lookupd connections
        lookup_connector.server = &server;
        for (options.lookup_tcp_addresses) |addr| try lookup_connector.addLookupd(addr);
    }

    var tcp_listener = try tcp.Listener.init(allocator, &io, &server, options);
    defer tcp_listener.deinit();
    try tcp_listener.accept(tcp_socket);

    var http_listener = try http.Listener.init(allocator, &io, &server, options);
    defer http_listener.deinit();
    try http_listener.accept(http_socket);

    var statsd_connector: ?statsd.Connector = try statsd.Connector.init(allocator, &io, &server, options);
    if (statsd_connector) |*sc| try sc.start();
    defer if (statsd_connector) |*sc| sc.deinit();

    catchSignals();
    while (true) {
        try io.tick();

        const sig = signal.load(.monotonic);
        if (sig != 0) {
            signal.store(0, .release);
            switch (sig) {
                posix.SIG.USR1 => try showStat(&tcp_listener, &io, &server),
                posix.SIG.USR2 => {
                    mallocInfo();
                    mallocTrim();
                    mallocInfo();
                },
                posix.SIG.TERM, posix.SIG.INT => break,
                else => {},
            }
        }
    }

    log.info("draining", .{});
    if (statsd_connector) |*sc| try sc.close();
    try server.stopTimers();
    try lookup_connector.close();
    try http_listener.close();
    try tcp_listener.close();
    try io.drain();
    log.info("drain finished", .{});
}

fn mallocTrim() void {
    const c = @cImport(@cInclude("malloc.h"));
    //c.malloc_stats();
    const ret = c.malloc_trim(0);
    log.info("malloc_trim: {}", .{ret});
    //c.malloc_stats();
}

fn mallocInfo() void {
    const c = @cImport(@cInclude("malloc.h"));
    // const mi = c.mallinfo2();
    // std.debug.print("mi: {}\n", .{mi});
    // c.malloc_stats();
    _ = c.malloc_info(0, c.stdout);
    //log.info("malloc_info: {}", .{ret});
    _ = c.fflush(c.stdout);
    //c.malloc_stats();
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
    _ = @import("Options.zig");
    _ = @import("io.zig");
    _ = @import("tcp.zig");
    _ = @import("http.zig");
    _ = @import("lookup.zig");
    _ = @import("statsd.zig");
}
