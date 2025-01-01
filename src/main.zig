const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const Atomic = std.atomic.Value;
const net = std.net;
const mem = std.mem;

const Options = @import("Options.zig");
const fatal = @import("Options.zig").fatal;
const Io = @import("io/io.zig").Io;
const tcp = @import("tcp.zig");
const http = @import("http.zig");
const lookup = @import("lookup.zig");
const statsd = @import("statsd.zig");
const timer = @import("timer.zig");
pub const Broker = @import("broker.zig").BrokerType(tcp.Conn);

pub const std_options = std.Options{
    .log_level = if (builtin.mode == .Debug) .debug else .warn,
};

const log = std.log.scoped(.main);

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = if (builtin.mode == .ReleaseFast) std.heap.c_allocator else gpa.allocator();

    var options = try Options.initFromArgs(allocator);
    defer options.deinit(allocator);

    var data_dir = std.fs.cwd().openDir(options.data_path, .{}) catch |err| switch (err) {
        error.FileNotFound => fatal("unable to open data path {s}", .{options.data_path}),
        else => return err,
    };
    defer data_dir.close();
    log.debug("using data dir {s}", .{options.data_path});

    var io: Io = undefined;
    try io.init(allocator, options.io);
    defer io.deinit();

    var broker = Broker.init(allocator, io.now(), options.broker);
    defer broker.deinit();

    var lookup_connector: lookup.Connector = undefined;
    try lookup_connector.init(allocator, &io, &broker.registrations.stream, options.lookup_tcp_addresses, options);
    defer lookup_connector.deinit();
    broker.setRegistrationsCallback(&lookup_connector, lookup.Connector.onRegister);

    var tcp_listener: tcp.Listener = undefined;
    try tcp_listener.init(allocator, &io, &broker, options, try socket(options.tcp_address));
    defer tcp_listener.deinit();

    var http_listener: http.Listener = undefined;
    try http_listener.init(allocator, &io, &broker, options, try socket(options.http_address));
    defer http_listener.deinit();

    const statsd_connector: ?*statsd.Connector = if (options.statsd.address) |_| brk: {
        var sc: statsd.Connector = undefined;
        try sc.init(allocator, &io, &broker, options);
        break :brk &sc;
    } else null;
    defer if (statsd_connector) |sc| sc.deinit();

    try broker.restore(data_dir);

    // Run loop
    catchSignals();
    while (true) {
        const ts = brk: {
            const now = io.timestamp;
            const ts = broker.tick(now) catch broker.timer_queue.next();

            const min_ts = now + std.time.ns_per_ms; // 1 ms
            const max_ts = now + 10 * std.time.ns_per_s; // 10 s
            break :brk @max(min_ts, @min(ts, max_ts));
        };

        io.tickTs(ts) catch |err| {
            if (err != error.SignalInterrupt)
                log.err("io.tick failed {}", .{err});
            switch (err) {
                // OutOfMemory
                // SubmissionQueueFull - when unable to prepare io operation
                // all other errors are io_uring enter specific
                error.OutOfMemory => {
                    // Release glibc malloc memory
                    if (builtin.mode == .ReleaseFast) mallocTrim();
                },
                // Next tick will ring.submit at start
                error.SubmissionQueueFull => {},

                // io_uring enter errors
                // ref: https://manpages.debian.org/unstable/liburing-dev/io_uring_enter.2.en.html#RETURN_VALUE
                error.SignalInterrupt => {},
                // hopefully transient errors
                error.SystemResources,
                error.CompletionQueueOvercommitted,
                => {},
                // fatal errors
                error.FileDescriptorInvalid,
                error.FileDescriptorInBadState,
                error.SubmissionQueueEntryInvalid,
                error.BufferInvalid,
                error.RingShuttingDown,
                error.OpcodeNotSupported,
                error.Unexpected,
                => break,
            }
        };

        const sig = signal.load(.monotonic);
        if (sig != 0) {
            signal.store(0, .release);
            switch (sig) {
                // posix.SIG.USR1 => try showStat(&tcp_listener, &io, &server),
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

    try broker.dump(data_dir);
}

pub fn socket(addr: net.Address) !posix.socket_t {
    return (try addr.listen(.{ .reuse_address = true })).stream.handle;
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

// fn showStat(listener: *tcp.Listener, io: *Io, server: *tcp.Broker) !void {
//     const print = std.debug.print;
//     print("listener connections:\n", .{});
//     print("  active {}, accepted: {}, completed: {}\n", .{ listener.metric.accept - listener.metric.close, listener.metric.accept, listener.metric.close });

//     print("io operations: loops: {}, cqes: {}, cqes/loop {}\n", .{
//         io.metric.loops,
//         io.metric.cqes,
//         if (io.metric.loops > 0) io.metric.cqes / io.metric.loops else 0,
//     });
//     print("  all    {}\n", .{io.metric.all});
//     print("  recv   {}\n", .{io.metric.recv});
//     print("  sendv  {}\n", .{io.metric.sendv});
//     print("  timer  {}\n", .{io.metric.timer});
//     print("  close  {}\n", .{io.metric.close});
//     print("  accept {}\n", .{io.metric.accept});

//     print(
//         "  receive buffers group:\n    success: {}, no-buffs: {} {d:5.2}%\n",
//         .{ io.metric.recv_buf_grp.success, io.metric.recv_buf_grp.no_bufs, io.metric.recv_buf_grp.noBufs() },
//     );

//     print("server topics: {}\n", .{server.topics.count()});
//     var ti = server.topics.iterator();
//     while (ti.next()) |te| {
//         const topic_name = te.key_ptr.*;
//         const topic = te.value_ptr.*;
//         print("  {s} depth: {d} bytes: {d} sequence: {} pages: {}\n", .{
//             topic_name,
//             topic.metric.depth,
//             topic.metric.depth_bytes,
//             topic.stream.last_sequence,
//             topic.stream.pages.items.len,
//         });

//         var ci = topic.channels.iterator();
//         while (ci.next()) |ce| {
//             const channel_name = ce.key_ptr.*;
//             const channel = ce.value_ptr.*;
//             print("  --{s} consumers: {},  in flight messages: {}, deferred: {}\n", .{
//                 channel_name,
//                 channel.consumers.items.len,
//                 channel.in_flight.count(),
//                 channel.deferred.count(),
//             });
//             print("    pull: {}, finish: {}, timeout: {}, requeue: {}\n", .{
//                 channel.metric.pull,
//                 channel.metric.finish,
//                 channel.metric.timeout,
//                 channel.metric.requeue,
//             });
//         }
//     }

//     print("listener connections: {}\n", .{listener.conns.count()});
//     var iter = listener.conns.keyIterator();
//     while (iter.next()) |e| {
//         const conn = e.*;
//         conn.printStatus();
//     }
// }

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
    _ = @import("broker.zig");
    _ = @import("protocol.zig");
    _ = @import("Options.zig");
    _ = @import("io/io.zig");
    _ = @import("tcp.zig");
    _ = @import("http.zig");
    _ = @import("lookup.zig");
    _ = @import("statsd.zig");
}
