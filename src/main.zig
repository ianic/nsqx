const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const Atomic = std.atomic.Value;
const net = std.net;
const mem = std.mem;

const Options = @import("Options.zig");
const fatal = @import("Options.zig").fatal;
const io = @import("iox");
const tcp = @import("tcp.zig");
const http = @import("http.zig");
const lookup = @import("lookup.zig");
const statsd = @import("statsd.zig");
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

    var io_loop: io.Loop = undefined;
    try io_loop.init(allocator, options.io);
    defer io_loop.deinit();

    var broker = Broker.init(allocator, &io_loop.timestamp, &io_loop.timer_queue, options.broker);
    defer broker.deinit();

    var lookup_connector: lookup.Connector = undefined;
    try lookup_connector.init(allocator, &io_loop, &broker.registrations.stream, options.lookup_tcp_addresses, options);
    defer lookup_connector.deinit();
    broker.setRegistrationsCallback(&lookup_connector, lookup.Connector.onRegister);

    var tcp_listener: tcp.Listener = undefined;
    try tcp_listener.init(allocator, &io_loop, &broker, options, options.tcp_address);
    defer tcp_listener.deinit();

    var http_listener: http.Listener = undefined;
    try http_listener.init(allocator, &io_loop, &broker, options, options.http_address);
    defer http_listener.deinit();

    const statsd_sender: ?*statsd.Sender = if (options.statsd.address) |_| brk: {
        var sc: statsd.Sender = undefined;
        try sc.init(allocator, &io_loop, &broker, options);
        break :brk &sc;
    } else null;
    defer if (statsd_sender) |sc| sc.deinit();

    try broker.restore(data_dir);

    while (true) {
        const signal = io_loop.run() catch |err| {
            log.err("io.run failed {}", .{err});
            switch (err) {
                error.OutOfMemory => {
                    // Release glibc malloc memory
                    if (builtin.mode == .ReleaseFast) mallocTrim();
                },
                // When unable to prepare io operation
                // Next tick will ring.submit at start
                error.SubmissionQueueFull => {},

                // All other error are from io_uring enter.
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
            continue;
        };
        log.debug("interrupted by signal {}", .{signal});
        switch (signal) {
            posix.SIG.USR1 => {},
            posix.SIG.USR2 => mallocTrim(),
            posix.SIG.TERM, posix.SIG.INT => break,
            else => {},
        }
    }

    try broker.dump(data_dir);
}

fn mallocTrim() void {
    const c = @cImport(@cInclude("malloc.h"));
    if (!@hasDecl(c, "malloc_trim")) return;

    const ret = c.malloc_trim(0);
    log.debug("malloc_trim retrun value: {}", .{ret});
}

test {
    _ = @import("broker.zig");
    _ = @import("protocol.zig");
    _ = @import("Options.zig");
    _ = @import("iox");
    _ = @import("tcp.zig");
    _ = @import("http.zig");
    _ = @import("lookup.zig");
    _ = @import("statsd.zig");
}
