const std = @import("std");
const mem = std.mem;
const fmt = std.fmt;
const net = std.net;
const time = std.time;
const math = std.math;
const maxInt = math.maxInt;

pub const version = "0.1.0";

const usage =
    \\Usage of nsqd:
    \\
    \\  --data-path string (default current dir)
    \\        path to store disk-backed messages
    \\  --tcp-address string
    \\        address to listen on for TCP clients (<addr>:<port>) (default "0.0.0.0:4150")
    \\  --http-address string
    \\        address to listen on for HTTP clients (<addr>:<port>) (default "0.0.0.0:4151")
    \\
    \\  --lookupd-tcp-address value
    \\        lookupd TCP address (may be given multiple times)
    \\
    \\  --broadcast-address string
    \\        address that will be registered with lookupd (defaults to the OS hostname)
    \\  --broadcast-http-port int
    \\        HTTP port that will be registered with lookupd (defaults to the HTTP port that this nsqd is listening on)
    \\  --broadcast-tcp-port int
    \\        TCP port that will be registered with lookupd (defaults to the TCP port that this nsqd is listening on)
    \\
    \\  --max-heartbeat-interval duration
    \\        maximum client configurable duration of time between client heartbeats (default 1m0s)
    \\  --max-msg-timeout duration
    \\        maximum duration before a message will timeout (default 15m0s)
    \\  --max-rdy-count int
    \\        maximum RDY count for a client (default 2500)
    \\  --max-req-timeout duration
    \\        maximum requeuing timeout for a message (default 1h0m0s)
    \\  --msg-timeout duration
    \\        default duration to wait before auto-requeing a message (default 1m0s)
    \\
    \\  --statsd-address string
    \\        UDP <addr>:<port> of a statsd daemon for pushing stats
    \\  --statsd-interval duration
    \\        duration between pushing to statsd (default 5s)
    \\  --statsd-prefix string
    \\        prefix used for keys sent to statsd (%s for host replacement) (default "nsq.%s")
    \\  --statsd-udp-packet-size int
    \\        the size in bytes of statsd UDP packets (default 508)
    \\
    \\  limits:
    \\  --max-msg-size int(kMG)
    \\        maximum size of a single message in bytes (default 1M)
    \\  --max-body-size int(kMG)
    \\        maximum size of a single command body (default 5M)
    \\  --max-mem int(kMG)
    \\        maximum amount of memory used for all messages in broker (default 50% system memory)
    \\  --max-topic-mem int(kMG)
    \\        maximum amount of memory per topic (defalut unlimited)
    \\  In topic messages are stored in pages. Each page can hold multiple messages.
    \\  Initial page size is size of the first page in the topic. If needed size of the
    \\  subsequent pages will grow limited with max-page-size. Single message can still
    \\  allocate page bigger that max-page-size if max-msg-size allows that.
    \\  --initial-page-size int(kMG)
    \\        initial topic page size (default 64k)
    \\  --max-page-size int(kMG)
    \\        max topic memory page size (default 1M)
    \\
    \\  io_uring:
    \\  --io-entries
    \\        number of entries in io_uring submission queue (default 16k)
    \\        must be a power of two between 1 and 32768
    \\  --io-recv-buffers
    \\        number of buffers in io_uring provided buffer pool (default 1024)
    \\        buffers are shared among all receive operations
    \\  --io-recv-buffer-len
    \\        byte size of each io_uring provided buffer (default 64k)
    \\
    \\  --version
    \\        print version string
    \\
;
const Options = @This();

data_path: []const u8 = ".",

tcp_address: net.Address = net.Address.initIp4([4]u8{ 0, 0, 0, 0 }, 4150),
http_address: net.Address = net.Address.initIp4([4]u8{ 0, 0, 0, 0 }, 4151),

lookup_tcp_addresses: []net.Address = &.{},

hostname: []const u8,
broadcast_address: ?[]const u8 = null,
broadcast_tcp_port: u16 = 0,
broadcast_http_port: u16 = 0,

max_rdy_count: u16 = 2500,

// Duration values in milliseconds
max_heartbeat_interval: u32 = 60000, // 1m
max_msg_timeout: u32 = 60000 * 15, // 15m
max_req_timeout: u32 = 60000 * 60, // 1h
msg_timeout: u32 = 60000, // 1m

broker: Broker = .{},
io: Io = .{},
statsd: Statsd = .{},

pub const Broker = struct {
    max_msg_size: u32 = 1024 * 1024,
    max_body_size: u32 = 5 * 1024 * 1024,
    max_mem: u64 = maxInt(u64),
    max_topic_mem: u64 = maxInt(u64),

    initial_page_size: u32 = 64 * 1024,
    max_page_size: u32 = 1024 * 1024,
};

pub const Io = @import("iox").Options;

pub const Statsd = struct {
    /// statsd daemon for pushing stats
    address: ?std.net.Address = null,
    /// duration between pushing to statsd (in milliseconds)
    interval: u32 = 5 * 1000,
    /// prefix used for keys sent to statsd (%s for host replacement) (default "nsq.%s")
    prefix: []const u8 = &.{},
    /// the size in bytes of statsd UDP packets (default 508)
    udp_packet_size: u16 = 508,
};

pub fn broadcastAddress(self: Options) []const u8 {
    if (self.broadcast_address) |ba| return ba;
    return self.hostname;
}

pub fn deinit(self: *Options, allocator: mem.Allocator) void {
    allocator.free(self.hostname);
    if (self.broadcast_address) |ba| allocator.free(ba);
    allocator.free(self.lookup_tcp_addresses);
    allocator.free(self.statsd.prefix);
}

pub fn initFromArgs(allocator: mem.Allocator) !Options {
    var iter = try ArgIterator.init(allocator);
    defer iter.deinit();

    var hostname_buf: [std.posix.HOST_NAME_MAX]u8 = undefined;
    const hostname = try allocator.dupe(u8, try std.posix.gethostname(&hostname_buf));

    var lookup_tcp_addresses = std.ArrayList(net.Address).init(allocator);
    defer lookup_tcp_addresses.deinit();

    var opt: Options = .{
        .hostname = hostname,
        .statsd = .{ .prefix = "nsq.%s" },
        .broker = .{
            .max_mem = totalSystemMemory() / 2,
        },
    };

    outer: while (iter.next()) |arg| {
        if (eql("help", arg) or eql("h", arg)) {
            std.debug.print("{s}", .{usage});
            std.process.exit(0);
        } else if (eql("version", arg)) {
            std.debug.print("nsqxd {s}\n", .{version});
            std.process.exit(0);
            // dump data path
        } else if (iter.string("data-path")) |str| {
            opt.data_path = str;

            // tcp/http address
        } else if (iter.address("tcp-address", opt.tcp_address.getPort())) |addr| {
            opt.tcp_address = addr;
        } else if (iter.address("http-address", opt.http_address.getPort())) |addr| {
            opt.http_address = addr;
            // lookup addresses
        } else if (iter.address("lookupd-tcp-address", 4160)) |addr| {
            try lookup_tcp_addresses.append(addr);
            // broadcast arguments
        } else if (iter.string("broadcast-address")) |str| {
            opt.broadcast_address = try allocator.dupe(u8, str);
        } else if (iter.int("broadcast-tcp-port", u16)) |port| {
            opt.broadcast_tcp_port = port;
        } else if (iter.int("broadcast-http-port", u16)) |port| {
            opt.broadcast_http_port = port;

            // client options
        } else if (iter.int("max-rdy-count", u16)) |count| {
            opt.max_rdy_count = count;
        } else if (iter.durationMs("max-heartbeat-interval")) |d| {
            opt.max_heartbeat_interval = d;
        } else if (iter.durationMs("max-msg-timeout")) |d| {
            opt.max_msg_timeout = d;
        } else if (iter.durationMs("max-req-timeout")) |d| {
            opt.max_req_timeout = d;
        } else if (iter.durationMs("msg-timeout")) |d| {
            opt.msg_timeout = d;

            // statsd options
        } else if (iter.address("statsd-address", 8125)) |addr| {
            opt.statsd.address = addr;
        } else if (iter.string("statsd-prefix")) |str| {
            opt.statsd.prefix = str;
        } else if (iter.byteSize(u16, "statsd-udp-packet-size")) |size| {
            if (size != 0) opt.statsd.udp_packet_size = size;
        } else if (iter.durationMs("statsd-interval")) |d| {
            opt.statsd.interval = d;

            // broker options
        } else if (iter.byteSize(u32, "max-msg-size")) |size| {
            opt.broker.max_msg_size = size;
        } else if (iter.byteSize(u32, "max-body-size")) |size| {
            opt.broker.max_body_size = size;
        } else if (iter.byteSize(u64, "max-mem")) |d| {
            opt.broker.max_mem = d;
        } else if (iter.byteSize(u64, "max-topic-mem")) |d| {
            opt.broker.max_topic_mem = d;
        } else if (iter.byteSize(u32, "initial-page-size")) |d| {
            opt.broker.initial_page_size = d;
        } else if (iter.byteSize(u32, "max-page-size")) |d| {
            opt.broker.max_page_size = d;

            // io_uring
        } else if (iter.int("io-entries", u16)) |i| {
            opt.io.entries = i;
        } else if (iter.int("io-recv-buffers", u16)) |i| {
            opt.io.recv_buffers = i;
        } else if (iter.int("io-recv-buffer-len", u32)) |i| {
            opt.io.recv_buffer_len = i;

            // Allow unchanged nsqd configuration to be used with nsqxd. Skip
            // nsqd arguments not used in nsqxd.
        } else {
            for (nsqd_arguments) |nsqd_arg| {
                if (eql(nsqd_arg, arg)) {
                    std.debug.print("info: skipping nsqd argument '{s}' unused in nsqxd\n", .{nsqd_arg});
                    _ = iter.next(); // skip value also
                    continue :outer;
                }
            }
            for (nsqd_flags) |nsqd_flag| {
                if (eql(nsqd_flag, arg)) {
                    std.debug.print("info: skipping nsqd argument '{s}' unused in nsqxd\n", .{nsqd_flag});
                    continue :outer;
                }
            }
            fatal("unknown argument {s}\n", .{arg});
        }
    }
    if (opt.broadcast_tcp_port == 0) opt.broadcast_tcp_port = opt.tcp_address.getPort();
    if (opt.broadcast_http_port == 0) opt.broadcast_http_port = opt.http_address.getPort();
    if (lookup_tcp_addresses.items.len > 0) opt.lookup_tcp_addresses = try lookup_tcp_addresses.toOwnedSlice();
    opt.statsd.prefix = try allocator.dupe(u8, opt.statsd.prefix);

    return opt;
}

const ArgIterator = struct {
    allocator: mem.Allocator,
    inner: std.process.ArgIterator,
    arg: [:0]const u8 = &.{},

    const Self = @This();

    pub fn init(allocator: mem.Allocator) !Self {
        return .{
            .allocator = allocator,
            .inner = try std.process.argsWithAllocator(allocator),
        };
    }

    pub fn next(self: *Self) ?([:0]const u8) {
        if (self.inner.inner.index == 0) _ = self.inner.skip();
        self.arg = self.inner.next() orelse return null;
        return self.arg;
    }

    fn address(self: *Self, flag: []const u8, default_port: u16) ?net.Address {
        if (eql(flag, self.arg)) {
            const val = self.value();
            return parseAddressFailing(self.allocator, val, default_port) catch {
                fatal("unable to parse address '{s}'", .{val});
            };
        }
        return null;
    }

    fn string(self: *Self, flag: []const u8) ?[]const u8 {
        if (eql(flag, self.arg)) {
            return self.value();
        }
        return null;
    }

    fn int(self: *Self, flag: []const u8, comptime T: type) ?T {
        if (eql(flag, self.arg)) {
            const val = self.value();
            return std.fmt.parseInt(T, val, 10) catch {
                fatal("unable to parse integer {s}", .{val});
            };
        }
        return null;
    }

    fn duration(self: *Self, flag: []const u8) ?u64 {
        if (eql(flag, self.arg)) {
            const val = self.value();
            return parseDuration(val) catch {
                fatal("unable to parse duration '{s}', valid duration format: 12h34m56s78ms", .{val});
            };
        }
        return null;
    }

    fn durationMs(self: *Self, flag: []const u8) ?u32 {
        if (self.duration(flag)) |d| {
            const ms = d / time.ns_per_ms;
            if (ms > maxInt(u32)) {
                fatal("duration in '{s}' overflow", .{self.arg});
            }
            return @intCast(ms);
        }
        return null;
    }

    fn byteSize(self: *Self, comptime T: type, flag: []const u8) ?T {
        if (eql(flag, self.arg)) {
            const val = self.value();
            return parseByteSize(T, val) catch |err| {
                fatal(
                    "{s}, parsing byte size '{s}', valid format: 123k, 456M, 789G",
                    .{ @errorName(err), val },
                );
            };
        }
        return null;
    }

    fn value(self: *Self) []const u8 {
        if (self.arg.len > 0) {
            if (mem.indexOfScalar(u8, self.arg, '=')) |pos| {
                if (self.arg.len > pos + 1)
                    return self.arg[pos + 1 ..];
            }
        }
        return self.inner.next() orelse {
            fatal("expected parameter after {s}", .{self.arg});
        };
    }

    pub fn deinit(self: *Self) void {
        self.inner.deinit();
    }
};

fn subArg(iter: *std.process.ArgIterator) ![]const u8 {
    return iter.next() orelse return error.MissingArgument;
}

fn eql(flag: []const u8, arg: []const u8) bool {
    if (arg.len < 2) return false;
    const start: usize = if (flag.len > 1 and arg[1] == '-') 2 else 1;
    if (mem.indexOfScalar(u8, arg, '=')) |end| {
        return mem.eql(u8, flag, arg[start..end]);
    }
    return mem.eql(u8, flag, arg[start..]);
}

fn parseAddress(allocator: mem.Allocator, arg: []const u8, default_port: u16) !net.Address {
    return parseAddressFailing(allocator, arg, default_port) catch |err| {
        std.debug.print("fail to parse address {s}, {}", .{ arg, err });
        return err;
    };
}

fn parseAddressFailing(allocator: mem.Allocator, arg: []const u8, default_port: u16) !net.Address {
    var addr = arg;
    var port: u16 = default_port;
    if (mem.indexOfScalar(u8, arg, ':')) |sep| {
        addr = arg[0..sep];
        port = try std.fmt.parseInt(u16, arg[sep + 1 ..], 10);
    }

    const list = try net.getAddressList(allocator, addr, port);
    defer list.deinit();
    if (list.addrs.len == 0) return error.UnknownHostName;
    return list.addrs[0];
}

const testing = std.testing;

test parseAddressFailing {
    _ = try parseAddressFailing(testing.allocator, "localhost", 4150);
    _ = try parseAddressFailing(testing.allocator, "127.0.0.1", 4150);
    _ = try parseAddressFailing(testing.allocator, "0.0.0.0", 4150);
    _ = try parseAddressFailing(testing.allocator, "google.com", 4150);
    const addr = try parseAddressFailing(testing.allocator, "google.com:80", 4150);
    try testing.expectEqual(80, addr.getPort());
}

// test example:
// $ zig run -lc Options.zig -- --max-mem 1G  --max-topic-mem 2M
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var opt = try Options.initFromArgs(allocator);
    defer opt.deinit(allocator);

    // connect options
    std.debug.print("tcp_address: {}\n", .{opt.tcp_address});
    std.debug.print("http_address: {}\n", .{opt.http_address});
    std.debug.print("lookup_tcp_addresses: {any}\n", .{opt.lookup_tcp_addresses});
    std.debug.print("broadcast_addresses: {s}\n", .{opt.broadcastAddress()});
    std.debug.print("broadcast_tcp_port: {}\n", .{opt.broadcast_tcp_port});
    std.debug.print("broadcast_http_port: {}\n", .{opt.broadcast_http_port});
    std.debug.print("\n", .{});

    // client options
    std.debug.print("max_rdy_count: {}\n", .{opt.max_rdy_count});
    std.debug.print("max_heartbeat_interval: {}\n", .{opt.max_heartbeat_interval});
    std.debug.print("msg_timeout: {}\n", .{opt.msg_timeout});
    std.debug.print("max_msg_timeout: {}\n", .{opt.max_msg_timeout});
    std.debug.print("max_req_timeout: {}\n", .{opt.max_req_timeout});
    std.debug.print("\n", .{});

    // broker options
    std.debug.print("max_msg_size: {}\n", .{opt.broker.max_msg_size});
    std.debug.print("max_body_size: {}\n", .{opt.broker.max_body_size});
    std.debug.print("max_mem: {} {}G\n", .{ opt.broker.max_mem, opt.broker.max_mem / 1024 / 1024 / 1024 });
    std.debug.print("max_topic_mem: {}\n", .{opt.broker.max_topic_mem});
    std.debug.print("initial_page_size: {}\n", .{opt.broker.initial_page_size});
    std.debug.print("max_page_size: {}\n", .{opt.broker.max_page_size});
    std.debug.print("\n", .{});

    // statsd options
    if (opt.statsd.address) |addr|
        std.debug.print("statsd_address: {}\n", .{addr});
    std.debug.print("statsd_interval: {}\n", .{opt.statsd.interval});
    std.debug.print("statsd_prefix: {s}\n", .{opt.statsd.prefix});
    std.debug.print("statsd_udp_packet_size: {}\n", .{opt.statsd.udp_packet_size});
    // std.debug.print("\n", .{});
}

fn parseDuration(arg: []const u8) !u64 {
    var duration: u64 = 0;
    var pos: usize = 0;
    if (mem.indexOfScalar(u8, arg, 'h')) |p| {
        duration += try fmt.parseInt(u64, arg[pos..p], 10) * time.ns_per_hour;
        pos = p + 1;
    }
    if (mem.indexOfScalarPos(u8, arg, pos, 'm')) |p| {
        duration += try fmt.parseInt(u64, arg[pos..p], 10) * time.ns_per_min;
        pos = p + 1;
    }
    if (mem.indexOfScalarPos(u8, arg, pos, 's')) |p| {
        duration += try fmt.parseInt(u64, arg[pos..p], 10) * time.ns_per_s;
        pos = p + 1;
    }
    if (mem.indexOfPos(u8, arg, pos, "ms")) |p| {
        duration += try fmt.parseInt(u64, arg[pos..p], 10) * time.ns_per_ms;
        pos = p + 2;
    }
    if (pos != arg.len) return error.DurationParse;
    return duration;
}

test parseDuration {
    try testing.expectEqual(
        12 * time.ns_per_hour + 34 * time.ns_per_min + 56 * time.ns_per_s,
        try parseDuration("12h34m56s"),
    );
    try testing.expectEqual(
        12 * time.ns_per_hour + 34 * time.ns_per_min + 56 * time.ns_per_s + 78 * time.ns_per_ms,
        try parseDuration("12h34m56s78ms"),
    );
    try testing.expectEqual(
        34 * time.ns_per_min + 56 * time.ns_per_s + 78 * time.ns_per_ms,
        try parseDuration("34m56s78ms"),
    );
    try testing.expectError(error.DurationParse, parseDuration("34m56s78ab"));
    try testing.expectError(error.DurationParse, parseDuration("34m56ss"));
    try testing.expectError(error.InvalidCharacter, parseDuration("3-m56ss"));
}

fn parseByteSize(comptime T: type, arg: []const u8) !T {
    if (arg.len < 1) return 0;
    const b_char = arg[arg.len - 1] == 'b' or arg[arg.len - 1] == 'B';
    if (b_char and arg.len < 2) return 0;
    var unit_pos: usize = arg.len - @as(usize, if (b_char) 2 else 1);
    if (unit_pos > arg.len) return 0;
    const unit: T = switch (arg[unit_pos]) {
        'b', 'B' => 1,
        'k', 'K' => 1024,
        'm', 'M' => if (@sizeOf(T) > 2) 1024 * 1024 else return error.Overflow,
        'g', 'G' => if (@sizeOf(T) > 2) 1024 * 1024 * 1024 else return error.Overflow,
        't', 'T' => if (@sizeOf(T) > 4) 1024 * 1024 * 1024 * 1024 else return error.Overflow,
        else => brk: {
            unit_pos += 1;
            break :brk 1;
        },
    };
    return try fmt.parseInt(T, arg[0..unit_pos], 10) * unit;
}

test parseByteSize {
    try testing.expectEqual(123 * 1024, try parseByteSize(usize, "123K"));
    try testing.expectEqual(4567 * 1024 * 1024, try parseByteSize(usize, "4567M"));
    try testing.expectEqual(89, try parseByteSize(usize, "89"));
    try testing.expectEqual(0, parseByteSize(usize, "B"));
    try testing.expectEqual(123, parseByteSize(usize, "123B"));

    try testing.expectEqual(123 * 1024, try parseByteSize(usize, "123KB"));
    try testing.expectEqual(4567 * 1024 * 1024, try parseByteSize(usize, "4567MB"));
}

pub fn fatal(comptime format: []const u8, args: anytype) noreturn {
    std.log.err(format, args);
    std.process.exit(1);
}

const nsqd_arguments = [_][]const u8{
    "auth-http-address",
    "auth-http-request-method",
    "broadcast-address",
    "broadcast-http-port",
    "broadcast-tcp-port",
    "config",
    "data-path",
    "e2e-processing-latency-percentile",
    "e2e-processing-latency-window-time",
    "http-address",
    "http-client-connect-timeout",
    "http-client-request-timeout",
    "https-address",
    "log-level",
    "log-prefix",
    "lookupd-tcp-address",
    "max-body-size",
    "max-bytes-per-file",
    "max-channel-consumers",
    "max-deflate-level",
    "max-heartbeat-interval",
    "max-msg-size",
    "max-msg-timeout",
    "max-output-buffer-size",
    "max-output-buffer-timeout",
    "max-rdy-count",
    "max-req-timeout",
    "mem-queue-size",
    "min-output-buffer-timeout",
    "msg-timeout",
    "node-id",
    "output-buffer-timeout",
    "queue-scan-selection-count",
    "queue-scan-worker-pool-max",
    "statsd-address",
    "statsd-exclude-ephemeral",
    "statsd-interval",
    "statsd-mem-stats",
    "statsd-prefix",
    "statsd-udp-packet-size",
    "sync-every",
    "sync-timeout",
    "tcp-address",
    "tls-cert",
    "tls-client-auth-policy",
    "tls-key",
    "tls-min-version",
    "tls-required",
    "tls-root-ca-file",
};
const nsqd_flags = [_][]const u8{
    "snappy",
    "verbose",
    "version",
    "worker-id",
    "deflate",
};

fn totalSystemMemory() usize {
    const c = @cImport(@cInclude("unistd.h"));
    const pages: usize = @intCast(c.sysconf(c._SC_PHYS_PAGES));
    const page_size: usize = @intCast(c.sysconf(c._SC_PAGE_SIZE));
    return pages * page_size;
}
