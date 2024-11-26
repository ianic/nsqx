const std = @import("std");
const nsq = @import("nsq");
const cli = @import("zig-cli");

const mem = std.mem;
const json = std.json;

var statArgs = struct {
    address: []const u8 = "localhost:4170",
    topic: []const u8 = "",
    channel: []const u8 = "",
}{};

pub fn main() !void {
    try parseArgs();
}

fn parseArgs() !void {
    var r = try cli.AppRunner.init(std.heap.page_allocator);
    const app = cli.App{
        .command = cli.Command{
            .name = "nsq",
            .description = cli.Description{
                .one_line = "nsq daemon client",
            },
            .target = cli.CommandTarget{
                .subcommands = &.{
                    cli.Command{
                        .name = "stat",
                        .description = cli.Description{
                            .one_line = "show nsql server statistic",
                            // .detailed =
                            // \\this is my awesome multiline description.
                            // \\This is already line 2.
                            // \\And this is line 3.
                            // ,
                        },
                        .options = &.{
                            .{
                                .long_name = "nsqd-http-address",
                                .help = "nsql HTTP address",
                                .short_alias = 'a',
                                .value_ref = r.mkRef(&statArgs.address),
                                .value_name = "HOST:PORT",
                            },
                            .{
                                .long_name = "topic",
                                .short_alias = 't',
                                .help = "NSQ topic",
                                .value_ref = r.mkRef(&statArgs.topic),
                            },
                            .{
                                .long_name = "channel",
                                .short_alias = 'c',
                                .help = "NSQ channel",
                                .value_ref = r.mkRef(&statArgs.channel),
                            },
                        },
                        .target = cli.CommandTarget{
                            .action = cli.CommandAction{ .exec = showStat },
                        },
                    },
                },
            },
        },
    };
    return r.run(&app);
}

fn showStat() !void {
    var gpa_impl = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa_impl.deinit();
    const gpa = gpa_impl.allocator();

    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const allocator = arena.allocator();

    const parsed = try getStat(allocator);
    defer parsed.deinit();
    const stat = parsed.value;
    //std.debug.print("stat: {}\n", .{stat});

    const print = std.debug.print;

    print("{s}\n", .{stat.version});
    print("start_time {d}\n", .{stat.start_time});

    if (stat.topics.len == 0) {
        print("\nTopics: None\n", .{});
    } else {
        print("\nTopics:", .{});
    }
    for (stat.topics) |topic| {
        print("\n{s}{s:<15} depth: {d:<5} msgs: {d:<8} bytes: {d:<8}\n", .{
            if (topic.paused) "*P " else "   ",
            topic.topic_name,
            topic.depth,
            topic.message_count,
            topic.message_bytes,
        });
        for (topic.channels) |channel| {
            print("{s}{s:<25} depth: {d:<5} inflt: {d:<4} def: {d:<4} re-q: {d:<5} timeout: {d:<5} msgs: {d:<8}\n", .{
                if (channel.paused) "   *P " else "      ",
                channel.channel_name,
                channel.depth,
                channel.in_flight_count,
                channel.deferred_count,
                channel.requeue_count,
                channel.timeout_count,
                channel.message_count,
            });
            for (channel.clients) |client| {
                const client_id = try std.fmt.allocPrint(allocator, "{s}:{d}", .{ client.hostname, client.socket });
                print("        {s:<23} ready: {d:<5} inflt: {d:<4} fin: {d:<4} re-q: {d:<5}\n", .{
                    client_id,
                    client.ready_count,
                    client.in_flight_count,
                    client.finish_count,
                    client.requeue_count,
                });
            }
        }
    }
}

const headers_max_size = 1024;
const body_max_size = 1024 * 1024;

fn getStat(allocator: mem.Allocator) !json.Parsed(nsq.Stat) {
    const ref_url = "http://localhost:4151/stats?format=json&include_pages=0&include_clients=1";
    const url = try std.Uri.parse(ref_url);

    var client = std.http.Client{ .allocator = allocator };
    defer client.deinit();

    var header_buffer: [headers_max_size]u8 = undefined;
    const options = std.http.Client.RequestOptions{ .server_header_buffer = &header_buffer };

    // Call the API endpoint
    var request = try client.open(std.http.Method.GET, url, options);
    defer request.deinit();
    _ = try request.send();
    _ = try request.finish();
    _ = try request.wait();

    // Check the HTTP return code
    if (request.response.status != std.http.Status.ok) {
        return error.WrongStatusResponse;
    }

    const body = try request.reader().readAllAlloc(allocator, body_max_size);
    return try std.json.parseFromSlice(nsq.Stat, allocator, body, .{});
}
