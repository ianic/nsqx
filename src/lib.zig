const std = @import("std");
const assert = std.debug.assert;

pub const Stat = struct {
    const Topic = struct {
        topic_name: []const u8,
        depth: usize = 0,
        message_count: usize,
        message_bytes: usize,
        paused: bool,
        channels: []Channel,
        e2e_processing_latency: struct { count: usize = 0 } = .{},
        store: Store,
    };
    const Channel = struct {
        channel_name: []const u8,
        depth: usize,
        in_flight_count: usize,
        deferred_count: usize,
        message_count: usize,
        requeue_count: usize,
        timeout_count: usize,
        client_count: usize,
        clients: []Client,
        paused: bool,
        sequence: u64,
        e2e_processing_latency: struct { count: usize = 0 } = .{},
    };
    const Client = struct {
        client_id: []const u8,
        hostname: []const u8,
        socket: u32,
        user_agent: []const u8,
        version: []const u8 = "V2",
        remote_address: []const u8,
        state: u8 = 3,
        ready_count: usize,
        in_flight_count: usize,
        message_count: usize,
        finish_count: usize,
        requeue_count: usize,
        connect_ts: usize,
        msg_timeout: usize,
        pub_counts: []PubCount = &.{},
    };
    const PubCount = struct {
        topic: []const u8,
        count: usize,
    };
    const Store = struct {
        last_page: u32,
        last_sequence: u64,
        message_count: usize,
        message_bytes: usize,
        capacity: usize,
        page_size: usize,
        pages_count: usize,
        pages: []Page,
    };
    const Page = struct {
        no: u32,
        first_sequence: u64,
        message_count: usize,
        message_bytes: usize,
        references: u32,
        capacity: usize,
    };

    version: []const u8 = "0.1.0",
    health: []const u8 = "OK",
    start_time: u64,
    topics: []Topic,
    store: struct {
        pages: u32,
        bytes: u64,
    },
    producers: []Client,
};
