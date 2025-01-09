# nsqxd

[nsqd](https://nsq.io/components/nsqd.html) daemon written in Zig. It is Linux only, because it uses io_uring for input/output.

Limitations/difference from nsqd:
* Linux only
* requires Linux kernel 6.0 or newer
* in memory only, no diskqueue
* no tls implementation 
* no compression
* doesn't measure end to end processing latency

In other aspects functions like nsqd. Implements [nsq tcp](https://nsq.io/clients/tcp_protocol_spec.html) and [http](https://nsq.io/components/nsqd.html#http-api) protocol (except the Go specific /debug/pprof endpoints). All the same client can be used to connect to. Works with nsqlookup's,  nsqadmin, statsd in the same way.

## Performance

Using [bench](https://github.com/nsqio/nsq/tree/master/bench) from nsq project. nsqd is configured to not use diskqueue raising it's mem-queue-size above max number of messages used in test.  

nsqd:
```
PUB: [bench_writer] 2025/01/09 18:58:23 duration: 10s - 527.127mb/s - 2763665.089ops/s - 0.362us/op messages: 27638800
SUB: [bench_reader] 2025/01/09 18:58:33 duration: 10s - 129.623mb/s - 679596.744ops/s - 1.471us/op messages: 6800786
```

nsqdx:
```
PUB: [bench_writer] 2025/01/09 19:08:50 duration: 12s - 2064.445mb/s - 10823639.233ops/s - 0.092us/op messages: 129885400
SUB: [bench_reader] 2025/01/09 19:09:00 duration: 10s - 2149.133mb/s - 11267647.191ops/s - 0.089us/op messages: 112745333
```

nsqdx performs roughly 4 times better in writes and more than 16 time better in reads.


Using bench from the project to read and write in the same time gives throughput of almost 5Gb/s on my system (Intel i9-13900, I226-V NIC). Both nsqxd, reader and writer are running on the same host.

```
[bench_writer] 2025/01/09 19:13:51 duration: 10s - 2409.497mb/s - 12632702.142ops/s - 0.079us/op messages: 126328400
[bench_reader] 2025/01/09 19:13:51 duration: 10s - 2408.409mb/s - 12626996.800ops/s - 0.079us/op messages: 126330200
[bench_writer] 2025/01/09 19:14:01 duration: 10s - 2427.462mb/s - 12726890.058ops/s - 0.079us/op messages: 127270600
[bench_reader] 2025/01/09 19:14:01 duration: 10s - 2427.336mb/s - 12726233.717ops/s - 0.079us/op messages: 127280800
[bench_writer] 2025/01/09 19:14:11 duration: 10s - 2433.169mb/s - 12756813.976ops/s - 0.078us/op messages: 127570600
[bench_reader] 2025/01/09 19:14:11 duration: 10s - 2432.899mb/s - 12755395.118ops/s - 0.078us/op messages: 127582711
[bench_writer] 2025/01/09 19:14:21 duration: 10s - 2395.150mb/s - 12557481.585ops/s - 0.080us/op messages: 125576200
[bench_reader] 2025/01/09 19:14:21 duration: 10s - 2395.069mb/s - 12557059.634ops/s - 0.080us/op messages: 125590489
[bench_writer] 2025/01/09 19:14:31 duration: 10s - 2419.875mb/s - 12687111.974ops/s - 0.079us/op messages: 126872400
[bench_reader] 2025/01/09 19:14:31 duration: 10s - 2419.806mb/s - 12686752.558ops/s - 0.079us/op messages: 126889600
[bench_writer] 2025/01/09 19:14:41 duration: 10s - 2351.639mb/s - 12329362.387ops/s - 0.081us/op messages: 123295000
[bench_reader] 2025/01/09 19:14:41 duration: 10s - 2351.526mb/s - 12328770.982ops/s - 0.081us/op messages: 123317759
[bench_writer] 2025/01/09 19:14:51 duration: 10s - 2382.419mb/s - 12490738.464ops/s - 0.080us/op messages: 124908600
[bench_reader] 2025/01/09 19:14:51 duration: 10s - 2382.622mb/s - 12491801.432ops/s - 0.080us/op messages: 124940041
```
