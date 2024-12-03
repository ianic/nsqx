#!/bin/bash
# set -u
set -m

killall nsql  >> /dev/null 2>&1
killall nsqadmin  >> /dev/null 2>&1
killall nsqlookup >> /dev/null 2>&1

set -e

cd ~/Code/nsql
zig build -Doptimize=ReleaseFast
#zig build

cd ~/Code/go/nsq/bench/bench_writer/
go build
cd ~/Code/go/nsq/bench/bench_reader/
go build



cd ~/Code/nsql
rm -f ./tmp/nsql.dump ./tmp/sub_bench

./zig-out/bin/nsql \
    --data-path ./tmp \
    --max-mem=4G \
    --statsd-address localhost \
    --statsd-prefix "nsq" \
    --statsd-udp-packet-size 8k \
    > tmp/nsql 2>&1 &
nsqd_pid=$!

# ./zig-out/bin/nsql --data-path ./tmp --max-mem=40G > tmp/nsql 2>&1 &
# sudo valgrind --tool=callgrind ./zig-out/bin/nsql --data-path ./tmp > tmp/nsql 2>&1 &
# ~/Code/go/nsq/apps/nsqd/nsqd --mem-queue-size=100000000 > tmp/nsql 2>&1 &


#sh -c 'while pkill -usr1 nsql; do sleep 10; done' &
#stat_pid=$!

sleep 1

sh -c 'while ~/Code/go/nsq/bench/bench_writer/bench_writer --size 200 --runfor 10s ; do : ; done' &
writer_pid=$!
sh -c 'while ~/Code/go/nsq/bench/bench_reader/bench_reader --size 200 --runfor 10s; do : ; done' &
reader_pid=$!

~/Code/go/nsq/build/nsqadmin \
    --nsqd-http-address localhost:4151 \
    --graphite-url 'http://localhost:8080' \
    --statsd-prefix "nsq" \
    --statsd-interval 5s \
    --statsd-counter-format 'stats.%s' \
        >> /dev/null 2>&1 &
admin_pid=$!

cleanup() {
    set +e

    killall bench_reader >> /dev/null
    killall bench_writer >> /dev/null
    # kill $stat_pid >> /dev/null
    kill $admin_pid >> /dev/null

    sleep 1
    kill $nsqd_pid
}
trap cleanup INT TERM #EXIT


# pstree -A -p $$

# echo wait $nsqd_pid
wait $nsqd_pid
# echo wait done


sudo chown ianic callgrind.out.* && mv callgrind.out.* ./tmp
exit 0

cd ~/Code/go/nsq/bench/bench_writer/
go build
~/Code/go/nsq/bench/bench_writer/bench_writer --size 200 --runfor 2s

~/Code/go/nsq/bench/bench_reader/bench_reader --size 200 --runfor 10s
