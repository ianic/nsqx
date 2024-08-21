#!/bin/bash -e

cd ~/Code/nsql
zig build -Doptimize=ReleaseFast

./zig-out/bin/nsql 2>stat &
nsqd_pid=$!

sh -c 'while pkill -usr1 nsql; do sleep 10; done' &
stat_pid=$!

sh -c 'while true ; do ~/Code/go/nsq/bench/bench_writer/bench_writer ; done' &
writer_pid=$!
sh -c 'while true ; do ~/Code/go/nsq/bench/bench_reader/bench_reader ; done' &
reader_pid=$!


cleanup() {
    kill $writer_pid
    kill $reader_pid
    kill $stat_pid
    kill $nsqd_pid
    rm -f nsqd/*.dat
}
trap cleanup INT TERM EXIT

wait $nsqd_pid
