#!/bin/bash
# set -e
# set -u
set -m

cd ~/Code/nsql
zig build -Doptimize=ReleaseFast

cd ~/Code/go/nsq/bench/bench_writer/
go build
cd ~/Code/go/nsq/bench/bench_reader/
go build

cd ~/Code/nsql
./zig-out/bin/nsql 2>stat &
nsqd_pid=$!

sh -c 'while pkill -usr1 nsql; do sleep 10; done' &
stat_pid=$!

sh -c 'while ~/Code/go/nsq/bench/bench_writer/bench_writer ; do : ; done' &
writer_pid=$!
sh -c 'while ~/Code/go/nsq/bench/bench_reader/bench_reader ; do : ; done' &
reader_pid=$!

cleanup() {
    # echo cleanup
    # echo writer $writer_pid
    # echo reader $reader_pid
    # echo nsqd $nsqd_pid

    killall bench_reader >> /dev/null
    killall bench_writer >> /dev/null
    #kill $writer_pid
    #kill $reader_pid
    kill $stat_pid >> /dev/null

    sleep 1
    # echo nsql $nsqd_pid
    kill $nsqd_pid
    #killall nsql
}
trap cleanup INT TERM #EXIT


# pstree -A -p $$

# echo wait $nsqd_pid
wait $nsqd_pid
# echo wait done
