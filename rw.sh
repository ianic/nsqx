#!/bin/bash


# reference: https://stackoverflow.com/questions/3601515/how-to-check-if-a-variable-is-set-in-bash
POSITIONAL_ARGS=()
while [[ $# -gt 0 ]]; do
  case $1 in
    -s|--silent)
      SILENT=YES
      shift # past argument
      ;;
    -c|--clear)
      CLEAR=YES
      shift # past argument
      ;;
    -*|--*)
      echo "Unknown option $1"
      exit 1
      ;;
    *)
      POSITIONAL_ARGS+=("$1") # save positional arg
      shift # past argument
      ;;
  esac
done

# set -u
set -m

killall nsql  >> /dev/null 2>&1
killall nsqadmin  >> /dev/null 2>&1
killall nsqlookup >> /dev/null 2>&1

set -e

cd ~/Code/nsql
zig build -Doptimize=ReleaseFast
#zig build -Doptimize=ReleaseSmall


cd ~/Code/go/nsq/bench/bench_writer/
go build
cd ~/Code/go/nsq/bench/bench_reader/
go build
cd ~/Code/nsql

if [ ! -z ${CLEAR+x} ]; then # if clear is set
    rm -f ./tmp/nsql.dump ./tmp/sub_bench
fi

# sudo valgrind --tool=callgrind ./zig-out/bin/nsql \
./zig-out/bin/nsql \
    --data-path ./tmp \
    --max-mem=16G \
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



if [ -z ${SILENT+x} ]; then
    sleep 1
    workers=6
    sh -c "while ~/Code/go/nsq/bench/bench_writer/bench_writer --size 200 --runfor 10s --workers $workers ; do : ; done" &
    writer_pid=$!
    sh -c "while ~/Code/go/nsq/bench/bench_reader/bench_reader --runfor 10s --workers $workers ; do : ; done" &
    reader_pid=$!
fi

~/Code/go/nsq/build/nsqadmin \
    --nsqd-http-address localhost:4151 \
    --graphite-url 'http://localhost:8080' \
    --statsd-prefix "nsq" \
    --statsd-interval 10s \
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
~/Code/go/nsq/bench/bench_writer/bench_writer --size 200 --runfor 1s

~/Code/go/nsq/bench/bench_reader/bench_reader --size 200 --runfor 10s

for i in $(seq 1 32); do
    echo running $i writer workers
    ~/Code/go/nsq/bench/bench_writer/bench_writer --size 200 --runfor 1s --workers $i

    curl -s "http://localhost:4151/topic/empty?topic=sub_bench"
    #curl -s "http://localhost:4151/channel/empty?topic=sub_bench&channel=ch"
    #curl -s "http://localhost:4151/channel/delete?topic=sub_bench&channel=ch"
    # ~/Code/go/nsq/bench/bench_reader/bench_reader --size 200 --runfor 2s
done


for i in $(seq 1 32); do
    echo running $i writer workers
    sh -c "~/Code/go/nsq/bench/bench_writer/bench_writer --size 200 --runfor 1s --workers $i" &
    ~/Code/go/nsq/bench/bench_reader/bench_reader --size 200 --runfor 1s --workers $i

    curl -s "http://localhost:4151/topic/empty?topic=sub_bench"
    curl -s "http://localhost:4151/channel/empty?topic=sub_bench&channel=ch"
    # curl -s "http://localhost:4151/channel/delete?topic=sub_bench&channel=ch"
done

workers=6
sh -c "while ~/Code/go/nsq/bench/bench_reader/bench_reader --runfor 10s --workers $workers ; do : ; done" &
for (( i=1; i<=1024*1024; i=i*2 )); do
    echo size $i
    ~/Code/go/nsq/bench/bench_writer/bench_writer --size $i --runfor 1s --workers $workers
done


curl -s http://localhost:4151/metric/broker
curl -s http://localhost:4151/metric/mem
curl -s http://localhost:4151/metric/io
watch -n1 "curl -s 'http://localhost:4151/metric/broker'  | jq"


~/Code/go/nsq/bench/bench_writer/bench_writer --size 200 --runfor 5s --workers 6
while ~/Code/go/nsq/bench/bench_reader/bench_reader --runfor 1s --workers 6; do : ; done
