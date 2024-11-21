#!/bin/bash

# set -u
set -m

killall nsql  >> /dev/null 2>&1
killall nsqadmin  >> /dev/null 2>&1
killall nsqlookupd >> /dev/null 2>&1
killall lookup >> /dev/null 2>&1
killall nsq_tail >> /dev/null 2>&1

set -e

cd ~/Code/nsql
zig build

# lookup for monitoring registrations
cd ~/Code/nsql && zig run test/lookup.zig > tmp/registrations 2>&1 &
lookupd3_pid=$!

# default lookupd
~/Code/go/nsq/build/nsqlookupd > tmp/lookupd1 2>&1 &
lookupd_pid=$!

# starting another lookupd
~/Code/go/nsq/build/nsqlookupd \
    -http-address localhost:4163 \
    -tcp-address localhost:4162  \
    -broadcast-address localhost > tmp/lookupd2 2>&1 &
lookupd2_pid=$!

# admin
~/Code/go/nsq/build/nsqadmin \
    -lookupd-http-address localhost:4161 \
    -lookupd-http-address localhost:4163 > tmp/nsqadmin 2>&1 &
admin_pid=$!

# daemon
cd ~/Code/nsql
./zig-out/bin/nsql \
  --data-path ./tmp \
  --statsd-prefix "" \
  --lookupd-tcp-address localhost:4160 \
  --lookupd-tcp-address localhost:4162 \
  --lookupd-tcp-address localhost:4164 \
  > tmp/nsql 2>&1 &
# --io-entries 16 \
# --statsd-address localhost:8125 \
nsql_pid=$!

sleep 1

nsq_tail -nsqd-tcp-address localhost:4150 -channel channel1 -topic topic1  | sed "s/^/[channel1] /" &

nsq_tail -nsqd-tcp-address localhost:4150 -channel channel2 -topic topic1  | sed "s/^/[channel2 conusmer1] /" &
nsq_tail -nsqd-tcp-address localhost:4150 -channel channel2 -topic topic1  | sed "s/^/[channel2 consumer2] /" &

nsq_tail -nsqd-tcp-address localhost:4150 -channel channel3 -topic topic1  | sed "s/^/[channel3 conusmer1] /" &
nsq_tail -nsqd-tcp-address localhost:4150 -channel channel3 -topic topic1  | sed "s/^/[channel3 consumer2] /" &
nsq_tail -nsqd-tcp-address localhost:4150 -channel channel3 -topic topic1  | sed "s/^/[channel3 consumer3] /" &

cleanup() {
    set +e
    killall nsq_tail >> /dev/null 2>&1
    kill $admin_pid >> /dev/null
    kill $lookupd_pid >> /dev/null
    kill $lookupd2_pid >> /dev/null
    kill $lookupd3_pid >> /dev/null
    kill $nsql_pid >> /dev/null
}
trap cleanup INT TERM #EXIT

echo pids:
echo admin: $admin_pid
echo lookupd: $lookupd_pid $lookupd2_pid $lookupd3_pid
echo nsql: $nsql_pid

wait $nsql_pid
wait $lookupd_pid

exit 0

# for ti in $(seq 99); do
#     for ci in $(seq 9); do
#         nsq_tail -nsqd-tcp-address localhost:4150 -channel channel-$ci -topic topic-$ti -n 1 &
#         echo $ti-$ci  | to_nsq -nsqd-tcp-address localhost:4150 -topic topic-$ti
#     done
# done

for ti in $(seq 99); do
  for ci in $(seq 9); do
    curl "http://127.0.0.1:4151/channel/create?topic=topic-$ti&channel=channel-$ci"
  done
done

for ti in $(seq 99); do
  curl -X POST "http://127.0.0.1:4151/topic/delete?topic=topic-$ti"
  curl -X POST "http://127.0.0.1:4161/topic/delete?topic=topic-$ti"
  curl -X POST "http://127.0.0.1:4163/topic/delete?topic=topic-$ti"
done
