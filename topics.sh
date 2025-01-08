#!/bin/bash
set -e
# set -u
# set -m

cd ~/Code/nsqx
zig build

# default lookupd
~/Code/go/nsq/apps/nsqlookupd/nsqlookupd >> tmp/lookupd1 2>&1 &
lookupd_pid=$!

# starting another lookupd
~/Code/go/nsq/apps/nsqlookupd/nsqlookupd \
    -http-address localhost:4163 \
    -tcp-address localhost:4162  \
    -broadcast-address localhost >> tmp/lookupd2 2>&1 &
lookupd2_pid=$!

# admin
~/Code/go/nsq/apps/nsqadmin/nsqadmin \
    -lookupd-http-address localhost:4161 \
    -lookupd-http-address localhost:4163 >> tmp/nsqadmin 2>&1 &
admin_pid=$!

# deamon
cd ~/Code/nsqx
./zig-out/bin/nsqx &
nsqx_pid=$!

sleep 1
cd ~/Code/nsqx/test
go run topics_producer.go >> ~/Code/nsqx/tmp/producer 2>&1

go run topics_consumer.go >> ~/Code/nsqx/tmp/consumer 2>&1 &
consumer_pid=$!

cleanup() {
    set +e
    kill $nsqx_pid >> /dev/null
    kill $admin_pid >> /dev/null
    kill $lookupd_pid >> /dev/null
    kill $lookupd2_pid >> /dev/null
    kill $consumer_pid >> /dev/null
}
trap cleanup INT TERM #EXIT

wait $nsqx_pid
wait $lookupd_pid
