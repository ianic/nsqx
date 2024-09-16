#!/bin/bash
set -e
# set -u
# set -m

cd ~/Code/nsql
zig build


~/Code/go/nsq/apps/nsqlookupd/nsqlookupd >> tmp/lookupd1 2>&1 &
lookupd_pid=$!

# starting another lookupd
~/Code/go/nsq/apps/nsqlookupd/nsqlookupd \
    -http-address localhost:4163 \
    -tcp-address localhost:4162  \
    -broadcast-address localhost >> tmp/lookupd2 2>&1 &
lookupd2_pid=$!

~/Code/go/nsq/apps/nsqadmin/nsqadmin \
    -lookupd-http-address localhost:4161 \
    -lookupd-http-address localhost:4163 >> tmp/nsqadmin 2>&1 &
admin_pid=$!

cd ~/Code/nsql
./zig-out/bin/nsql &
nsql_pid=$!

sleep 1
cd ~/Code/nsql/test
go run topics_producer.go >> ~/Code/nsql/tmp/producer 2>&1

go run topics_consumer.go >> ~/Code/nsql/tmp/consumer 2>&1 &
consumer_pid=$!

cleanup() {
    set +e
    kill $nsql_pid >> /dev/null
    kill $admin_pid >> /dev/null
    kill $lookupd_pid >> /dev/null
    kill $lookupd2_pid >> /dev/null
    kill $consumer_pid >> /dev/null
}
trap cleanup INT TERM #EXIT

wait $nsql_pid
wait $lookupd_pid
