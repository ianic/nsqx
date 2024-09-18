#!/bin/bash

# set -u
# set -m

killall nsql  >> /dev/null 2>&1
killall nsqadmin  >> /dev/null 2>&1
killall nsqlookup >> /dev/null 2>&1
set -e

cd ~/Code/nsql
zig build

# lookup for monitoring registrations
cd ~/Code/nsql && zig run test/lookup.zig > tmp/registrations 2>&1 &


# default lookupd
~/Code/go/nsq/apps/nsqlookupd/nsqlookupd > tmp/lookupd1 2>&1 &
lookupd_pid=$!

# starting another lookupd
~/Code/go/nsq/apps/nsqlookupd/nsqlookupd \
    -http-address localhost:4163 \
    -tcp-address localhost:4162  \
    -broadcast-address localhost > tmp/lookupd2 2>&1 &
lookupd2_pid=$!

# admin
~/Code/go/nsq/apps/nsqadmin/nsqadmin \
    -lookupd-http-address localhost:4161 \
    -lookupd-http-address localhost:4163 > tmp/nsqadmin 2>&1 &
admin_pid=$!

# daemon
cd ~/Code/nsql
./zig-out/bin/nsql &
nsql_pid=$!

nsq_tail -nsqd-tcp-address localhost:4150 -channel channel1 -topic topic1  | sed "s/^/[channel1] /" &

nsq_tail -nsqd-tcp-address localhost:4150 -channel channel2 -topic topic1  | sed "s/^/[channel2 conusmer1] /" &
nsq_tail -nsqd-tcp-address localhost:4150 -channel channel2 -topic topic1  | sed "s/^/[channel2 consumer2] /" &

nsq_tail -nsqd-tcp-address localhost:4150 -channel channel3 -topic topic1  | sed "s/^/[channel3 conusmer1] /" &
nsq_tail -nsqd-tcp-address localhost:4150 -channel channel3 -topic topic1  | sed "s/^/[channel3 consumer2] /" &
nsq_tail -nsqd-tcp-address localhost:4150 -channel channel3 -topic topic1  | sed "s/^/[channel3 consumer3] /" &

cleanup() {
    set +e
    killall nsq_tila >> /dev/null 2>&1
    kill $nsql_pid >> /dev/null
    kill $admin_pid >> /dev/null
    kill $lookupd_pid >> /dev/null
    kill $lookupd2_pid >> /dev/null
}
trap cleanup INT TERM #EXIT

wait $nsql_pid
wait $lookupd_pid
