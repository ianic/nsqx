#!/bin/bash
set -e
# set -u
# set -m

cd ~/Code/nsql
zig build


~/Code/go/nsq/apps/nsqlookupd/nsqlookupd &
lookupd_pid=$!

# ~/Code/go/nsq/apps/nsqlookupd/nsqlookupd -http-address localhost:4163 -tcp-address localhost:4162 &
# lookupd_pid=$!

~/Code/go/nsq/apps/nsqadmin/nsqadmin -lookupd-http-address localhost:4161 -lookupd-http-address localhost:4163 &
admin_pid=$!

cd ~/Code/nsql
./zig-out/bin/nsql &
nsql_pid=$!


cleanup() {
    kill $nsql_pid >> /dev/null
    kill $admin_pid >> /dev/null
    kill $lookupd_pid >> /dev/null
}
trap cleanup INT TERM #EXIT

wait $nsqd_pid
wait $lookupd_pid
