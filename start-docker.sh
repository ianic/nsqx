#!/bin/bash

# remove containers
podman rm $(podman ps -a --filter ancestor=nsqxd -q)
# remove images
podman rmi $(podman images --filter=reference='nsqxd' --format "{{.ID}}")

set -e

# Build with musl libc when in Dockerfile we use alpine base image
# FROM alpine:latest
zig build -Doptimize=ReleaseFast -Dtarget=x86_64-linux-musl
podman build -t nsqxd:0.1.0 .
podman run --security-opt seccomp=unconfined -p 4150:4150 -p 4151:4151 localhost/nsqxd:0.1.0 --broadcast-address 127.0.0.1

exit 0

# To use glibc with mallinfo2 (glibc 2.35) use Dockerfile with Ubuntu 22.04 base image
# FROM ubuntu:22.04
zig build -Doptimize=ReleaseFast -Dtarget=x86_64-linux-gnu.2.35

podman pull docker.io/ubuntu:22.04
podman build -t nsqxd:0.1.0 .
podman run --security-opt seccomp=unconfined -p 4150:4150 -p 4151:4151 localhost/nsqxd:0.1.0 --broadcast-address 127.0.0.1

# To make github release
gh auth login
zig build -Doptimize=ReleaseFast -Dtarget=x86_64-linux-musl
mv zig-out/bin/nsqxd zig-out/bin/nsqxd-linux-musl
zig build -Doptimize=ReleaseFast -Dtarget=x86_64-linux-gnu.2.35
mv zig-out/bin/nsqxd zig-out/bin/nsqxd-linux-gnu.2.35
gh release create 0.1.0 zig-out/bin/nsqxd-linux-musl zig-out/bin/nsqxd-linux-gnu.2.35
