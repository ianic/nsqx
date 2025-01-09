FROM alpine:latest

EXPOSE 4150 4151

RUN mkdir -p /data
RUN mkdir -p /app
WORKDIR      /app

COPY zig-out/bin/nsqxd /app/

ENTRYPOINT ["/app/nsqxd"]
CMD ["--data-path", "/data"]
