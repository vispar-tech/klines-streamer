#!/bin/sh
set -e

chown -R streamer:streamer /usr/src/output /usr/src/logs 2>/dev/null || true
exec runuser -u streamer -- "$@"
