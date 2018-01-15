#!/usr/bin/env bash

PID_FILE=server.pid

./server -log_dir=. -log_level=debug -id $1.$2 &
echo $! >> ${PID_FILE}
