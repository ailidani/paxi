#!/usr/bin/env bash

PID_FILE=server.pid

./server -log_dir=logs -log_level=info -sid $1 -nid $2 -master $3 &
echo $! >> ${PID_FILE}
