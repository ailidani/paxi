#!/usr/bin/env bash

PID_FILE=server.pid

./server -log_dir=. -log_level=info -sid $1 -nid $2 -master $3 &
echo $! >> ${PID_FILE}
