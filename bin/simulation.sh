#!/usr/bin/env bash

PID_FILE=server.pid

PID=$(cat "${PID_FILE}");

if [ -z "${PID}" ]; then
    echo "Process id for servers is written to location: {$PID_FILE}"
    go build ../server/
    go build ../client/
    go build ../cmd/
    ./server -sim=true -log_dir=. -log_level=debug -algorithm=paxos &
    echo $! >> ${PID_FILE}
else
    echo "Servers are already started in this folder."
    exit 0
fi
