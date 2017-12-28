#!/usr/bin/env bash

PID_FILE=server.pid

PID=$(cat "${PID_FILE}");

if [ -z "${PID}" ]; then
    echo "Process id for servers is written to location: {$PID_FILE}"
    go build ../master/
    go build ../server/
    go build ../client/
    go build ../cmd/
    ./master -n 9 -transport chan -algorithm paxos &
    echo $! > ${PID_FILE}
    sleep 3
    ./server -log_dir=logs -master "127.0.0.1" -simulation &
    echo $! >> ${PID_FILE}
else
    echo "Servers are already started in this folder."
    exit 0
fi
