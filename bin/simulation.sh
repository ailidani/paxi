#!/usr/bin/env bash

PID_FILE=server.pid

PID=$(cat "${PID_FILE}");

if [ -z "${PID}" ]; then
    echo "Process id for servers is written to location: {$PID_FILE}"
    go build ../server/
    go build ../client/
    go build ../cmd/
    ./server -sim=true -log_dir=logs -log_level=info &
    echo $! >> ${PID_FILE}
    sleep 3
    ./client -id=1.1 -log_level=debug > c1 &
    echo $! >> ${PID_FILE}
    # ./client -id=2.1 -log_level=info > c2 &
    # echo $! >> ${PID_FILE}
    # ./client -id=3.1 -log_level=info > c3 &
    # echo $! >> ${PID_FILE}
else
    echo "Servers are already started in this folder."
    exit 0
fi
