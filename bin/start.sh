#!/usr/bin/env bash

master=127.0.0.1

PID_FILE=server.pid

PID=$(cat "${PID_FILE}");

if [ -z "${PID}" ]; then
    echo "Process id for servers is written to location: {$PID_FILE}"
    ./server -v=1 -log_dir=logs -sid $1 -nid $2 -master $master &
    echo $! >> ${PID_FILE}
else
    echo "Servers are already started in this folder."
    exit 0
fi
