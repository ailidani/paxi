#!/usr/bin/env bash

PID_FILE=server.pid

PID=$(cat "${PID_FILE}");

if [ -z "${PID}" ]; then
    echo "Process id for servers is written to location: {$PID_FILE}"
    go build ../master/
    go build ../server/
    go build ../client/
    go build ../cmd/
    ./server -log_dir=logs -id 1.1 &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -id 1.2 &
	echo $! >> ${PID_FILE}
	./server -log_dir=logs -id 1.3 &
	echo $! >> ${PID_FILE}
	./server -log_dir=logs -id 2.1 &
	echo $! >> ${PID_FILE}
	./server -log_dir=logs -id 2.2 &
	echo $! >> ${PID_FILE}
	./server -log_dir=logs -id 2.3 &
	echo $! >> ${PID_FILE}
	./server -log_dir=logs -id 3.1 &
	echo $! >> ${PID_FILE}
	./server -log_dir=logs -id 3.2 &
	echo $! >> ${PID_FILE}
	./server -log_dir=logs -id 3.3 &
	echo $! >> ${PID_FILE}
    # sleep 5
    # ./client -id 1.1 > c1 &
    # echo $! >> ${PID_FILE}
    # ./client -id 2.1 > c2 &
    # echo $! >> ${PID_FILE}
else
    echo "Servers are already started in this folder."
    exit 0
fi
