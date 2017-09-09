#!/usr/bin/env bash

PID_FILE=server.pid

PID=$(cat "${PID_FILE}");

if [ -z "${PID}" ]; then
    echo "Process id for servers is written to location: {$PID_FILE}"
    go build ../master/
    go build ../server/
    go build ../client/
    go build ../cmd/
    ./master -n 6 $1 &
    echo $! > ${PID_FILE}
    sleep 3
    ./server -v=2 -log_dir=logs -sid 1 -nid 1 &
    echo $! >> ${PID_FILE}
    ./server -v=2 -log_dir=logs -sid 1 -nid 2 &
	echo $! >> ${PID_FILE}
	./server -v=2 -log_dir=logs -sid 2 -nid 1 &
	echo $! >> ${PID_FILE}
	./server -v=2 -log_dir=logs -sid 2 -nid 2 &
	echo $! >> ${PID_FILE}
	./server -v=2 -log_dir=logs -sid 3 -nid 1 &
	echo $! >> ${PID_FILE}
	./server -v=2 -log_dir=logs -sid 3 -nid 2 &
	echo $! >> ${PID_FILE}
    #sleep 5
    #./client -sid 1 -nid 1 -T 1 -k 1000 -c 50
else
    echo "Servers are already started in this folder."
    exit 0
fi
