#!/usr/bin/env bash

PID_FILE=server.pid

PID=$(cat "${PID_FILE}");

if [ -z "${PID}" ]; then
    echo "Process id for servers is written to location: {$PID_FILE}"
    go build ../server/
    go build ../client/
    go build ../cmd/
    rm logs/*.log
    rm *.err
    rm *.log
    ./server -log_dir=logs -log_level=debug -id 1.1 >1.1.log 2>1.1.err &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -log_level=debug -id 1.2 >1.2.log 2>1.2.err &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -log_level=debug -id 1.3 >1.3.log 2>1.3.err &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -log_level=debug -id 2.1 >2.1.log 2>2.1.err &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -log_level=debug -id 2.2 >2.2.log 2>2.2.err &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -log_level=debug -id 2.3 >2.3.log 2>2.3.err &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -log_level=debug -id 3.1 >3.1.log 2>3.1.err &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -log_level=debug -id 3.2 >3.2.log 2>3.2.err &
    echo $! >> ${PID_FILE}
    ./server -log_dir=logs -log_level=debug -id 3.3 >3.3.log 2>3.3.err &
    echo $! >> ${PID_FILE}
else
    echo "Servers are already started in this folder."
    exit 0
fi