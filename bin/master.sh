#!/usr/bin/env bash

PID_FILE=master.pid

PID=$(cat "${PID_FILE}");

if [ -z "${PID}" ]; then
    echo "Process id for master is written to location: {$PID_FILE}"
    ./master "$@" &
    echo $! > ${PID_FILE}
else
    echo "Master are already started in this folder."
    exit 0
fi
