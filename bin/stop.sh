#!/usr/bin/env bash

PID_FILE=server.pid

if [ ! -f "${PID_FILE}" ]; then
    echo "No server is running."
else
	while read pid; do
		if [ -z "${pid}" ]; then
			echo "No server is running."
		else
			kill -15 "${pid}"
			echo "Server with PID ${pid} shutdown."
    	fi
	done < "${PID_FILE}"
	rm "${PID_FILE}"
fi


PID_FILE=master.pid

if [ ! -f "${PID_FILE}" ]; then
    echo "No master is running."
else
	while read pid; do
		if [ -z "${pid}" ]; then
			echo "No master is running."
		else
			kill -15 "${pid}"
			echo "Master with PID ${pid} shutdown."
    	fi
	done < "${PID_FILE}"
	rm "${PID_FILE}"
fi

#rm logs/*

pkill -x client
