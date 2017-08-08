#!/usr/bin/env bash

master=127.0.0.1

./client -sid $1 -nid 1 -master $master -T 10 -k 1000 -r 1 -c 50
