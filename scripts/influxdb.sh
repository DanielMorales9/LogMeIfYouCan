#!/bin/bash

influxd -config /usr/local/etc/influxdb.conf
echo "kill -9 $! # influxdb server"
