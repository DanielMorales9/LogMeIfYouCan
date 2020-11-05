#!/bin/bash

pg_ctl -D /usr/local/var/postgresql@11 start
echo "kill -9 $! # timescaledb server"
