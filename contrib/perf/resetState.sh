#!/bin/bash

echo "Killing all java processes, get rid of running blueflood and cassandra."
killall java
sleep 10
echo "Clearing out data and commitlog directories"
rm -rf /var/cassdata/*
rm -rf /var/casssyscommitlog/*
echo "Starting cass..."
/opt/cassandra/bin/cassandra
sleep 15
echo "Loading schema..."
/opt/cassandra/bin/cassandra-cli -f /opt/blueflood/src/cassandra/cli/load.script -h 127.0.0.1 -p 9160
echo "DONE"
