#!/bin/bash

for n in 1 5 10 50 100 200 300 400 500 600 700 800 900 1000 2000 3000 5000;
do
  for b in 1 5 10 20 30 40 50 75 100 125 150 200 250 500;
  do
    echo "Starting from the top. Batch size: $b Metrics $n"
    ./resetState.sh
    sleep 5
    echo "starting bf"
    java \
      -Dblueflood.config=file:/opt/blueflood/blueflood.conf \
      -Dlog4j.configuration=file:/opt/blueflood/blueflood-log4j.properties \
      -Xms1G \
      -Xmx8G \
      -Dcom.sun.management.jmxremote.authenticate=false \
      -Dcom.sun.management.jmxremote.ssl=false \
      -Dcom.sun.management.jmxremote.port=62391 \
      -classpath /opt/blueflood/blueflood.jar com.rackspacecloud.blueflood.service.BluefloodServiceStarter 2>&1 > /opt/blueflood/log &
    echo "started bf"
    sleep 5
    echo "Warming up blueflood"
    node test.js -n 200 -p 1 -i 10 -b 10 --id warmup -d 120 -r 1 | tee -a warmup.log
    sleep 5
    echo "Testing for real"
    node test.js -n $n -p 1 -i 10 -b $b --id test -d 240 -r 18 | tee -a realResults.log
    sleep 10
  done
done
