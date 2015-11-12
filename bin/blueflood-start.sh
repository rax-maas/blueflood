#!/bin/bash
/usr/bin/java \
        -Dblueflood.config=file:./demo/local/config/blueflood.conf \
        -Dlog4j.configuration=file:./demo/local/config/blueflood-log4j.properties \
        -Xms1G \
        -Xmx1G \
        -Dcom.sun.management.jmxremote.authenticate=false \
        -Dcom.sun.management.jmxremote.ssl=false \
        -Djava.rmi.server.hostname=localhost \
        -Dcom.sun.management.jmxremote.port=9180 \
        -classpath ./blueflood-all/target/blueflood-all-*-jar-with-dependencies.jar com.rackspacecloud.blueflood.service.BluefloodServiceStarter 2>&1
