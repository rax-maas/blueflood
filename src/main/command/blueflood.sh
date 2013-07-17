#!/bin/bash
/usr/bin/java \
	-Dblueflood.config=file:blueflood.conf \
	-Dlog4j.configuration=file:blueflood-log4j.properties \
	-Xms2G \
	-Xmx2G \
	-Dcom.sun.management.jmxremote.authenticate=false \
	-Dcom.sun.management.jmxremote.ssl=false \
	-Djava.rmi.server.hostname=166.78.105.31 \
	-Dcom.sun.management.jmxremote.port=9180 \
	-classpath blueflood-1.0.0-SNAPSHOT.jar:blueflood-1.0.0-SNAPSHOT-jar-with-dependencies.jar com.cloudkick.blueflood.service.Main 2>&1
