#!/bin/bash

#
# This is a simple script that can be used to start blueflood service.
# To run this, you must have built the blueflood package by running:
#     mvn clean package
#

# directory where the script is located
SCRIPTDIR=`pwd`/`dirname $0`

# top level source directory
TOPDIR=$SCRIPTDIR/..

if [ ! -d $TOPDIR/blueflood-all/target ]; then
    echo "Error: blueflood-all/target directory does not exist. Please run build:"
    echo "    mvn clean package"
    exit 1
fi

dbg=""
[ "$DEBUG" = true ] && dbg="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"

java \
        $dbg \
        -Dblueflood.config=file:///$TOPDIR/contrib/getting-started/blueflood.properties \
        -Dlog4j.configuration=file:///$TOPDIR/contrib/getting-started/log4j.properties \
        -Xms1G \
        -Xmx1G \
        -Dcom.sun.management.jmxremote.authenticate=false \
        -Dcom.sun.management.jmxremote.ssl=false \
        -Djava.rmi.server.hostname=localhost \
        -Dcom.sun.management.jmxremote.port=9180 \
        -classpath $TOPDIR/blueflood-all/target/blueflood-all-*-jar-with-dependencies.jar \
        com.rackspacecloud.blueflood.service.BluefloodServiceStarter 2>&1
