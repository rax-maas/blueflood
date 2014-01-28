#!/bin/bash
IP=`ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{print $1}'`
echo $IP

if [ -z $1 ]; then
  SEED_IP=$IP
else
  SEED_IP=$1
fi

# Set addresses in cassandra config files
sed -i "s/%seed_ip%/$SEED_IP/g" /src/config/cassandra.yaml
sed -i "s/%listen_ip%/$IP/g" /src/config/cassandra.yaml
sed -i "s/%listen_ip%/$IP/g" /src/config/cassandra-env.sh
sed -i "s/%listen_ip%/$IP/g" /src/config/blueflood.conf

# Copy cassandra configs to /etc/cassandra
cp /src/config/cassandra-env.sh /etc/cassandra/
cp /src/config/cassandra.yaml /etc/cassandra/

#export MAX_HEAP_SIZE="512M"
#export HEAP_NEWSIZE="256M"

cassandra
sleep 5;

# Setup cassandra schema
if [ -z $1 ]; then
  cassandra-cli -h ${IP} -f /src/init.cql
fi

# Copy blueflood configs to /src/blueflood
cp /src/config/blueflood.conf /src/blueflood/
cp /src/config/blueflood-log4j.properties /src/blueflood/

# Build blueflood
cd /src/blueflood/
mvn package -P all-modules
cp /src/blueflood/blueflood-all/target/blueflood-all-2.0.0-SNAPSHOT-jar-with-dependencies.jar /src/blueflood/

/usr/bin/java \
        -Dblueflood.config=file:blueflood.conf \
        -Dlog4j.configuration=file:blueflood-log4j.properties \
        -Xms1G \
        -Xmx1G \
        -Dcom.sun.management.jmxremote.authenticate=false \
        -Dcom.sun.management.jmxremote.ssl=false \
        -Djava.rmi.server.hostname=${IP} \
        -Dcom.sun.management.jmxremote.port=9180 \
        -classpath blueflood-all-2.0.0-SNAPSHOT-jar-with-dependencies.jar com.rackspacecloud.blueflood.service.BluefloodServiceStarter
