#!/bin/bash
IP=`ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{print $1}'`
echo $IP

if [ -z $1 ]; then
  SEED_IP=$IP
else
  SEED_IP=$1
fi

# Set addresses in cassandra config files
sed -i "s/%seed_ip%/$SEED_IP/g" /opt/config/cassandra.yaml
sed -i "s/%listen_ip%/$IP/g" /opt/config/cassandra.yaml
sed -i "s/%listen_ip%/$IP/g" /opt/config/cassandra-env.sh
sed -i "s/%listen_ip%/$IP/g" /opt/config/blueflood.conf

# Copy cassandra configs to /etc/cassandra
cp /opt/config/cassandra-env.sh /etc/cassandra/
cp /opt/config/cassandra.yaml /etc/cassandra/

#export MAX_HEAP_SIZE="512M"
#export HEAP_NEWSIZE="256M"

cassandra
sleep 5;

# Setup cassandra schema
if [ -z $1 ]; then
  cassandra-cli -h ${IP} -f /opt/init.cql
fi

# Copy blueflood configs to /opt/blueflood
cp /opt/config/blueflood.conf /opt/blueflood/
cp /opt/config/blueflood-log4j.properties /opt/blueflood/

# Build blueflood
cd /opt/blueflood/
mvn package -P all-modules
cp /opt/blueflood/blueflood-all/target/blueflood-all-2.0.0-SNAPSHOT-jar-with-dependencies.jar /opt/blueflood/

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
