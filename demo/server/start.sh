#!/bin/bash
IP="$(ifconfig | grep 'inet 172' | awk '{print $2}')"
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

while ! grep -i 'listening for CQL clients' <(tail -100 /var/log/cassandra/system.log); do
  echo " ** Waiting for Cassandra..."
  sleep 1
done

# Setup cassandra schema
if [ -z "$1" ]; then
  echo " ** Cassandra is up, creating schema"
  cqlsh ${IP} -f /src/init.cql
fi

exec /usr/bin/java \
        -Dblueflood.config=file:///src/config/blueflood.conf \
        -Dlog4j.configuration=file:///src/config/blueflood-log4j.properties \
        -Xms1G \
        -Xmx1G \
        -Dcom.sun.management.jmxremote.authenticate=false \
        -Dcom.sun.management.jmxremote.ssl=false \
        -Djava.rmi.server.hostname=${IP} \
        -Dcom.sun.management.jmxremote.port=9180 \
        -classpath /src/blueflood/blueflood-all/target/blueflood-all-*-jar-with-dependencies.jar \
        com.rackspacecloud.blueflood.service.BluefloodServiceStarter
