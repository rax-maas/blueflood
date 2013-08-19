#!/bin/bash
IP=`ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{print $1}'`
echo $IP

if [ -z $1 ]; then
  SEED_IP=$IP
else
  SEED_IP=$1
fi

sed -i "s/%seed_ip%/$SEED_IP/g" /src/config/cassandra.yaml
sed -i "s/%listen_ip%/$IP/g" /src/config/cassandra.yaml
sed -i "s/%listen_ip%/$IP/g" /src/config/cassandra-env.sh

cp /src/config/* /etc/cassandra/

export MAX_HEAP_SIZE="512M"
export HEAP_NEWSIZE="256M"

cassandra
sleep 5;

if [ -z $1 ]; then
  cassandra-cli -h ${IP} -f /src/init.cql
fi

/bin/bash
