#!/usr/bin/env bash

######## Connecting to Cassandra and loading Blueflood's schema #######
while :
do
	nc -z $CASSANDRA_PORT_9160_TCP_ADDR 9160 > /dev/null
	if [ $? == 0 ]
		then
                echo "Connected to Cassandra at $CASSANDRA_PORT_9160_TCP_ADDR"
		break
	else
		echo "Waiting for Cassandra..."
	fi
	sleep 2	
done

echo "CASSANDRA_HOSTS=$CASSANDRA_PORT_9160_TCP_ADDR:9160" >> blueflood.conf

cqlsh $CASSANDRA_PORT_9042_TCP_ADDR -f blueflood.cdl

######## Connecting to Elasticsearch #######
while :
do
        nc -z $ELASTICSEARCH_PORT_9300_TCP_ADDR 9300 > /dev/null
        if [ $? == 0 ]
                then
                echo "Connected to Elasticsearch at $ELASTICSEARCH_PORT_9300_TCP_ADDR"
                break
        else
                echo "Waiting for ElasticSearch..."
        fi
        sleep 2 
done

echo "ELASTICSEARCH_HOSTS=$ELASTICSEARCH_PORT_9300_TCP_ADDR:9300" >> blueflood.conf

cd ES-Setup
./init-es.sh $ELASTICSEARCH_PORT_9200_TCP_ADDR:9200
cd ..

/usr/bin/java \
        -Dblueflood.config=file:./blueflood.conf \
        -Dlog4j.configuration=file:./blueflood-log4j.properties \
        -Xms1G \
        -Xmx1G \
        -Dcom.sun.management.jmxremote.authenticate=false \
        -Dcom.sun.management.jmxremote.ssl=false \
        -Djava.rmi.server.hostname=localhost \
        -Dcom.sun.management.jmxremote.port=9180 \
        -classpath blueflood-all-2.0.0-SNAPSHOT-jar-with-dependencies.jar com.rackspacecloud.blueflood.service.BluefloodServiceStarter 2>&1
