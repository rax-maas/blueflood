#!/bin/bash

if [[ -z $GRAPHITE_PREFIX ]] #default graphite prefix 
then        
export GRAPHITE_PREFIX=$HOSTNAME 
fi

######## If using BF with docker-compose / linking with C* and ES containers #######
if [[ -n "$CASSANDRA_PORT_9160_TCP_ADDR" ]]
then
export CASSANDRA_HOST="$CASSANDRA_PORT_9160_TCP_ADDR"
fi

if [[ -n "$ELASTICSEARCH_PORT_9300_TCP_ADDR" ]]
then
export ELASTICSEARCH_HOST="$ELASTICSEARCH_PORT_9300_TCP_ADDR"
fi

######## Connecting to Cassandra and loading Blueflood's schema #######
CASSCOUNTER=0
trap "exit" INT
while [[ $CASSCOUNTER -lt 180 ]];  #Wait for 180 seconds for cassandra to get ready.
do
        let CASSCOUNTER=CASSCOUNTER+2
	nc -z $CASSANDRA_HOST 9160 > /dev/null
	if [ $? == 0 ]
		then
                echo "Connected to Cassandra at $CASSANDRA_HOST"
		break
	elif [[ $CASSCOUNTER == 180 ]]
                then
                echo "Error connecting to Cassandra"
                exit 1
        else
		echo "Waiting for Cassandra..."
	fi 
	sleep 2	
done

export CASSANDRA_HOSTS="$CASSANDRA_HOST:9160"

cqlsh $CASSANDRA_HOST -f blueflood.cdl

######## Connecting to Elasticsearch #######
ESCOUNTER=0
trap "exit" INT
while [[ $ESCOUNTER -lt 120 ]];  #Wait for 120 seconds for elasticsearch to get ready.
do
        let ESCOUNTER=ESCOUNTER+2
        nc -z $ELASTICSEARCH_HOST 9300 > /dev/null
        if [ $? == 0 ]
                then
                echo "Connected to Elasticsearch at $ELASTICSEARCH_HOST"
                break
        elif [[ $ESCOUNTER == 120 ]]
                then
                echo "Error connecting to Elasticsearch"
                exit 2
        else
                echo "Waiting for ElasticSearch..."
        fi
        sleep 2 
done

export ELASTICSEARCH_HOSTS="$ELASTICSEARCH_HOST:9300"

cd ES-Setup
./init-es.sh -u $ELASTICSEARCH_HOST:9200
cd ..

printenv > blueflood.conf

cat > blueflood-log4j.properties << EOL
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %-20.20c{1}:%-3L - %m%n
log4j.logger.com.rackspacecloud.blueflood=$LOG_LEVEL # Change to DEBUG for more output from Blueflood
log4j.logger.httpclient.wire.header=WARN
log4j.logger.httpclient.wire.content=WARN
log4j.logger.org.apache.http.client.protocol=INFO
log4j.logger.org.apache.http.wire=INFO
log4j.logger.org.apache.http.impl=INFO
log4j.logger.org.apache.http.headers=INFO
log4j.rootLogger=INFO, console
EOL

/usr/bin/java \
        -Dblueflood.config=file:./blueflood.conf \
        -Dlog4j.configuration=file:./blueflood-log4j.properties \
        -Xms$MIN_HEAP_SIZE \
        -Xmx$MAX_HEAP_SIZE \
        -Dcom.sun.management.jmxremote.authenticate=false \
        -Dcom.sun.management.jmxremote.ssl=false \
        -Djava.rmi.server.hostname=localhost \
        -Dcom.sun.management.jmxremote.port=9180 \
        -classpath blueflood-all-2.0.0-SNAPSHOT-jar-with-dependencies.jar com.rackspacecloud.blueflood.service.BluefloodServiceStarter 2>&1
