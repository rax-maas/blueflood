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
if [ "$INIT_CASSANDRA" != false ]; then
  echo "Setting up initial Cassandra schema"
  CASSCOUNTER=0
  trap "exit" INT
  while [[ $CASSCOUNTER -lt 180 ]];  #Wait for 180 seconds for cassandra to get ready.
  do
    let CASSCOUNTER=CASSCOUNTER+2
    if nc -zw1 $CASSANDRA_HOST 9042 > /dev/null || nc -zw1 $CASSANDRA_HOST 9160 > /dev/null; then
      echo "Connected to Cassandra at $CASSANDRA_HOST"
      break
    elif [[ $CASSCOUNTER == 180 ]]; then
      echo "Error connecting to Cassandra at $CASSANDRA_HOST"
      exit 1
    else
      echo "Waiting for Cassandra..."
    fi
    sleep 2
  done

  export CASSANDRA_HOSTS="$CASSANDRA_HOST:9160"
  export CASSANDRA_BINXPORT_HOSTS="$CASSANDRA_HOST:9042"
  if [ -n "$ROLLUP_KEYSPACE" ] && [ "$ROLLUP_KEYSPACE" != "DATA" ]; then
    sed -i "s/\"DATA\"/\"$ROLLUP_KEYSPACE\"/g" blueflood.cdl
  fi

  cqlsh $CASSANDRA_HOST -f blueflood.cdl
fi

######## Connecting to Elasticsearch #######
if [ "$INIT_ELASTICSEARCH" != false ]; then
  echo "Setting up Elasticsearch indexes"
  ESCOUNTER=0
  trap "exit" INT
  while [[ $ESCOUNTER -lt 120 ]]; do #Wait for 120 seconds for elasticsearch to get ready.
    let ESCOUNTER=ESCOUNTER+2
    if nc -zw1 $ELASTICSEARCH_HOST 9200 > /dev/null || nc -zw1 $ELASTICSEARCH_HOST 9300 > /dev/null; then
      echo "Connected to Elasticsearch at $ELASTICSEARCH_HOST"
      break
    elif [[ $ESCOUNTER == 120 ]]; then
      echo "Error connecting to Elasticsearch at $ELASTICSEARCH_HOST"
      exit 2
    else
      echo "Waiting for ElasticSearch..."
    fi
    sleep 2
  done

  export ELASTICSEARCH_HOSTS="$ELASTICSEARCH_HOST:9300"
  export ELASTICSEARCH_HOST_FOR_REST_CLIENT="$ELASTICSEARCH_HOST:9200"

  /ES-Setup/init-es.sh -u $ELASTICSEARCH_HOST:9200 -r false
fi

if [ -z "$BLUEFLOOD_CONF_LOCATION" ]; then
  BLUEFLOOD_CONF_LOCATION="./blueflood.conf"
  printenv > blueflood.conf
fi

if [ -z "$BLUEFLOOD_LOG4J_CONF_LOCATION" ]; then
  BLUEFLOOD_LOG4J_CONF_LOCATION="./blueflood-log4j.properties"
  : ${BLUEFLOOD_LOG_LEVEL=INFO}
  cat > blueflood-log4j.properties << EOL
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-5p %-20.20c{1}:%-3L - %m%n
log4j.logger.com.rackspacecloud.blueflood=$BLUEFLOOD_LOG_LEVEL
log4j.logger.httpclient.wire.header=WARN
log4j.logger.httpclient.wire.content=WARN
log4j.logger.org.apache.http.client.protocol=INFO
log4j.logger.org.apache.http.wire=INFO
log4j.logger.org.apache.http.impl=INFO
log4j.logger.org.apache.http.headers=INFO
log4j.rootLogger=INFO, console
EOL
fi

dbg=""
suspend="n"
[ "$DEBUG_JAVA_SUSPEND" = true ] && suspend="y"
[ "$DEBUG_JAVA" = true ] && dbg="-agentlib:jdwp=transport=dt_socket,server=y,suspend=$suspend,address=5005"

echo "Starting Blueflood"
exec java \
        $dbg \
        -Dblueflood.config=file://$BLUEFLOOD_CONF_LOCATION \
        -Dlog4j.configuration=file://$BLUEFLOOD_LOG4J_CONF_LOCATION \
        -Xms$MIN_HEAP_SIZE \
        -Xmx$MAX_HEAP_SIZE \
        -Dcom.sun.management.jmxremote.authenticate=false \
        -Dcom.sun.management.jmxremote.ssl=false \
        -Djava.rmi.server.hostname=localhost \
        -Dcom.sun.management.jmxremote.port=9180 \
        -classpath blueflood-all-jar-with-dependencies.jar com.rackspacecloud.blueflood.service.BluefloodServiceStarter 2>&1
