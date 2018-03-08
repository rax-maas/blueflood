#!/bin/bash

BASE_DIR=`pwd`

# Create Cassandra and Elasticsearch containers to run the integration test
docker-compose -f contrib/blueflood-docker-compose/docker-compose-integration-test.yml up -d

# Copy Cassandra schema file into Cassandra container
docker cp src/cassandra/cli/load.cdl cassandra-node:/tmp/load.cdl

# Sleep seconds to make sure cassandra and elasticsearch are up and running
sleep 60

# Load Cassandra schema
docker-compose -f contrib/blueflood-docker-compose/docker-compose-integration-test.yml exec cassandra cqlsh -f /tmp/load.cdl

# Elasticsearch setup
cd $BASE_DIR/blueflood-elasticsearch/src/main/resources 
./init-es.sh

cd $BASE_DIR/blueflood-elasticsearch/src/test/resources 
./init-es-test.sh

cd $BASE_DIR

# Run test
mvn clean package -Pall-modules

# Remove Cassandra and Elasticsearch containers
docker-compose -f contrib/blueflood-docker-compose/docker-compose-integration-test.yml down
