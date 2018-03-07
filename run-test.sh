#!/bin/bash

BASE_DIR=`pwd`

# Elasticsearch setup
cd $BASE_DIR/blueflood-elasticsearch/src/main/resources 
./init-es.sh

cd $BASE_DIR/blueflood-elasticsearch/src/test/resources 
./init-es-test.sh

cd $BASE_DIR

# Run test
mvn clean package -Pall-modules
