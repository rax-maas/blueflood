#!/bin/bash

WORKING_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

CP=${WORKING_DIR}/../blueflood-all/target/blueflood-all-2.0.0-SNAPSHOT-jar-with-dependencies.jar:${WORKING_DIR}/../blueflood-core/target/classes

java -classpath $CP com.rackspacecloud.blueflood.tools.ops.Migration $@
