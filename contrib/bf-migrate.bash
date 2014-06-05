#!/bin/bash

#  So you want to migrate data from one cluster to another? Use this.
#  
#  Usage:
#    bf-migrate
#      -src           [required] source cluster specified via host:port:keyspace.
#      -dst           [required] destination cluster specified via host:port:keyspace.
#      -cf            [required] which column family to migrate.
#      -ttl           [optional] time to live (in seconds) for migrated data. defaults to 5x the TTL for the column family.
#      -from          [optional] millis since epoch (or ISO 6801 datetime) of when to start migrating data. defaults to one year ago.
#      -to            [optional] millis since epoch (or ISO 6801 datetime) of when to stop migrating data. defaults to right now.
#      -batchsize     [optional] number of rows to read per query. default=100
#      -limit         [optional] maximum number of keys to migrate. default=MAX_INT.
#      -skip          [optional] number of keys to skip before starting to migrate data. default=0
#      -readthreads   [optional] number of threads to use to read data. default=1
#      -writethreads  [optional] number of threads to use to write data. default=1
#      -verify        [optional] forces verifying that 0.5% of data is copied.
#      -discover      [optional] will utilize other cassandra nodes as they are discovered.
#      
#  `skip` and `limit` are independent of each other. That is, if you set skip=N and limit=M, the first N rows are skipped
#  and the next M rows are migrated. Skipping still forces a read, so skipping large numbers of rows will not be
#  performant.

WORKING_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

CP=${WORKING_DIR}/../blueflood-all/target/blueflood-all-2.0.0-SNAPSHOT-jar-with-dependencies.jar:${WORKING_DIR}/../blueflood-core/target/classes

java -classpath $CP com.rackspacecloud.blueflood.tools.ops.Migration $@
