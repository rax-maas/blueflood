#! /usr/bin/python
# Licensed to Rackspace under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# Rackspace licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License."

# This script retrieve the metric locators from the cassandra
# and count them across the shards to get the total metrics in blueflood.

import pycassa
import sys


def __is_more_available(len_fetched, page_size):
    return (len_fetched >= page_size)


def get_metrics_for_shard(shard, cf):
    page_size = 100  # Pycassa has an implicit max limit of 100
    start = ''
    metrics = []

    while True:
        # given a shard,
        # retrieve the next page of keys and add to metrics array
        batch = cf.get(shard, column_start=start,
                       column_finish='', column_count=page_size).keys()
        metrics.extend(batch)

        if not __is_more_available(len(batch), page_size):
            # there are no more columns left
            break

        start = batch[len(batch) - 1]

    return metrics


def get_metrics_for_shards(shards, server):
    pool = pycassa.ConnectionPool('DATA', server_list=[server])
    cf = pycassa.ColumnFamily(pool, 'metrics_locator')
    metrics_for_shards = {}

    for shard in range(128):
        # retrieve metrics locators for each shard
        metrics_for_shards[shard] = get_metrics_for_shard(shard, cf)

    return metrics_for_shards


def find_duplicates(shards, metrics_for_shards):
    for i in shards:
        metrics = metrics_for_shards[i]
        for j in shards:
            if i == j:
                continue
            metrics_other = metrics_for_shards[j]
            intersection = [val for val in metrics if val in metrics_other]
            for item in intersection:
                print 'Shard: %d, Other Shard: %d, item: %s' % (i, j, item)


def print_stats_for_metrics(metrics_for_shards):
    output = []
    total_metrics_in_bf = 0
    # count the number of metrics in each shard and add them up
    for shard in metrics_for_shards:
        count = len(metrics_for_shards[shard])
        total_metrics_in_bf += count

    output.append('%s %s %s %s' % ('metric', 'total_metrics_in_bf',
                                   'int', total_metrics_in_bf))
    print 'status ok bf_metrics_per_shard_check'
    print '\n' . join(output)


def main(server):
    shards = range(128)
    metrics_for_shards = get_metrics_for_shards(shards, server)
    print_stats_for_metrics(metrics_for_shards)
    # find_duplicates(shards, metrics_for_shards)

if __name__ == "__main__":
    main(sys.argv[1])
