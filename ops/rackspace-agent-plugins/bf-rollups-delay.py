#!/usr/bin/env python
'''Blueflood Rollup Delay'''
'''For each rollup level, lists the number of slots which need to processed by blueflood.  For the 5m range, one day is 288 slots.'''
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
#
# The following is an example 'criteria' for a Rackspace Monitoring Alarm:
#
# if (metric['metrics_5m_delay'] > 300 ) {
#     return new AlarmStatus( WARNING, 'metrics_5m_delay has > 300 slots waiting to be rolled up.' );
# }
#

import pycassa
import sys
import time
import logging
import os
import argparse
from collections import defaultdict

SLOTS = 4032
MILLIS_IN_BASE_SLOT = 300000
GRAN_MAPPINGS = {
    'metrics_5m': {'max_slots': 4032, 'milliseconds_in_slot': 300000},
    'metrics_20m': {'max_slots': 1008, 'milliseconds_in_slot': 1200000},
    'metrics_60m': {'max_slots': 336, 'milliseconds_in_slot': 3600000},
    'metrics_240m': {'max_slots': 84, 'milliseconds_in_slot': 14400000},
    'metrics_1440m': {'max_slots': 14, 'milliseconds_in_slot': 86400000}
}


def __is_more_available(len_fetched, page_size):
    return (len_fetched >= page_size)


def get_metrics_state_for_shard(shard, cf):
    page_size = 100  # Pycassa has an implicit max limit of 100
    start = ''
    states = {}

    while True:
        batch = cf.get(shard, column_start=start,
                       column_finish='', column_count=page_size)
        keys = batch.keys()
        states.update(batch)

        if not __is_more_available(len(batch), page_size):
            # there are no more columns left
            break

        start = keys[len(batch) - 1]

    return states


def get_metrics_state_for_shards(shards, servers):
    pool = pycassa.ConnectionPool('DATA',
                                  server_list=servers)
    cf = pycassa.ColumnFamily(pool, 'metrics_state')
    metrics_state_for_shards = {}

    for shard in shards:
        metrics_state_for_shards[shard] = get_metrics_state_for_shard(shard,
                                                                      cf)

    return metrics_state_for_shards


def _millis_to_slot(now_millis):
    return int((now_millis % (SLOTS * MILLIS_IN_BASE_SLOT))
               / MILLIS_IN_BASE_SLOT)


def _get_slot_for_time(now_millis, gran):
    full_slot = _millis_to_slot(now_millis)
    return (GRAN_MAPPINGS[gran]['max_slots'] * full_slot) / SLOTS


def print_stats_for_metrics_state(metrics_state_for_shards, print_res):
    delayed_slots = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    now = int(time.time() * 1000)

    for shard in metrics_state_for_shards:
        states_per_shard = metrics_state_for_shards[shard]
        for resolution in GRAN_MAPPINGS.keys():
            max_slots = GRAN_MAPPINGS[resolution]['max_slots']
            for slot in range(max_slots):
                last_active_key = ',' .join([resolution, str(slot), 'A'])
                rolled_up_at_key = ',' .join([resolution, str(slot), 'X'])
                last_active_timestamp = states_per_shard[last_active_key] if last_active_key in states_per_shard else 0
                rolled_up_at_timestamp = states_per_shard[rolled_up_at_key] if rolled_up_at_key in states_per_shard else 0
                current_slot = _get_slot_for_time(now, resolution)
                if (current_slot > slot
                        and rolled_up_at_timestamp < last_active_timestamp):
                    # if slot is not rolled up yet, delay measured in slots
                    delayed_slots[
                        resolution][shard][slot] = current_slot - slot

                    if ( print_res == resolution ):

                        print "shard: %4s        last_active_key: %19s        rolled_up_at_key: %19s  current_slot: %s slot: %s" % ( shard, last_active_key, rolled_up_at_key, current_slot, slot)
                        print "             last_active_timestamp: %19s  rolled_up_at_timestamp: %19s" % (last_active_timestamp, rolled_up_at_timestamp)
                        print "             last_active_timestamp: %19s  rolled_up_at_timestamp: %19s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.localtime( last_active_timestamp/1000)), time.strftime( '%Y-%m-%d %H:%M:%S', time.localtime(rolled_up_at_timestamp/1000)) )

                if ( print_res == resolution ):  
                    if (last_active_key not in states_per_shard):
                        print "WARNING: %s does not exist in shard %s" % (last_active_key, shard)
                    if (rolled_up_at_key not in states_per_shard):
                        print "WARNING: %s does not exist in shard %s" % (rolled_up_at_key, shard)
    output = {}
    for resolution in GRAN_MAPPINGS.keys():
        across_shards_most_delay = []
        for shard in delayed_slots[resolution].keys():
            max_delay = max(delayed_slots[resolution][shard].values())
            # print 'Most delay: %d, Res: %s' % (float(max_delay/(1000*60)),
            #                                    resolution)
            across_shards_most_delay.append(max_delay)

        if (len(across_shards_most_delay)):
            output[resolution] = max(across_shards_most_delay)
        else:
            output[resolution] = 0

    for resol, delay in output.items():
        print 'metric %s uint32 %u' % ('_'.join([resol, 'delay']), delay)


def main():
    parser = argparse.ArgumentParser(description='For each rollup level, lists the number of slots which need to '
                                                 'be processed by blueflood.  One day is approximately 300 slots.')
    parser.add_argument( '-s', '--servers', help='Cassandra server IP addresses, space separated', required=True, nargs="+")
    parser.add_argument( '-v', '--verbose', help='Print out the unprocessed slots for each shard, for the given granuality.  Default: metrics_5m',
                         required=False, nargs="?", choices=['metrics_5m', 'metrics_20m', 'metrics_60m', 'metrics_240m', 'metrics_1440m'], const='metrics_5m' )
    args = parser.parse_args()

    try:
        logfile = os.path.expanduser('~') + '/bf-rollup.log'
        logging.basicConfig(format='%(asctime)s %(message)s',
                            filename=logfile, level=logging.DEBUG)
        shards = range(128)
        logging.debug('getting metrics state for shards')
        metrics_state_for_shards = get_metrics_state_for_shards(shards,
                                                                args.servers)
        print 'status ok bf_health_check'
        logging.debug('printing stats for metrics state')
        print_stats_for_metrics_state(metrics_state_for_shards,
                                      args.verbose)
    except Exception, ex:
        logging.exception(ex)
        print "status error", ex
        raise ex

if __name__ == "__main__":
    main()
