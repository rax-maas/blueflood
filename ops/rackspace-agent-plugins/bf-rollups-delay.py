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

import pycassa
import sys
import time
import logging
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


def _get_server_list(servers_string):
    return [x.strip() for x in servers_string.split(',')]


def get_metrics_state_for_shards(shards, servers):
    pool = pycassa.ConnectionPool('DATA',
                                  server_list=_get_server_list(servers))
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


def print_stats_for_metrics_state(metrics_state_for_shards):
    delayed_slots = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    now = int(time.time() * 1000)

    for shard in metrics_state_for_shards:
        states_per_shard = metrics_state_for_shards[shard]
        for resolution in GRAN_MAPPINGS.keys():
            max_slots = GRAN_MAPPINGS[resolution]['max_slots']
            for slot in range(max_slots):
                last_active_key = ',' .join([resolution, str(slot), 'A'])
                rolled_up_at_key = ',' .join([resolution, str(slot), 'X'])
                last_active_timestamp = states_per_shard[last_active_key]
                rolled_up_at_timestamp = states_per_shard[rolled_up_at_key]
                current_slot = _get_slot_for_time(now, resolution)
                if (current_slot > slot
                        and rolled_up_at_timestamp < last_active_timestamp):
                    # if slot is not rolled up yet, delay measured in slots
                    delayed_slots[
                        resolution][shard][slot] = current_slot - slot

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

    for resol, delay in output.items():
        print 'metric %s float %f slots' % ('_'.join([resol, 'delay']), delay)


def main(servers):
    try:
        logging.basicConfig(format='%(asctime)s %(message)s',
                            filename='/tmp/bf-rollup.log', level=logging.DEBUG)
        shards = range(128)
        logging.debug('getting metrics state for shards')
        metrics_state_for_shards = get_metrics_state_for_shards(shards,
                                                                servers)
        print 'status ok bf_health_check'
        logging.debug('printing stats for metrics state')
        print_stats_for_metrics_state(metrics_state_for_shards)
        # find_duplicates(shards, metrics_for_shards)
    except Exception, ex:
        logging.exception(ex)
        print "status error", ex
        raise ex

if __name__ == "__main__":
    main(sys.argv[1])
