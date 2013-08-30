#!/usr/bin/env python
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
# limitations under the License.

import time
import uuid
import random
from optparse import OptionParser

try:
    import simplejson as json
except ImportError:
    import json

try:
    import requests
except ImportError:
    raise ImportError('Missing dependency requests. ' +
                      'Please install it using pip.')


def _generate_metrics_data(tenantId, metricName):
    data = []
    # Blueflood understands millis since epoch only
    now = long(time.time() * 1000)
    # Publish metrics with older timestamps (2 hrs before current time)
    startTimestamp = now - 2 * 60 * 60 * 1000
    endTimestamp = startTimestamp
    for i in range(100):
        metric = {}
        metric['collectionTime'] = endTimestamp
        metric['metricName'] = metricName
        metric['metricValue'] = random.randint(1, 100)
        metric['ttlInSeconds'] = 2 * 24 * 60 * 60  # 2 days
        metric['unit'] = 'seconds'
        data.append(metric)
        endTimestamp += 30 * 1000  # 30s spaced metric samples

    return data, startTimestamp, endTimestamp


def _get_metrics_url(host, port, scheme, tenantId):
    return scheme + '://' + host + ':' + port + '/v1.0/'\
        + tenantId + '/experimental/metrics'


def main():
    usage = 'usage: %prog \n' + \
            '--host=<host running blueflood> \n' + \
            '--port=<blueflood HTTP metrics ingestion port>'
    parser = OptionParser(usage=usage)
    parser.add_option('--host', dest='host', help='Blueflood host')
    parser.add_option('--port', dest='port', help='HTTP ingestion port')

    (options, args) = parser.parse_args()
    if not options.host:
        options.host = 'localhost'
    if not options.port:
        options.port = '19000'

    tenantId = 'ac' + str(uuid.uuid1())
    metricName = 'met.' + str(uuid.uuid1())

    (payload, start, end) = _generate_metrics_data(tenantId, metricName)
    prettyjsondata = json.dumps(payload, indent=4, separators=(',', ': '))
    print(prettyjsondata)

    url = _get_metrics_url(options.host, options.port, 'http', tenantId)
    print(url)

    try:
        print('Writing metrics for tenant: %s, metric name: %s,\
            start: %d, end: %d' % (tenantId, metricName, start, end))
        r = requests.post(url, data=json.dumps(payload))
        print('Response from server %s' % (r))
        print('To retrive the generated data with retrieve.py script, use the following command (assuming port number 20000):')
        print('')
        print('./retrieve.py --host %s --port 20000 --metric %s --tenant %s --from %s --to %s --points 100' \
            % (options.host, metricName, tenantId, start - 100000000, end + 100000000))
        print('')
    except Exception, ex:
        print(ex)
        raise Exception('Cannot ingest metrics into bluflood')

main()

