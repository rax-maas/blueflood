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

import sys
import time
from optparse import OptionParser

try:
    import requests
except ImportError:
    raise ImportError('Missing dependency requests. ' +
                      'Please install it using pip.')


def _get_metrics_query_url(host, port, scheme, tenantId,
                           metricName, start, end, points):
    return scheme + '://' + host + ':' + port + '/v1.0/' + tenantId\
        + '/experimental/views/metric_data/' + metricName\
        + '?from=' + str(start) + '&to=' + str(end) + '&points=' + str(points)


def main():
    usage = 'usage: %prog \n' + \
            '--host=<host running blueflood> \n' + \
            '--port=<blueflood HTTP metrics query port> \n' + \
            '--tenant=<blueflood tenant id> \n' + \
            '--metric=<name of the metric to fetch data for> \n' + \
            '--from=<start timestamp (millis sinch epoch)> \n' + \
            '--to=<end timestamp (millis sinch epoch)> \n' + \
            '--points=<number of points to fetch>'

    parser = OptionParser(usage=usage)
    parser.add_option('--host', dest='host', help='Blueflood host')
    parser.add_option('--port', dest='port', help='HTTP query port')
    parser.add_option('--tenant', dest='tenantId', help='Tenant id')
    parser.add_option('--metric', dest='metricName', help='Metric to\
        fetch data for')
    parser.add_option('--from', dest='startTime', help='Start timestamp')
    parser.add_option('--to', dest='endTime', help='End timestamp')
    parser.add_option('--points', dest='points', help='Number of\
        points to fetch')

    (options, args) = parser.parse_args()
    if not options.host:
        options.host = 'localhost'
    if not options.port:
        options.port = '20000'
    if not options.tenantId:
        print(usage)
        sys.exit(1)
    if not options.metricName:
        print(usage)
        sys.exit(1)
    if not options.points:
        options.points = 100

    now = long(time.time() * 1000)
    if not options.startTime:
        options.startTime = now - 3 * 60 * 60 * 1000
    if not options.endTime:
        options.endTime = now

    url = _get_metrics_query_url(options.host, options.port, 'http',
                                 options.tenantId, options.metricName,
                                 options.startTime, options.endTime,
                                 options.points)
    print(url)

    try:
        r = requests.get(url)
        if (r.status_code == requests.codes.ok):
            print(r.content)
        else:
            print('Failed fetching metrics. HTTP status: %s' % r.status_code)
            if r.content is not None:
                print(r.content)
    except Exception, ex:
        print(ex)
        raise Exception('Cannot retrieve metrics from bluflood')

main()
