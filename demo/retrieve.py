#!/usr/bin/python
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

import os
import sys
from optparse import OptionParser

try:
    import simplejson as json
except ImportError:
    import json

# Import thrift generated code
sys.path.append(os.path.join(os.getcwd(), 'gen-py'))
from telescope import RollupServer
from telescope.ttypes import *
from telescope.constants import *

try:
    from thrift import Thrift
    from thrift.transport import TSocket
    from thrift.transport import TTransport
    from thrift.protocol import TBinaryProtocol
except ImportError, e:
    raise Exception("Missing dependency thrift. Install using pip.")


def _get_value(thrift_metric):
    if thrift_metric.valueI64 is not None:
        return thrift_metric.valueI64
    elif thrift_metric.valueI32 is not None:
        return thrift_metric.valueI32
    elif thrift_metric.valueDbl is not None:
        return thrift_metric.valueDbl
    elif thrift_metric.valueStr is not None:
        return thrift_metric.valueStr


def get_metrics(client, account, metric, from_time, to_time, points):
    rollups = client.GetRollupByPoints(account + ',' + metric, from_time,
                                       to_time, points)
    output = []
    rollup = {}
    for rollupThrift in rollups.metrics:
        rollup['timestamp'] = rollupThrift.timestamp
        rollup['numPoints'] = rollupThrift.numPoints
        rollup['average'] = _get_value(rollupThrift.average)
        rollup['variance'] = _get_value(rollupThrift.average)
        rollup['min'] = _get_value(rollupThrift.min)
        rollup['max'] = _get_value(rollupThrift.max)
        rollup['unit'] = rollups.unit
        output.append(rollup)

    return output


def _validate_args(args):
    if args.account is None:
        raise Exception('Missing argument \'account\'')
    elif args.metric is None:
        raise Exception('Missing arg \'metric\'')
    elif args.start is None:
        raise Exception('Missing arg \'start\'')
    elif args.end is None:
        raise Exception('Missing arg \'end\'')
    elif args.points is None:
        raise Exception('Missing arg \'points\'')


def main():
    usage = 'usage: %prog \n' + \
        '--host=<blueflood host> \n' +\
        '--port=<blueflood query port> \n' +\
        '--account=<account id> \n' +\
        '--metric=<metric name> \n' +\
        '--from=<start time (milliseconds since epoch)> \n' +\
        '--to=<end time (milliseconds since epoch)> \n' +\
        '--points=<number of points>'

    parser = OptionParser(usage=usage)
    parser.add_option('--host', dest='host',
                      help='Blueflood host', type='string')
    parser.add_option('--port', dest='port',
                      help='Blueflood query port')
    parser.add_option('--account', dest='account',
                      help='Blueflood account id', type='string')
    parser.add_option('--metric', dest='metric',
                      help='Name of the metric', type='string')
    parser.add_option('--from', dest='start',
                      help='start time (milliseconds since epoch)',
                      type='long')
    parser.add_option('--to', dest='end',
                      help='end time (milliseconds since epoch)',
                      type='long')
    parser.add_option('--points', dest='points',
                      help='end time (milliseconds since epoch)',
                      type='int')

    (options, args) = parser.parse_args()

    if not options.host:
        options.host = 'localhost'

    if not options.port:
        options.port = 2467

    try:
        _validate_args(options)
    except Exception, e:
        print(e)
        print(usage)
        sys.exit(1)

    try:
        transport = TSocket.TSocket(options.host, options.port)
        transport = TTransport.TFramedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        client = RollupServer.Client(protocol)
        transport.open()
        results = get_metrics(client, options.account, options.metric,
                              options.start, options.end, options.points)
        prettyjsondata = json.dumps(results, indent=4, separators=(',', ': '))
        print(prettyjsondata)
        transport.close()
    except Thrift.TException, tx:
        print(tx)
        raise Exception('Thrift exception retreiving metrics')

main()
