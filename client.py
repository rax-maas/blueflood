#!/usr/bin/env python

import argparse
from os import environ
import datetime
import requests
import time


def get_unix_time(dt):
    return int(time.mktime(dt.timetuple()))


def main():

    parser = argparse.ArgumentParser()

    BF_URL = environ.get('BF_URL', None)
    BF_TOKEN = environ.get('BF_TOKEN', None)

    parser.add_argument('--debug', action='store_true',
                        help='Display additional info.')
    parser.add_argument('--url', type=str, action='store', default=BF_URL,
                        help='The endpoint to send HTTP requests to.')
    parser.add_argument('--token', type=str, action='store', default=BF_TOKEN,
                        help='The authentication token of the account making '
                             'the request')

    subs = parser.add_subparsers(help='subparsers?', dest='command')

    ingest_sub = subs.add_parser('ingest', help='Send metrics to blueflood.')
    ingest_sub.add_argument('tenant')
    ingest_sub.add_argument(metavar='metric-name', dest='metric_name')
    ingest_sub.add_argument('unit',
                            choices=('minutes', 'hours', 'days', 'months',
                                     'years', 'decades'))
    ingest_sub.add_argument('value', type=int)
    ingest_sub.add_argument('--ttl-seconds', type=int, default=172800)
    ingest_sub.add_argument('--collection-time')

    args = parser.parse_args()

    print('args: {}'.format(args))

    if args.command == 'ingest':
        base_url = args.url
        if not base_url:
            print('Error: No url specified.')
            exit(1)

        tenant = args.tenant
        metric_name = args.metric_name
        unit = args.unit
        value = args.value
        ttl_seconds = args.ttl_seconds
        collection_time = args.collection_time
        if collection_time is None:
            collection_time = datetime.datetime.now()

        url = '{}/v2.0/{}/ingest/multi'.format(base_url, tenant)

        payload = [{
            'tenantId': str(tenant),
            'metricName': metric_name,
            'unit': unit,
            'metricValue': value,
            'ttlInSeconds': ttl_seconds,
            'collectionTime': get_unix_time(collection_time) * 1000
        }]

        request = requests.Request('POST', url, json=payload)
        if args.token:
            request.headers['X-Auth-Token'] = args.token
        preq = request.prepare()

        if args.debug:
            print('Sending:')
            print('    {} {}'.format(preq.method, preq.path_url))
            for name, value in preq.headers.iteritems():
                print('    {}: {}'.format(name, value))
            if preq.body:
                print('')
                print('    {}'.format(preq.body))
            print('')

        session = requests.session()
        response = session.send(preq)

        if args.debug:
            print('')
            print('Received:')
            print('    {} {}'.format(response.status_code, response.reason))
            for name, value in response.headers.iteritems():
                print('    {}: {}'.format(name, value))
            print('')
            if response.text:
                print('    {}'.format(response.text))

        success = 200 <= response.status_code < 300
        print(response.text)
        exit(0 if success else 1)

        # print(payload_dict)
        # exit(0)

    else:
        print('Unknown command "{}"'.format(args.command))

if __name__ == '__main__':
    main()
