#!/usr/bin/env python

import argparse
from os import environ
import datetime
import requests
import time


debug = False


def get_unix_time(dt):
    return int(time.mktime(dt.timetuple()))


def print_request(request):
    print('Sending:')
    print('    {} {}'.format(request.method, request.path_url))
    for name, value in request.headers.iteritems():
        print('    {}: {}'.format(name, value))
    if request.body:
        print('')
        print('    {}'.format(request.body))
    print('')


def print_response(response):
    print('Received:')
    print('    {} {}'.format(response.status_code, response.reason))
    for name, value in response.headers.iteritems():
        print('    {}: {}'.format(name, value))
    print('')
    if response.text:
        print('    {}'.format(response.text))


def make_ingest_request(base_url, token, tenant, metric_name, unit, value,
                        ttl_seconds=None, collection_time=None):
    if ttl_seconds is None:
        ttl_seconds = 172800
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
    if token:
        request.headers['X-Auth-Token'] = token
    preq = request.prepare()

    if debug:
        print_request(preq)

    session = requests.session()
    response = session.send(preq)

    if debug:
        print('')
        print_response(response)

    success = 200 <= response.status_code < 300
    print(response.text)
    return success


def make_search_query_request(base_url, token, tenant, query):

    url = "{}/v2.0/{}/metrics/search?query={}".format(base_url, tenant, query)

    request = requests.Request('GET', url)
    if token:
        request.headers['X-Auth-Token'] = token
    preq = request.prepare()

    if debug:
        print_request(preq)

    session = requests.session()
    response = session.send(preq)

    if debug:
        print('')
        print_response(response)

    success = 200 <= response.status_code < 300
    print(response.text)
    return success


def main():

    parser = argparse.ArgumentParser()

    BF_URL = environ.get('BF_URL', None)
    BF_TOKEN = environ.get('BF_TOKEN', None)

    parser.add_argument('--debug', action='store_true',
                        help='Display additional info.')
    parser.add_argument('--url', type=str, action='store', default=BF_URL,
                        help='The endpoint to send HTTP requests to. Defaults '
                             'to the value of the BF_URL envvar.')
    parser.add_argument('--token', type=str, action='store', default=BF_TOKEN,
                        help='The authentication token of the account making '
                             'the request. Defaults to the value of the '
                             'BF_TOKEN envvar.')

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

    global debug
    debug = args.debug

    print('args: {}'.format(args))

    if args.command == 'ingest':
        base_url = args.url
        if not base_url:
            print('Error: No url specified.')
            exit(1)

        success = make_ingest_request(base_url, args.token, args.tenant,
                                      args.metric_name, args.unit, args.value,
                                      args.ttl_seconds, args.collection_time)

        exit(0 if success else 1)

    else:
        print('Unknown command "{}"'.format(args.command))

if __name__ == '__main__':
    main()
