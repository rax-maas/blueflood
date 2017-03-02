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


def make_single_plot_request(base_url, token, tenant, metric_name, from_, to,
                             resolution=None):
    if resolution is None:
        resolution = 'FULL'

    url = "{}/v2.0/{}/views/{}?from={}&to={}&resolution={}".format(
        base_url, tenant, metric_name, from_, to, resolution)

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


def make_multi_plot_request(base_url, token, tenant, metric_names, from_, to,
                            resolution=None):
    if resolution is None:
        resolution = 'FULL'

    url = "{}/v2.0/{}/views?from={}&to={}&resolution={}".format(
        base_url, tenant, from_, to, resolution)

    payload = list(metric_names)

    request = requests.Request('POST', url, json=payload)
    request.headers['Content-Type'] = 'application/json'
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

    BF_TOKEN = environ.get('BF_TOKEN', None)

    parser.add_argument('--debug', action='store_true',
                        help='Display additional info.')
    parser.add_argument('--token', type=str, action='store', default=BF_TOKEN,
                        help='The authentication token of the account making '
                             'the request. Defaults to the value of the '
                             'BF_TOKEN envvar.')

    subs = parser.add_subparsers(help='subparsers?', dest='command')

    BF_INGEST_URL = environ.get('BF_INGEST_URL', None)
    ingest_url_help = 'The endpoint to send HTTP ingest requests to. ' \
                      'Defaults to the value of the BF_INGEST_URL envvar.'

    ingest_sub = subs.add_parser('ingest', help='Send metrics to blueflood.')
    ingest_sub.add_argument('tenant')
    ingest_sub.add_argument(metavar='metric-name', dest='metric_name')
    ingest_sub.add_argument('unit',
                            choices=('minutes', 'hours', 'days', 'months',
                                     'years', 'decades'))
    ingest_sub.add_argument('value', type=int)
    ingest_sub.add_argument('--ttl-seconds', type=int, default=172800)
    ingest_sub.add_argument('--collection-time')
    ingest_sub.add_argument('--url', type=str, action='store',
                            default=BF_INGEST_URL, help=ingest_url_help)

    BF_QUERY_URL = environ.get('BF_QUERY_URL', None)
    query_url_help = 'The endpoint to send HTTP query requests to. Defaults ' \
                     'to the value of the BF_QUERY_URL envvar.'

    search_sub = subs.add_parser('search', help='Search for things.')
    search_sub.add_argument('tenant')
    search_sub.add_argument('query')
    search_sub.add_argument('--url', type=str, action='store',
                            default=BF_QUERY_URL, help=query_url_help)

    single_plot_sub = subs.add_parser('single-plot', help='Query for things')
    single_plot_sub.add_argument('tenant')
    single_plot_sub.add_argument(metavar='metric-name', dest='metric_name')
    single_plot_sub.add_argument(metavar='from-milliseconds',
                                 dest='from_milliseconds', type=int)
    single_plot_sub.add_argument(metavar='to-milliseconds',
                                 dest='to_milliseconds', type=int)
    single_plot_sub.add_argument('--resolution', choices=['FULL'])
    single_plot_sub.add_argument('--url', type=str, action='store',
                                 default=BF_QUERY_URL, help=query_url_help)

    multi_plot_sub = subs.add_parser('multi-plot', help='Query for things')
    multi_plot_sub.add_argument('tenant')
    multi_plot_sub.add_argument(metavar='metric-name', dest='metric_name')
    multi_plot_sub.add_argument(metavar='from-milliseconds',
                                dest='from_milliseconds', type=int)
    multi_plot_sub.add_argument(metavar='to-milliseconds',
                                dest='to_milliseconds', type=int)
    multi_plot_sub.add_argument('--resolution', choices=['FULL'])
    multi_plot_sub.add_argument('--url', type=str, action='store',
                                default=BF_QUERY_URL, help=query_url_help)

    args = parser.parse_args()

    global debug
    debug = args.debug

    if debug:
        print('args: {}'.format(args))
        print('BF_INGEST_URL={}'.format(BF_INGEST_URL))
        print('BF_QUERY_URL={}'.format(BF_QUERY_URL))

    base_url = args.url
    if not base_url:
        print('Error: No url specified.')
        exit(1)

    success = False
    if args.command == 'ingest':
        success = make_ingest_request(base_url, args.token, args.tenant,
                                      args.metric_name, args.unit, args.value,
                                      args.ttl_seconds, args.collection_time)
    elif args.command == 'search':
        success = make_search_query_request(base_url, args.token, args.tenant,
                                            args.query)
    elif args.command == 'single-plot':
        success = make_single_plot_request(base_url, args.token, args.tenant,
                                           args.metric_name,
                                           args.from_milliseconds,
                                           args.to_milliseconds,
                                           args.resolution)
    elif args.command == 'multi-plot':
        success = make_multi_plot_request(base_url, args.token, args.tenant,
                                          [args.metric_name],
                                          args.from_milliseconds,
                                          args.to_milliseconds,
                                          args.resolution)
    else:
        print('Unknown command "{}"'.format(args.command))
        exit(1)

    exit(0 if success else 1)

if __name__ == '__main__':
    main()
