from __future__ import absolute_import

import re
import time
import requests
import json
import auth

try:
    from graphite_api.intervals import Interval, IntervalSet
    from graphite_api.node import LeafNode, BranchNode
except ImportError:
    from graphite.intervals import Interval, IntervalSet
    from graphite.node import LeafNode, BranchNode

# curl -XPOST -H "Accept: application/json, text/plain, */*" -H "Content-Type: application/x-www-form-urlencoded" 'http://127.0.0.1:8888/render' -d "target=rackspace.*.*.*.*.*.*.*.*.available&from=-6h&until=now&format=json&maxDataPoints=1552"

class TenantBluefloodFinder(object):

  def __init__(self, config=None):
    authentication_module = None
    if config is not None:
      if 'urls' in config['blueflood']:
        urls = config['blueflood']['urls']
      else:
        urls = [config['blueflood']['url'].strip('/')]
      tenant = config['blueflood']['tenant']
      if 'authentication_module' in config['blueflood']:
        authentication_module = config['blueflood']['authentication_module']
        authentication_class = config['blueflood']['authentication_class']
    else:
      from django.conf import settings
      urls = getattr(settings, 'BF_QUERY')
      tenant = getattr(settings, 'BF_TENANT')
      authentication_module = getattr(settings, 'BF_AUTHENTICATION_MODULE', None)
      authentication_class = getattr(settings, 'BF_AUTHENTICATION_CLASS', None)

    if authentication_module:
      module = __import__(authentication_module)
      class_ = getattr(module, authentication_class)
      bfauth = class_(config)
      auth.set_auth(bfauth)

    self.tenant = tenant
    self.bf_query_endpoint = urls[0]


  def find_nodes(self, query):
    try:
      query_depth = len(query.pattern.split('.'))
      #print 'DAS QUERY ' + str(query_depth) + ' ' + query.pattern
      client = Client(self.bf_query_endpoint, self.tenant)
      values = client.find_metrics(query.pattern)
      
      for obj in values:
        metric = obj['metric']
        parts = metric.split('.')
        metric_depth = len(parts)
        if metric_depth > query_depth:
          yield BranchNode('.'.join(parts[:query_depth]))
        else:
          yield LeafNode(metric, TenantBluefloodReader(metric, self.tenant, self.bf_query_endpoint))
    except Exception as e:
     print "Exception in Blueflood find_nodes: " 
     print e
     raise e

class TenantBluefloodReader(object):
  __slots__ = ('metric', 'tenant', 'bf_query_endpoint')
  supported = True

  def __init__(self, metric, tenant, endpoint):
    # print 'READER ' + tenant + ' ' + metric
    self.metric = metric
    self.tenant = tenant
    self.bf_query_endpoint = endpoint

  def get_intervals(self):
    # todo: make this a lot smarter.
    intervals = []
    millis = int(round(time.time() * 1000))
    intervals.append(Interval(0, millis))
    return IntervalSet(intervals)

  def fetch(self, start_time, end_time):
    # remember, graphite treats time as seconds-since-epoch. BF treats time as millis-since-epoch.
    if not self.metric:
      return ((start_time, end_time, 1), [])
    else:
      client = Client(self.bf_query_endpoint, self.tenant)
      values = client.get_values(self.metric, start_time, end_time)
      # value keys in order of preference.
      value_res_order = ['average', 'latest', 'numPoints']

      # determine the step
      minTime = 0x7fffffffffffffff
      maxTime = 0
      lastTime = 0
      step = 1
      value_arr = []
      for obj in values:
        timestamp = obj['timestamp'] / 1000
        step = timestamp - lastTime
        lastTime = timestamp
        minTime = min(minTime, timestamp)
        maxTime = max(maxTime, timestamp)
        present_keys = [_ for _ in value_res_order if _ in obj]
        if present_keys:
            value_arr.append(obj[present_keys[0]])

      time_info = (minTime, maxTime, step)
      return (time_info, value_arr)

SECONDS_IN_5MIN = 300
SECONDS_IN_20MIN = 1200
SECONDS_IN_60MIN = 3600
SECONDS_IN_240MIN = 14400
SECONDS_IN_1440MIN = 86400

class Client(object):
  def __init__(self, host, tenant):
    self.host = host
    self.tenant = tenant

  def make_request(self, url, payload, headers):
    if auth.is_active():
      headers['X-Auth-Token'] = auth.get_token(False)
    r = requests.get(url, params=payload, headers=headers)
    if r.status_code == 401 and auth.is_active():
      headers['X-Auth-Token'] = auth.get_token(True)
      r = requests.get(url, params=payload, headers=headers)
    return r


  def find_metrics(self, query):
    payload = {'query': query}
    headers = auth.headers()
    r = self.make_request("%s/v2.0/%s/metrics/search" % (self.host, self.tenant), payload, headers)
    if r.status_code != 200:
      print str(r.status_code) + ' in find_metrics ' + r.url + ' ' + r.text
      return []
    else:
      try:
        return r.json()
      except TypeError:
        # we need to parse the json ourselves.
        return json.loads(r.text)
      except ValueError:
        return ['there was an error']

  def get_values(self, metric, start, stop):
    # make an educated guess about the likely number of data points returned.
    num_points = (stop - start) / 60
    res = 'FULL'

    if num_points > 800:
      num_points = (stop - start) / SECONDS_IN_5MIN
      res = 'MIN5'
    if num_points > 800:
      num_points = (stop - start) / SECONDS_IN_20MIN
      res = 'MIN20'
    if num_points > 800:
      num_points = (stop - start) / SECONDS_IN_60MIN
      res = 'MIN60'
    if num_points > 800:
      num_points = (stop - start) / SECONDS_IN_240MIN
      res = 'MIN240'
    if num_points > 800:
      num_points = (stop - start) / SECONDS_IN_1440MIN
      res = 'MIN1440'

    payload = {
      'from': start * 1000,
      'to': stop * 1000,
      'resolution': res
    }
    #print 'USING RES ' + res
    headers = auth.headers()
    r = self.make_request("%s/v2.0/%s/views/%s" % (self.host, self.tenant, metric), payload, headers)
    if r.status_code != 200:
      print str(r.status_code) + ' in get_values ' + r.text
      return {'values': []}
    else:
      try:
        return r.json()['values']
      except TypeError:
        # parse that json yo
        return json.loads(r.text)['values']
      except ValueError:
        print 'ValueError in get_values'
        return {'values': []}
