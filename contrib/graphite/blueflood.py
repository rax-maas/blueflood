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

# YO!
# sudo pip install git+https://github.com/graphite-project/graphite-web.git
# PYTHONPATH=/opt/graphite/webapp python

# edit graphite-web/webapp/graphite/local_settings.py
#STORAGE_DIR='/opt/graphite/storage'
#STORAGE_FINDERS = (
#    'graphite.finders.standard.StandardFinder',
#    'graphite.finders.blueflood.TenantBluefloodFinder'
#)
#BF_QUERY = [
#  'http://127.0.0.1:19020'
#]
#
# #!/bin/bash
# GRAPHITE_ROOT=/home/gdusbabek/graphite-web
# PYTHONPATH=$PYTHONPATH:$GRAPHITE_ROOT/webapp:$PATH_TO_THIS_MODULE
# export GRAPHITE_ROOT
# $GRAPHITE_ROOT/bin/run-graphite-devel-server.py $GRAPHITE_ROOT
# # curl -XPOST -H "Accept: application/json, text/plain, */*" -H "Content-Type: application/x-www-form-urlencoded" 'http://127.0.0.1:8080/render' -d "from=-6h&until=now&target=rackspace.monitoring.entities.*.checks.agent.cpu.*.usage_average&format=json&maxDataPoints=1072"

class TenantBluefloodFinder(object):

  def __init__(self, config=None):
    if config is not None:
      if 'urls' in config['blueflood']:
        urls = config['blueflood']['urls']
      else:
        urls = [config['blueflood']['url'].strip('/')]
      tenant = config['blueflood']['tenant']
      authentication_module = config['blueflood']['authentication_module']
    else:
      from django.conf import settings
      urls = getattr(settings, 'BF_QUERY')
      tenant = getattr(settings, 'BF_TENANT')
      authentication_module = getattr(settings, 'BF_AUTHENTICATION_MODULE')

    if authentication_module:
      module = __import__(authentication_module)
      class_ = getattr(module, "BluefloodAuth")
      bfauth = class_(config)
      auth.setAuth(bfauth)

    self.tenant = tenant
    self.bf_query_endpoint = urls[0]


  def find_nodes(self, query):
    queryDepth = len(query.pattern.split('.'))
    #print 'DAS QUERY ' + str(queryDepth) + ' ' + query.pattern
    client = Client(self.bf_query_endpoint, self.tenant)
    values = client.findMetrics(query.pattern)

    for obj in values:
      metric = obj['metric']
      parts = metric.split('.')
      metricDepth = len(parts)
      if metricDepth > queryDepth:
        yield BranchNode('.'.join(parts[:queryDepth]))
      else:
        yield LeafNode(metric, TenantBluefloodReader(metric, self.tenant, self.bf_query_endpoint))

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

  def fetch(self, startTime, endTime):
    # remember, graphite treats time as seconds-since-epoch. BF treats time as millis-since-epoch.
    if not self.metric:
      return ((startTime, endTime, 1), [])
    else:
      client = Client(self.bf_query_endpoint, self.tenant)
      values = client.getValues(self.metric, startTime, endTime)

      # determine the step
      minTime = 0x7fffffffffffffff
      maxTime = 0
      lastTime = 0
      step = 1
      valueArr = []
      for obj in values:
        timestamp = obj['timestamp'] / 1000
        step = timestamp - lastTime
        lastTime = timestamp
        minTime = min(minTime, timestamp)
        maxTime = max(maxTime, timestamp)
        valueArr.append(obj['average'])

      time_info = (minTime, maxTime, step)
      return (time_info, valueArr)

SECONDS_IN_5MIN = 300
SECONDS_IN_20MIN = 1200
SECONDS_IN_60MIN = 3600
SECONDS_IN_240MIN = 14400
SECONDS_IN_1440MIN = 86400

class Client(object):
  def __init__(self, host, tenant):
    self.host = host
    self.tenant = tenant

  def findMetrics(self, query):
    payload = {'query': query}
    headers = auth.headers()
    if auth.isActive():
      headers['X-Auth-Token'] = auth.getToken()
    r = requests.get("%s/v2.0/%s/metrics/search" % (self.host, self.tenant), params=payload, headers=headers)
    if r.status_code is not 200:
      print str(r.status_code) + ' in findMetrics ' + r.text
      return []
    else:
      try:
        return r.json()
      except TypeError:
        # we need to parse the json ourselves.
        return json.loads(r.text)
      except ValueError:
        return ['there was an error']

  def getValues(self, metric, start, stop):
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
    if auth.isActive():
      headers['X-Auth-Token'] = auth.getToken()
    r = requests.get("%s/v2.0/%s/views/%s" % (self.host, self.tenant, metric), params=payload, headers=headers)
    if r.status_code is not 200:
      print str(r.status_code) + ' in getValues ' + r.text
      return {'values': []}
    else:
      try:
        return r.json()['values']
      except TypeError:
        # parse that json yo
        return json.loads(r.text)['values']
      except ValueError:
        print 'ValueError in getValues'
        return {'values': []}
