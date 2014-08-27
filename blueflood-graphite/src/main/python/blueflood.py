from __future__ import absolute_import

import re
import time
import requests
import json
from django.conf import settings

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
#    'graphite.finders.blueflood.BluefloodFinder'
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

class TenantBluefloodFinder(object):

  def __init__(self):
    self.tenant = settings.BF_TENANT

  def find_nodes(self, query):
    #print 'FIND_NODES ' + query.pattern
    client = Client(settings.BF_QUERY[0], self.tenant)
    values = client.findMetrics(query.pattern)
    tree = Tree()
    for obj in values:
      #print 'ADDING ' + obj['metric']
      tree.add(obj['metric'])

    # split the query term into parts
    searchPattern = query.pattern
    node = Node('','')
    node.children = tree.roots

    # drill down into the tree
    # match as far as possible
    for term in searchPattern.split('.'):
      #print 'TERM ' + term
      for k in node.children.keys():
        #print 'CHECK ' + k
        if node.children[k].name == term:
          #print 'RECURSING WITH: ' + node.children[k].longName
          node = node.children[k]
          break

    if node.hasChildren():
      for k in node.children.keys():
        if node.children[k].hasChildren():
          #print 'BRANCH ' + node.children[k].longName
          yield BranchNode(node.children[k].longName)
        else:
          #print 'LEAF ' + node.children[k].longName
          yield LeafNode(node.children[k].longName, TenantBluefloodReader(node.children[k].longName, self.tenant))
    else:
      yield LeafNode(node.longName, TenantBluefloodReader(node.longName, self.tenant))

    # for node in tree.flatten():
    #   if node.hasChildren():
    #     print 'BRANCH ' + node.longName
    #     #yield BranchNode(node.longName)
    #   else:
    #     print 'LEAF ' + node.longName
    #     yield LeafNode(node.longName, TenantBluefloodReader(node.longName, self.tenant))


class TenantBluefloodReader(object):
  __slots__ = ('metric', 'tenant')
  supported = True

  def __init__(self, metric, tenant):
    # print 'READER ' + tenant + ' ' + metric
    self.metric = metric
    self.tenant = tenant

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
      client = Client(settings.BF_QUERY[0], self.tenant)
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
        maxTime = max(minTime, timestamp)
        valueArr.append(obj['average'])

      time_info = (minTime, maxTime, step)
      return (time_info, valueArr)

headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json'
}

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
    r = requests.get(self.host + '/v2.0/' + self.tenant + '/metrics/search', params=payload, headers=headers)
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
      res = 'MIN144'

    payload = {
      'from': start * 1000,
      'to': stop * 1000,
      'resolution': res
    }
    print 'USING RES ' + res
    r = requests.get(self.host + '/v2.0/' + self.tenant + '/views/' + metric, params=payload, headers=headers)
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

class Node:
  def __init__(self, parentName, name):
    self.name = name
    if parentName:
      self.longName = parentName + '.' + name
    else:
      self.longName = name;
    self.children = {}
    #print 'NNODE ' + self.longName

  def getChild(self, name):
    if not name in self.children:
      self.children[name] = Node(self.longName, name)
    return self.children[name]

  def add(self, path):
    #print 'ADDING to ' + self.longName + ', ' + path
    parts = path.split('.', 1)
    child = self.getChild(parts[0])
    if len(parts) == 2:
      child.add(parts[1])

  def ____add(self, name, path=None):
    child = self.getChild(name)
    if path:
      parts = path.split('.', 1)
      if len(parts) == 2:
        child.add(parts[0], parts[1])
      else:
        child.add(parts[0])

  def flatten(self, lst):
    for k in self.children:
      self.children[k].flatten(lst)
    lst.append(self)

  def hasChildren(self):
    return len(self.children) > 0

class Tree:
  def __init__(self):
    self.roots = {}

  def add(self, path):
    #print 'TREE ADD ' + path
    parts = path.split('.' , 1)
    if not parts[0] in self.roots:
      self.roots[parts[0]] = Node(None, parts[0])
    if len(parts) == 2:
      self.roots[parts[0]].add(parts[1])

  def flatten(self):
    lst = []
    for k in self.roots:
      self.roots[k].flatten(lst)
    #print 'FLATTENED ' + str(len(lst))
    return lst
