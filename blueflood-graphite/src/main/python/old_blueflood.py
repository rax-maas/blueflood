from __future__ import absolute_import

import re
import time
import requests
from django.conf import settings
from graphite.node import BranchNode, LeafNode
from graphite.intervals import Interval, IntervalSet

# YO!
# sudo pip install git+https://github.com/graphite-project/graphite-web.git
# PYTHONPATH=/opt/graphite/webapp python

class BluefloodFinder:
  def __init__(self):
    print 'new Blueflood finder'
    
  def find_nodes(self, query):
    print 'BF looking for ' + query.pattern
    bfClient = BfClient(settings.BF_QUERY[0])
    pattern = query.pattern;
    if pattern == '*':
      pattern = '.*'
    p = re.compile(pattern)
    tree = Tree()
    values = bfClient.findMetrics(query.pattern)
    for v in values:
      if p.match(v):
        #print 'good match for ' + v
        tree.add(v)
      else:
        #print 'no match for ' + v
        pass
    
    # for h in hits:
    for node in tree.flatten():
      if node.hasChildren():
        yield BranchNode(node.longName)
      else:
        yield LeafNode(node.longName, BluefloodReader(node.longName))

class BluefloodReader(object):
  __slots__ = ('metric')
  supported = True
  
  def __init__(self, metric=None):
    print 'BFReader for ' + metric
    self.metric = metric
    
  def get_intervals(self):
    # print 'intervals for ' + self.metric
    intervals = []
    millis = int(round(time.time() * 1000))
    intervals.append( Interval(0, millis) )
    return IntervalSet(intervals)
  
  def fetch(self, startTime, endTime):
    print 'BF fetch ' + self.metric + ' ' + str (startTime) + ' ' + str (endTime) + ' ' + str (endTime-startTime)
    if not self.metric:
      # empty
      return ((startTime, endTime, 1), [])
    else:
      client = BfClient(settings.BF_QUERY[0])
      values = client.getValues(self.metric, startTime, endTime)
      
      # determine the step.
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
        valueArr.append(obj['value'])
      
      print 'GOT DATA ' + str (step) + ' ' + str (minTime) + ' ' + str (maxTime)
      time_info = (minTime, maxTime, step)
      return (time_info, valueArr)


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

class BfClient:
  def __init__(self, host):
    self.host = host
  
  def findMetrics(self, filter):
    payload = {'filter': filter}
    r = requests.get(self.host + '/exp/lucene/find', params=payload)
    try:
      return r.json()['data']
    except ValueError:
      print 'Problem parsing json ' + r.statusCode
      return ['there was an error']
  
  def getValues(self, metric, start, stop):
    payload = {
      'from': start * 1000 ,
      'to': stop * 1000 ,
      'points': 1000000 # forces full res. I don't want to do the math right now.
    }
    tenant, metricName = metric.split('.', 1)
    r = requests.get(self.host + '/v2.0/' + tenant + '/view/series/' + metricName, params=payload)
    try:
      jsonData = r.json()
      return jsonData['values']
    except ValueError:
      print 'problem parsing json'
      return []
