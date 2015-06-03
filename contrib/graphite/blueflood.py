from __future__ import absolute_import

import re
import time
import requests
import json
import auth
import sys
import traceback
import os.path
import fnmatch

try:
    from graphite_api.intervals import Interval, IntervalSet
    from graphite_api.node import LeafNode, BranchNode
except ImportError:
    from graphite.intervals import Interval, IntervalSet
    from graphite.node import LeafNode, BranchNode

# curl -XPOST -H "Accept: application/json, text/plain, */*" -H "Content-Type: application/x-www-form-urlencoded" 'http://127.0.0.1:8888/render' -d "target=rackspace.*.*.*.*.*.*.*.*.available&from=-6h&until=now&format=json&maxDataPoints=1552"


secs_per_res = {
  'FULL': 1,
  'MIN5': 5*60,
  'MIN20': 20*60,
  'MIN60': 60*60,
  'MIN240': 240*60,
  'MIN1440': 1440*60}

def calc_res(start, stop):
  # make an educated guess about the likely number of data points returned.
  num_points = (stop - start) / 60
  res = 'FULL'
  if num_points > 400:
    num_points = (stop - start) / secs_per_res['MIN5']
    res = 'MIN5'
  if num_points > 800:
    num_points = (stop - start) / secs_per_res['MIN20']
    res = 'MIN20'
  if num_points > 800:
    num_points = (stop - start) / secs_per_res['MIN60']
    res = 'MIN60'
  if num_points > 800:
    num_points = (stop - start) / secs_per_res['MIN240']
    res = 'MIN240'
  if num_points > 800:
    num_points = (stop - start) / secs_per_res['MIN1440']
    res = 'MIN1440'
  return res

class TenantBluefloodFinder(object):
  __fetch_multi__ = 'tenant_blueflood'
  def __init__(self, config=None):
    print("Blueflood Finder v25")
    if config is not None:
      bf_config = config.get('blueflood', {})
      urls = bf_config.get('urls', bf_config.get('url', '').strip('/'))
      tenant = bf_config.get('tenant', None)
      authentication_module =  bf_config.get('authentication_module', None)
      authentication_class =  bf_config.get('authentication_class', None)
      enable_submetrics = bf_config.get('enable_submetrics', False)
      submetric_aliases = bf_config.get('submetric_aliases', {})
    else:
      from django.conf import settings
      urls = getattr(settings, 'BF_QUERY')
      tenant = getattr(settings, 'BF_TENANT')
      authentication_module = getattr(settings, 'BF_AUTHENTICATION_MODULE', None)
      authentication_class = getattr(settings, 'BF_AUTHENTICATION_CLASS', None)
      enable_submetrics = getattr(settings, 'BF_ENABLE_SUBMETRICS', False)
      submetric_aliases = getattr(settings, 'BF_SUBMETRIC_ALIASES', {})

    if authentication_module:
      module = __import__(authentication_module)
      class_ = getattr(module, authentication_class)
      bfauth = class_(config)
      auth.set_auth(bfauth)

    self.tenant = tenant
    self.bf_query_endpoint = urls[0]
    self.enable_submetrics = enable_submetrics
    self.submetric_aliases = submetric_aliases
    self.client = BluefloodClient(self.bf_query_endpoint, self.tenant, 
                                  self.enable_submetrics, self.submetric_aliases)
    print("BF finder submetrics enabled: ", enable_submetrics)

  def complete(self, metric, query_depth):
    metric_parts = metric.split('.')
    if self.enable_submetrics:
      complete_len = query_depth - 1
    else:
      complete_len = query_depth 
    return (len(metric_parts) == complete_len)



  def make_request(self, url, payload, headers):
    if auth.is_active():
      headers['X-Auth-Token'] = auth.get_token(False)
    r = requests.get(url, params=payload, headers=headers)
    if r.status_code == 401 and auth.is_active():
      headers['X-Auth-Token'] = auth.get_token(True)
      r = requests.get(url, params=payload, headers=headers)
    return r

  def find_metrics_endpoint(self, endpoint, tenant):
    return "%s/v2.0/%s/metrics/search" % (endpoint, tenant)

  def find_metrics(self, query):
    print "BluefloodClient.find_metrics: " + str(query)
    payload = {'query': query}
    headers = auth.headers()
    endpoint = self.find_metrics_endpoint(self.bf_query_endpoint, self.tenant)
    r = self.make_request(endpoint, payload, headers)

    if r.status_code == 200:
        return [m['metric'] for m in r.json()]
    else:
      return []

  def find_nodes_with_submetrics(self, query):
    # By definition, when using submetrics, the names of all Leafnodes must end in a submetric alias
    # BF doesn't know about the submetric aliases and so the aliases, (or globs corresponding to them,)
    # must be massaged before querying BF.
    # There are two cases above:

    # 1. When you want a list of valid submetric aliases, i.e. a complete metric name followed by ".*"
    # 2. When you have a query contains a valid submetric alias, i.e a complete metric name followed
    #    by a valid submetric alias like "._avg"

    # Every submetric leaf node is covered by one of the above two cases.  Everything else is a 
    # branch node.

    query_parts = query.pattern.split('.')
    query_depth = len(query_parts)

    # The pattern which all complete metrics will match
    complete_pattern = '.'.join(query_parts[0:-1])

    if (query_depth > 1) and (query_parts[-1] == '*'):
      #  In this if clause we are searching for complete metric names
      #  followed by a ".*". If so, we are requesting a list of submetric
      #  names, so return leaf nodes for each submetric alias that is
      #  enabled
      
      # First modify the pattern to get a superset that includes already complete
      #  submetrics
      new_pattern = complete_pattern + '*'
      for metric in self.find_metrics(new_pattern):
        metric_parts = metric.split('.')
        if self.complete(metric, query_depth) and fnmatch.fnmatchcase(metric, complete_pattern):
          for alias, _ in self.submetric_aliases.items():
            yield TenantBluefloodLeafNode('.'.join(metric_parts + [alias]),
                                          TenantBluefloodReader(metric, self.tenant, 
                                                                self.bf_query_endpoint, 
                                                                self.enable_submetrics, 
                                                                self.submetric_aliases))
        else:
          # Make sure the branch nodes match the original pattern
          if fnmatch.fnmatchcase(metric, query.pattern):
            yield BranchNode('.'.join(metric_parts[:query_depth]))


    #if searching for a particular submetric alias, create a leaf node for it
    elif (query_depth > 1) and (query_parts[-1] in self.submetric_aliases):
      for metric in self.find_metrics(complete_pattern):
        if self.complete(metric, query_depth):
          yield TenantBluefloodLeafNode('.'.join([metric, query_parts[-1]]), TenantBluefloodReader(metric, self.tenant, self.bf_query_endpoint, self.enable_submetrics, self.submetric_aliases))

    #everything else is a branch node
    else: 
      for metric in self.find_metrics(query.pattern):
        metric_parts = metric.split('.')
        if not self.complete(metric, query_depth):
          yield BranchNode('.'.join(metric_parts[:query_depth]))

  def find_nodes_without_submetrics(self, query):
    query_parts = query.pattern.split('.')
    query_depth = len(query_parts)

    for metric in self.find_metrics(query.pattern):
      metric_parts = metric.split('.')
      if not self.complete(metric, query_depth):
        yield BranchNode('.'.join(metric_parts[:query_depth]))
      else:
        yield TenantBluefloodLeafNode(metric, TenantBluefloodReader(metric, self.tenant, self.bf_query_endpoint, self.enable_submetrics, self.submetric_aliases))


  def find_nodes(self, query):
    #yields all valid metric names matching glob based query
    try:
      print "TenantBluefloodFinder.query: " + str(query.pattern)

      if self.enable_submetrics:
        return self.find_nodes_with_submetrics(query)
      else:
        return self.find_nodes_without_submetrics(query)

    except Exception as e:
     print "Exception in Blueflood find_nodes: "
     print e
     exc_info = sys.exc_info()
     tb = traceback.format_exception(*exc_info)
     for line in tb:
       print(line)
     raise e

  def fetch_multi(self, nodes, start_time, end_time):
    return self.client.fetch_multi(nodes, start_time, end_time)

class TenantBluefloodReader(object):
  __slots__ = ('metric', 'tenant', 'bf_query_endpoint', 
               'enable_submetrics', 'submetric_aliases')
  supported = True

  def __init__(self, metric, tenant, endpoint, enable_submetrics, submetric_aliases):

    # print 'READER ' + tenant + ' ' + metric
    self.metric = metric
    self.tenant = tenant
    self.bf_query_endpoint = endpoint
    self.enable_submetrics = enable_submetrics
    self.submetric_aliases = submetric_aliases

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
      client = BluefloodClient(self.bf_query_endpoint, self.tenant,
                                  self.enable_submetrics, self.submetric_aliases)
      reader = TenantBluefloodReader(self.metric, self.tenant, self.bf_query_endpoint, 
                                     self.enable_submetrics, self.submetric_aliases)
      node = TenantBluefloodLeafNode(self.metric, reader)

      time_info, dictionary = client.fetch_multi([node], start_time, end_time)
      
      return (time_info, dictionary[self.metric])

class BluefloodClient(object):
  def __init__(self, host, tenant, enable_submetrics, submetric_aliases):
    self.host = host
    self.tenant = tenant
    self.enable_submetrics = enable_submetrics
    self.submetric_aliases = submetric_aliases
    # This is the maximum number of json characters permitted by the
    #  BF multiplot handler.
    # The actual max is 8000, but I want to leave a margin for error
    #  because I can't be certain of the exact way the json is generated
    self.maxlen_per_req = 7000
    # this is for: "'' ," that surround each metric
    self.overhead_per_metric = 4
    self.maxmetrics_per_req = 100

  def gen_data_key(self, values):
    # Determines which key to use for the data
    if not len(values):
      return None
    value_res_order = ['average', 'latest', 'numPoints']
    present_keys = [v for v in value_res_order if v in values[0]]
    if present_keys:
      return present_keys[0]
    else:
      return None

  def current_datapoint_passed(self, v_iter, ts):
    # Determines if the current datapoint is too old to be considered
    if not len(v_iter):
      return False
    datapoint_ts = v_iter[0]['timestamp']/1000
    if (ts > datapoint_ts):
      return True
    return False

  def current_datapoint_valid(self, v_iter, data_key, ts, step):
    # Determines if the current datapoint be included in the current step
    #assumes current_datapoint_passed() is true
    if (not len(v_iter)) or not (data_key in v_iter[0]):
      return False
    datapoint_ts = v_iter[0]['timestamp']/1000
    if (datapoint_ts < (ts + step)):
      return True
    return False

  def process_path(self, values, start_time, end_time, step, data_key):
    # Generates datapoints in graphite-api format
    # Graphite-api requires the points be "step" seconds apart in time
    #  with Null's interleaved if needed
    if not data_key:
      print "No valid keys present"
      return []
    v_iter = values
    ret_arr = []
    for ts in range(start_time, end_time, step):

      # Skip datapoints that have already passed
      #  NOTE/TODO: this while loop has the effect of dropping all but the first datapoint in each step
      #  (Graphite requires exactly one datapoint/step.)  It would be better to rollup the datapoints
      #  if there are more than one/step.  However that will only happen when we have full res metrics
      #  with sub-second frequencies.  We believe that case is too rare to worry about right now.
      while self.current_datapoint_passed(v_iter, ts):
        v_iter = v_iter[1:]
      if self.current_datapoint_valid(v_iter, data_key, ts, step):
        ret_arr.append(v_iter[0][data_key])
      else:
        ret_arr.append(None)

    return ret_arr

  def get_multi_endpoint(self, endpoint, tenant):
    return "%s/v2.0/%s/views" % (endpoint, tenant)

  def get_metric_data(self, endpoint, tenant, metric_list, payload, headers):
    #Generate Multiplot query to get metrics in list
    url = self.get_multi_endpoint(endpoint,tenant)
    if auth.is_active():
      headers['X-Auth-Token'] = auth.get_token(False)
    r = requests.post(url, params=payload, data=json.dumps(metric_list), headers=headers)
    if r.status_code == 401 and auth.is_active():
      headers['X-Auth-Token'] = auth.get_token(True)
      r = requests.post(url, params=payload, data=json.dumps(metric_list), headers=headers)
    if r.status_code != 200:
      print("get_metric_data failed; response: ", r.status_code, tenant, metric_list)
      return None
    else:
      return r.json()['metrics']

  def gen_payload(self, start_time, end_time, res):
    payload = {
      'from': start_time * 1000,
      'to': end_time * 1000,
      'points': 1000
    }
    if self.enable_submetrics:
      payload['select'] = ','.join(self.submetric_aliases.values())
    return payload

  def gen_keys(self, node, metrics):
    # returns metrics_key and data_key
    #  metrics_key, (the name of the metric, which varies
    #   depending on if submetric aliases are used.)
    #  data_key, (the name of the key to use for the individual datapoint
    #   returned by BF
    metrics_key = None
    data_key = None
    if self.enable_submetrics:
      path_parts = node.path.split('.')
      test_key = '.'.join(path_parts[0:-1])
      if test_key in metrics:
        metrics_key = test_key
        data_key = self.submetric_aliases[path_parts[-1]]
    elif node.path in metrics:
      metrics_key = node.path
      data_key = self.gen_data_key(metrics[metrics_key])
    return metrics_key, data_key

  def gen_dict(self, nodes, responses, start_time, real_end_time, step):
    metrics = {x['metric'] : x['data'] for x in responses}
    dictionary = {}
    for n in nodes:
      metrics_key, data_key = self.gen_keys(n, metrics)
      if metrics_key:
        dictionary[n.path] = self.process_path(metrics[metrics_key], start_time, real_end_time, step, data_key)
    return dictionary

  def group_has_room(self, cur_metric, cur_path, tot_len, remaining_paths):
    if cur_metric >= self.maxmetrics_per_req:
      return False
    if cur_path >= len(remaining_paths):
      return False
    new_total = tot_len + len(remaining_paths[cur_path]) + self.overhead_per_metric
    if new_total >= self.maxlen_per_req:
      return False
    return True
  
  def gen_next_group(self, remaining_paths, groups):
    # creates a group of metrics that doesn't exceed limits
    cur_metric = 0
    cur_path = 0
    tot_len = 2 # for the brackets
    while self.group_has_room(cur_metric, cur_path, tot_len, remaining_paths):
      tot_len += len(remaining_paths[cur_path]) + 2 # for the ", "
      cur_metric += 1
      cur_path += 1
    return remaining_paths[cur_path:], groups + [remaining_paths[0:cur_path]]

  def gen_groups(self, nodes):
    # creates groups of metrics none of which exceed limits
    groups = []
    remaining_paths = [node.path for node in nodes]
    if self.enable_submetrics:
      remaining_paths = ['.'.join(p.split('.')[0:-1]) for p in remaining_paths]
    while remaining_paths:
      new_remaining_paths, groups = self.gen_next_group(remaining_paths, groups)
      #check for infinite loops/should never happen
      if len(remaining_paths) == len(new_remaining_paths):
        raise IndexError("Invalid path found; breaking out of loop.")
      else:
        remaining_paths = new_remaining_paths
    return groups

  def gen_responses(self, groups, payload):
    #converts groups of requests into a single list of responses
    headers = auth.headers()
    responses = reduce(lambda x,y: x+y,
                     [self.get_metric_data(self.host,
                                           self.tenant, g, payload, headers)
                      for g in groups])
    responses = responses or []
    return responses
  
  def fetch_multi(self, nodes, start_time, end_time):
    try:
      res = calc_res(start_time, end_time)
      step = secs_per_res[res]
      payload = self.gen_payload(start_time, end_time, res)
      # Limit size of MPlot requests by dividing into groups
      groups = self.gen_groups(nodes)
      responses = self.gen_responses(groups, payload)
      real_end_time = end_time + step
      dictionary = self.gen_dict(nodes, responses, start_time, real_end_time, step)
      time_info = (start_time, real_end_time, step)
      return (time_info, dictionary)

    except Exception as e:
      print "Exception in Blueflood fetch_multi: "
      print e
      exc_info = sys.exc_info()
      tb = traceback.format_exception(*exc_info)
      for line in tb:
        print(line)
      raise e

class TenantBluefloodLeafNode(LeafNode):
  __fetch_multi__ = 'tenant_blueflood'
