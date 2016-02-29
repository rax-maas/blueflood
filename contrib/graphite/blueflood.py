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

import Queue
import threading

import logging

logger = logging.getLogger('blueflood_finder')

try:
    from graphite_api.intervals import Interval, IntervalSet
    from graphite_api.node import LeafNode, BranchNode
except ImportError:
    from graphite.intervals import Interval, IntervalSet
    from graphite.node import LeafNode, BranchNode

# curl -XPOST -H "Accept: application/json, text/plain, */*"
#             -H "Content-Type: application/x-www-form-urlencoded"
#             'http://127.0.0.1:8888/render'
#             -d "target=rackspace.*.*.*.*.*.*.*.*.available&from=-6h&until=now&format=json&maxDataPoints=1552"


secs_per_res = {
  'FULL': 60,   # Setting this to 60 seems to best emulate graphite/whisper for dogfooding
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

class TenantBluefloodFinder(threading.Thread):
  __fetch_multi__ = 'tenant_blueflood'
  __fetch_events__ = 'tenant_blueflood'

  def __init__(self, config=None):
    logger.debug("Blueflood Finder v31")
    threading.Thread.__init__(self)
    if os.path.isfile("/root/pdb-flag"):
      import remote_pdb
      remote_pdb.RemotePdb('127.0.0.1', 4444).set_trace()
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

    self.exit_flag = 0
    self.metrics_q = Queue.Queue(1)
    self.data_q = Queue.Queue(1)

    self.tenant = tenant
    self.bf_query_endpoint = urls[0]
    self.enable_submetrics = enable_submetrics
    self.submetric_aliases = submetric_aliases
    self.client = BluefloodClient(self.bf_query_endpoint, self.tenant,
                                  self.enable_submetrics, self.submetric_aliases)
    self.daemon = True
    self.start()
    logger.debug("BF finder submetrics enabled: %s", enable_submetrics)


  def run(self):
    #This separate thread allows queued reads to happen in the background
    logger.debug("BF enum thread started: ")
    while not self.exit_flag:
      metric = self.metrics_q.get()
      self.data_q.put(self.find_metrics_with_enum_values(metric))


  def complete(self, metric, complete_len):
    #returns true if metric is a complete metric name wrt the query
    metric_parts = metric.split('.')
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
    return "%s/v2.0/%s/metrics/search?include_enum_values=true" % (endpoint, tenant)

  def find_metrics_with_enum_values(self, query):
    #BF search command that returns enum values as well as metric names
    logger.info("BluefloodClient.find_metrics: %s", str(query))
    payload = {'query': query}
    headers = auth.headers()
    endpoint = self.find_metrics_endpoint(self.bf_query_endpoint, self.tenant)
    r = self.make_request(endpoint, payload, headers)
    ret_dict = {}
    if r.status_code == 200:
      for m in r.json():
        if 'enum_values' in m:
          v = m['enum_values']
        else:
          v = None
        ret_dict[m['metric']] = v
      return ret_dict
    else:
      return {}

  def find_metrics(self, query):
    #BF search command that returns metric names without enum values
    return self.find_metrics_with_enum_values(query)

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

    def submetric_is_enum_value(submetric_alias):
      return self.submetric_aliases[submetric_alias] == 'enum'

    query_parts = query.pattern.split('.')
    query_depth = len(query_parts)
    submetric_alias = query_parts[-1]
    complete_len = query_depth - 1
    # The pattern which all complete metrics will match
    complete_pattern = '.'.join(query_parts[:-1])

    #handle enums
    # enums are required to have an "enum" submetric alias so
    # this is the only read required, (no background read.)
    if (query_depth > 2) and (submetric_alias in self.submetric_aliases):
      if (submetric_is_enum_value(submetric_alias)):
        enum_name = '.'.join(query_parts[:-2])
        for metric, enums in self.find_metrics_with_enum_values(enum_name).items():
          for n in self.make_enum_nodes(metric, enums, complete_pattern):
            yield n
        return
    if (query_depth > 1) and (submetric_alias == '*'):
      #  In this if clause we are searching for complete metric names
      #  followed by a ".*". If so, we are requesting a list of submetric
      #  names, so return leaf nodes for each submetric alias that is
      #  enabled

      # First modify the pattern to get a superset that includes already complete
      #  submetrics
      new_pattern = complete_pattern + '*'
      for (metric, enums) in self.find_metrics(new_pattern).items():
        metric_parts = metric.split('.')
        if self.complete(metric, complete_len) and fnmatch.fnmatchcase(metric, complete_pattern):
          for alias, _ in self.submetric_aliases.items():
            yield TenantBluefloodLeafNode('.'.join(metric_parts + [alias]),
                                          TenantBluefloodReader(metric, self.tenant,
                                                                self.bf_query_endpoint,
                                                                self.enable_submetrics,
                                                                self.submetric_aliases, None))
        else:
          # Make sure the branch nodes match the original pattern
          if fnmatch.fnmatchcase(metric, query.pattern):
            yield BranchNode('.'.join(metric_parts[:query_depth]))


    #if searching for a particular submetric alias, create a leaf node for it
    elif (query_depth > 1) and (submetric_alias in self.submetric_aliases):
      for (metric, enums) in self.find_metrics(complete_pattern).items():
        if self.complete(metric, complete_len):
          yield TenantBluefloodLeafNode('.'.join([metric, submetric_alias]),
                                        TenantBluefloodReader(metric, self.tenant, self.bf_query_endpoint,
                                                              self.enable_submetrics, self.submetric_aliases, None))

    #everything else is a branch node
    else:
      for (metric, enums) in self.find_metrics(query.pattern).items():
        metric_parts = metric.split('.')
        if not self.complete(metric, complete_len):
          yield BranchNode('.'.join(metric_parts[:query_depth]))


  def make_non_enum_node(self, metric, enums, complete_len):
    if not self.complete(metric, complete_len):
      metric_parts = metric.split('.')
      yield BranchNode('.'.join(metric_parts[:complete_len]))
    else:
      if enums:
        yield BranchNode(metric)
      else:
        yield TenantBluefloodLeafNode(metric,
                                    TenantBluefloodReader(metric, self.tenant, self.bf_query_endpoint,
                                                          self.enable_submetrics, self.submetric_aliases, None))

  def make_enum_nodes(self, metric, enums, pattern):
    for e in enums:
      metric_with_enum = metric + '.' + e
      if fnmatch.fnmatchcase(metric_with_enum, pattern):
        yield TenantBluefloodLeafNode(metric_with_enum,
                                      TenantBluefloodReader(metric_with_enum, self.tenant, self.bf_query_endpoint,
                                                            self.enable_submetrics, self.submetric_aliases, e))



  def find_nodes_without_submetrics(self, query):
    query_parts = query.pattern.split('.')
    complete_len = len(query_parts)

    # do a background read to see if this is an enum metric
    if complete_len > 1:
      enum_name = ".".join(query_parts[:-1])
      self.metrics_q.put(enum_name)

    # find non enum nodes
    for (metric, enums) in self.find_metrics(query.pattern).items():
      for n in self.make_non_enum_node(metric, enums, complete_len):
        logger.debug("Node: %s", n)
        yield n

    # get the results of the background read to add enums
    if complete_len > 1:
      d = self.data_q.get()
      for metric, enums in d.items():
        metric_len = len(metric.split('.'))
        if enums and (metric_len + 1 == complete_len):
          for n in self.make_enum_nodes(metric, enums, query.pattern):
            logger.debug("Node: %s", n)
            yield n

  def find_nodes(self, query):
    """
    Returns a list of metric names based on a glob pattern, and corresponds to the BF "/search" endpoint.
    """
    #yields all valid metric names matching glob based query
    try:
      logger.debug("TenantBluefloodFinder.query: %s", str(query.pattern))

      if self.enable_submetrics:
        return self.find_nodes_with_submetrics(query)
      else:
        return self.find_nodes_without_submetrics(query)

    except Exception as e:
     logger.exception("Exception in Blueflood find_nodes: ")
     raise e

  def fetch_multi(self, nodes, start_time, end_time):
    """
    Returns the data for a list of metrics and corresponds to the BF "multiplot" endpoint.
    """
    return self.client.fetch_multi(nodes, start_time, end_time)

  def find_events_endpoint(self, endpoint, tenant):
    return "%s/v2.0/%s/events/getEvents" % (endpoint, tenant)

  def getEvents(self, start_time, end_time, tags):
      url = self.find_events_endpoint(self.bf_query_endpoint, self.tenant)
      payload = {
        'from': start_time * 1000,
        'until': end_time * 1000
      }

      if tags is not None:
        payload['tags'] = tags
      headers = auth.headers()

      r = self.make_request(url, payload, headers)
      r = r.json()
      for event in r:
        event['when'] = int(event['when']/1000)
      return r

class TenantBluefloodReader(object):
  __slots__ = ('metric', 'tenant', 'bf_query_endpoint',
               'enable_submetrics', 'submetric_aliases', 'enum_value')
  supported = True

  def __init__(self, metric, tenant, endpoint, enable_submetrics, submetric_aliases, enum_value):

    # print 'READER ' + tenant + ' ' + metric
    self.metric = metric
    self.tenant = tenant
    self.bf_query_endpoint = endpoint
    self.enable_submetrics = enable_submetrics
    self.submetric_aliases = submetric_aliases
    self.enum_value = enum_value

  def get_intervals(self):
    # todo: make this a lot smarter.
    intervals = []
    millis = int(round(time.time() * 1000))
    intervals.append(Interval(0, millis))
    return IntervalSet(intervals)

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
      return NonNestedDataKey(None)
    value_res_order = ['average', 'latest', 'numPoints']
    present_keys = [v for v in value_res_order if v in values[0]]
    if present_keys:
      return NonNestedDataKey(present_keys[0])
    else:
      return NonNestedDataKey(None)

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
    if (not len(v_iter)) or not data_key.exists(v_iter[0]):
      return False
    datapoint_ts = v_iter[0]['timestamp']/1000
    if (datapoint_ts < (ts + step)):
      return True
    return False

  def fixup(self, values, fixup_list):
    # Replace the None's in "values" with interpolations of the
    # surrounding non-null values
    # "fixup_list" lists the null datapoints in the values
    if fixup_list:
      #preserve the type of the values
      set_type = type(values[fixup_list[0][0]])
    for f in fixup_list:
      start = f[0]
      end = f[1]
      increment = (float(values[end]) - values[start])/(end - start)
      nextval = values[start]
      for x in range(start + 1, end):
        nextval += increment
        values[x] = set_type(nextval)


  def process_path(self, values, start_time, end_time, step, data_key):
    # Generates datapoints in graphite-api format
    # Graphite-api requires the points be "step" seconds apart in time
    #  with Null's interleaved if needed
    # Note that even if there are no datapoints in "values" it will fill
    #  them with nulls
    v_iter = values
    ret_arr = []
    # A fixup is the start and end position of the current range of Null datapoints
    current_fixup = None
    fixup_list = []
    for ts in range(start_time, end_time, step):

      # Skip datapoints that have already passed
      #  NOTE/TODO: this while loop has the effect of dropping all but the first datapoint in each step
      #  (Graphite requires exactly one datapoint/step.)  It would be better to rollup the datapoints
      #  if there are more than one/step.  However that will only happen when we have full res metrics
      #  with frequencies less than 60 seconds.  And since Graphite/Whisper doesn't seem to do that
      #  this approach seems to better emulate the results produced by Graphite
      while self.current_datapoint_passed(v_iter, ts):
        v_iter = v_iter[1:]
      if self.current_datapoint_valid(v_iter, data_key, ts, step):
        ret_arr.append(data_key.get_datapoints(v_iter[0]))
        if current_fixup != None:
          # we have found the end of the current fixup, so
          #  add the start and end of the current fixup into fixup list
          fixup_list.append([current_fixup, len(ret_arr) - 1])
          current_fixup = None
      else:
        l = len(ret_arr)
        #if previous element was not None, start a new fixup
        #  and set it to point to the current position
        if (l > 0) and (ret_arr[l - 1] != None):
          current_fixup = l - 1
        ret_arr.append(None)
    self.fixup(ret_arr, fixup_list)
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
      logger.info("get_metric_data failed endpoint: [%s] status code: [%s] tenant: [%s] metric_list: [%s]",
                   endpoint, r.status_code, tenant, metric_list)
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
    #   returned by BF)
    metrics_key = None
    data_key = NonNestedDataKey(None)
    if node.reader.enum_value != None:
      evalue = node.reader.enum_value
      metrics_key = node.path[:-(len(evalue) + 1)]
      data_key = NestedDataKey('enum_values', evalue)
    elif self.enable_submetrics:
      path_parts = node.path.split('.')
      test_key = '.'.join(path_parts[:-1])
      if test_key in metrics:
        metrics_key = test_key
        data_key = NonNestedDataKey(self.submetric_aliases[path_parts[-1]])
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
    return remaining_paths[cur_path:], groups + [remaining_paths[:cur_path]]

  def gen_paths(self, nodes):
    #This modifies the metrics names used by graphite to match
    # the input expected by the BF mplot api.
    # These differ when submetrics are used because the submetric alias
    # is included in the graphite path but the BF metric name.
    # Also, if enums are used, the enum_value is included in the
    # graphite path, but not the BF metric name
    path_set = set()
    paths = []
    for n in nodes:
      if n.reader.enum_value != None:
        #add to a "set" here because some of the names may be dupes
        path_set.add(n.path[:-(len(n.reader.enum_value) + 1)])
      else:
        if self.enable_submetrics:
          path_set.add('.'.join(n.path.split('.')[:-1]))
        else:
          paths.append(n.path)
    for p in path_set:
      paths.append(p)
    return paths

  def gen_groups(self, nodes):
    # creates groups of metrics none of which exceed limits
    groups = []
    remaining_paths = self.gen_paths(nodes)
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
      logger.exception("Exception in Blueflood fetch_multi: ")
      raise e



class TenantBluefloodLeafNode(LeafNode):
  __fetch_multi__ = 'tenant_blueflood'

class NonNestedDataKey(object):
  def __init__(self, key1):
    self.key1 = key1

  def exists(self, value):
    return self.key1 in value

  def get_datapoints(self, value):
    if not self.exists(value):
      return None
    else:
      return value[self.key1]

class NestedDataKey(object):
  #as the name implies a "NestedDataKey is used to deref
  # a nested value
  def __init__(self, key1, key2):
    self.key1 = key1
    self.key2 = key2

  def exists(self, value):
    return self.key1 in value and self.key2 in value[self.key1]

  def get_datapoints(self, value):
    if not self.exists(value):
      return None
    else:
      return value[self.key1][self.key2]

