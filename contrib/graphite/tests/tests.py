from unittest import TestCase
import unittest
import os
import datetime
import requests_mock
import sys
import urllib
import threading
import logging.config
from blueflood import TenantBluefloodFinder, TenantBluefloodReader, TenantBluefloodLeafNode, \
     BluefloodClient, auth, calc_res, secs_per_res, NonNestedDataKey, NestedDataKey

logging_file = os.path.join(os.path.dirname(__file__), 'logging.ini')
logging.config.fileConfig(logging_file)

#To run these tests you need to set up the environment vars below
try:
  auth_api_key = os.environ['AUTH_API_KEY']
  auth_user_name = os.environ['AUTH_USER_NAME']
  auth_tenant = os.environ['AUTH_TENANT']
  auth_url = os.environ['AUTH_URL']
  auth_config = {'blueflood':
                 {'authentication_module': 'rax_auth',
                  'authentication_class': 'BluefloodAuth',
                  'username': auth_user_name,
                  'apikey': auth_api_key,
                  'urls': [auth_url],
                  'tenant': auth_tenant}}
except Exception as e:
  print e
  print "Auth env undefined, not running auth tests"
  auth_config = None

try:
  no_auth_tenant = os.environ['NO_AUTH_TENANT']
  no_auth_url = os.environ['NO_AUTH_URL']
  no_auth_config = {'blueflood':
                    { 'urls': [no_auth_url],
                      'tenant': no_auth_tenant}}
except:
  print "NO_AUTH env undefined, not running no_auth tests"
  no_auth_config = None

try:
  from graphite.storage import FindQuery
  print 'using graphite.storage.FindQuery'
except:
  try:
    from graphite_api.storage import FindQuery
    print 'using graphite_api.storage.FindQuery'
  except:
    print 'rolling my own FindQuery'
    class FindQuery(object):
      def __init__(self, pattern, startTime, endTime):
        self.pattern = pattern
        self.startTime = startTime
        self.endTime = endTime

def exc_callback(request, context):
  raise ValueError("Test exceptions")

class BluefloodTests(TestCase):
  # The "requests_mock" mocking framework we use is not thread-safe.
  # This mock inserts a lock to fix that
  fm_lock = threading.Lock()
  orig_find_metrics_with_enum_values = TenantBluefloodFinder.find_metrics_with_enum_values
  def mock_find_metrics_with_enum_values(s, query):
    with BluefloodTests.fm_lock:
      return BluefloodTests.orig_find_metrics_with_enum_values(s,query)
  TenantBluefloodFinder.find_metrics_with_enum_values = mock_find_metrics_with_enum_values

  def setUp(self):
    if not (auth_config or no_auth_config):
        self.fail("Failing: Environment variables not set")
    self.alias_key = '_avg'
    config = {'blueflood':
              {'urls':["http://dummy.com"],
               'tenant':'dummyTenant',
               'submetric_aliases': {self.alias_key:'average',
                                     "_enum": 'enum'  }}}
    self.finder = TenantBluefloodFinder(config)
    self.metric1 = "a.b.c"
    self.metric2 = "e.f.g"
    self.metric3 = "x.y.z"
    self.reader = TenantBluefloodReader(self.metric1, self.finder.tenant, self.finder.bf_query_endpoint,
                                        self.finder.enable_submetrics, self.finder.submetric_aliases, None)
    metric_with_enum1 = self.metric3 + '.' + 'v1'
    metric_with_enum2 = self.metric3 + '.' + 'v2'
    self.enum_reader1 = TenantBluefloodReader(metric_with_enum1, self.finder.tenant, self.finder.bf_query_endpoint,
                                             self.finder.enable_submetrics, self.finder.submetric_aliases, "v1")
    self.enum_reader2 = TenantBluefloodReader(metric_with_enum2, self.finder.tenant, self.finder.bf_query_endpoint,
                                             self.finder.enable_submetrics, self.finder.submetric_aliases, "v2")
    self.node1 = TenantBluefloodLeafNode(self.metric1, self.reader)
    self.node2 = TenantBluefloodLeafNode(self.metric2, self.reader)
    self.node3 = TenantBluefloodLeafNode(metric_with_enum1, self.enum_reader1)
    self.node4 = TenantBluefloodLeafNode(metric_with_enum2, self.enum_reader2)
    self.bfc = BluefloodClient(self.finder.bf_query_endpoint, self.finder.tenant,
                               self.finder.enable_submetrics, self.finder.submetric_aliases)
    auth.set_auth(None)

  def run_find(self, finder):
    nodes = list(finder.find_nodes(FindQuery('*', 0, 100)))
    self.assertTrue(len(nodes) > 0)

  def setup_UTC_mock(self):
    #setup a mock that forces expiration
    self.orig_get_current_UTC = type(auth.auth).get_current_UTC
    self.orig_do_auth = type(auth.auth).do_auth
    this = self
    self.authCount = 0
    def mock_get_current_UTC(self):
      return auth.auth.expiration_UTC + datetime.timedelta(days=1)
    def mock_do_auth(self):
      this.authCount += 1
      this.orig_do_auth(self)
    type(auth.auth).get_current_UTC = mock_get_current_UTC
    type(auth.auth).do_auth = mock_do_auth

  def unset_UTC_mock(self):
    type(auth.auth).get_current_UTC = self.orig_get_current_UTC
    type(auth.auth).do_auth = self.orig_do_auth

  def test_finder(self):
    if no_auth_config:
      print "\nRunning NO_AUTH tests"
      finder = TenantBluefloodFinder(no_auth_config)
      self.run_find(finder)

    if auth_config:
      print "\nRunning AUTH tests"
      finder = TenantBluefloodFinder(auth_config)
      self.run_find(finder)

      #force re-auth
      auth.auth.token = ""
      self.run_find(finder)
      
      #test expired UTC
      self.setup_UTC_mock()
      self.run_find(finder)
      self.unset_UTC_mock()
      self.assertTrue(self.authCount == 1)

  def test_gen_groups(self):
    # one time through without submetrics
    self.bfc.enable_submetrics = False
    
    #only 1 metric per group even though room for more
    self.bfc.maxmetrics_per_req = 1
    self.bfc.maxlen_per_req = 20
    groups = self.bfc.gen_groups([self.node1, self.node2])
    self.assertSequenceEqual(groups,[['a.b.c'], ['e.f.g']])

    #check that enum values get reduced to single metric name
    self.bfc.maxmetrics_per_req = 1
    self.bfc.maxlen_per_req = 20
    groups = self.bfc.gen_groups([self.node3, self.node4])
    self.assertSequenceEqual(groups,[['x.y.z']])

    #allow 2 metrics per group
    self.bfc.maxmetrics_per_req = 2
    groups = self.bfc.gen_groups([self.node1, self.node2])
    self.assertSequenceEqual(groups,[['a.b.c', 'e.f.g']])

    #now only room for 1 per group
    self.bfc.maxlen_per_req = 12
    groups = self.bfc.gen_groups([self.node1, self.node2])
    self.assertSequenceEqual(groups,[['a.b.c'], ['e.f.g']])

    #no room for metric in a group
    self.bfc.maxlen_per_req = 11
    with self.assertRaises(IndexError):
      groups = self.bfc.gen_groups([self.node1, self.node2])

    # now with submetrics
    self.bfc.enable_submetrics = True

    #only 1 metric per group even though room for more
    self.bfc.maxmetrics_per_req = 1
    self.bfc.maxlen_per_req = 15
    groups = self.bfc.gen_groups([self.node1, self.node2])
    self.assertSetEqual(set(tuple(map(tuple, groups))),set([('e.f',), ('a.b',)]))

    #allow 2 metrics per group
    self.bfc.maxmetrics_per_req = 2
    groups = self.bfc.gen_groups([self.node1, self.node2])
    self.assertSetEqual(set(tuple(map(tuple, groups))),set([('e.f', 'a.b',)]))


    #now only room for 1 per group
    self.bfc.maxlen_per_req = 10
    groups = self.bfc.gen_groups([self.node1, self.node2])
    self.assertSetEqual(set(tuple(map(tuple, groups))),set([('e.f',), ('a.b',)]))


    #no room for metric in a group
    self.bfc.maxlen_per_req = 9
    with self.assertRaises(IndexError):
      groups = self.bfc.gen_groups([self.node1, self.node2])

  def make_data(self, start, step):
    # should be 0th element in response
    first_timestamp = start * 1000
    # should be skipped because it overlaps first_timestamp + 1000*step
    second_timestamp = first_timestamp + (1000 * step - 1)
    # should be 4th element
    third_timestamp = first_timestamp + (5000 * step - 1)
    # should be 7th element
    fourth_timestamp = first_timestamp + (7000 * step + 1)
    
    metric1 = self.metric1
    metric2 = self.metric2
    if self.bfc.enable_submetrics:
      submetric = '.' + self.alias_key
      metric1 += submetric
      metric2 += submetric
    node1 = TenantBluefloodLeafNode(metric1, self.reader)
    node2 = TenantBluefloodLeafNode(metric2, self.reader)
    return ([node1, node2],
            [{u'data':
              [{u'timestamp': third_timestamp, u'average': 4449.97, u'numPoints': 1},
               {u'timestamp': fourth_timestamp, u'average': 14449.97, u'numPoints': 1}],
              u'metric': self.metric1, u'type': u'number', u'unit': u'unknown'},
             {u'data':
              [{u'timestamp': first_timestamp, u'average': 6421.18, u'numPoints': 1},
               {u'timestamp': second_timestamp, u'average': 26421.18, u'numPoints': 1}], 
              u'metric': self.metric2, u'type': u'number', u'unit': u'unknown'}])
    
  def make_enum_data(self, start, step):
    # should be 0th element in response
    first_timestamp = start * 1000
    # should be skipped because it overlaps first_timestamp + 1000*step
    second_timestamp = first_timestamp + (1000 * step - 1)
    # should be 4th element
    third_timestamp = first_timestamp + (5000 * step - 1)
    # should be 7th element
    fourth_timestamp = first_timestamp + (7000 * step + 1)

    return ([self.node3, self.node4],
            [{u'data':
              [{u'timestamp': third_timestamp, u'average': 4449.97, u'numPoints': 1},
               {u'timestamp': fourth_timestamp, u'average': 14449.97, u'numPoints': 1}],
              u'metric': self.metric1, u'type': u'number', u'unit': u'unknown'},
             {u'data':
              [{u'timestamp': third_timestamp, u'enum_values': {u'v1': 13, u'v2': 7}, u'numPoints': 20},
               {u'timestamp': fourth_timestamp, u'enum_values': {u'v1': 11, u'v2': 3}, u'numPoints': 14}], 
              u'metric': self.metric3, u'type': u'number', u'unit': u'unknown'}])

  def test_gen_dict(self):
    step = 3000
    start = 1426120000
    end = 1426147000
    nodes, responses = self.make_data(start, step)
    dictionary = self.bfc.gen_dict(nodes, responses, start, end, step)
    self.assertDictEqual(dictionary,
                    {nodes[1].path: [6421.18, None, None, None, None, None, None, None, None], 
                     nodes[0].path: [None, None, None, None, 4449.97, 7783.303333333333, 
                                     11116.636666666667, 14449.97, None]})

    # check that it handles unfound metric correctly
    nodes[1].path += '.dummy'
    dictionary = self.bfc.gen_dict(nodes, responses, start, end, step)
    self.assertDictEqual(dictionary,
                    {nodes[0].path: [None, None, None, None, 4449.97, 7783.303333333333, 
                                     11116.636666666667, 14449.97, None]})

    # check enums
    nodes, responses = self.make_enum_data(start, step)
    dictionary = self.bfc.gen_dict(nodes, responses, start, end, step)
    self.assertDictEqual(dictionary,
                    {nodes[1].path: [None, None, None, None, 7, 5, 4, 3, None], 
                     nodes[0].path: [None, None, None, None, 13, 12, 11, 11, None]})

    # now with submetrics
    self.bfc.enable_submetrics = True
    nodes, responses = self.make_data(start, step)
    dictionary = self.bfc.gen_dict(nodes, responses, start, end, step)
    self.assertDictEqual(dictionary,
                    {nodes[1].path: [6421.18, None, None, None, None, None, None, None, None], 
                     nodes[0].path: [None, None, None, None, 4449.97, 7783.303333333333, 
                                     11116.636666666667, 14449.97, None]})

    # check enums with submetrics
    nodes, responses = self.make_enum_data(start, step)
    dictionary = self.bfc.gen_dict(nodes, responses, start, end, step)
    self.assertDictEqual(dictionary,
                    {nodes[1].path: [None, None, None, None, 7, 5, 4, 3, None], 
                     nodes[0].path: [None, None, None, None, 13, 12, 11, 11, None]})


  def test_gen_responses(self):
    step = 3000
    start = 1426120000
    end = 1426147000
    groups1 = [[self.metric1, self.metric2]]
    payload = self.bfc.gen_payload(start, end, 'FULL')
    endpoint = self.bfc.get_multi_endpoint(self.finder.bf_query_endpoint, self.finder.tenant)
    # test 401 error
    with requests_mock.mock() as m:
      m.post(endpoint, json={}, status_code=401)
      responses = self.bfc.gen_responses(groups1, payload)
      self.assertSequenceEqual(responses,[])
    
    #test single group
    _, responses = self.make_data(start, step)
    with requests_mock.mock() as m:
      m.post(endpoint, json={'metrics':responses}, status_code=200)
      new_responses = self.bfc.gen_responses(groups1, payload)
      self.assertSequenceEqual(responses,new_responses)

    #test multiple groups
    groups2 = [[self.metric1], [self.metric2]]
    with requests_mock.mock() as m:
      global json_data
      json_data = [{'metrics':responses[:1]},{'metrics':responses[1:]}]
      def json_callback(request, context):
        global json_data
        response = json_data[0]
        json_data = json_data[1:]
        return response

      m.post(endpoint, json=json_callback, status_code=200)
      new_responses = self.bfc.gen_responses(groups2, payload)
      self.assertSequenceEqual(responses,new_responses)

  
  def test_find_nodes(self):
    endpoint = self.finder.find_metrics_endpoint(self.finder.bf_query_endpoint, self.finder.tenant)

    # one time through without submetrics
    self.finder.enable_submetrics = False
    with requests_mock.mock() as m:
      #test 401 errors
      query = FindQuery("*", 1, 2)
      m.get(endpoint, json={}, status_code=401)
      metrics = self.finder.find_nodes(query)
      self.assertTrue(list(metrics) == [])

    with requests_mock.mock() as m:
      query = FindQuery("*", 1, 2)
      m.get(endpoint, json=exc_callback, status_code=401)
      with self.assertRaises(ValueError):
        list(self.finder.find_nodes(query))

    def get_start(x):
      return lambda y: '.'.join(y.split('.')[:x])

    get_path = lambda x: x.path

    def query_test(query_pattern, fg_data, search_results, bg_data=[]):
      # query_pattern is the pattern to search for
      # fg_data is the data returned by the "mainthread" call to find metrics
      # search_results are the expected results
      # bg_data - non submetric calls do 2 calls to find_metrics, (one in a background thread,)
      #   bg_data simulates what gets returnd by the background thread
      def json_callback(request, context):
        print("json thread callback" + threading.current_thread().name)
        full_query = "include_enum_values=true&" + urllib.urlencode({'query':query_pattern}).lower()
        if not self.finder.enable_submetrics:
          if request.query == full_query:
            return fg_data
          else:
            return bg_data
        else:
          return fg_data
      qlen = len(query_pattern.split("."))
      with requests_mock.mock() as m:
        query = FindQuery(query_pattern, 1, 2)
        m.get(endpoint, json=json_callback, status_code=200)
        metrics = self.finder.find_nodes(query)
        self.assertSetEqual(set(map(get_path, list(metrics))),
                                 set(map(get_start(qlen), search_results)))

    enum_vals = ['v1', 'v2']

    query_test("*",
               [{u'metric': self.metric1, u'unit': u'percent'},
                {u'metric': self.metric2, u'unit': u'percent'}],
               [self.metric1, self.metric2])

    query_test("a.*",
               [{u'metric': self.metric1, u'unit': u'percent'}],
               [self.metric1])

    query_test("a.*",
               [{u'metric': self.metric1 + 'd', u'unit': u'percent'}],
               [self.metric1],
               [{u'metric': self.metric1, u'unit': u'percent', u'enum_values': enum_vals}])

    query_test("a.b.*",
               [{u'metric': self.metric1, u'unit': u'percent'}],
               [self.metric1])

    query_test("a.b.c",
               [{u'metric': self.metric1, u'unit': u'percent'}],
               [self.metric1])

    query_test("a.b.c.*",
               [{u'metric': self.metric1 + '.d.e', u'unit': u'percent'}],
               [self.metric1 + '.' + v for v in enum_vals] + [self.metric1 + '.d.e'],
               [{u'metric': self.metric1, u'unit': u'percent', u'enum_values': enum_vals}])

    query_test("a.b.*.v*",
               [{u'metric': self.metric1 + '.v.e', u'unit': u'percent'}],
               [self.metric1 + '.' + v for v in enum_vals] + [self.metric1 + '.v.e'],
               [{u'metric': self.metric1, u'unit': u'percent', u'enum_values': enum_vals}])

    query_test("a.b.*.v*.*",
               [{u'metric': self.metric1 + '.v.e.f', u'unit': u'percent'}],
               [self.metric1 + '.v.e.f'],
               [{u'metric': self.metric1, u'unit': u'percent', u'enum_values': enum_vals}])


    # now again, with submetrics
    self.finder.enable_submetrics = True
    query_test("*", 
               [{u'metric': self.metric1, u'unit': u'percent'}, 
               {u'metric': self.metric2, u'unit': u'percent'}],
               [self.metric1, self.metric2])

    query_test("a.*", 
               [{u'metric': self.metric1, u'unit': u'percent'}],
               [self.metric1])

    query_test("a.*",
               [{u'metric': self.metric1, u'unit': u'percent', u'enum_values': enum_vals}],
               [self.metric1])

    query_test("a.b.*", 
               [{u'metric': self.metric1, u'unit': u'percent'},
                {u'metric': 'a.bb.c', u'unit': u'percent'}],
               [self.metric1])

    query_test("a.b.c", 
               [{u'metric': self.metric1, u'unit': u'percent'}],
               [self.metric1])

    query_test("a.b.c.*", 
               [{u'metric': self.metric1, u'unit': u'percent'},
               {u'metric': (self.metric1 + 'd'), u'unit': u'percent'}],
               [self.metric1 + '.' + k for k in self.finder.submetric_aliases])

    query_test("a.b.c.*", 
               [{u'metric': self.metric1, u'unit': u'percent', u'enum_values': enum_vals},
               {u'metric': (self.metric1 + 'd'), u'unit': u'percent'}],
               [self.metric1 + '.' + k for k in self.finder.submetric_aliases])

    query_test("a.b.c._avg", 
               [{u'metric': self.metric1, u'unit': u'percent'}],
               [self.metric1 + '.' + self.alias_key])

    query_test("a.b.c._avg", 
               [{u'metric': self.metric1, u'unit': u'percent', u'enum_values': enum_vals}],
               [self.metric1 + '.' + self.alias_key])

    query_test("a.b.c.v1._enum", 
               [{u'metric': self.metric1, u'unit': u'percent', u'enum_values': enum_vals}],
               [self.metric1 + '.v1'])

    query_test("a.b.c.*._enum", 
               [{u'metric': self.metric1, u'unit': u'percent', u'enum_values': enum_vals}],
               [self.metric1 + '.' + v for v in enum_vals])

    query_test("a.b.*.*._enum", 
               [{u'metric': self.metric1, u'unit': u'percent', u'enum_values': enum_vals}],
               [self.metric1 + '.' + v for v in enum_vals])

    query_test("a.b.c.v*._enum", 
               [{u'metric': self.metric1, u'unit': u'percent', u'enum_values': enum_vals}],
               [self.metric1 + '.' + v for v in enum_vals])


  def test_fetch(self):
    step = 3000
    start = 1426120000
    end = 1426147000
    endpoint = self.bfc.get_multi_endpoint(self.finder.bf_query_endpoint, self.finder.tenant)
    nodes, responses = self.make_data(start, step)
    with requests_mock.mock() as m:
      m.post(endpoint, json={'metrics':responses}, status_code=200)
      time_info, dictionary = self.finder.fetch_multi(nodes, start, end)
      self.assertSequenceEqual(time_info, (1426120000, 1426147300, 300))
      self.assertDictEqual(dictionary,
                           {'e.f.g': 
                            [6421.18, 8643.402222222223, 10865.624444444446, 13087.846666666668, 15310.06888888889,
                             17532.291111111113, 19754.513333333336, 21976.73555555556, 24198.95777777778, 26421.18,
                             None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                             None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                             None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                             None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                             None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                             None, None, None, None, None, None],
                            'a.b.c': 
                            [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                             None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                             None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                             None, None, None, None, 4449.97, 4926.160476190476, 5402.350952380953, 5878.541428571429,
                             6354.731904761905, 6830.922380952381, 7307.112857142857, 7783.303333333333,
                             8259.49380952381, 8735.684285714287, 9211.874761904764, 9688.065238095242,
                             10164.255714285719, 10640.446190476196, 11116.636666666673, 11592.82714285715,
                             12069.017619047627, 12545.208095238104, 13021.39857142858, 13497.589047619058,
                             13973.779523809535, 14449.97, None, None, None, None, None, None, None, None, None, None,
                             None, None, None, None, None, None, None, None, None, None]})

    with requests_mock.mock() as m:
      m.post(endpoint, json=exc_callback, status_code=200)
      with self.assertRaises(ValueError):
        time_info, dictionary = self.finder.fetch_multi(nodes, start, end)

  def test_calc_res(self):
    start = 0
    stop1 = secs_per_res['MIN240']*801
    stop2 = stop1 - 1
    self.assertEqual(calc_res(start, stop1), 'MIN1440')
    self.assertEqual(calc_res(start, stop2), 'MIN240')

  def test_process_path(self):
    b = BluefloodClient("host", "tenant", False, None)
    step = 100
    big_step = step * 1000
    val_step = 12
    first_time = 1385074800000
    first_val = 48
    second_time = first_time + big_step
    second_val = first_val + val_step
    third_time = second_time + big_step
    third_val = second_val + val_step
    data_key = NonNestedDataKey(u'average')
    values = [{u'timestamp': first_time, u'average': first_val, u'numPoints': 97},
              {u'timestamp': second_time, u'average': second_val, u'numPoints': 3},
              {u'timestamp': third_time, u'average': third_val, u'numPoints': 3}]

    enum_values = [{u'timestamp': first_time, u'enum_values': {u'v1': first_val}, u'numPoints': 97},
                   {u'timestamp': second_time, u'enum_values': {u'v1': second_val}, u'numPoints': 3},
                   {u'timestamp': third_time, u'enum_values': {u'v1': third_val}, u'numPoints': 3}]
    enum_data_key = NestedDataKey('enum_values', 'v1')

    start_time = first_time/1000
    end_time = third_time/1000 + 1

    #test that start and end time exactly match the datapoints
    ret = b.process_path(values, start_time, end_time, step, data_key)
    self.assertSequenceEqual(ret, (first_val, second_val, third_val))

    ret = b.process_path(enum_values, start_time, end_time, step, enum_data_key)
    self.assertSequenceEqual(ret, (first_val, second_val, third_val))

    ret = b.process_path(values, start_time, end_time, step, data_key)
    self.assertSequenceEqual(ret, (first_val, second_val, third_val))

    ret = b.process_path(enum_values, start_time, end_time, step, enum_data_key)
    self.assertSequenceEqual(ret, (first_val, second_val, third_val))

    #test end time past end of data
    end_time += 2 * step
    ret = b.process_path(values, start_time, end_time, step, data_key)
    self.assertSequenceEqual(ret, (first_val, second_val, third_val, None, None))

    ret = b.process_path(enum_values, start_time, end_time, step, enum_data_key)
    self.assertSequenceEqual(ret, (first_val, second_val, third_val, None, None))

    #test start time before beginning of data
    end_time -= 2 * step
    start_time -= 2 * step
    ret = b.process_path(values, start_time, end_time, step, data_key)
    self.assertSequenceEqual(ret, (None, None, first_val, second_val, third_val))

    ret = b.process_path(enum_values, start_time, end_time, step, enum_data_key)
    self.assertSequenceEqual(ret, (None, None, first_val, second_val, third_val))

    #test end time before beginning of data
    end_time -= 3 * step
    start_time -= 3 * step
    ret = b.process_path(values, start_time, end_time, step, data_key)
    self.assertSequenceEqual(ret, (None, None, None, None, None))

    ret = b.process_path(enum_values, start_time, end_time, step, enum_data_key)
    self.assertSequenceEqual(ret, (None, None, None, None, None))



    #test start and end outside of data and interpolation in the middle
    second_time = first_time + (3 * big_step)
    third_time = second_time + (3 * big_step)
    start_time =  first_time - (2 * big_step)
    start_time /= 1000
    end_time =  third_time + (2 * big_step)
    end_time = (end_time/1000) + 1
    values = [{u'timestamp': first_time, u'average': first_val, u'numPoints': 97},
              {u'timestamp': second_time, u'average': second_val, u'numPoints': 3},
              {u'timestamp': third_time, u'average': third_val, u'numPoints': 3}]

    enum_values = [{u'timestamp': first_time, u'enum_values': {u'v1': first_val}, u'numPoints': 97},
                   {u'timestamp': second_time, u'enum_values': {u'v1': second_val}, u'numPoints': 3},
                   {u'timestamp': third_time, u'enum_values': {u'v1': third_val}, u'numPoints': 3}]

    ret = b.process_path(values, start_time, end_time, step, data_key)
    self.assertSequenceEqual(ret, (None, None, first_val, first_val + 4, first_val + 8, second_val,
                                   second_val + 4, second_val + 8, third_val , None, None))

    ret = b.process_path(enum_values, start_time, end_time, step, enum_data_key)
    self.assertSequenceEqual(ret, (None, None, first_val, first_val + 4, first_val + 8, second_val,
                                   second_val + 4, second_val + 8, third_val , None, None))

if __name__ == '__main__':
  unittest.main()
