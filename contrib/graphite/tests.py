from unittest import TestCase
import unittest
import os
import datetime
import requests_mock
from blueflood import TenantBluefloodFinder, TenantBluefloodReader, TenantBluefloodLeafNode, \
     BluefloodClient, auth, calc_res, secs_per_res

#To run these test you need to set up the environment vars below
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
  def setUp(self):
    self.alias_key = '_avg'
    config = {'blueflood':
              {'urls':["http://dummy.com"],
               'tenant':'dummyTenant',
               'submetric_aliases': {self.alias_key:'average'}}}
    self.finder = TenantBluefloodFinder(config)
    self.metric1 = "a.b.c"
    self.metric2 = "e.f.g"
    self.reader = TenantBluefloodReader(self.metric1, self.finder.tenant, self.finder.bf_query_endpoint, 
                                     self.finder.enable_submetrics, self.finder.submetric_aliases)
    self.node1 = TenantBluefloodLeafNode(self.metric1, self.reader)
    self.node2 = TenantBluefloodLeafNode(self.metric2, self.reader)
    self.bfc = BluefloodClient(self.finder.bf_query_endpoint, self.finder.tenant, 
                               self.finder.enable_submetrics, self.finder.submetric_aliases)
    auth.set_auth(None)

  def run_find(self, finder):
    nodes = list(finder.find_nodes(FindQuery('rackspace.*', 0, 100)))
    self.assertTrue(len(nodes) > 0)

  def setup_UTC_mock(self):
    #setup a mock that forces expiration
    self.orig_get_current_UTC = type(auth.auth).get_current_UTC
    self.orig_do_auth = type(auth.auth).do_auth
    this = self
    self.authCount = 0
    def mock_get_current_UTC(self):
      return this.orig_get_current_UTC(self) + datetime.timedelta(days=1)
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
    self.assertTrue(groups == [['a.b.c'], ['e.f.g']])

    #allow 2 metrics per group
    self.bfc.maxmetrics_per_req = 2
    groups = self.bfc.gen_groups([self.node1, self.node2])
    self.assertTrue(groups == [['a.b.c', 'e.f.g']])

    #now only room for 1 per group
    self.bfc.maxlen_per_req = 12
    groups = self.bfc.gen_groups([self.node1, self.node2])
    self.assertTrue(groups == [['a.b.c'], ['e.f.g']])

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
    self.assertTrue(groups == [['a.b'], ['e.f']])

    #allow 2 metrics per group
    self.bfc.maxmetrics_per_req = 2
    groups = self.bfc.gen_groups([self.node1, self.node2])
    self.assertTrue(groups == [['a.b', 'e.f']])

    #now only room for 1 per group
    self.bfc.maxlen_per_req = 10
    groups = self.bfc.gen_groups([self.node1, self.node2])
    self.assertTrue(groups == [['a.b'], ['e.f']])

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
    
  def test_gen_dict(self):
    step = 3000
    start = 1426120000
    end = 1426147000
    nodes, responses = self.make_data(start, step)
    dictionary = self.bfc.gen_dict(nodes, responses, start, end, step)
    self.assertDictEqual(dictionary,
                    {nodes[1].path: [6421.18, None, None, None, None, None, None, None, None], 
                     nodes[0].path: [None, None, None, None, 4449.97, None, None, 14449.97, None]})

    # check that it handles unfound metric correctly
    nodes[1].path += '.dummy'
    dictionary = self.bfc.gen_dict(nodes, responses, start, end, step)
    self.assertTrue(dictionary == 
                    {nodes[0].path: [None, None, None, None, 4449.97, None, None, 14449.97, None]})

    # now with submetrics
    self.bfc.enable_submetrics = True
    nodes, responses = self.make_data(start, step)
    dictionary = self.bfc.gen_dict(nodes, responses, start, end, step)
    self.assertTrue(dictionary == 
                    {nodes[1].path: [6421.18, None, None, None, None, None, None, None, None], 
                     nodes[0].path: [None, None, None, None, 4449.97, None, None, 14449.97, None]})

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
      self.assertTrue(responses == [])
    
    #test single group
    _, responses = self.make_data(start, step)
    with requests_mock.mock() as m:
      m.post(endpoint, json={'metrics':responses}, status_code=200)
      new_responses = self.bfc.gen_responses(groups1, payload)
      self.assertTrue(responses == new_responses)

    #test multiple groups
    groups2 = [[self.metric1], [self.metric2]]
    with requests_mock.mock() as m:
      global json_data
      json_data = [{'metrics':responses[0:1]},{'metrics':responses[1:]}]
      def json_callback(request, context):
        global json_data
        response = json_data[0]
        json_data = json_data[1:]
        return response

      m.post(endpoint, json=json_callback, status_code=200)
      new_responses = self.bfc.gen_responses(groups2, payload)
      self.assertTrue(responses == new_responses)

  
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
      return lambda y: '.'.join(y.split('.')[0:x])

    get_path = lambda x: x.path
    def query_test(query_pattern, jdata, qlen, search_results):
      with requests_mock.mock() as m:
        query = FindQuery(query_pattern, 1, 2)
        m.get(endpoint, json=jdata, status_code=200)
        metrics = self.finder.find_nodes(query)
        self.assertSequenceEqual(map(get_path, list(metrics)),
                                 map(get_start(qlen), search_results))

    query_test("*", 
               [{u'metric': self.metric1, u'unit': u'percent'}, 
                {u'metric': self.metric2, u'unit': u'percent'}],
               1, [self.metric1, self.metric2])

    query_test("a.*", 
               [{u'metric': self.metric1, u'unit': u'percent'}],
               2, [self.metric1])

    query_test("a.b.*", 
               [{u'metric': self.metric1, u'unit': u'percent'}],
               3, [self.metric1])

    query_test("a.b.c", 
               [{u'metric': self.metric1, u'unit': u'percent'}],
               3, [self.metric1])

    # now again, with submetrics
    self.finder.enable_submetrics = True
    query_test("*", 
               [{u'metric': self.metric1, u'unit': u'percent'}, 
                {u'metric': self.metric2, u'unit': u'percent'}],
               1, [self.metric1, self.metric2])

    query_test("a.*", 
               [{u'metric': self.metric1, u'unit': u'percent'}],
               2, [self.metric1])

    query_test("a.b.*", 
               [{u'metric': self.metric1, u'unit': u'percent'},
                {u'metric': 'a.bb.c', u'unit': u'percent'}],
               3, [self.metric1])

    query_test("a.b.c", 
               [{u'metric': self.metric1, u'unit': u'percent'}],
               3, [self.metric1])

    query_test("a.b.c.*", 
               [{u'metric': self.metric1, u'unit': u'percent'},
                {u'metric': (self.metric1 + 'd'), u'unit': u'percent'}],
               4, [self.metric1 + '.' + self.alias_key])

    query_test("a.b.c._avg", 
               [{u'metric': self.metric1, u'unit': u'percent'}],
               4, [self.metric1 + '.' + self.alias_key])

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
                            [6421.18, None, None, None, None, None, None, None, None, 26421.18, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None], 
                            'a.b.c': 
                            [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 4449.97, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 14449.97, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None]})

      time2, seq = self.reader.fetch(start, end)
      self.assertSequenceEqual(time2, (1426120000, 1426147300, 300))
      self.assertSequenceEqual(seq, dictionary[self.metric1])

    with requests_mock.mock() as m:
      m.post(endpoint, json=exc_callback, status_code=200)
      with self.assertRaises(ValueError):
        self.reader.fetch(start, end)


  def test_calc_res(self):
    start = 0
    stop1 = secs_per_res['MIN240']*801
    stop2 = stop1 - 1
    self.assertEqual(calc_res(start, stop1), 'MIN1440')
    self.assertEqual(calc_res(start, stop2), 'MIN240')

if __name__ == '__main__':
  unittest.main()
