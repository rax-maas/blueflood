# It is difficult to invoke the Python coverage tool externally with Jython, so it
# is being invoked internally here:
from __future__ import division
import net.grinder.script.Grinder
package_path = net.grinder.script.Grinder.grinder.getProperties().getProperty("grinder.package_path")
import sys
sys.path.append(package_path)
from coverage import coverage
cov = coverage()
cov.start()

import time
import utils
import ingest
import query
import annotationsingest
import unittest
import random
import math
import grinder


try: 
  from com.xhaus.jyson import JysonCodec as json
except ImportError:
  import json
import pprint

pp = pprint.pprint
sleep_time = -1
get_url = None
post_url = None
post_payload = None

def mock_sleep(cls, x):
  global sleep_time
  sleep_time = x

class MockReq():
  def POST(self, url, payload):
    global post_url, post_payload
    post_url = url
    post_payload = payload
    return url, payload

  def GET(self, url):
    global get_url
    get_url = url
    return url

class BluefloodTests(unittest.TestCase):
  def setUp(self):
    self.real_shuffle = random.shuffle
    self.real_randint = random.randint
    self.real_time = utils.AbstractThread.time
    self.real_sleep = utils.AbstractThread.sleep
    self.tm = ingest.ThreadManager(net.grinder.script.Grinder.grinder)
    req = MockReq()
    ingest.IngestThread.request = req
    annotationsingest.AnnotationsIngestThread.request = req
    for x in query.QueryThread.query_types:
      x.query_request = req
    random.shuffle = lambda x: None
    random.randint = lambda x,y: 0
    utils.AbstractThread.time = lambda x:1000
    utils.AbstractThread.sleep = mock_sleep

    test_config = {'report_interval': (1000 * 6),
                   'num_tenants': 3,
                   'annotations_num_tenants': 3,
                   'metrics_per_tenant': 7,
                   'annotations_per_tenant': 2,
                   'batch_size': 3,
                   'ingest_concurrency': 2,
                   'query_concurrency': 10,
                   'annotations_concurrency':2,
                   'singleplot_per_interval': 11,
                   'multiplot_per_interval': 10,
                   'search_queries_per_interval': 9,
                   'annotations_queries_per_interval': 8,
                   'name_fmt': "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.%d",
                   'num_nodes': 2}

    ingest.default_config.update(test_config)
    
  def test_init_process(self):

    #confirm that threadnum 0 is an ingest thread
    t1 = self.tm.setup_thread(0)
    self.assertEqual(type(t1), ingest.IngestThread)

    #confirm that the threadnum after all ingest threads is a query thread
    t1 = self.tm.setup_thread(ingest.default_config['ingest_concurrency'])
    self.assertEqual(type(t1), query.QueryThread)

    #confirm that the threadnum after all ingest+query threads is an annotations query thread
    t1 = self.tm.setup_thread(ingest.default_config['ingest_concurrency']+ingest.default_config['query_concurrency'])
    self.assertEqual(type(t1), annotationsingest.AnnotationsIngestThread)

    #confirm that a threadnum after all valid thread types raises an exception
    tot_threads = (ingest.default_config['ingest_concurrency'] + ingest.default_config['query_concurrency'] + ingest.default_config['annotations_concurrency'])
    self.assertRaises(Exception,self.tm.setup_thread, tot_threads)

    #confirm that the correct batches of ingest metrics are created for worker 0
    self.tm.create_all_metrics(0)
    self.assertEqual(annotationsingest.AnnotationsIngestThread.annotations,
                             [[0, 0], [0, 1], [1, 0], [1, 1]])

    thread = annotationsingest.AnnotationsIngestThread(0)
    self.assertEqual(thread.slice, [[0, 0], [0, 1]])

    thread = annotationsingest.AnnotationsIngestThread(1)
    self.assertEqual(thread.slice, [[1, 0], [1, 1]])

    self.assertEqual(ingest.IngestThread.metrics,
                             [[[0, 0], [0, 1], [0, 2]],
                              [[0, 3], [0, 4], [0, 5]],
                              [[0, 6], [1, 0], [1, 1]],
                              [[1, 2], [1, 3], [1, 4]],
                              [[1, 5], [1, 6]]])
    

    #confirm that the correct batch slices are created for individual threads
    thread = ingest.IngestThread(0)
    self.assertEqual(thread.slice,
                             [[[0, 0], [0, 1], [0, 2]],
                              [[0, 3], [0, 4], [0, 5]],
                              [[0, 6], [1, 0], [1, 1]]])
    thread = ingest.IngestThread(1)
    self.assertEqual(thread.slice,
                             [[[1, 2], [1, 3], [1, 4]], 
                              [[1, 5], [1, 6]]])

    # confirm that the number of queries is correctly distributed across
    #  each thread in this worker process

    num_query_nodes = query.default_config['num_nodes']
    single_plot_queries_agent0 = int(math.ceil(query.default_config['singleplot_per_interval']/num_query_nodes))
    multi_plot_queries_agent0 = int(math.ceil(query.default_config['multiplot_per_interval']/num_query_nodes))
    search_queries_agent0 = int(math.ceil(query.default_config['search_queries_per_interval']/num_query_nodes))
    annotation_queries_agent0 = int(math.ceil(query.default_config['annotations_queries_per_interval']/num_query_nodes))
    
    self.assertEqual(query.QueryThread.queries,
                     ([query.SinglePlotQuery] * single_plot_queries_agent0 + [query.MultiPlotQuery] * multi_plot_queries_agent0 + [query.SearchQuery] * search_queries_agent0 + [query.AnnotationsQuery] * annotation_queries_agent0))

    thread = query.QueryThread(0)
    self.assertEqual(thread.slice, [query.SinglePlotQuery] * 2)

    thread = query.QueryThread(6)
    self.assertEqual(thread.slice, [query.SearchQuery] * 2)

    thread = query.QueryThread(9)
    self.assertEqual(thread.slice, [query.AnnotationsQuery] * 2)


    #confirm that the correct batches of ingest metrics are created for worker 1
    self.tm.create_all_metrics(1)
    self.assertEqual(ingest.IngestThread.metrics,
                             [[[2, 0], [2, 1], [2, 2]], 
                              [[2, 3], [2, 4], [2, 5]], 
                              [[2, 6]]])

    self.assertEqual(annotationsingest.AnnotationsIngestThread.annotations,
                     [[2, 0], [2, 1]])

    
    thread = ingest.IngestThread(0)
    self.assertEqual(thread.slice,
                             [[[2, 0], [2, 1], [2, 2]], 
                              [[2, 3], [2, 4], [2, 5]]])
    thread = ingest.IngestThread(1)
    self.assertEqual(thread.slice,
                             [[[2, 6]]])

    #confirm that the correct batches of queries are created for worker 1
    single_plot_queries_agent1 = query.default_config['singleplot_per_interval'] - single_plot_queries_agent0
    multi_plot_queries_agent1 = query.default_config['multiplot_per_interval'] - multi_plot_queries_agent0
    search_queries_agent1 = query.default_config['search_queries_per_interval'] - search_queries_agent0
    annotation_queries_agent1 = query.default_config['annotations_queries_per_interval'] - annotation_queries_agent0

    self.assertEqual(query.QueryThread.queries,
                     ([query.SinglePlotQuery] * single_plot_queries_agent1 + [query.MultiPlotQuery] * multi_plot_queries_agent1 + [query.SearchQuery] * search_queries_agent1 + [query.AnnotationsQuery] * annotation_queries_agent1))

    thread = query.QueryThread(0)
    self.assertEqual(thread.slice, [query.SinglePlotQuery] * 2)

    thread = query.QueryThread(6)
    self.assertEqual(thread.slice, [query.SearchQuery] * 2)

    thread = query.QueryThread(9)
    self.assertEqual(thread.slice, [query.AnnotationsQuery])

  def test_generate_payload(self):
    self.tm.create_all_metrics(1)
    thread = ingest.IngestThread(0)
    payload = json.loads(thread.generate_payload(0, [[2, 3], [2, 4], [2, 5]]))
    valid_payload = [{u'collectionTime': 0,
                      u'metricName': u'int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.3',
                      u'metricValue': 0,
                      u'tenantId': u'2',
                      u'ttlInSeconds': 172800,
                      u'unit': u'days'},
                     {u'collectionTime': 0,
                      u'metricName': u'int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.4',
                      u'metricValue': 0,
                      u'tenantId': u'2',
                      u'ttlInSeconds': 172800,
                      u'unit': u'days'},
                     {u'collectionTime': 0,
                      u'metricName': u'int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.5',
                      u'metricValue': 0,
                      u'tenantId': u'2',
                      u'ttlInSeconds': 172800,
                      u'unit': u'days'}]
    self.assertEqual(payload, valid_payload)

  def test_generate_annotations_payload(self):
    self.tm.create_all_metrics(1)
    thread = annotationsingest.AnnotationsIngestThread(0)
    payload = json.loads(thread.generate_payload(0, 3))
    valid_payload = {'what': 'annotation int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.3',
                      'when': 0,
                      'tags': 'tag',
                      'data': 'data'}
    self.assertEqual(payload, valid_payload)

  def test_annotationsingest_make_request(self):
    global sleep_time
    thread = annotationsingest.AnnotationsIngestThread(0)
    thread.slice = [[2, 0]]
    thread.position = 0
    thread.finish_time = 10000
    valid_payload = {"what": "annotation int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.0", "when": 1000, "tags": "tag", "data": "data"}

    url, payload = thread.make_request(pp)
    #confirm request generates proper URL and payload
    self.assertEqual(url, 
                     'http://qe01.metrics-ingest.api.rackspacecloud.com/v2.0/2/events')
    self.assertEqual(eval(payload), valid_payload)

    #confirm request increments position if not at end of report interval
    self.assertEqual(thread.position, 1)
    self.assertEqual(thread.finish_time, 10000)
    thread.position = 2
    thread.make_request(pp)

    #confirm request resets position at end of report interval
    self.assertEqual(sleep_time, 9000)
    self.assertEqual(thread.position, 1)
    self.assertEqual(thread.finish_time, 16000)

  def test_ingest_make_request(self):
    global sleep_time
    thread = ingest.IngestThread(0)
    thread.slice = [[[2, 0], [2, 1]]]
    thread.position = 0
    thread.finish_time = 10000
    valid_payload = [{"collectionTime": 1000, "ttlInSeconds": 172800, "tenantId": "2", "metricValue": 0, "unit": "days", "metricName": "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.0"}, {"collectionTime": 1000, "ttlInSeconds": 172800, "tenantId": "2", "metricValue": 0, "unit": "days", "metricName": "int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.1"}]

    url, payload = thread.make_request(pp)
    #confirm request generates proper URL and payload
    self.assertEqual(url,
                     'http://qe01.metrics-ingest.api.rackspacecloud.com/v2.0/tenantId/ingest/multi')
    self.assertEqual(eval(payload), valid_payload)

    #confirm request increments position if not at end of report interval
    self.assertEqual(thread.position, 1)
    self.assertEqual(thread.finish_time, 10000)
    thread.position = 2
    thread.make_request(pp)
    #confirm request resets position at end of report interval
    self.assertEqual(sleep_time, 9000)
    self.assertEqual(thread.position, 1)
    self.assertEqual(thread.finish_time, 16000)


  def test_query_make_request(self):
    thread = query.QueryThread(0)
    thread.slice = [query.SinglePlotQuery, query.SearchQuery, query.MultiPlotQuery, query.AnnotationsQuery]
    thread.position = 0
    thread.make_request(pp)
    self.assertEqual(get_url, "http://qe01.metrics.api.rackspacecloud.com/v2.0/0/views/int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.0?from=-86399000&to=1000&resolution=FULL")

    random.randint = lambda x,y: 10
    thread.make_request(pp)
    self.assertEqual(get_url, "http://qe01.metrics.api.rackspacecloud.com/v2.0/10/metrics/search?query=int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.*")

    random.randint = lambda x,y: 20
    thread.make_request(pp)
    self.assertEqual(post_url, "http://qe01.metrics.api.rackspacecloud.com/v2.0/20/views?from=-86399000&to=1000&resolution=FULL")
    self.assertEqual(eval(post_payload), ["int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.0","int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.1","int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.2","int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.3","int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.4","int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.5","int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.6","int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.7","int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.8","int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.9"])

    random.randint = lambda x,y: 30
    thread.make_request(pp)
    self.assertEqual(get_url, "http://qe01.metrics.api.rackspacecloud.com/v2.0/30/events/getEvents?from=-86399000&until=1000")

  def tearDown(self):
    random.shuffle = self.real_shuffle
    random.randint = self.real_randint
    utils.AbstractThread.time = self.real_time
    utils.AbstractThread.sleep = self.real_sleep

#if __name__ == '__main__':
unittest.TextTestRunner().run(unittest.TestLoader().loadTestsFromTestCase(BluefloodTests))

cov.stop()
cov.save()
class TestRunner:
  def __init__(self):
    pass

  def __call__(self):
    pass
