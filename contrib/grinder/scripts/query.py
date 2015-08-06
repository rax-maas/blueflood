import random
try: 
  from com.xhaus.jyson import JysonCodec as json
except ImportError:
  import json
from utils import *
from net.grinder.script import Test
from net.grinder.plugin.http import HTTPRequest
import itertools

class AbstractQuery(object):
  one_day = (1000 * 60 * 60 * 24)

  query_interval_name = None
  num_queries_for_current_node = 0
  query_request = None
  query_name = None
  test_number = 0

  @classmethod
  def create_metrics(cls, agent_number):
    #divide the total number of each query type into the ones need by this worker
    total_queries = default_config[cls.query_interval_name]
    start_job, end_job = generate_job_range(total_queries, 
                                                default_config['num_nodes'], agent_number)
    cls.num_queries_for_current_node = end_job - start_job

    # Grinder test infrastructure
    test = Test(cls.test_number, cls.query_name)
    cls.query_request = HTTPRequest()
    test.record(cls.query_request)
    return [cls] * cls.num_queries_for_current_node

  def generate(self, time, logger):
    raise Exception("Can't instantiate abstract query")

  
class SinglePlotQuery(AbstractQuery):
  query_interval_name = 'singleplot_per_interval'
  query_name = "SinglePlotQuery"
  test_number = 3

  def generate(self, time, logger):
    tenant_id = random.randint(0, default_config['num_tenants'])
    metric_name = generate_metric_name(random.randint(0, default_config['metrics_per_tenant']))
    to = time
    frm = time - self.one_day
    resolution = 'FULL'
    url =  "%s/v2.0/%d/views/%s?from=%d&to=%s&resolution=%s" % (default_config['query_url'],
                                                                tenant_id, metric_name, frm,
                                                                to, resolution)
    result = self.query_request.GET(url)
#    logger(result.getText())
    return result


class MultiPlotQuery(AbstractQuery):
  query_interval_name = 'multiplot_per_interval'
  query_name = "MultiPlotQuery"
  test_number = 4

  def generate_multiplot_payload(self):
    metrics_count = min(default_config['max_multiplot_metrics'], 
                        random.randint(0, default_config['metrics_per_tenant']))
    metrics_list = map(generate_metric_name, range(metrics_count))
    return json.dumps(metrics_list)

  def generate(self, time, logger):
    tenant_id = random.randint(0, default_config['num_tenants'])
    payload = self.generate_multiplot_payload()
    to = time
    frm = time - self.one_day
    resolution = 'FULL'
    url = "%s/v2.0/%d/views?from=%d&to=%d&resolution=%s"  % (default_config['query_url'],
                                                                tenant_id, frm,
                                                                to, resolution)
    result = self.query_request.POST(url, payload)
#    logger(result.getText())
    return result


class SearchQuery(AbstractQuery):
  query_interval_name = 'search_queries_per_interval'
  query_name = "SearchQuery"
  test_number = 5
    
  def generate_metrics_regex(self):
    metric_name = generate_metric_name(random.randint(0, default_config['metrics_per_tenant']))
    return ".".join(metric_name.split('.')[0:-1]) + ".*"

  def generate(self, time, logger):
    tenant_id = random.randint(0, default_config['num_tenants'])
    metric_regex = self.generate_metrics_regex()
    url = "%s/v2.0/%d/metrics/search?query=%s" % (default_config['query_url'],
                                                                tenant_id, metric_regex)
    result = self.query_request.GET(url)
#    logger(result.getText())
    return result


class AnnotationsQuery(AbstractQuery):
  query_interval_name = 'annotations_queries_per_interval'
  query_name = "AnnotationsQuery"
  test_number = 6


  def generate(self, time, logger):
    tenant_id = random.randint(0, default_config['annotations_num_tenants'])
    to = time
    frm = time - self.one_day
    url = "%s/v2.0/%d/events/getEvents?from=%d&until=%d" % (default_config['query_url'], tenant_id, frm, to)
    result = self.query_request.GET(url)
#    logger(result.getText())
    return result



class QueryThread(AbstractThread):
  # The list of queries to be invoked across all threads in this worker
  queries = []

  query_types = [SinglePlotQuery, MultiPlotQuery, SearchQuery, AnnotationsQuery]

  @classmethod
  def create_metrics(cls, agent_number):
    cls.queries = list(itertools.chain(*[x.create_metrics(agent_number) for x in cls.query_types]))
    random.shuffle(cls.queries)
    
  @classmethod
  def num_threads(cls):
    return default_config['query_concurrency']

  def __init__(self, thread_num):
    AbstractThread.__init__(self, thread_num)
    self.query_instances = [x(thread_num, self.num_threads()) for x in self.query_types]
    total_queries_for_current_node = reduce(lambda x,y: x+y, 
                                            [x.num_queries_for_current_node 
                                             for x in self.query_instances])
    start, end = generate_job_range(total_queries_for_current_node,
                                                                 self.num_threads(),
                                                                 thread_num)
       
    self.slice = self.queries[start:end]
    self.query_fn_dict =  dict([[type(x), x.generate] for x in self.query_instances])


  def make_request(self, logger):
    if len(self.slice) == 0:
      logger("Warning: no work for current thread")
      self.sleep(1000000)
      return None
    self.check_position(logger, len(self.slice))
    result = self.query_fn_dict[self.slice[self.position]](int(self.time()), logger)
    self.position += 1
    return result

ThreadManager.add_type(QueryThread)
