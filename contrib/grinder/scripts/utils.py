import time
import random
from net.grinder.script.Grinder import grinder

default_config = {
  'name_fmt': "t4.int.abcdefg.hijklmnop.qrstuvw.xyz.ABCDEFG.HIJKLMNOP.QRSTUVW.XYZ.abcdefg.hijklmnop.qrstuvw.xyz.met.%d",
  'report_interval': (1000 * 10),
  'annotations_num_tenants':5,
  'num_tenants': 4,
  'metrics_per_tenant': 15,
  'annotations_per_tenant': 10,
  'batch_size': 5,
  'ingest_concurrency': 15,
  'num_nodes': 1,
  'url': "http://localhost:19000",
  'query_url': "http://localhost:20000",
  'query_concurrency': 10,
  'annotations_concurrency':5,
  'max_multiplot_metrics': 10,
  'search_queries_per_interval': 10,
  'multiplot_per_interval': 10,
  'singleplot_per_interval': 10,
  'annotations_queries_per_interval': 8}

units_map = {0: 'minutes',
             1: 'hours',
             2: 'days',
             3: 'months',
             4: 'years',
             5: 'decades'}


RAND_MAX =  982374239

class ThreadManager(object):
  #keep track of the various thread types
  types = []

  @classmethod
  def add_type(cls, type):
    cls.types.append(type)

  def convert(self, s):
    try:
      return int(s)
    except:
      return eval(s)

  def setup_config(self, grinder):
    #Parse the properties file and update default_config dictionary
    for entry in grinder.getProperties().entrySet():
      if entry.value.startswith(".."):
        continue
      if entry.key == "grinder.threads":
        self.tot_threads = self.convert(entry.value)
      if entry.key.find("concurrency") >= 0:
        self.concurrent_threads += self.convert(entry.value)
      if not entry.key.startswith("grinder.bf."):
        continue
      k = entry.key.replace("grinder.bf.","")
      default_config[k] = self.convert(entry.value)

  def __init__(self, grinder):
    # tot_threads is the value passed to the grinder at startup for the number of threads to start
    self.tot_threads = 0

    # concurrent_threads is the sum of the various thread types, (currently ingest and query)
    self.concurrent_threads = 0
    self.setup_config(grinder)
    
    # Sanity check the concurrent_threads to make sure they are the same as the value
    #  passed to the grinder
    if self.tot_threads != self.concurrent_threads:
      raise Exception("Configuration error: grinder.threads doesn't equal total concurrent threads")

  def create_all_metrics(self, agent_number):
    """Step through all the attached types and have them create their metrics"""
    for x in self.types:
      x.create_metrics(agent_number)

  def setup_thread(self, thread_num):
    """Figure out which type thread to create based on thread_num and return it

    Creates threads of various types for use by the grinder to load
    test various parts of blueflood.  The code is structured so that
    the thread type is determined by the thread num.  The grinder
    properties file determines how many of each type to create based
    on the "ingest_concurrency" and "query_concurrency" options.

    So for example, if "ingest_concurrency" is set to 4, and "query_concurrency"
    is set to 2, thread numbers 0-3 will be ingest threads and thread numbers 4-5
    will be query threads.

    """
    thread_type = None
    server_num = thread_num

    for x in self.types:
      if server_num < x.num_threads():
        thread_type = x
        break
      else:
        server_num -= x.num_threads()

    if thread_type == None:
      raise Exception("Invalid Thread Type")

    return thread_type(server_num)


def generate_job_range(total_jobs, total_servers, server_num):
  """ Determine which subset of the total work the current server is to do.

  The properties file is the same for all the distributed workers and lists the
  total amount of work to be done for each report interval.  This method allows
  you to split that work up into the exact subset to be done by the "server_num" worker
  """
  jobs_per_server = total_jobs/total_servers
  remainder = total_jobs % total_servers
  start_job = jobs_per_server * server_num
  start_job += min(remainder, server_num)
  end_job = start_job + jobs_per_server
  if server_num < remainder:
    end_job += 1
  return (start_job, end_job)

def generate_metrics_tenants(num_tenants, metrics_per_tenant,
                             agent_number, num_nodes, gen_fn):
  """ generate the subset of the total metrics to be done by this agent"""
  tenants_in_shard = range(*generate_job_range(num_tenants, num_nodes, agent_number))
  metrics = []
  for y in map(lambda x: gen_fn(x, metrics_per_tenant), tenants_in_shard):
    metrics += y
  random.shuffle(metrics)
  return metrics

def generate_metric_name(metric_id):
  return default_config['name_fmt'] % metric_id

class AbstractThread(object):
  #superclass for the various thread types
  @classmethod
  def create_metrics(cls, agent_number):
    raise Exception("Can't create abstract thread")

  @classmethod
  def num_threads(cls):
    raise Exception("Can't create abstract thread")

  def make_request(self, logger):
    raise Exception("Can't create abstract thread")

  def __init__(self, thread_num):
    # The threads only do so many invocations for each 'report_interval'
    # position refers to the current position for current interval
    self.position = 0

    # finish_time is the end time of the interval
    self.finish_time = self.time() + default_config['report_interval']

  def generate_unit(self, tenant_id):
    unit_number = tenant_id % 6
    return units_map[unit_number]

  def check_position(self, logger, max_position):
    """Sleep if finished all work for report interval"""
    if self.position >= max_position:
      self.position = 0
      sleep_time = self.finish_time - self.time()
      self.finish_time += default_config['report_interval']
      if sleep_time < 0:
        #return error
        logger("finish time error")
      else:
        logger("pausing for %d" % sleep_time)
        self.sleep(sleep_time)

  @classmethod
  def time(cls):
    return int(time.time() * 1000)

  @classmethod
  def sleep(cls, x):
    return time.sleep(x/1000)
