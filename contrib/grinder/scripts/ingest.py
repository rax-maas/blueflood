import random
try: 
  from com.xhaus.jyson import JysonCodec as json
except ImportError:
  import json
from utils import *
from net.grinder.script import Test
from net.grinder.plugin.http import HTTPRequest

class IngestThread(AbstractThread):
  # The list of metric numbers for all threads in this worker
  metrics = []
  
  # Grinder test reporting infrastructure
  test1 = Test(1, "Ingest test")
  request = HTTPRequest()
  test1.record(request)


  @classmethod
  def create_metrics(cls, agent_number):
    """ Generate all the metrics for this worker

    The metrics are a list of batches.  Each batch is a list of metrics processed by
    a single metrics ingest request.
    """
    metrics =  generate_metrics_tenants(default_config['num_tenants'], 
                                            default_config['metrics_per_tenant'], agent_number, 
                                            default_config['num_nodes'], 
                                            cls.generate_metrics_for_tenant)

    cls.metrics = cls.divide_metrics_into_batches(metrics, default_config['batch_size'])

  @classmethod
  def num_threads(cls):
    return default_config['ingest_concurrency']

  @classmethod
  def generate_metrics_for_tenant(cls, tenant_id, metrics_per_tenant):
    l = [];
    for x in range(metrics_per_tenant):
      l.append([tenant_id, x])
    return l

  @classmethod
  def divide_metrics_into_batches(cls, metrics, batch_size):
    b = []
    for i in range(0, len(metrics), batch_size):
      b.append(metrics[i:i+batch_size])
    return b

  def __init__(self, thread_num):
    AbstractThread.__init__(self, thread_num)
    # Initialize the "slice" of the metrics to be sent by this thread
    start, end = generate_job_range(len(self.metrics), 
                                    self.num_threads(), thread_num)
    self.slice = self.metrics[start:end]

  def generate_metric(self, time, tenant_id, metric_id):
    return {'tenantId': str(tenant_id),
            'metricName': generate_metric_name(metric_id),
            'unit': self.generate_unit(tenant_id),
            'metricValue': random.randint(0, RAND_MAX),
            'ttlInSeconds': (2 * 24 * 60 * 60),
            'collectionTime': time}

  def generate_payload(self, time, batch):
    payload = map(lambda x:self.generate_metric(time,*x), batch)
    return json.dumps(payload)

  def ingest_url(self):
    return "%s/v2.0/tenantId/ingest/multi" % default_config['url']


  def make_request(self, logger):
    if len(self.slice) == 0:
      logger("Warning: no work for current thread")
      self.sleep(1000000)
      return None
    self.check_position(logger, len(self.slice))
    payload = self.generate_payload(int(self.time()),
                                           self.slice[self.position])
    self.position += 1
    result = self.request.POST(self.ingest_url(), payload)
    return result

ThreadManager.add_type(IngestThread)
