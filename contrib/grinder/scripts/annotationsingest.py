import random
try: 
  from com.xhaus.jyson import JysonCodec as json
except ImportError:
  import json
from utils import *
from net.grinder.script import Test
from net.grinder.plugin.http import HTTPRequest

class AnnotationsIngestThread(AbstractThread):
  # The list of metric numbers for all threads in this worker
  annotations = []
  
  # Grinder test reporting infrastructure
  test1 = Test(1, "Annotations Ingest test")
  request = HTTPRequest()
  test1.record(request)


  @classmethod
  def create_metrics(cls, agent_number):
    """ Generate all the metrics for this worker

    The metrics are a list of batches.  Each batch is a list of metrics processed by
    a single metrics ingest request.
    """
    annotations =  generate_metrics_tenants(default_config['num_tenants'],
                                            default_config['annotations_per_tenant'], agent_number,
                                            default_config['num_nodes'], 
                                            cls.generate_annotations_for_tenant)

    cls.annotations = cls.divide_metrics_into_batches(annotations, 1)

  @classmethod
  def num_threads(cls):
    return default_config['annotations_concurrency']

  @classmethod
  def generate_annotations_for_tenant(cls, tenant_id, annotations_per_tenant):
    l = [];
    for x in range(annotations_per_tenant):
      l.append([tenant_id, x])
    return l

  @classmethod
  def divide_metrics_into_batches(cls, annotations, batch_size=1):
    b = []

    for i in range(0, len(annotations), batch_size):
      b.append(annotations[i:i+batch_size])
    return b

  def __init__(self, thread_num):
    AbstractThread.__init__(self, thread_num)
    # Initialize the "slice" of the metrics to be sent by this thread
    start, end = generate_job_range(len(self.annotations),
                                    self.num_threads(), thread_num)
    self.slice = self.annotations[start:end]

  def generate_annotation(self, time, tenant_id, metric_id):

    metric_name = generate_metric_name(metric_id)

    return {'what': 'annotation '+metric_name,
            'when': time,
            'tags': 'tag',
            'data': 'data'}

  def generate_payload(self, time, batch):
    payload = map(lambda x:self.generate_annotation(time,*x), batch)
    return json.dumps(payload)

  def ingest_url(self):
    return "%s/v2.0/tenantId/events" % default_config['url']

  def make_request(self, logger):
    if len(self.slice) == 0:
      logger("Warning: no work for current thread")
      self.sleep(1000)
      return None
    self.check_position(logger, len(self.slice))
    payload = self.generate_payload(int(self.time()),
                                           self.slice[self.position])
    self.position += 1
    result = self.request.POST(self.ingest_url(), payload)
    return result

ThreadManager.add_type(AnnotationsIngestThread)
