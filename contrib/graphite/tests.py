from unittest import TestCase
import unittest

from blueflood import TenantBluefloodFinder

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

rax_auth_config = {'blueflood': {'username': 'bf0testenv1', 'apikey': '473d1cde4e8bccf60142e23690ccc31d', 'urls': ['http://iad.metrics.api.rackspacecloud.com'], 'authentication_module': 'rax_auth', 'tenant': "836986"}}

no_auth_config = {'blueflood': { 'urls': ['http://127.0.0.1:2500'],  'tenant': "000000"}}

class BluefloodTests(TestCase):

  def test_conf(self):
    pass

  def test_finder(self):
    finder = TenantBluefloodFinder(rax_auth_config)
    nodes = list(finder.find_nodes(FindQuery('rackspace.*', 0, 100)))
    self.assertTrue(len(nodes) > 0)

    finder = TenantBluefloodFinder(no_auth_config)
    nodes = list(finder.find_nodes(FindQuery('rackspace.*', 0, 100)))
    self.assertTrue(len(nodes) > 0)

if __name__ == '__main__':
  unittest.main()
