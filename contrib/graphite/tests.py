from mock import patch
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

config = {
  'BF_TENANT': '000000',
  'BF_QUERY': 'http://127.0.0.1:2500'
}

class BluefloodTests(TestCase):

  def test_conf(self):
    pass

  def test_finder(self):
    finder = TenantBluefloodFinder(config)
    nodes = list(finder.find_nodes(FindQuery('rackspace.*', 0, 100)))
    self.assertTrue(len(nodes) > 0)

if __name__ == '__main__':
  unittest.main()
