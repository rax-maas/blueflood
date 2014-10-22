from unittest import TestCase
import unittest
import os

from blueflood import TenantBluefloodFinder, auth

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

# This needs to be an env var corresponding to bf0testenv1's api key:
api_key = os.environ['RAX_API_KEY']

rax_auth_config = {'blueflood':
                   {'username': 'bf0testenv1',
                    'apikey': api_key,
                    'urls': ['http://iad.metrics.api.rackspacecloud.com'],
                    'authentication_module': 'rax_auth',
                    'tenant': "836986"}}

#port 2500 needs a tunnel like so:
#ssh -L 2500:localhost:2500 app00.iad.stage.bf.k1k.me

no_auth_config = {'blueflood':
                  { 'urls': ['http://127.0.0.1:2500'],
                    'tenant': "000000"}}

class BluefloodTests(TestCase):

  def test_conf(self):
    pass

  def test_finder(self):
    #test rax auth
    finder = TenantBluefloodFinder(rax_auth_config)
    nodes = list(finder.find_nodes(FindQuery('rackspace.*', 0, 100)))
    self.assertTrue(len(nodes) > 0)

    #force re-auth
    auth.auth.token = ""
    nodes = list(finder.find_nodes(FindQuery('rackspace.*', 0, 100)))
    self.assertTrue(len(nodes) > 0)

    #test no auth
    finder = TenantBluefloodFinder(no_auth_config)
    nodes = list(finder.find_nodes(FindQuery('rackspace.*', 0, 100)))
    self.assertTrue(len(nodes) > 0)

if __name__ == '__main__':
  unittest.main()
