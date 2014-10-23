from unittest import TestCase
import unittest
import os
import datetime

from blueflood import TenantBluefloodFinder, auth

#To run this test you need to set up the ssh tunnel and environment var as
# described below

# This needs to be an env var corresponding to bf0testenv1's api key:
api_key = os.environ['RAX_API_KEY']

#port 2500 needs a tunnel like so:
#ssh -L 2500:localhost:2500 app00.iad.stage.bf.k1k.me
no_auth_config = {'blueflood':
                  { 'urls': ['http://127.0.0.1:2500'],
                    'tenant': "000000"}}

rax_auth_config = {'blueflood':
                   {'username': 'bf0testenv1',
                    'apikey': api_key,
                    'urls': ['http://iad.metrics.api.rackspacecloud.com'],
                    'authentication_module': 'rax_auth',
                    'tenant': "836986"}}

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

class BluefloodTests(TestCase):
  def test_conf(self):
    pass

  def run_find(self, finder):
    nodes = list(finder.find_nodes(FindQuery('rackspace.*', 0, 100)))
    self.assertTrue(len(nodes) > 0)

  def setup_UTC_mock(self):
    #setup a mock that forces expiration
    self.origGetCurrentUTC = type(auth.auth).getCurrentUTC
    self.origDoAuth = type(auth.auth).doAuth
    this = self
    self.authCount = 0
    def mockGetCurrentUTC(self):
      return this.origGetCurrentUTC(self) + datetime.timedelta(days=1)
    def mockDoAuth(self):
      this.authCount += 1
      this.origDoAuth(self)
    type(auth.auth).getCurrentUTC = mockGetCurrentUTC
    type(auth.auth).doAuth = mockDoAuth

  def unset_UTC_mock(self):
    type(auth.auth).getCurrentUTC = self.origGetCurrentUTC
    type(auth.auth).doAuth = self.origDoAuth

  def test_finder(self):
    #test no auth
    finder = TenantBluefloodFinder(no_auth_config)
    self.run_find(finder)

    #test rax auth
    finder = TenantBluefloodFinder(rax_auth_config)
    self.run_find(finder)

    #force re-auth
    auth.auth.token = ""
    self.run_find(finder)

    #test expired UTC
    self.setup_UTC_mock()
    self.run_find(finder)
    self.unset_UTC_mock()
    self.assertTrue(self.authCount == 1)

if __name__ == '__main__':
  unittest.main()
