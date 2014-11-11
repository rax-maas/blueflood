from unittest import TestCase
import unittest
import os
import datetime

from blueflood import TenantBluefloodFinder, auth

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
except:
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

class BluefloodTests(TestCase):
  def test_conf(self):
    pass

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

if __name__ == '__main__':
  unittest.main()
