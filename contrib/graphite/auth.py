import datetime, time
from dateutil.parser import parse as dateparse
import requests
import json
from pytz import timezone

IDENTITY_ENDPOINT = 'https://identity.api.rackspacecloud.com/v2.0/'

def headers():
  headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
  }
  return headers

class RaxAuth(object):
  def __init__(self, username, apiKey):
    self.username = username
    self.apiKey = apiKey
    self.expirationUTC = None

  def getToken(self, forceNew=False):
    if not self.expirationUTC or datetime.datetime.utcnow().replace(tzinfo=timezone('UTC')) > self.expirationUTC:
      self.doAuth()
    return self.token

  def doAuth(self):
    payload = '{"auth":{"RAX-KSKEY:apiKeyCredentials"{"username":"%s","apiKey":"%s"}}}' % (self.username, self.apiKey)
    r = requests.post(IDENTITY_ENDPOINT + 'tokens', data=payload, headers=headers())
    jsonObj = r.json()
    self.token = jsonObj['access']['token']['id']
    self.expirationUTC = dateparse(jsonObj['access']['token']['expires']).replace(tzinfo=timezone('UTC'))
    # todo mark the expiration.
    # todo handle errors

class NoAuth(object):
  def __init__(self):
    self.token = ''

  def getToken(self, forceNew=False):
    return self.token


auth = NoAuth()

def isActive():
  return auth is not None

def getToken():
  return auth.getToken()

def setAuth(newAuth):
  global auth
  auth = newAuth
