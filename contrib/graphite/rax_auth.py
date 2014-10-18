import datetime, time
from dateutil.parser import parse as dateparse
import requests
import json
from pytz import timezone

IDENTITY_ENDPOINT = 'https://identity.api.rackspacecloud.com/v2.0/'

class BluefloodAuth(object):
  def __init__(self, config):
    if config is not None:
      self.username = config['blueflood']['username']
      self.apiKey = config['blueflood']['apikey']
    else:
      from django.conf import settings
      self.username = getattr(settings, 'RAX_USER')
      self.apikey = getattr(settings, 'RAX_API_KEY')

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

