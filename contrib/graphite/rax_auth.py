import datetime, time
from dateutil.parser import parse as dateparse
import requests
import json
from pytz import timezone
import auth

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

    self.expiration_UTC = None

  def get_current_UTC(self):
    return datetime.datetime.utcnow().replace(tzinfo=timezone('UTC'))

  def get_token(self, force_new):
    if force_new or not self.expiration_UTC or self.get_current_UTC() > self.expiration_UTC:
      self.do_auth()
    return self.token

  def do_auth(self):
    payload = '{"auth":{"RAX-KSKEY:apiKeyCredentials"{"username":"%s","apiKey":"%s"}}}' % (self.username, self.apiKey)
    r = requests.post(IDENTITY_ENDPOINT + 'tokens', data=payload, headers=auth.headers())
    jsonObj = r.json()
    self.token = jsonObj['access']['token']['id']
    self.expiration_UTC = dateparse(jsonObj['access']['token']['expires']).replace(tzinfo=timezone('UTC'))
    # todo handle errors

