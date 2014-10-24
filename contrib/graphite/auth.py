def headers():
  headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
  }
  return headers

class NoAuth(object):
  def __init__(self):
    self.token = ''

  def get_token(self, force_new=False):
    return self.token


auth = NoAuth()

def is_active():
  return auth is not None

def get_token(force_new=False):
  return auth.get_token(force_new)

def set_auth(new_auth):
  global auth
  auth = new_auth
