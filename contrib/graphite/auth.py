def headers():
  headers = {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
  }
  return headers

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
