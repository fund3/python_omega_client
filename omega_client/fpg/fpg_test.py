import json, hmac, hashlib, time, requests
from requests.auth import AuthBase

# Before implementation, set environmental variables API_KEY and API_SECRET

with open('fpg_key.json', 'r') as f:
    creds = json.load(f)
API_KEY = creds['FPG_API_KEY']
API_SECRET = creds['FPG_API_SECRET']


class FPGAuth(AuthBase):
    def __init__(self, api_key, secret_key):
        self.api_key = api_key
        self.secret_key = secret_key

    def __call__(self, request):
        timestamp = str(int(time.time()))
        message = timestamp + self.api_key + request.path_url
        signature = hmac.new(bytes(self.secret_key, 'latin-1'),
                             bytes(message, 'latin-1'),
                             hashlib.sha256).hexdigest()

        request.headers.update({
            'FPG-ACCESS-SIGN': signature,
            'FPG-ACCESS-TIMESTAMP': timestamp,
            'FPG-ACCESS-KEY': self.api_key,
        })
        return request


auth = FPGAuth(API_KEY, API_SECRET)

# Hit an error trying to fetch a nonexistent order
api_url = 'https://fund3-staging.floating.group/v1/orders/foo'
r = requests.get(api_url, auth=auth)
print(r.status_code)
print(r.json())
