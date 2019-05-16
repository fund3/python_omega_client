import json, hmac, hashlib, time, requests
from requests.auth import AuthBase
from omega_client.fpg.fpg_lib import FPGAuth, create_fpg_SOR_order
from omega_client.messaging.common_types import AccountInfo, Exchange, Order, \
    OrderType, Side

# Before implementation, set environmental variables API_KEY and API_SECRET

with open('fpg_key.json', 'r') as f:
    creds = json.load(f)
API_KEY = creds['FPG_API_KEY']
API_SECRET = creds['FPG_API_SECRET']


auth = FPGAuth(API_KEY, API_SECRET)

# Hit an error trying to fetch a nonexistent order
api_url = 'https://fund3-staging.floating.group/v1/orders/foo'
r = requests.get(api_url, auth=auth)
print(r.status_code)
print(r.json())


# Create an DMA Order to be split up using SOR
sor_api_url = 'https://fund3-staging.floating.group/v1/orders'
sor_body = {
    "base": "BTC",
    "quote": "USD",
    "size": 6,
    "price": 5000,
    "algo": "DMA",
    # "algo": "ACTIVE",
    "orderType": "BUY",
    "exchangeNames": [
        "GEMINI", "KRAKEN"
    ]
}
r1 = requests.post(sor_api_url, json=sor_body, auth=auth)
print(r1.status_code)
print(r1.json())

# successful usage of create_fpg_SOR_order function
orders, status_code, error_message = create_fpg_SOR_order(
    order=Order(
        account_info=AccountInfo(account_id=100),   # ignored, junk
        client_order_id='vnuiebwe',     # ignored, junk
        symbol='BTC/USD',
        side=Side.buy.name,
        order_type=OrderType.limit.name,
        quantity=6.,
        price=5000.),
    accounts={Exchange.gemini.name: AccountInfo(account_id=200),
              Exchange.kraken.name: AccountInfo(account_id=201)},
    auth=auth
)
for order in orders:
    print('order:', order)

print('status_code', status_code)
print('error_message', error_message)
