import json, requests
from omega_client.fpg.fpg_lib import FPGAuth, create_SOR_order
from omega_client.common_types.trading_structs import AccountInfo, Exchange, Order, \
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

# successful usage of create_SOR_order function
orders, status_code, error_message = create_SOR_order(
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


# unsuccessful usage of create_SOR_order function (itBit is unsupported)
# error code 400
orders, status_code, error_message = create_SOR_order(
    order=Order(
        account_info=AccountInfo(account_id=100),   # ignored, junk
        client_order_id='vnuiebwe',     # ignored, junk
        symbol='BTC/USD',
        side=Side.buy.name,
        order_type=OrderType.limit.name,
        quantity=6.,
        price=5000.),
    accounts={Exchange.itBit.name: AccountInfo(account_id=200),
              Exchange.kraken.name: AccountInfo(account_id=201)},
    auth=auth
)
for order in orders:
    print('order:', order)
print('status_code', status_code)
print('error_message', error_message)
