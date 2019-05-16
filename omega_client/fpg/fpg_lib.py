# external imports
import hmac, hashlib, logging, time, requests
from json import JSONDecodeError
from requests.auth import AuthBase
from typing import Dict
# omega library imports
from omega_client.fpg.fpg_orders_convert import (
    convert_f3_order_to_fpg_order, convert_fpg_orders_to_omg_orders)
from omega_client.messaging.common_types import AccountInfo, Order, OrderType

logger = logging.getLogger(__name__)

FPG_BASE_URL = 'https://fund3-staging.floating.group'
ORDERS_URL = FPG_BASE_URL + '/v1/orders'


class FPGAuth(AuthBase):
    def __init__(self, api_key: str, secret_key: str):
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


def create_SOR_order(order: Order,
                     accounts: Dict[str, AccountInfo],
                     auth: FPGAuth):
    """

    :param order: (Order) parent Order to be spilt up amongst exchanges (
    account_info field is discarded)
    :param accounts: (Dict[str, AccountInfo]) dict of exchanges: AccountInfo
    for which we will split up orders on
    :param auth: (FPGAuth) authentication used to sign request
    :return: List[Order] list of child Orders to be placed (empty list [] if
    status code is not 200), status_code (int), error_message (str)
    """
    sor_body = convert_f3_order_to_fpg_order(order=order, accounts=accounts)
    logger.info('json to be sent to fpg', extra={'sor_body': sor_body})
    r = requests.post(ORDERS_URL, json=sor_body, auth=auth)
    try:
        status_code, json_response = r.status_code, r.json()
    except JSONDecodeError:
        status_code, json_response = r.status_code, ''
    logger.info('fpg response status code', extra={'status_code': status_code})
    logger.info('fpg response body', extra={'response': json_response})

    orders = []
    error_message = ''
    if status_code == 200:  # successful response
        fpg_order_list = json_response.get('createOrderResponse', []).get(
            'immediates', [])
        for child_order in fpg_order_list:
            orders.append(convert_fpg_orders_to_omg_orders(
                fpg_order=child_order, accounts=accounts))
    elif status_code == 400:  # unsuccessful response
        error_message = ''
    elif status_code == 404:  # unsuccessful response 
        error_message = json_response.get('error').get('message')
    else:
        error_message = ''

    return orders, status_code, error_message
