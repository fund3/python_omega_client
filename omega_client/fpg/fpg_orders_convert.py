from typing import Dict
import random

from omega_client.common_types.trading_structs import Order, AccountInfo
from omega_client.common_types.enum_types import OrderType


def convert_f3_order_to_fpg_order(order: Order,
                                  accounts: Dict[str, AccountInfo]):
    """

    :param order: (Order) parent Order to be spilt up amongst exchanges (
    account_info field is discarded)
    :param accounts: (Dict[str, AccountInfo]) dict of exchanges: AccountInfo
    for which we will split up orders on
    :return: json dict for the body of the order request to send to fpg
    """
    [base, quote] = order.symbol.split('/')
    algo = 'DMA'
    return {
        "base": base,
        "quote": quote,
        "size": float(order.quantity),
        "price": float(order.price),
        "algo": algo,
        "orderType": order.side.upper(),
        "exchangeNames": [exchange.upper() for exchange in list(
            accounts.keys())],
        "userName": str(random.choice(list(accounts.values())).account_id)
    }


def convert_fpg_orders_to_omg_orders(fpg_order: dict,
                                     accounts: Dict[str, AccountInfo]):
    symbol = fpg_order.get('base') + '/' + fpg_order.get('quote')
    exchange = fpg_order.get('exchangeName').lower()
    quantity = float(fpg_order.get('expectedSize'))
    price = float(fpg_order.get('expectedPrice'))
    side = fpg_order.get('orderType').lower()
    client_order_id = str(fpg_order.get('name').split('/')[-1])
    return Order(
        account_info=accounts.get(exchange),
        client_order_id=client_order_id,
        symbol=symbol,
        side=side,
        order_type=OrderType.limit.name,
        quantity=quantity,
        price=price
    )
