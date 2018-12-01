from typing import List

import zmq

from tes_client.common_types import AccountCredentials, AccountInfo, Order, \
    OrderInfo, OrderType, TimeInForce
from tes_client.tes_connection import TesConnection


class SingleClientTesConnection(TesConnection):
    """
    Mostly identical with TesConnection, but added boilerplate code support
    for connections with only 1 clientID.
    """
    def __init__(self, tes_connection_string: str, zmq_context: zmq.Context,
                 clientID: int, senderCompID: str):
        self._clientID = clientID
        self._senderCompID = senderCompID
        super().__init__(tes_connection_string, zmq_context)

    """
    ############################################################################

    ~~~~~~~~~~~~~~~~~~~~~~~~~~~ Outgoing TESMessages ~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ----------------- Public Methods to be called by client -------------------

    ############################################################################
    """

    def logon(self, credentials: List[AccountCredentials], clientID: int=None,
              senderCompID: str=None):
        return super().logon(credentials, self._clientID, self._senderCompID)

    def logoff(self, clientID: int=None, senderCompID: str=None):
        return super().logoff(self._clientID, self._senderCompID)

    def send_heartbeat(self, clientID: int=None, senderCompID: str=None):
        return super().send_heartbeat(self._clientID, self._senderCompID)

    def place_order(self, order: Order, clientID: int=None,
                    senderCompID: str=None):
        return super().place_order(order, self._clientID, self._senderCompID)

    def replace_order(self, accountInfo: AccountInfo,
                      orderID: str,
                      clientID: int=None,
                      senderCompID: str=None,
                      orderType: str=OrderType.market.name,
                      quantity: float=-1.0,
                      price: float=-1.0,
                      timeInForce: str=TimeInForce.gtc.name):
        return super().replace_order(accountInfo,
                                     orderID,
                                     self._clientID,
                                     self._senderCompID,
                                     orderType,
                                     quantity,
                                     price,
                                     timeInForce)

    def cancel_order(self, accountInfo: AccountInfo,
                     orderID: str,
                     clientID: int=None,
                     senderCompID: str=None):
        return super().cancel_order(accountInfo, orderID, self._clientID,
                                    self._senderCompID)

    def request_account_data(self, accountInfo: AccountInfo,
                             clientID: int=None,
                             senderCompID: str=None):
        return super().request_account_data(accountInfo, self._clientID,
                                            self._senderCompID)

    def request_open_positions(self, accountInfo: AccountInfo,
                               clientID: int=None,
                               senderCompID: str=None):
        return super().request_open_positions(accountInfo, self._clientID,
                                              self._senderCompID)

    def request_account_balances(self, accountInfo: AccountInfo,
                                 clientID: int=None,
                                 senderCompID: str=None):
        return super().request_account_balances(accountInfo,
                                                self._clientID,
                                                self._senderCompID)

    def request_working_orders(self, accountInfo: AccountInfo,
                               clientID: int=None,
                               senderCompID: str=None):
        return super().request_working_orders(accountInfo, self._clientID,
                                              self._senderCompID)

    def request_order_status(self, accountInfo: AccountInfo,
                             orderID: str,
                             clientID: int=None,
                             senderCompID: str=None):
        return super().request_order_status(accountInfo,
                                            orderID,
                                            self._clientID,
                                            self._senderCompID)

    def request_completed_orders(self, accountInfo: AccountInfo,
                                 clientID: int=None,
                                 senderCompID: str=None,
                                 count: int=None,
                                 since: float=None):
        return super().request_completed_orders(accountInfo,
                                                self._clientID,
                                                self._senderCompID,
                                                count,
                                                since)

    def request_order_mass_status(self, accountInfo: AccountInfo,
                                  orderInfo: List[OrderInfo],
                                  clientID: int=None,
                                  senderCompID: str=None):
        return super().request_order_mass_status(
            accountInfo, orderInfo, self._clientID, self._senderCompID)

    def request_exchange_properties(self, exchange: str,
                                    clientID: int=None,
                                    senderCompID: str=None):
        return super().request_exchange_properties(
            exchange, self._clientID, self._senderCompID)
