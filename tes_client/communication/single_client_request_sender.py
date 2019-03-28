from queue import Queue
from typing import List

import zmq

from tes_client.messaging.common_types import AccountCredentials, AccountInfo, \
    Order, OrderInfo, OrderType, TimeInForce
from tes_client.communication.request_sender import RequestSender


class SingleClientRequestSender(RequestSender):
    """
    Mostly identical with TesConnection, but added boilerplate code support
    for connections with only 1 client_id.
    """
    def __init__(self, zmq_context: zmq.Context,
                 connection_string: str,
                 client_id: int,
                 sender_comp_id: str,
                 outgoing_message_queue: Queue = None):
        self._client_id = client_id
        self._sender_comp_id = sender_comp_id
        super().__init__(zmq_context, connection_string,
                         outgoing_message_queue=outgoing_message_queue)

    """
    ############################################################################

    ~~~~~~~~~~~~~~~~~~~~~~~~~~~ Outgoing TESMessages ~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ----------------- Public Methods to be called by client -------------------

    ############################################################################
    """
    def logon(self, credentials: List[AccountCredentials],
              client_id: int = None,
              sender_comp_id: str = None):
        return super().logon(credentials, self._client_id, self._sender_comp_id)

    def logoff(self, client_id: int = None, sender_comp_id: str = None):
        return super().logoff(self._client_id, self._sender_comp_id)

    def send_heartbeat(self, client_id: int = None, sender_comp_id: str = None):
        return super().send_heartbeat(self._client_id, self._sender_comp_id)

    def place_order(self, order: Order, client_id: int = None,
                    sender_comp_id: str = None):
        return super().place_order(order, self._client_id, self._sender_comp_id)

    def replace_order(self, account_info: AccountInfo,
                      order_id: str,
                      client_id: int = None,
                      sender_comp_id: str = None,
                      order_type: str=OrderType.market.name,
                      quantity: float=-1.0,
                      price: float=-1.0,
                      time_in_force: str=TimeInForce.gtc.name):
        return super().replace_order(account_info,
                                     order_id,
                                     self._client_id,
                                     self._sender_comp_id,
                                     order_type,
                                     quantity,
                                     price,
                                     time_in_force)

    def cancel_order(self, account_info: AccountInfo,
                     order_id: str,
                     client_id: int = None,
                     sender_comp_id: str = None):
        return super().cancel_order(account_info, order_id, self._client_id,
                                    self._sender_comp_id)

    def cancel_all_orders(self, account_info: AccountInfo,
                          client_id: int = None,
                          sender_comp_id: str = None,
                          symbol: str = None,
                          side: str = None):
        return super().cancel_all_orders(account_info, self._client_id,
                                         self._sender_comp_id, symbol, side)

    def request_account_data(self, account_info: AccountInfo,
                             client_id: int = None,
                             sender_comp_id: str = None):
        return super().request_account_data(account_info, self._client_id,
                                            self._sender_comp_id)

    def request_open_positions(self, account_info: AccountInfo,
                               client_id: int = None,
                               sender_comp_id: str = None):
        return super().request_open_positions(account_info, self._client_id,
                                              self._sender_comp_id)

    def request_account_balances(self, account_info: AccountInfo,
                                 client_id: int = None,
                                 sender_comp_id: str = None):
        return super().request_account_balances(account_info,
                                                self._client_id,
                                                self._sender_comp_id)

    def request_working_orders(self, account_info: AccountInfo,
                               client_id: int = None,
                               sender_comp_id: str = None):
        return super().request_working_orders(account_info, self._client_id,
                                              self._sender_comp_id)

    def request_order_status(self, account_info: AccountInfo,
                             order_id: str,
                             client_id: int = None,
                             sender_comp_id: str = None):
        return super().request_order_status(account_info,
                                            order_id,
                                            self._client_id,
                                            self._sender_comp_id)

    def request_completed_orders(self, account_info: AccountInfo,
                                 client_id: int = None,
                                 sender_comp_id: str = None,
                                 count: int = None,
                                 since: float = None):
        return super().request_completed_orders(account_info,
                                                self._client_id,
                                                self._sender_comp_id,
                                                count,
                                                since)

    def request_order_mass_status(self, account_info: AccountInfo,
                                  order_info: List[OrderInfo],
                                  client_id: int = None,
                                  sender_comp_id: str = None):
        return super().request_order_mass_status(
            account_info, order_info, self._client_id, self._sender_comp_id)

    def request_exchange_properties(self, exchange: str,
                                    client_id: int = None,
                                    sender_comp_id: str = None):
        return super().request_exchange_properties(
            exchange, self._client_id, self._sender_comp_id)
