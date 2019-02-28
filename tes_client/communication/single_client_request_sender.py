from queue import Queue
from typing import List

import zmq

from tes_client.messaging.common_types import AccountCredentials, AccountInfo, \
    Order, OrderInfo, OrderType, RequestHeader, TimeInForce
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
        self._request_header = RequestHeader(client_id=client_id,
                                             sender_comp_id=sender_comp_id,
                                             access_token='')
        self._client_id = client_id
        self._sender_comp_id = sender_comp_id
        self._access_token = None
        super().__init__(zmq_context=zmq_context,
                         zmq_endpoint=connection_string,
                         outgoing_message_queue=outgoing_message_queue)

    def set_access_token(self, access_token: str):
        """
        Sets the access_token in self._request_header.
        :param access_token: (str) Access token granted by TES.  Note that
            access_token is ignored in logon.
        """
        self._request_header.access_token = access_token

    """
    ############################################################################

    ~~~~~~~~~~~~~~~~~~~~~~~~~~~ Outgoing TESMessages ~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ----------------- Public Methods to be called by client -------------------

    ############################################################################
    """
    def logon(self,
              client_secret: str,
              credentials: List[AccountCredentials],
              request_header: RequestHeader = None):
        return super().logon(client_secret=client_secret,
                             credentials=credentials,
                             request_header=self._request_header)

    def logoff(self, request_header: RequestHeader = None):
        return super().logoff(request_header=self._request_header)

    def send_heartbeat(self, request_header: RequestHeader = None):
        return super().send_heartbeat(request_header=self._request_header)

    def request_server_time(self, request_header: RequestHeader = None):
        return super().request_server_time(request_header=self._request_header)

    def place_order(self, order: Order, request_header: RequestHeader = None):
        return super().place_order(order=order,
                                   request_header=self._request_header)

    def replace_order(self, account_info: AccountInfo,
                      order_id: str,
                      request_header: RequestHeader = None,
                      order_type: str=OrderType.market.name,
                      quantity: float = -1.0,
                      price: float = -1.0,
                      time_in_force: str = TimeInForce.gtc.name):
        return super().replace_order(account_info=account_info,
                                     order_id=order_id,
                                     request_header=self._request_header,
                                     order_type=order_type,
                                     quantity=quantity,
                                     price=price,
                                     time_in_force=time_in_force)

    def cancel_order(self, account_info: AccountInfo,
                     order_id: str,
                     request_header: RequestHeader = None):
        return super().cancel_order(account_info=account_info,
                                    order_id=order_id,
                                    request_header=self._request_header)

    def request_account_data(self, account_info: AccountInfo,
                             request_header: RequestHeader = None):
        return super().request_account_data(
            account_info=account_info, request_header=self._request_header)

    def request_open_positions(self, account_info: AccountInfo,
                               request_header: RequestHeader = None):
        return super().request_open_positions(
            account_info=account_info, request_header=self._request_header)

    def request_account_balances(self, account_info: AccountInfo,
                                 request_header: RequestHeader = None):
        return super().request_account_balances(
            account_info=account_info, request_header=self._request_header)

    def request_working_orders(self, account_info: AccountInfo,
                               request_header: RequestHeader = None):
        return super().request_working_orders(
            account_info=account_info, request_header=self._request_header)

    def request_order_status(self, account_info: AccountInfo,
                             order_id: str,
                             request_header: RequestHeader = None):
        return super().request_order_status(
            account_info=account_info,
            order_id=order_id,
            request_header=self._request_header)

    def request_completed_orders(self, account_info: AccountInfo,
                                 request_header: RequestHeader = None,
                                 count: int = None,
                                 since: float = None):
        return super().request_completed_orders(
            account_info=account_info,
            request_header=self._request_header,
            count=count,
            since=since)

    def request_order_mass_status(self, account_info: AccountInfo,
                                  order_info: List[OrderInfo],
                                  request_header: RequestHeader = None):
        return super().request_order_mass_status(
            account_info=account_info,
            order_info=order_info,
            request_header=self._request_header)

    def request_exchange_properties(self, exchange: str,
                                    request_header: RequestHeader = None):
        return super().request_exchange_properties(
            exchange=exchange, request_header=self._request_header)
