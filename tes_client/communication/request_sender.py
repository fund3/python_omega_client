import logging
from queue import Empty, Queue
from threading import Event, Thread
import time
from typing import List

import capnp
import zmq

from tes_client.messaging.common_types import AccountBalancesReport, \
    AccountCredentials, AccountInfo, AuthorizationGrant, AuthorizationRefresh, \
    CompletedOrdersReport, ExchangePropertiesReport, \
    ExecutionReport, OpenPositionsReport, Order, OrderInfo, \
    OrderType, RequestHeader, TimeInForce, WorkingOrdersReport
from tes_client.messaging.message_factory import cancel_all_orders_capnp, \
    cancel_order_capnp, heartbeat_capnp, logoff_capnp, logon_capnp, \
    place_order_capnp, replace_order_capnp, request_account_balances_capnp, \
    request_account_data_capnp, request_auth_refresh_capnp, \
    request_completed_orders_capnp, request_exchange_properties_capnp, \
    request_open_positions_capnp, request_order_status_capnp, \
    request_server_time_capnp, request_working_orders_capnp

logger = logging.getLogger(__name__)

# TODO: Remove return types after adding easy conversion and access to
# message body for debugging and testing


class RequestSender(Thread):
    """
    Runs as an individual thread to send requests to TesConnection,
    which then gets routed to Omega.  The motivation of the design is different
    threads should not share zmq sockets, and that the TesConnection event
    loop should not be blocked.

    When a request is "sent" from this class, it is placed into an internal
    thread-safe queue.  The request sender loop checks if the queue has a
    message, and if there is one, sends it to TesConnection through an inproc
    connection.

    Attributes:
        _ZMQ_CONTEXT: (zmq.Context) Required to create sockets. It is
            recommended that one application use one shared zmq context for
            all sockets.
        _ZMQ_ENDPOINT: (str) The zmq endpoint to connect to.
        _QUEUE_POLLING_TIMEOUT_SECONDS: (int) The polling timeout for the
            internal queue.
        _outgoing_message_queue: (Queue) Internal message queue for outgoing
            Omega Messages.
        _is_running: (Event) Event object that indicates on/ off
            behavior for the response handler loop.
    """
    def __init__(self,
                 zmq_context: zmq.Context,
                 zmq_endpoint: str,
                 outgoing_message_queue: Queue = None,
                 queue_polling_timeout_seconds: int = 1,
                 name: str='TesRequestSender'):
        assert zmq_context
        assert zmq_endpoint

        self._ZMQ_CONTEXT = zmq_context
        self._ZMQ_ENDPOINT = zmq_endpoint
        self._QUEUE_POLLING_TIMEOUT_SECONDS = queue_polling_timeout_seconds

        self._outgoing_message_queue = outgoing_message_queue or Queue()

        self._is_running = Event()
        super().__init__(name=name)

    def _queue_message(self, tes_message_capnp: capnp._DynamicStructBuilder):
        """
        Put a capnp message into the internal queue for sending to
        TesConnection.
        :param tes_message_capnp:
        """
        self._outgoing_message_queue.put(tes_message_capnp)

    def cleanup(self):
        """
        Stop the response receiver gracefully and join the thread.
        """
        self.stop()
        self.join()

    def stop(self):
        """
        Clear the _is_running Event, which terminates the request sender loop.
        """
        self._is_running.clear()

    def is_running(self):
        """
        Return True if the thread is running, False otherwise.
        """
        return self._is_running.is_set()

    def run(self):
        """
        Message sending loop.
        Create the request_socket as a zmq.DEALER socket and then connect to
        the provided _ZMQ_ENDPOINT.

        Try to get a message for _QUEUE_POLLING_TIMEOUT_SECONDS and then send
        it out to TesConnection.
        """
        request_socket = self._ZMQ_CONTEXT.socket(zmq.DEALER)
        request_socket.connect(self._ZMQ_ENDPOINT)
        self._is_running.set()
        while self._is_running.is_set():
            try:
                # Block for 1 second
                capnp_request = self._outgoing_message_queue.get(
                    timeout=self._QUEUE_POLLING_TIMEOUT_SECONDS)
                request_socket.send(capnp_request.to_bytes())
            except Empty:
                continue
        time.sleep(2.)
        request_socket.close()

    ###########################################################################
    #                                                                         #
    # ~~~~~~~~~~~~~~~~~~~~~~~~~ Outgoing TESMessages ~~~~~~~~~~~~~~~~~~~~~~~~ #
    # ---------------- Public Methods to be called by client----------------- #
    #                                                                         #
    ###########################################################################

    def logon(self,
              request_header: RequestHeader,
              client_secret: str,
              credentials: List[AccountCredentials]):
        """
        Logon to Omega for a specific client_id and set of credentials.
        :param request_header: Header parameter object for requests.
        :param client_secret: (str) client_secret key assigned by Fund3.
        :param credentials: (List[AccountCredentials]) List of exchange
            credentials in the form of AccountCredentials.
        :return: (capnp._DynamicStructBuilder) Logon capnp object.
        """
        tes_message, logon = logon_capnp(
            request_header=request_header,
            client_secret=client_secret,
            credentials=credentials
        )
        self._queue_message(tes_message)
        return logon

    def logoff(self, request_header: RequestHeader):
        """
        Logoff Omega for a specific client_id.
        :param request_header: Header parameter object for requests.
        :return: (capnp._DynamicStructBuilder) Logoff capnp object.
        """
        tes_message, body = logoff_capnp(request_header=request_header)
        self._queue_message(tes_message)
        return body

    def send_heartbeat(self, request_header: RequestHeader):
        """
        Sends a heartbeat to Omega for maintaining and verifying connection.
        Only clients that are logged on will receive heartbeat back from Omega.
        :param request_header: Header parameter object for requests.
        :return: (capnp._DynamicStructBuilder) heartbeat capnp object.
        """
        tes_message, body = heartbeat_capnp(request_header=request_header)
        self._queue_message(tes_message)
        return body

    def request_server_time(self, request_header: RequestHeader):
        """
        Request Omega server time for syncing client and server timestamps.
        :param request_header: Header parameter object for requests.
        :return: (capnp._DynamicStructBuilder) heartbeat capnp object.
        """
        tes_message, body = request_server_time_capnp(
            request_header=request_header)
        self._queue_message(tes_message)
        return body

    def place_order(self, request_header: RequestHeader, order: Order):
        """
        Sends a request to Omega to place an order.
        :param request_header: Header parameter object for requests.
        :param order: (Order) Python object containing all required fields.
        :return: (capnp._DynamicStructBuilder) place_order capnp object.
        """
        tes_message, place_order = place_order_capnp(
            request_header=request_header, order=order)
        self._queue_message(tes_message)
        return place_order

    def replace_order(self,
                      request_header: RequestHeader,
                      account_info: AccountInfo,
                      order_id: str,
                      # pylint: disable=E1101
                      order_type: str = OrderType.undefined.name,
                      quantity: float = 0.0,
                      price: float = 0.0,
                      stop_price: float = 0.0,
                      time_in_force: str = TimeInForce.gtc.name,
                      # pylint: enable=E1101
                      expire_at: float = 0.0):
        """
        Sends a request to Omega to replace an order.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account on which to cancel order.
        :param order_id: (str) order_id as returned from the ExecutionReport.
        :param order_type: (OrderType) (optional)
        :param quantity: (float) (optional)
        :param price: (float) (optional)
        :param stop_price: (float) (optional)
        :param time_in_force: (TimeInForce) (optional)
        :param expire_at: (float) (optional)
        :return: (capnp._DynamicStructBuilder) replaceOrder capnp object.
        """
        tes_message, replace_order = replace_order_capnp(
            request_header=request_header,
            account_info=account_info,
            order_id=order_id,
            order_type=order_type,
            quantity=quantity,
            price=price,
            stop_price=stop_price,
            time_in_force=time_in_force,
            expire_at=expire_at
        )
        self._queue_message(tes_message)
        return replace_order

    def cancel_order(self,
                     request_header: RequestHeader,
                     account_info: AccountInfo,
                     order_id: str):
        """
        Sends a request to Omega to cancel an order.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account on which to cancel order.
        :param order_id: (str) order_id as returned from the ExecutionReport.
        :return: (capnp._DynamicStructBuilder) cancel_order object.
        """
        tes_message, cancel_order = cancel_order_capnp(
            request_header=request_header,
            account_info=account_info,
            order_id=order_id
        )
        self._queue_message(tes_message)
        return cancel_order

    def cancel_all_orders(self,
                          request_header: RequestHeader,
                          account_info: AccountInfo,
                          symbol: str = None,
                          side: str = None):
        """
        Sends a request to Omega to cancel all orders. Optionally including
        side and/or symbol
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account on which to cancel order.
        :param symbol: str (optional)
        :param side: str (optional)
        :return: (capnp._DynamicStructBuilder) cancel_all_orders object.
        """
        tes_message, cancel_all_orders = cancel_all_orders_capnp(
            request_header=request_header,
            account_info=account_info,
            symbol=symbol,
            side=side)
        logger.debug('Cancelling All Orders.', extra={'symbol': symbol,
                                                      'side': side})
        self._queue_message(tes_message)
        return cancel_all_orders

    def request_account_data(self,
                             request_header: RequestHeader,
                             account_info: AccountInfo):
        """
        Sends a request to Omega for full account snapshot including balances,
        open positions, and working orders on specified account.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account from which to retrieve data.
        :return: (capnp._DynamicStructBuilder) get_account_data capnp object.
        """
        tes_message, get_account_data = request_account_data_capnp(
            request_header=request_header, account_info=account_info)
        self._queue_message(tes_message)
        return get_account_data

    def request_open_positions(self,
                               request_header: RequestHeader,
                               account_info: AccountInfo):
        """
        Sends a request to Omega for open positions on an Account.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account from which to retrieve data.
        :return: (capnp._DynamicStructBuilder) get_open_positions capnp
        object.
        """
        tes_message, get_open_positions = request_open_positions_capnp(
            request_header=request_header, account_info=account_info)
        self._queue_message(tes_message)
        return get_open_positions

    def request_account_balances(self,
                                 request_header: RequestHeader,
                                 account_info: AccountInfo):
        """
        Sends a request to Omega for full account balances snapshot on an
        Account.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account from which to retrieve data.
        :return: (capnp._DynamicStructBuilder) get_account_balances capnp
        object.
        """
        tes_message, get_account_balances = request_account_balances_capnp(
            request_header=request_header, account_info=account_info)
        self._queue_message(tes_message)
        return get_account_balances

    def request_working_orders(self,
                               request_header: RequestHeader,
                               account_info: AccountInfo):
        """
        Sends a request to Omega for all working orders snapshot on an
        Account.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account from which to retrieve data.
        :return: (capnp._DynamicStructBuilder) get_working_orders capnp object.
        """
        tes_message, get_working_orders = request_working_orders_capnp(
            request_header=request_header, account_info=account_info)
        self._queue_message(tes_message)
        return get_working_orders

    def request_order_status(self,
                             request_header: RequestHeader,
                             account_info: AccountInfo,
                             order_id: str):
        """
        Sends a request to Omega to request status of a specific order.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account from which to retrieve data.
        :param order_id: (str) The id of the order of interest.
        :return: (capnp._DynamicStructBuilder) get_order_status capnp object.
        """
        tes_message, get_order_status = request_order_status_capnp(
            request_header=request_header,
            account_info=account_info,
            order_id=order_id
        )
        self._queue_message(tes_message)
        return get_order_status

    def request_completed_orders(self,
                                 request_header: RequestHeader,
                                 account_info: AccountInfo,
                                 count: int = None,
                                 since: float = None):
        """
        Sends a request to Omega for all completed orders on specified
        account.  If both 'count' and 'from_unix' are None, returns orders
        for last 24h.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account from which to retrieve data.
        :param count: (int) optional, number of returned orders (most recent
            ones).
        :param since: (float) optional, returns all orders from provided unix
            timestamp to present.
        :return: (capnp._DynamicStructBuilder) get_completed_orders capnp
            object.
        """
        tes_message, get_completed_orders = request_completed_orders_capnp(
            request_header=request_header,
            account_info=account_info,
            count=count,
            since=since
        )
        self._queue_message(tes_message)
        return get_completed_orders

    def request_exchange_properties(self,
                                    request_header: RequestHeader,
                                    exchange: str):
        """
        Sends a request to Omega for supported currencies, symbols and their
        associated properties, timeInForces, and orderTypes on an exchange.
        :param request_header: Header parameter object for requests.
        :param exchange: (str) The exchange of interest.
        :return: (capnp._DynamicStructBuilder) get_exchange_properties capnp
            object.
        """
        tes_message, get_exchange_properties = (
            request_exchange_properties_capnp(
                request_header=request_header, exchange=exchange)
        )
        self._queue_message(tes_message)
        return get_exchange_properties

    def request_authorization_refresh(self,
                                      request_header: RequestHeader,
                                      auth_refresh: AuthorizationRefresh):
        """
        Sends a request to Omega to refresh the session
        :param request_header: Header parameter object for requests.
        :param auth_refresh: AuthorizationRefresh python object
        :return: (capnp._DynamicStructBuilder) authorization_refresh capnp
            object.
        """
        tes_message, authorization_refresh = (
            request_auth_refresh_capnp(
                request_header=request_header, auth_refresh=auth_refresh)
        )
        self._queue_message(tes_message)
        return authorization_refresh
