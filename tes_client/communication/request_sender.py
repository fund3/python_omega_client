import logging
from queue import Empty, Queue
from threading import Event, Thread
import time
from typing import List

import capnp
import zmq

from tes_client.messaging.common_types import AccountBalancesReport, \
    AccountCredentials, AccountDataReport, AccountInfo, \
    CompletedOrdersReport, ExchangePropertiesReport, \
    ExecutionReport, OpenPositionsReport, Order, OrderInfo, \
    OrderType, TimeInForce, WorkingOrdersReport
from tes_client.messaging.message_factory import cancel_all_orders_capnp,\
    cancel_order_capnp, heartbeat_capnp, logoff_capnp, logon_capnp, \
    place_order_capnp, replace_order_capnp, request_account_balances_capnp, \
    request_account_data_capnp, request_completed_orders_capnp, \
    request_exchange_properties_capnp, request_open_positions_capnp, \
    request_order_mass_status_capnp, request_order_status_capnp, \
    request_working_orders_capnp

logger = logging.getLogger(__name__)

# TODO: Remove return types after adding easy conversion and access to
# message body for debugging and testing


class RequestSender(Thread):
    """
    Runs as an individual thread to send requests to TesConnection,
    which then gets routed to TES.  The motivation of the design is different
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
            TES Messages.
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
              credentials: List[AccountCredentials],
              client_id: int,
              sender_comp_id: str):
        """
        Logon to TES for a specific client_id and set of credentials.
        :param client_id: (int) The assigned client_id.
        :param sender_comp_id: (str) uuid unique to the session the user is
        on.
            It is recommended to use Python3 built-in uuid.uuid1() to avoid
            collision when running tes_connection on multiple machines.
        :param credentials: (List[AccountCredentials]) List of exchange
            credentials in the form of AccountCredentials.
        :return: (capnp._DynamicStructBuilder) Logon capnp object.
        """
        tes_message, logon = logon_capnp(client_id, sender_comp_id,
                                         credentials)
        logger.debug('Sending TES logon msg.',
                     extra={'client_id': client_id,
                            'sender_comp_id': sender_comp_id})
        self._queue_message(tes_message)
        return logon

    def logoff(self, client_id: int, sender_comp_id: str):
        """
        Logoff TES for a specific client_id.
        :param client_id: (int) The assigned client_id.
        :param sender_comp_id: (str) uuid unique to the session the user is
        on.
        :return: (capnp._DynamicStructBuilder) Logoff capnp object.
        """
        tes_message, body = logoff_capnp(client_id, sender_comp_id)
        logger.debug('Sending logoff',
                     extra={'client_id': client_id,
                            'sender_comp_id': sender_comp_id})
        self._queue_message(tes_message)
        return body

    def send_heartbeat(self, client_id: int, sender_comp_id: str):
        """
        Sends a heartbeat to TES for maintaining and verifying connection.
        Only clients that are logged on will receive heartbeat back from TES.
        :param client_id: (int) The assigned client_id.
        :param sender_comp_id: (str) uuid unique to the session the user is
        on.
        :return: (capnp._DynamicStructBuilder) heartbeat capnp object.
        """
        tes_message, body = heartbeat_capnp(client_id, sender_comp_id)
        logger.debug('Sending heartbeat.',
                     extra={'client_id': client_id,
                            'sender_comp_id': sender_comp_id})
        self._queue_message(tes_message)
        return body

    def place_order(self, order: Order, client_id: int, sender_comp_id: str):
        """
        Sends a request to TES to place an order.
        :param client_id: (int) The assigned client_id.
        :param sender_comp_id: (str) uuid unique to the session the user is
        on.
        :param order: (Order) Python object containing all required fields.
        :return: (capnp._DynamicStructBuilder) place_order capnp object.
        """
        tes_message, place_order = place_order_capnp(
            client_id, sender_comp_id, order)
        logger.debug('Sending Place Order.',
                     extra={'accountID': order.account_info.account_id,
                            'clientOrderID': order.client_order_id,
                            'clientOrderLinkID': order.client_order_link_id,
                            'symbol': order.symbol,
                            'side': order.side,
                            'orderType': order.order_type,
                            'quantity': str(order.quantity),
                            'price': str(order.price),
                            'timeInForce': order.time_in_force,
                            'leverageType': order.leverage_type,
                            'leverage': str(order.leverage)})
        self._queue_message(tes_message)
        return place_order

    def replace_order(self, account_info: AccountInfo,
                      order_id: str,
                      client_id: int,
                      sender_comp_id: str,
                      # pylint: disable=E1101
                      order_type: str = OrderType.undefined.name,
                      quantity: float = -1.0, price: float = -1.0,
                      time_in_force: str = TimeInForce.gtc.name
                      # pylint: enable=E1101
                      ):
        """
        Sends a request to TES to replace an order.
        :param client_id: (int) The assigned client_id.
        :param sender_comp_id: (str) uuid unique to the session the user is
        on.
        :param account_info: (AccountInfo) Account on which to cancel order.
        :param order_id: (str) order_id as returned from the ExecutionReport.
        :param order_type: (OrderType) (optional)
        :param quantity: (float) (optional)
        :param price: (float) (optional)
        :param time_in_force: (TimeInForce) (optional)
        :return: (capnp._DynamicStructBuilder) replaceOrder capnp object.
        """
        tes_message, replace_order = replace_order_capnp(
            client_id, sender_comp_id, account_info, order_id, order_type,
            quantity, price, time_in_force)
        logger.debug('Sending Replace Order ',
                     extra={'orderID': order_id,
                            'orderType': order_type,
                            'quantity': str(quantity),
                            'price': str(price),
                            'timeInForce': time_in_force})
        self._queue_message(tes_message)
        return replace_order

    def cancel_order(self,
                     account_info: AccountInfo,
                     order_id: str,
                     client_id: int,
                     sender_comp_id: str):
        """
        Sends a request to TES to cancel an order.
        :param client_id: (int) The assigned client_id.
        :param sender_comp_id: (str) uuid unique to the session the user is
        on.
        :param account_info: (AccountInfo) Account on which to cancel order.
        :param order_id: (str) order_id as returned from the ExecutionReport.
        :return: (capnp._DynamicStructBuilder) cancel_order object.
        """
        tes_message, cancel_order = cancel_order_capnp(
            client_id, sender_comp_id, account_info, order_id)
        logger.debug('Cancelling Order.', extra={'order_id': order_id})
        self._queue_message(tes_message)
        return cancel_order

    def cancel_all_orders(self, account_info: AccountInfo,
                          client_id: int,
                          sender_comp_id: str,
                          symbol: str = None,
                          side: str = None):
        """
        Sends a request to TES to cancel an order.
        :param account_info: (AccountInfo) Account on which to cancel order.
        :param client_id: (int) The assigned client_id.
        :param sender_comp_id: (str) uuid unique to the session the user is
        on.
        :param symbol: str (optional)
        :param side: str (optional)
        :return: (capnp._DynamicStructBuilder) cancel_all_orders object.
        """
        tes_message, cancel_all_orders = cancel_all_orders_capnp(
            client_id, sender_comp_id, account_info, symbol, side)
        logger.debug('Cancelling All Orders.', extra={'symbol': symbol,
                                                      'side': side})
        self._queue_message(tes_message)
        return cancel_all_orders

    def request_account_data(self,
                             account_info: AccountInfo,
                             client_id: int,
                             sender_comp_id: str):
        """
        Sends a request to TES for full account snapshot including balances,
        open positions, and working orders on specified account.
        :param client_id: (int) The assigned client_id.
        :param sender_comp_id: (str) uuid unique to the session the user is
        on.
        :param account_info: (AccountInfo) Account from which to retrieve
        data.
        :return: (capnp._DynamicStructBuilder) get_account_data capnp object.
        """
        tes_message, get_account_data = request_account_data_capnp(
            client_id, sender_comp_id, account_info)
        logger.debug('Requesting Account Data.',
                     extra={'accountID': str(account_info.account_id)})
        self._queue_message(tes_message)
        return get_account_data

    def request_open_positions(self,
                               account_info: AccountInfo,
                               client_id: int,
                               sender_comp_id: str):
        """
        Sends a request to TES for open positions on an Account.
        :param client_id: (int) The assigned client_id.
        :param sender_comp_id: (str) uuid unique to the session the user is
        on.
        :param account_info: (AccountInfo) Account from which to retrieve
        data.
        :return: (capnp._DynamicStructBuilder) get_open_positions capnp
        object.
        """
        tes_message, get_open_positions = request_open_positions_capnp(
            client_id, sender_comp_id, account_info)
        logger.debug('Requesting Open Positions',
                     extra={'account_id': str(account_info.account_id)})
        self._queue_message(tes_message)
        return get_open_positions

    def request_account_balances(self,
                                 account_info: AccountInfo,
                                 client_id: int,
                                 sender_comp_id: str):
        """
        Sends a request to TES for full account balances snapshot on an
        Account.
        :param client_id: (int) The assigned client_id.
        :param sender_comp_id: (str) uuid unique to the session the user is
        on.
        :param account_info: (AccountInfo) Account from which to retrieve
        data.
        :return: (capnp._DynamicStructBuilder) get_account_balances capnp
        object.
        """
        tes_message, get_account_balances = request_account_balances_capnp(
            client_id, sender_comp_id, account_info)
        logger.debug('Requesting Account Balances',
                     extra={'account_id': str(account_info.account_id)})
        self._queue_message(tes_message)
        return get_account_balances

    def request_working_orders(self,
                               account_info: AccountInfo,
                               client_id: int,
                               sender_comp_id: str):
        """
        Sends a request to TES for all working orders snapshot on an
        Account.
        :param client_id: (int) The assigned client_id.
        :param sender_comp_id: (str) uuid unique to the session the user is
        on.
        :param account_info: (AccountInfo) Account from which to retrieve
        data.
        :return: (capnp._DynamicStructBuilder) get_working_orders capnp
        object.
        """
        tes_message, get_working_orders = request_working_orders_capnp(
            client_id, sender_comp_id, account_info)
        logger.debug('Requesting Working Orders.',
                     extra={'account_id': str(account_info.account_id)})
        self._queue_message(tes_message)
        return get_working_orders

    def request_order_status(self,
                             account_info: AccountInfo,
                             order_id: str,
                             client_id: int,
                             sender_comp_id: str):
        """
        Sends a request to TES to request status of a specific order.
        :param client_id: (int) The assigned client_id.
        :param sender_comp_id: (str) uuid unique to the session the user is
        on.
        :param account_info: (AccountInfo) Account from which to retrieve
        data.
        :param order_id: (str) The id of the order of interest.
        :return: (capnp._DynamicStructBuilder) get_order_status capnp object.
        """
        tes_message, get_order_status = request_order_status_capnp(
            client_id, sender_comp_id, account_info, order_id)
        logger.debug('Requesting Order Status.',
                     extra={'account_id': str(account_info.account_id),
                            'order_id': order_id})
        self._queue_message(tes_message)
        return get_order_status

    def request_completed_orders(self,
                                 account_info: AccountInfo,
                                 client_id: int,
                                 sender_comp_id: str,
                                 count: int = None,
                                 since: float = None):
        """
        Sends a request to TES for all completed orders on specified
        account.
        If both 'count' and 'from_unix' are None, returns orders for last
        24h.
        :param client_id: (int) The assigned client_id.
        :param sender_comp_id: (str) uuid unique to the session the user is
        on.
        :param account_info: (AccountInfo) Account from which to retrieve
        data.
        :param count: (int) optional, number of returned orders (most recent
            ones).
        :param since: (float) optional, returns all orders from provided
        unix
            timestamp to present.
        :return: (capnp._DynamicStructBuilder) get_completed_orders capnp
        object.
        """
        tes_message, get_completed_orders = request_completed_orders_capnp(
            client_id, sender_comp_id, account_info, count, since)
        logger.debug('Requesting Working Orders.',
                     extra={'account_id': str(account_info.account_id)})
        self._queue_message(tes_message)
        return get_completed_orders

    def request_order_mass_status(self,
                                  account_info: AccountInfo,
                                  order_info: List[OrderInfo],
                                  client_id: int,
                                  sender_comp_id: str):
        """
        Sends a request to TES for status of multiple orders.
        :param client_id: (int) The assigned client_id.
        :param sender_comp_id: (str) uuid unique to the session the user is
        on.
        :param account_info: (AccountInfo) Account from which to retrieve
        data.
        :param order_info: (List[OrderInfo]) List of orderIDs to get status
            updates.
        :return: (capnp._DynamicStructBuilder) get_order_mass_status capnp
        object.
        """
        tes_message, get_order_mass_status = request_order_mass_status_capnp(
            client_id, sender_comp_id, account_info, order_info)
        logger.debug('Requesting Mass Order Status.',
                     extra={'account_id': str(account_info.account_id),
                            'order_ids': str([oid.order_id for oid in
                                             order_info])})
        self._queue_message(tes_message)
        return get_order_mass_status

    def request_exchange_properties(self,
                                    exchange: str,
                                    client_id: int,
                                    sender_comp_id: str):
        """
        Sends a request to TES for supported currencies, symbols and their
        associated properties, timeInForces, and orderTypes on an exchange.
        :param client_id: (int) The assigned client_id.
        :param sender_comp_id: (str) uuid unique to the session the user is
        on.
        :param exchange: (str) The exchange of interest.
        :return: (capnp._DynamicStructBuilder) get_exchange_properties capnp
            object.
        """
        tes_message, get_exchange_properties = (
            request_exchange_properties_capnp(client_id,
                                              sender_comp_id,
                                              exchange))
        logger.debug('Requesting Exchange Properties.',
                     extra={'exchange': exchange})
        self._queue_message(tes_message)
        return get_exchange_properties
