"""
TES Connection class.  Send and receive messages to and from TES.
"""
import logging
from threading import Event, Thread
import time
from typing import List

import zmq

from tes_client.communication.request_sender import RequestSender
from tes_client.communication.response_receiver import ResponseReceiver
from tes_client.communication.single_client_request_sender import \
    SingleClientRequestSender
from tes_client.messaging.common_types import AccountBalancesReport, \
    AccountCredentials, AccountDataReport, AccountInfo, \
    CompletedOrdersReport, ExchangePropertiesReport, \
    ExecutionReport, OpenPositionsReport, Order, OrderInfo, \
    OrderType, RequestHeader, TimeInForce, WorkingOrdersReport
from tes_client.messaging.message_factory import \
    cancel_order_capnp, heartbeat_capnp, logoff_capnp, logon_capnp, \
    place_order_capnp, replace_order_capnp, request_account_balances_capnp, \
    request_account_data_capnp, request_completed_orders_capnp, \
    request_exchange_properties_capnp, request_open_positions_capnp, \
    request_order_status_capnp, \
    request_server_time_capnp, request_working_orders_capnp
from tes_client.messaging.response_handler import ResponseHandler

logger = logging.getLogger(__name__)


class TesConnection(Thread):
    """
    Base TesConnection class that abstracts out ZMQ connection, capn-proto
    parsing, and communication with the TES.
    Actions like Placing Orders, Requesting Account Balances and passing
    their associated responses from TES as callbacks are handled by this class.

    Attributes:
        _ZMQ_CONTEXT: (zmq.Context) Required to create sockets. It is
            recommended that one application use one shared zmq context for
            all sockets.
        _TES_ENDPOINT: (str) The zmq endpoint to connect to TES, in the form of
            a zmq connection str 'protocol://interface:port', e.g.
            'tcp://0.0.0.0:9999'.
        _REQUEST_SENDER_ENDPOINT: (str) The zmq endpoint used to connect to
            _request_sender.  By default it is a local, inproc endpoint that
            lives in another thread of the same process.
        _RESPONSE_RECEIVER_ENDPOINT: (str) The zmq endpoint used to connect to
            _response_receiver.  By default it is a local, inproc endpoint that
            lives in another thread of the same process.
        _TES_POLLING_TIMEOUT_MILLI: (int) The polling timeout for
            _tes_connection_socket.
        _TES_SOCKET_IDENTITY: (bytes) The socket identity in bytes used for the
            ROUTER socket on the other side to identify the DEALER socket in
            this class. Optional since zmq DEALER socket generates a default
            identity.
        _SERVER_ZMQ_ENCRYPTION_KEY: (str) The public key of the TES server
            used to encrypt data flowing between the client and server.
        _response_receiver: (ResponseReceiver) The response receiver object.
        _request_sender: (RequestSender) The request sender object.
        _is_running: (Event) An event to indicate if the connection is running.
    """
    def __init__(self,
                 zmq_context: zmq.Context,
                 tes_endpoint: str,
                 request_sender_endpoint: str,
                 response_receiver_endpoint: str,
                 request_sender: RequestSender,
                 response_receiver: ResponseReceiver,
                 tes_polling_timeout_milli: int = 1000,
                 name: str = 'TesConnection',
                 tes_socket_identity: bytes = None,
                 server_zmq_encryption_key: str = None):
        assert zmq_context
        assert tes_endpoint
        assert request_sender_endpoint
        assert response_receiver_endpoint
        assert request_sender
        assert response_receiver

        self._ZMQ_CONTEXT = zmq_context
        self._TES_ENDPOINT = tes_endpoint
        self._REQUEST_SENDER_ENDPOINT = request_sender_endpoint
        self._RESPONSE_RECEIVER_ENDPOINT = response_receiver_endpoint
        self._TES_POLLING_TIMEOUT_MILLI = tes_polling_timeout_milli
        self._TES_SOCKET_IDENTITY = tes_socket_identity
        self._SERVER_ZMQ_ENCRYPTION_KEY = server_zmq_encryption_key

        self._response_receiver = response_receiver
        self._request_sender = request_sender

        super().__init__(name=name)
        self._is_running = Event()

    ############################################################################
    #                                                                          #
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Thread Methods ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
    #                                                                          #
    ############################################################################

    def cleanup(self):
        """
        Stop the response receiver gracefully and join the thread.
        """
        self.stop()
        self.join()

    def is_running(self):
        """
        Return True if the thread is running, False otherwise.
        """
        return self._is_running.is_set()

    def wait_until_running(self):
        self._is_running.wait()

    def stop(self):
        """
        Clear the _is_running Event, which terminates the response receiver
        loop.
        """
        self._is_running.clear()

    def _set_curve_keypair(self, socket: zmq.Socket):
        """
        Generate a client keypair using CURVE encryption mechanism, and set
        the server key for encryption.
        :param socket: (zmq.Socket) The socket to set CURVE key.
        """
        client_public, client_secret = zmq.curve_keypair()
        socket.curve_publickey = client_public
        socket.curve_secretkey = client_secret
        socket.setsockopt_string(zmq.CURVE_SERVERKEY,
                                 self._SERVER_ZMQ_ENCRYPTION_KEY)

    def run(self):
        """
        Main loop for TES connection.
        Set up 3 sockets:
        1. tes_socket - the socket that sends and receives messages from TES.
        2. request_listener_socket - listens to requests from request sender
            and forward them to tes_socket.
        3. response_forwarding_socket - forwards responses to response_receiver
            when responses are received from TES.
        """
        # pylint: disable=E1101
        tes_socket = self._ZMQ_CONTEXT.socket(zmq.DEALER)
        # pylint: enable=E1101
        if self._SERVER_ZMQ_ENCRYPTION_KEY:
            self._set_curve_keypair(tes_socket)
        if self._TES_SOCKET_IDENTITY:
            tes_socket.setsockopt(zmq.IDENTITY, self._TES_SOCKET_IDENTITY)
        tes_socket.connect(self._TES_ENDPOINT)

        request_listener_socket = self._ZMQ_CONTEXT.socket(zmq.DEALER)
        request_listener_socket.bind(self._REQUEST_SENDER_ENDPOINT)
        self._request_sender.start()

        response_forwarding_socket = self._ZMQ_CONTEXT.socket(zmq.DEALER)
        response_forwarding_socket.bind(self._RESPONSE_RECEIVER_ENDPOINT)
        self._response_receiver.start()

        poller = zmq.Poller()
        #pylint: disable=E1101
        poller.register(tes_socket, zmq.POLLIN)
        poller.register(request_listener_socket, zmq.POLLIN)
        #pylint: enable=E1101
        self._is_running.set()
        while self.is_running():
            socks = dict(poller.poll(self._TES_POLLING_TIMEOUT_MILLI))
            if socks.get(tes_socket) == zmq.POLLIN:
                incoming_message = tes_socket.recv()
                response_forwarding_socket.send(incoming_message)

            if socks.get(request_listener_socket) == zmq.POLLIN:
                outgoing_message = request_listener_socket.recv()
                tes_socket.send(outgoing_message)
        time.sleep(2.)
        tes_socket.close()
        request_listener_socket.close()
        self._request_sender.cleanup()
        response_forwarding_socket.close()
        self._response_receiver.stop()

    ############################################################################
    #                                                                          #
    # ~~~~~~~~~~~~~~~~~~~~~~ Wrapper for Request Sender ~~~~~~~~~~~~~~~~~~~~~~ #
    #                                                                          #
    ############################################################################
    def logon(self,
              request_header: RequestHeader,
              client_secret: str,
              credentials: List[AccountCredentials]):
        """
        Logon to TES for a specific client_id and set of credentials.
        :param request_header: Header parameter object for requests.
        :param client_secret: (str) client_secret key assigned by Fund3.
        :param credentials: (List[AccountCredentials]) List of exchange
            credentials in the form of AccountCredentials.
        :return: (capnp._DynamicStructBuilder) Logon capnp object.
        """
        return self._request_sender.logon(
            request_header=request_header,
            client_secret=client_secret,
            credentials=credentials
        )

    def logoff(self, request_header: RequestHeader):
        """
        Logoff TES for a specific client_id.
        :param request_header: Header parameter object for requests.
        :return: (capnp._DynamicStructBuilder) Logoff capnp object.
        """
        return self._request_sender.logoff(request_header=request_header)

    def send_heartbeat(self, request_header: RequestHeader):
        """
        Sends a heartbeat to TES for maintaining and verifying connection.
        Only clients that are logged on will receive heartbeat back from TES.
        :param request_header: Header parameter object for requests.
        :return: (capnp._DynamicStructBuilder) heartbeat capnp object.
        """
        return self._request_sender.send_heartbeat(
            request_header=request_header)

    def request_server_time(self, request_header: RequestHeader):
        """
        Request TES server time for syncing client and server timestamps.
        :param request_header: Header parameter object for requests.
        :return: (capnp._DynamicStructBuilder) heartbeat capnp object.
        """
        return self._request_sender.request_server_time(
            request_header=request_header
        )

    def place_order(self, request_header: RequestHeader, order: Order):
        """
        Sends a request to TES to place an order.
        :param request_header: Header parameter object for requests.
        :param order: (Order) Python object containing all required fields.
        :return: (capnp._DynamicStructBuilder) place_order capnp object.
        """
        return self._request_sender.place_order(
            request_header=request_header, order=order)

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
        Sends a request to TES to replace an order.
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
        return self._request_sender.replace_order(
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

    def cancel_order(self,
                     request_header: RequestHeader,
                     account_info: AccountInfo,
                     order_id: str):
        """
        Sends a request to TES to cancel an order.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account on which to cancel order.
        :param order_id: (str) order_id as returned from the ExecutionReport.
        :return: (capnp._DynamicStructBuilder) cancel_order object.
        """
        return self._request_sender.cancel_order(
            request_header=request_header,
            account_info=account_info,
            order_id=order_id
        )

    def request_account_data(self,
                             request_header: RequestHeader,
                             account_info: AccountInfo):
        """
        Sends a request to TES for full account snapshot including balances,
        open positions, and working orders on specified account.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account from which to retrieve data.
        :return: (capnp._DynamicStructBuilder) get_account_data capnp object.
        """
        return self._request_sender.request_account_data(
            request_header=request_header, account_info=account_info
        )

    def request_open_positions(self,
                               request_header: RequestHeader,
                               account_info: AccountInfo):
        """
        Sends a request to TES for open positions on an Account.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account from which to retrieve data.
        :return: (capnp._DynamicStructBuilder) get_open_positions capnp
        object.
        """
        return self._request_sender.request_open_positions(
            request_header=request_header, account_info=account_info
        )

    def request_account_balances(self,
                                 request_header: RequestHeader,
                                 account_info: AccountInfo):
        """
        Sends a request to TES for full account balances snapshot on an
        Account.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account from which to retrieve data.
        :return: (capnp._DynamicStructBuilder) get_account_balances capnp
        object.
        """
        return self._request_sender.request_account_balances(
            request_header=request_header, account_info=account_info
        )

    def request_working_orders(self,
                               request_header: RequestHeader,
                               account_info: AccountInfo):
        """
        Sends a request to TES for all working orders snapshot on an
        Account.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account from which to retrieve data.
        :return: (capnp._DynamicStructBuilder) get_working_orders capnp object.
        """
        return self._request_sender.request_working_orders(
            request_header=request_header, account_info=account_info
        )

    def request_order_status(self,
                             request_header: RequestHeader,
                             account_info: AccountInfo,
                             order_id: str):
        """
        Sends a request to TES to request status of a specific order.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account from which to retrieve data.
        :param order_id: (str) The id of the order of interest.
        :return: (capnp._DynamicStructBuilder) get_order_status capnp object.
        """
        return self._request_sender.request_order_status(
            request_header=request_header,
            account_info=account_info,
            order_id=order_id
        )

    def request_completed_orders(self,
                                 request_header: RequestHeader,
                                 account_info: AccountInfo,
                                 count: int = None,
                                 since: float = None):
        """
        Sends a request to TES for all completed orders on specified
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
        return self._request_sender.request_completed_orders(
            request_header=request_header,
            account_info=account_info,
            count=count,
            since=since
        )

    def request_exchange_properties(self,
                                    request_header: RequestHeader,
                                    exchange: str):
        """
        Sends a request to TES for supported currencies, symbols and their
        associated properties, timeInForces, and orderTypes on an exchange.
        :param request_header: Header parameter object for requests.
        :param exchange: (str) The exchange of interest.
        :return: (capnp._DynamicStructBuilder) get_exchange_properties capnp
            object.
        """
        return self._request_sender.request_exchange_properties(
            request_header=request_header, exchange=exchange
        )


def configure_default_tes_connection(tes_endpoint: str,
                                     tes_server_key: str,
                                     response_handler: ResponseHandler):
    """
    Set up a TesConnection that comes with request_sender and response_receiver.
    :param tes_endpoint: (str) The zmq endpoint to connect to TES.
    :param tes_server_key: (str) The public key of the TES server.
    :param response_handler: (ResponseHandler) The handler object that will
        be called in a callback function when tes_connection receives a
        message.
    :return: tes_connection, request_sender, response_receiver
    """
    ZMQ_CONTEXT = zmq.Context.instance()
    request_sender = RequestSender(ZMQ_CONTEXT,
                                   REQUEST_SENDER_ENDPOINT)
    response_receiver = ResponseReceiver(ZMQ_CONTEXT,
                                         RESPONSE_RECEIVER_ENDPOINT,
                                         response_handler)
    tes_connection = TesConnection(ZMQ_CONTEXT,
                                   tes_endpoint,
                                   REQUEST_SENDER_ENDPOINT,
                                   RESPONSE_RECEIVER_ENDPOINT,
                                   request_sender,
                                   response_receiver,
                                   server_zmq_encryption_key=tes_server_key)
    return tes_connection, request_sender, response_receiver


def configure_single_client_tes_connection(tes_endpoint: str,
                                           tes_server_key: str,
                                           client_id: int,
                                           sender_comp_id: str,
                                           response_handler: ResponseHandler):
    """
    Set up a TesConnection that comes with request_sender and
    response_receiver.  Sets the default client_id and sender_comp_id for the
    request sender.
    Note that each machine should be assigned a unique sender_comp_id even
    when the client_id is the same.
    :param tes_endpoint: (str) The zmq endpoint to connect to TES.
    :param tes_server_key: (str) The public key of the TES server.
    :param client_id: (int) The client id assigned by Fund3.
    :param sender_comp_id: (str) str representation of a unique Python uuid.
    :param response_handler: (ResponseHandler) The handler object that will
        be called in a callback function when tes_connection receives a
        message.
    :return: tes_connection, request_sender, response_receiver
    """
    ZMQ_CONTEXT = zmq.Context.instance()
    request_sender = SingleClientRequestSender(ZMQ_CONTEXT,
                                               REQUEST_SENDER_ENDPOINT,
                                               client_id,
                                               sender_comp_id)
    response_receiver = ResponseReceiver(ZMQ_CONTEXT,
                                         RESPONSE_RECEIVER_ENDPOINT,
                                         response_handler)
    tes_connection = TesConnection(ZMQ_CONTEXT,
                                   tes_endpoint,
                                   REQUEST_SENDER_ENDPOINT,
                                   RESPONSE_RECEIVER_ENDPOINT,
                                   request_sender,
                                   response_receiver,
                                   server_zmq_encryption_key=tes_server_key)
    return tes_connection, request_sender, response_receiver
