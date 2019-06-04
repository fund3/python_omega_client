"""
Omega Connection class.  Send and receive messages to and from Omega.
"""
import logging
from threading import Event, Thread
import time
from typing import List, Union

import zmq

from omega_client.communication.request_sender import RequestSender
from omega_client.communication.response_receiver import ResponseReceiver
from omega_client.messaging.common_types import AccountBalancesReport, \
    AccountCredentials, AccountDataReport, AccountInfo, \
    AuthorizationRefresh, ExchangePropertiesReport, \
    ExecutionReport, OpenPositionsReport, Order, OrderInfo, \
    OrderType, RequestHeader, TimeInForce, WorkingOrdersReport, Batch, OCO, OPO
from omega_client.messaging.response_handler import ResponseHandler

logger = logging.getLogger(__name__)

REQUEST_SENDER_ENDPOINT = 'inproc://OMEGA_REQUEST_SENDER'
RESPONSE_RECEIVER_ENDPOINT = 'inproc://OMEGA_RESPONSE_RECEIVER'


class OmegaConnection(Thread):
    """
    Base OmegaConnection class that abstracts out ZMQ connection, capn-proto
    parsing, and communication with the Omega.
    Actions like Placing Orders, Requesting Account Balances and passing
    their associated responses from Omega as callbacks are handled by this class.

    Attributes:
        _ZMQ_CONTEXT: (zmq.Context) Required to create sockets. It is
            recommended that one application use one shared zmq context for
            all sockets.
        _OMEGA_ENDPOINT: (str) The zmq endpoint to connect to Omega, in the
            form of a zmq connection str 'protocol://interface:port', e.g.
            'tcp://0.0.0.0:9999'.
        _REQUEST_SENDER_ENDPOINT: (str) The zmq endpoint used to connect to
            _request_sender.  By default it is a local, inproc endpoint that
            lives in another thread of the same process.
        _RESPONSE_RECEIVER_ENDPOINT: (str) The zmq endpoint used to connect to
            _response_receiver.  By default it is a local, inproc endpoint that
            lives in another thread of the same process.
        _OMEGA_POLLING_TIMEOUT_MILLI: (int) The polling timeout for
            _omega_connection_socket.
        _OMEGA_SOCKET_IDENTITY: (bytes) The socket identity in bytes used for the
            ROUTER socket on the other side to identify the DEALER socket in
            this class. Optional since zmq DEALER socket generates a default
            identity.
        _SERVER_ZMQ_ENCRYPTION_KEY: (str) The public key of the Omega server
            used to encrypt data flowing between the client and server.
        _response_receiver: (ResponseReceiver) The response receiver object.
        _request_sender: (RequestSender) The request sender object.
        _is_running: (Event) An event to indicate if the connection is running.
    """
    def __init__(self,
                 zmq_context: zmq.Context,
                 omega_endpoint: str,
                 request_sender_endpoint: str,
                 response_receiver_endpoint: str,
                 request_sender: RequestSender,
                 response_receiver: ResponseReceiver,
                 omega_polling_timeout_milli: int = 1000,
                 name: str = 'OmegaConnection',
                 omega_socket_identity: bytes = None,
                 server_zmq_encryption_key: str = None):
        assert zmq_context
        assert omega_endpoint
        assert request_sender_endpoint
        assert response_receiver_endpoint
        assert request_sender
        assert response_receiver

        self._ZMQ_CONTEXT = zmq_context
        self._OMEGA_ENDPOINT = omega_endpoint
        self._REQUEST_SENDER_ENDPOINT = request_sender_endpoint
        self._RESPONSE_RECEIVER_ENDPOINT = response_receiver_endpoint
        self._OMEGA_POLLING_TIMEOUT_MILLI = omega_polling_timeout_milli
        self._OMEGA_SOCKET_IDENTITY = omega_socket_identity
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
        Main loop for Omega connection.
        Set up 3 sockets:
        1. omega_socket - the socket that sends and receives messages from
        Omega.
        2. request_listener_socket - listens to requests from request sender
            and forward them to omega_socket.
        3. response_forwarding_socket - forwards responses to response_receiver
            when responses are received from Omega.
        """
        # pylint: disable=E1101
        omega_socket = self._ZMQ_CONTEXT.socket(zmq.DEALER)
        # pylint: enable=E1101
        if self._SERVER_ZMQ_ENCRYPTION_KEY:
            self._set_curve_keypair(omega_socket)
        if self._OMEGA_SOCKET_IDENTITY:
            omega_socket.setsockopt(zmq.IDENTITY, self._OMEGA_SOCKET_IDENTITY)
        omega_socket.connect(self._OMEGA_ENDPOINT)

        request_listener_socket = self._ZMQ_CONTEXT.socket(zmq.DEALER)
        request_listener_socket.bind(self._REQUEST_SENDER_ENDPOINT)
        self._request_sender.start()

        response_forwarding_socket = self._ZMQ_CONTEXT.socket(zmq.DEALER)
        response_forwarding_socket.bind(self._RESPONSE_RECEIVER_ENDPOINT)
        self._response_receiver.start()

        poller = zmq.Poller()
        #pylint: disable=E1101
        poller.register(omega_socket, zmq.POLLIN)
        poller.register(request_listener_socket, zmq.POLLIN)
        #pylint: enable=E1101
        self._is_running.set()
        while self.is_running():
            socks = dict(poller.poll(self._OMEGA_POLLING_TIMEOUT_MILLI))
            if socks.get(omega_socket) == zmq.POLLIN:
                incoming_message = omega_socket.recv()
                response_forwarding_socket.send(incoming_message)

            if socks.get(request_listener_socket) == zmq.POLLIN:
                outgoing_message = request_listener_socket.recv()
                omega_socket.send(outgoing_message)
        time.sleep(2.)
        omega_socket.close()
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
        Logon to Omega for a specific client_id and set of credentials.
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
        Logoff Omega for a specific client_id.
        :param request_header: Header parameter object for requests.
        :return: (capnp._DynamicStructBuilder) Logoff capnp object.
        """
        return self._request_sender.logoff(request_header=request_header)

    def send_heartbeat(self, request_header: RequestHeader):
        """
        Sends a heartbeat to Omega for maintaining and verifying connection.
        Only clients that are logged on will receive heartbeat back from Omega.
        :param request_header: Header parameter object for requests.
        :return: (capnp._DynamicStructBuilder) heartbeat capnp object.
        """
        return self._request_sender.send_heartbeat(
            request_header=request_header)

    def request_server_time(self, request_header: RequestHeader):
        """
        Request Omega server time for syncing client and server timestamps.
        :param request_header: Header parameter object for requests.
        :return: (capnp._DynamicStructBuilder) request_server_time capnp object.
        """
        return self._request_sender.request_server_time(
            request_header=request_header
        )

    def place_order(self, request_header: RequestHeader, order: Order):
        """
        Sends a request to Omega to place an order.
        :param request_header: Header parameter object for requests.
        :param order: (Order) Python object containing all required fields.
        :return: (capnp._DynamicStructBuilder) place_order capnp object.
        """
        return self._request_sender.place_order(
            request_header=request_header, order=order)

    def place_contingent_order(self,
                               request_header: RequestHeader,
                               contingent_order: Union[Batch, OPO, OCO]):
        """
        Sends a request to Omega to place a contingent order.
        :param request_header: Header parameter object for requests.
        :param contingent_order: (Batch, OPO, or OCO) python object
        :return: (capnp._DynamicStructBuilder) placeContingentOrder capnp
        object.
        """
        return self._request_sender.place_contingent_order(
            request_header=request_header, contingent_order=contingent_order)

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
        Sends a request to Omega to cancel an order.
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

    def cancel_all_orders(self,
                          request_header: RequestHeader,
                          account_info: AccountInfo,
                          symbol: str = None,
                          side: str = None):
        """
        Sends a request to Omega to cancel an order.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account on which to cancel order.
        :param symbol: (str) (optional)
        :param side: (str) (optional)
        :return (capnp._DynamicStructBuilder) cancel_all_orders object.
        """
        return self._request_sender.cancel_all_orders(
            request_header=request_header,
            account_info=account_info,
            symbol=symbol,
            side=side
        )

    def request_account_data(self,
                             request_header: RequestHeader,
                             account_info: AccountInfo):
        """
        Sends a request to Omega for full account snapshot including balances,
        open positions, and working orders on specified account.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account from which to retrieve data.
        :return: (capnp._DynamicStructBuilder) request_account_data capnp
            object.
        """
        return self._request_sender.request_account_data(
            request_header=request_header, account_info=account_info
        )

    def request_open_positions(self,
                               request_header: RequestHeader,
                               account_info: AccountInfo):
        """
        Sends a request to Omega for open positions on an Account.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account from which to retrieve data.
        :return: (capnp._DynamicStructBuilder) request_open_positions capnp
            object.
        """
        return self._request_sender.request_open_positions(
            request_header=request_header, account_info=account_info
        )

    def request_account_balances(self,
                                 request_header: RequestHeader,
                                 account_info: AccountInfo):
        """
        Sends a request to Omega for full account balances snapshot on an
        Account.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account from which to retrieve data.
        :return: (capnp._DynamicStructBuilder) request_account_balances capnp
            object.
        """
        return self._request_sender.request_account_balances(
            request_header=request_header, account_info=account_info
        )

    def request_working_orders(self,
                               request_header: RequestHeader,
                               account_info: AccountInfo):
        """
        Sends a request to Omega for all working orders snapshot on an
        Account.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account from which to retrieve data.
        :return: (capnp._DynamicStructBuilder) request_working_orders capnp
            object.
        """
        return self._request_sender.request_working_orders(
            request_header=request_header, account_info=account_info
        )

    def request_order_status(self,
                             request_header: RequestHeader,
                             account_info: AccountInfo,
                             order_id: str):
        """
        Sends a request to Omega to request status of a specific order.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account from which to retrieve data.
        :param order_id: (str) The id of the order of interest.
        :return: (capnp._DynamicStructBuilder) request_order_status capnp
            object.
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
        Sends a request to Omega for all completed orders on specified
        account.  If both 'count' and 'from_unix' are None, returns orders
        for last 24h.
        :param request_header: Header parameter object for requests.
        :param account_info: (AccountInfo) Account from which to retrieve data.
        :param count: (int) optional, number of returned orders (most recent
            ones).
        :param since: (float) optional, returns all orders from provided unix
            timestamp to present.
        :return: (capnp._DynamicStructBuilder) request_completed_orders capnp
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
        Sends a request to Omega for supported currencies, symbols and their
        associated properties, timeInForces, and orderTypes on an exchange.
        :param request_header: Header parameter object for requests.
        :param exchange: (str) The exchange of interest.
        :return: (capnp._DynamicStructBuilder) request_exchange_properties capnp
            object.
        """
        return self._request_sender.request_exchange_properties(
            request_header=request_header, exchange=exchange
        )

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
        return self._request_sender.request_authorization_refresh(
            request_header=request_header, auth_refresh=auth_refresh
        )


def configure_default_omega_connection(
        zmq_context: zmq.Context,
        omega_endpoint: str,
        omega_server_key: str,
        response_handler: ResponseHandler):
    """
    Set up a TesConnection that comes with request_sender and response_receiver.
    :param omega_endpoint: (str) The zmq endpoint to connect to Omega.
    :param omega_server_key: (str) The public key of the Omega server.
    :param response_handler: (ResponseHandler) The handler object that will
        be called in a callback function when omega_connection receives a
        message.
    :return: omega_connection, request_sender, response_receiver
    """
    request_sender = RequestSender(zmq_context,
                                   REQUEST_SENDER_ENDPOINT)
    response_receiver = ResponseReceiver(zmq_context,
                                         RESPONSE_RECEIVER_ENDPOINT,
                                         response_handler)
    omega_connection = OmegaConnection(
        zmq_context,
        omega_endpoint,
        REQUEST_SENDER_ENDPOINT,
        RESPONSE_RECEIVER_ENDPOINT,
        request_sender,
        response_receiver,
        server_zmq_encryption_key=omega_server_key)
    return omega_connection, request_sender, response_receiver
