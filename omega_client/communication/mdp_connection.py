"""
MDP Omega Connection class.  Send and receive messages to and from Omega MDP.
"""
import logging
from threading import Event, Thread
import time
from typing import List, Union

import zmq

from omega_client.communication.mdp_request_sender import MDPRequestSender
from omega_client.communication.mdp_response_receiver import MDPResponseReceiver
from omega_client.common_types.market_data_structs import MDHeader
from omega_client.common_types.enum_types import Channel
from omega_client.messaging.mdp_response_handler import MDPResponseHandler

logger = logging.getLogger(__name__)

REQUEST_SENDER_ENDPOINT = 'inproc://OMEGA_MDP_REQUEST_SENDER'
RESPONSE_RECEIVER_ENDPOINT = 'inproc://OMEGA_MDP_RESPONSE_RECEIVER'


class MDPOmegaConnection(Thread):
    """
    Base MDPOmegaConnection class that abstracts out ZMQ connection, capn-proto
    parsing, and communication with the Omega.
    Actions like Placing Orders, Requesting Account Balances and passing
    their associated responses from Omega as callbacks are handled by this class.

    Attributes:
        _ZMQ_CONTEXT: (zmq.Context) Required to create sockets. It is
            recommended that one application use one shared zmq context for
            all sockets.
        _OMEGA_ENDPOINT: (str) The zmq endpoint to connect to Omega MDP, in the
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
                 request_sender: MDPRequestSender,
                 response_receiver: MDPResponseReceiver,
                 omega_polling_timeout_milli: int = 1000,
                 name: str = 'OmegaMDPConnection',
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
        Main loop for Omega MDP connection.
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
    # ~~~~~~~~~~~~~~~~~~~~ Wrapper for MDP Request Sender ~~~~~~~~~~~~~~~~~~~~ #
    #                                                                          #
    ############################################################################

    def request_mdp(self,
                    request_header: MDHeader,
                    channels: List[Channel],
                    exchange: str,
                    symbols: List[str],
                    market_depth: int,
                    is_subscribe: bool):
        """
        Sends a request to Omega MDP to subscribe/unscubscribe to a list of
        channels, symbol pairs on exchange
        :param request_header: (MDHeader) parameter object for requests.
        :param channels: (List[Channel]) list of ticker, orderbook (l2)
        elements you wish to subscribe/unsubscribe to
        :param exchange: (str) exchange containing "symbols" you'd like to
        subscribe/unsubscribe to "channels"
        :param symbols: (List[str]) list of pairs corresponding to channels
        you'd like to subscribe/unsubscribe to
        :param market_depth: (int) book depth (# of levels), 0 for full book
        :param is_subscribe: (bool) True for subscribe, False for unsubscribe
        :return: (capnp._DynamicStructBuilder) request capnp object.
        """
        return self._request_sender.request_mdp(
            request_header=request_header, channels=channels, exchange=exchange,
            symbols=symbols, market_depth=market_depth,
            is_subscribe=is_subscribe
        )


def configure_default_omega_mdp_connection(
        mdp_omega_endpoint: str, mdp_omega_server_key: str,
        mdp_response_handler: MDPResponseHandler):
    """
    Set up an OmegaMDPConnection that comes with mdp_request_sender and 
    mdp_response_receiver.
    :param mdp_omega_endpoint: (str) The zmq endpoint to connect to Omega MDP.
    :param mdp_omega_server_key: (str) The public key of the MDP Omega server.
    :param mdp_response_handler: (MDPResponseHandler) The handler object that 
    will be called in a callback function when omega_connection receives a 
    message.
    :return: mdp_omega_connection, mdp_request_sender, mdp_response_receiver
    """
    ZMQ_CONTEXT = zmq.Context.instance()
    request_sender = MDPRequestSender(ZMQ_CONTEXT,
                                      REQUEST_SENDER_ENDPOINT)
    response_receiver = MDPResponseReceiver(ZMQ_CONTEXT,
                                            RESPONSE_RECEIVER_ENDPOINT,
                                            mdp_response_handler)
    omega_connection = MDPOmegaConnection(
        ZMQ_CONTEXT, mdp_omega_endpoint, REQUEST_SENDER_ENDPOINT,
        RESPONSE_RECEIVER_ENDPOINT, request_sender, response_receiver,
        server_zmq_encryption_key=mdp_omega_server_key)
    return omega_connection, request_sender, response_receiver


def configure_single_client_omega_connection(
        omega_endpoint: str, omega_server_key: str, client_id: int,
        sender_comp_id: str, mdp_response_handler: MDPResponseHandler):
    """
    Set up a MDPOmegaConnection that comes with mdp_request_sender and
    mdp_response_receiver.  Sets the default client_id and sender_comp_id for the
    request sender.
    Note that each machine should be assigned a unique sender_comp_id even
    when the client_id is the same.
    :param omega_endpoint: (str) The zmq endpoint to connect to Omega.
    :param omega_server_key: (str) The public key of the Omega server.
    :param client_id: (int) The client id assigned by Fund3.
    :param sender_comp_id: (str) str representation of a unique Python uuid.
    :param mdp_response_handler: (MDPResponseHandler) The handler object that will
        be called in a callback function when mdp_omega_connection receives a
        message.
    :return: mdp_omega_connection, mdp_request_sender, mdp_response_receiver
    """
    ZMQ_CONTEXT = zmq.Context.instance()
    mdp_request_sender = MDPRequestSender(
        ZMQ_CONTEXT, REQUEST_SENDER_ENDPOINT, client_id, sender_comp_id)
    mdp_response_receiver = MDPResponseReceiver(
        ZMQ_CONTEXT, RESPONSE_RECEIVER_ENDPOINT, mdp_response_handler)
    mdp_omega_connection = MDPOmegaConnection(
        ZMQ_CONTEXT, omega_endpoint, REQUEST_SENDER_ENDPOINT,
        RESPONSE_RECEIVER_ENDPOINT, mdp_request_sender, mdp_response_receiver,
        server_zmq_encryption_key=omega_server_key)
    return mdp_omega_connection, mdp_request_sender, mdp_response_receiver
