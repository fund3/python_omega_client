"""
TES Connection class.  Send and receive messages to and from TES.
"""
import logging
from threading import Event, Thread
import time

import zmq

from tes_client.communication.request_sender import RequestSender
from tes_client.messaging.response_handler import ResponseHandler
from tes_client.communication.response_receiver import ResponseReceiver
from tes_client.communication.single_client_request_sender import \
    SingleClientRequestSender

logger = logging.getLogger(__name__)

REQUEST_SENDER_ENDPOINT = 'inproc://TES_REQUEST_SENDER'
RESPONSE_RECEIVER_ENDPOINT = 'inproc://TES_RESPONSE_RECEIVER'


class TesConnection(Thread):
    """
    Base TesConnection class that abstracts out ZMQ connection, capn-proto
    parsing, and communication with the TES.
    Actions like Placing Orders, Requesting Account Balances and passing
    their associated responses from TES as callbacks are handled by this class.
    Also handles heartbeats to maintain connection.

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
        self._response_receiver.cleanup()


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
