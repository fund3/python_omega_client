import logging
from threading import Event, Thread
import time

import zmq

from tes_client.communication.request_sender import RequestSender
from tes_client.communication.response_receiver import ResponseReceiver
from tes_client.communication.single_client_request_sender import \
    SingleClientRequestSender

logger = logging.getLogger(__name__)

REQUEST_SENDER_ENDPOINT = 'inproc://TES_REQUEST_SENDER'
RESPONSE_HANDLER_ENDPOINT = 'inproc://TES_RESPONSE_HANDLER'


class TesConnection(Thread):
    """
    Base TesConnection class that abstracts out ZMQ connection, capn-proto
    parsing, and communication with the TES.
    Actions like Placing Orders, Requesting Account Balances and passing
    their associated responses from TES as callbacks are handled by this class.
    Also handles heartbeats to maintain connection.

    Attributes:
        _TES_ENDPOINT: (str) of ip address, port for connecting to
        TES, in the form of a zmq connection str 'protocol://interface:port',
        e.g. 'tcp://0.0.0.0:9999'
        _TES_POLLING_TIMEOUT_MILLI: (int) The polling timeout for
            _tes_connection_socket.
        _SERVER_ZMQ_ENCRYPTION_KEY
        _zmq_context: (zmq.Context) Required to create sockets. It is
            recommended that one application use one shared zmq context for
            all sockets.
        _running:
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
        """
        :param tes_polling_timeout_milli: int millisecond TES polling
        interval. Leave as default unless specifically instructed to change.
        :param name: str name of the thread (used for debugging)
        """
        assert zmq_context
        assert tes_endpoint
        assert request_sender_endpoint
        assert response_receiver_endpoint
        assert request_sender
        assert response_receiver

        self._TES_ENDPOINT = tes_endpoint
        self._REQUEST_SENDER_ENDPOINT = request_sender_endpoint
        self._RESPONSE_RECEIVER_ENDPOINT = response_receiver_endpoint
        self._TES_POLLING_TIMEOUT_MILLI = tes_polling_timeout_milli
        self._TES_SOCKET_IDENTITY = tes_socket_identity
        self._SERVER_ZMQ_ENCRYPTION_KEY = server_zmq_encryption_key

        self._zmq_context = zmq_context
        self._response_receiver = response_receiver
        self._request_sender = request_sender

        super().__init__(name=name)
        self._running = Event()

    ############################################################################
    #                                                                          #
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Thread Methods ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
    #                                                                          #
    ############################################################################

    def cleanup(self):
        self.stop()
        self.join()

    def is_running(self):
        return self._running.is_set()

    def wait_until_running(self):
        self._running.wait()

    def stop(self):
        logger.debug('Stopping engine..')
        self._running.clear()
        logger.debug('..done.')

    def _set_curve_keypair(self, socket):
        client_public, client_secret = zmq.curve_keypair()
        socket.curve_publickey = client_public
        socket.curve_secretkey = client_secret
        socket.setsockopt_string(zmq.CURVE_SERVERKEY,
                                 self._SERVER_ZMQ_ENCRYPTION_KEY)

    def run(self):
        logger.debug('Creating TES DEALER socket:',
                     extra={'action': 'creating_socket',
                            'socket_type': 'zmq.DEALER'})
        # pylint: disable=E1101
        tes_socket = self._zmq_context.socket(zmq.DEALER)
        # pylint: enable=E1101
        if self._SERVER_ZMQ_ENCRYPTION_KEY:
            self._set_curve_keypair(tes_socket)
        if self._TES_SOCKET_IDENTITY:
            tes_socket.setsockopt(zmq.IDENTITY, self._TES_SOCKET_IDENTITY)
        logger.debug('Connecting to TES socket:',
                     extra={'action': 'connect_to_tes',
                            'tes_endpoint': self._TES_ENDPOINT})
        tes_socket.connect(self._TES_ENDPOINT)

        request_listener_socket = self._zmq_context.socket(zmq.DEALER)
        request_listener_socket.bind(self._REQUEST_SENDER_ENDPOINT)
        self._response_receiver.start()

        response_handler_socket = self._zmq_context.socket(zmq.DEALER)
        response_handler_socket.bind(self._RESPONSE_RECEIVER_ENDPOINT)
        self._request_sender.start()

        poller = zmq.Poller()
        #pylint: disable=E1101
        poller.register(tes_socket, zmq.POLLIN)
        poller.register(request_listener_socket, zmq.POLLIN)
        #pylint: enable=E1101
        logger.debug('Zmq poller registered.  Waiting for message execution '
                     'responses.', extra={'polling_interval':
                                          self._TES_POLLING_TIMEOUT_MILLI})
        self._running.set()
        while self.is_running():
            socks = dict(poller.poll(self._TES_POLLING_TIMEOUT_MILLI))
            if socks.get(tes_socket) == zmq.POLLIN:
                incoming_message = tes_socket.recv()
                response_handler_socket.send(incoming_message)

            if socks.get(request_listener_socket) == zmq.POLLIN:
                outgoing_message = request_listener_socket.recv()
                tes_socket.send(outgoing_message)
        time.sleep(2.)
        tes_socket.close()
        request_listener_socket.close()
        self._response_receiver.stop()
        response_handler_socket.close()
        self._request_sender.stop()


def configure_default_tes_connection(tes_endpoint,
                                     tes_server_key,
                                     response_handler):
    zmq_context = zmq.Context.instance()
    request_sender = RequestSender(zmq_context,
                                   REQUEST_SENDER_ENDPOINT)
    response_receiver = ResponseReceiver(zmq_context,
                                         RESPONSE_HANDLER_ENDPOINT,
                                         response_handler)
    tes_connection = TesConnection(zmq_context,
                                   tes_endpoint,
                                   REQUEST_SENDER_ENDPOINT,
                                   RESPONSE_HANDLER_ENDPOINT,
                                   request_sender,
                                   response_receiver,
                                   server_zmq_encryption_key=tes_server_key)
    return tes_connection, request_sender, response_receiver


def configure_single_client_tes_connection(tes_endpoint,
                                           tes_server_key,
                                           client_id,
                                           sender_comp_id,
                                           response_handler):
    zmq_context = zmq.Context.instance()
    request_sender = SingleClientRequestSender(zmq_context,
                                               REQUEST_SENDER_ENDPOINT,
                                               client_id,
                                               sender_comp_id)
    response_receiver = ResponseReceiver(zmq_context,
                                         RESPONSE_HANDLER_ENDPOINT,
                                         response_handler)
    tes_connection = TesConnection(zmq_context,
                                   tes_endpoint,
                                   REQUEST_SENDER_ENDPOINT,
                                   RESPONSE_HANDLER_ENDPOINT,
                                   request_sender,
                                   response_receiver,
                                   server_zmq_encryption_key=tes_server_key)
    return tes_connection, request_sender, response_receiver
