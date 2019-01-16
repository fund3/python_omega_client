import logging
from threading import Event, Thread
import time

# pylint: disable=W0611
import capnp
# pylint: enable=W0611
import zmq

# pylint: disable=E0611
# pylint: disable=E0401
import communication_protocol.TradeMessage_capnp as msgs_capnp
# pylint: enable=E0401
# pylint: enable=E0611
from tes_client.messaging.response_handler import ResponseHandler

logger = logging.getLogger(__name__)


class ResponseReceiver(Thread):
    """
    Acts as a separate thread that processes the messages coming from TES so
    that the message receive loop is not blocked.  Only does unidirectional
    message receiving from TesConnection.

    Future optimization plan: add this into a threadpool or processpool for
    sliced/ parallel processing of messages.

    Attributes:
        _POLLING_TIMEOUT_MILLI: (int) The polling timeout for response_socket.
        _zmq_context: (zmq.Context) Required to create sockets. It is
            recommended that one application use one shared zmq context for
            all sockets.
        _response_handler: (ResponseHandler) The ResponseHandler object that
            holds the logic of handling each type of response.
        _running: (Event) Event object that indicates on/ off
            behavior for the response handler loop.
    """
    def __init__(self,
                 zmq_context: zmq.Context,
                 connection_string: str,
                 response_handler: ResponseHandler,
                 polling_timeout_milli: int = 1000,
                 name: str = 'ResponseHandler',
                 socket_identity: bytes = None):
        assert zmq_context
        assert connection_string
        assert response_handler

        self._CONNECTION_STRING = connection_string
        self._POLLING_TIMEOUT_MILLI = polling_timeout_milli
        self._SOCKET_IDENTITY = socket_identity

        self._zmq_context = zmq_context
        self._response_handler = response_handler

        self._running = Event()
        super().__init__(name=name)

    def cleanup(self):
        self.stop()
        self.join()

    def stop(self):
        self._running.clear()

    def run(self):
        logger.debug('Creating TES Response Handler DEALER socket:',
                     extra={'action': 'creating_socket',
                            'socket_type': 'zmq.DEALER',
                            'name': 'tes_response_handler'})
        response_socket = self._zmq_context.socket(zmq.DEALER)
        logger.debug(
            'Connecting to TES Response Handler DEALER socket:',
            extra={'action': 'connecting_to_tes_response_handler',
                   'connection_string': self._CONNECTION_STRING})
        if self._SOCKET_IDENTITY:
            response_socket.setsockopt(zmq.IDENTITY, self._SOCKET_IDENTITY)
        response_socket.connect(self._CONNECTION_STRING)
        poller = zmq.Poller()
        # pylint: disable=E1101
        poller.register(response_socket, zmq.POLLIN)
        # pylint: enable=E1101
        logger.debug(
            'Zmq poller registered.  Waiting for message execution responses.',
            extra={'polling_interval': self._POLLING_TIMEOUT_MILLI,
                   'name': 'tes_response_handler',
                   'status': 'poller_registered'})
        self._running.set()
        while self._running.is_set():
            socks = dict(poller.poll(self._POLLING_TIMEOUT_MILLI))
            if socks.get(response_socket) == zmq.POLLIN:
                message = response_socket.recv()
                self._handle_binary_tes_message(message)
        time.sleep(2.)
        response_socket.close()

    def _handle_response(self, response_type, response):
        self._response_handler.handle_response(response_type, response)

    def _handle_binary_tes_message(self, binary_msg):
        """
        Callback when a tes_message is received and passed to an appropriate
        event handler method.
        :param binary_msg: (capnp._DynamicStructBuilder) The received
            tesMessage.
        """
        logger.debug('Received TESMessage..')
        try:
            tes_msg = msgs_capnp.TradeMessage.from_bytes(binary_msg)
            response = tes_msg.type.response
            response_type = response.body.which()
            self._handle_response(response_type, response)
        except (Exception, TypeError) as e:
            logger.error('Exception in decoding message' + repr(e),
                         extra={'exception': repr(e)})
            return
