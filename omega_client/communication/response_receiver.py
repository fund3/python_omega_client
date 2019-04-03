"""
Omega Response Receiver class.  Receive messages from a local TesConnection
that is connected to Omega.
"""
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
from omega_client.messaging.response_handler import ResponseHandler

logger = logging.getLogger(__name__)


class ResponseReceiver(Thread):
    """
    Acts as a separate thread that processes the messages coming from Omega so
    that the message receive loop is not blocked.  Only does unidirectional
    message receiving from TesConnection.

    Future optimization plan: add this into a threadpool or processpool for
    sliced/ parallel processing of messages.

    Attributes:
        _ZMQ_CONTEXT: (zmq.Context) Required to create sockets. It is
            recommended that one application use one shared zmq context for
            all sockets.
        _ZMQ_ENDPOINT: (str) The zmq endpoint to connect to.
        _RESPONSE_HANDLER: (ResponseHandler) The ResponseHandler object that
            holds the logic of handling each type of response.
        _POLLING_TIMEOUT_MILLI: (int) The polling timeout for response_socket.
        _SOCKET_IDENTITY: (bytes) The socket identity in bytes used for the
            ROUTER socket on the other side to identify the DEALER socket in
            this class.  Optional since zmq DEALER socket generates a default
            identity.
        _is_running: (Event) Event object that indicates on/ off
            behavior for the response handler loop.
    """
    def __init__(self,
                 zmq_context: zmq.Context,
                 zmq_endpoint: str,
                 response_handler: ResponseHandler,
                 polling_timeout_milli: int = 1000,
                 name: str = 'ResponseHandler',
                 socket_identity: bytes = None):
        assert zmq_context
        assert zmq_endpoint
        assert response_handler

        self._ZMQ_CONTEXT = zmq_context
        self._ZMQ_ENDPOINT = zmq_endpoint
        self._RESPONSE_HANDLER = response_handler

        self._POLLING_TIMEOUT_MILLI = polling_timeout_milli
        self._SOCKET_IDENTITY = socket_identity

        self._is_running = Event()
        super().__init__(name=name)

    def set_response_handler(self, response_handler: ResponseHandler):
        """
        Set _RESPONSE_HANDLER.
        :param response_handler:
        """
        self._RESPONSE_HANDLER = response_handler

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

    def stop(self):
        """
        Clear the _is_running Event, which terminates the response receiver
        loop.
        """
        self._is_running.clear()

    def run(self):
        """
        Message receiving loop.
        Create the response_socket as a zmq.DEALER socket and then connect to
        the provided _ZMQ_ENDPOINT.  After that, set up the poller and handle
        received messages.

        Normally zmq socket generates a default socket identity, but for
        testing or other purposes, the socket identity can be set by passing
        in a binary identity when creating the ResponseReceiver class.

        The poller exists so that the response receiver can be stopped
        gracefully and not get blocked by socket.recv() or stuck in a loop.
        """
        response_socket = self._ZMQ_CONTEXT.socket(zmq.DEALER)
        if self._SOCKET_IDENTITY:
            response_socket.setsockopt(zmq.IDENTITY, self._SOCKET_IDENTITY)
        response_socket.connect(self._ZMQ_ENDPOINT)
        poller = zmq.Poller()
        # pylint: disable=E1101
        poller.register(response_socket, zmq.POLLIN)
        # pylint: enable=E1101
        self._is_running.set()
        while self._is_running.is_set():
            socks = dict(poller.poll(self._POLLING_TIMEOUT_MILLI))
            if socks.get(response_socket) == zmq.POLLIN:
                message = response_socket.recv()
                self._handle_binary_omega_message(message)
        time.sleep(2.)
        response_socket.close()

    def _handle_response(self,
                         response_type: str,
                         response: capnp._DynamicStructBuilder):
        """
        Pass response_type and response to the registered response handler.
        :param response_type: (str) The type of TradeMessage embedded in the
            response from Omega.
        :param response: (capnp._DynamicStructBuilder) One of the types under
            "TradeMessage.Response.body".
            See communication_protocol.TradeMessage.capnp.
        """
        self._RESPONSE_HANDLER.handle_response(response_type, response)

    def _handle_binary_omega_message(self, binary_msg: bytes):
        """
        Pass a received message from Omega to an appropriate handler method.
        :param binary_msg: (bytes) The received binary message.
        """
        try:
            trade_message = msgs_capnp.TradeMessage.from_bytes(binary_msg)
            response = trade_message.type.response
            response_type = response.body.which()
            self._handle_response(response_type, response)
        except (Exception, TypeError) as e:
            logger.error('Exception in decoding message' + repr(e),
                         extra={'exception': repr(e)})
