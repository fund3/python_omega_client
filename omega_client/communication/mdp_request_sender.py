import logging
from queue import Empty, Queue
from threading import Event, Thread
import time
from typing import Dict, List, Union

import capnp
import zmq

from omega_client.common_types.market_data_structs import MDHeader
from omega_client.common_types.enum_types import Channel
from omega_client.messaging.message_factory import request_mdp_capnp

logger = logging.getLogger(__name__)


class MDPRequestSender(Thread):
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
                 name: str='MDPRequestSender'):
        assert zmq_context
        assert zmq_endpoint

        self._ZMQ_CONTEXT = zmq_context
        self._ZMQ_ENDPOINT = zmq_endpoint
        self._QUEUE_POLLING_TIMEOUT_SECONDS = queue_polling_timeout_seconds

        self._outgoing_message_queue = outgoing_message_queue or Queue()

        self._is_running = Event()
        super().__init__(name=name)

    def _queue_message(self, omega_message_capnp: capnp._DynamicStructBuilder):
        """
        Put a capnp message into the internal queue for sending to
        TesConnection.
        :param omega_message_capnp:
        """
        self._outgoing_message_queue.put(omega_message_capnp)

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
        omega_message, request_mdp = request_mdp_capnp(
            request_header=request_header,
            channels=channels,
            exchange=exchange,
            symbols=symbols,
            market_depth=market_depth,
            is_subscribe=is_subscribe
        )
        self._queue_message(omega_message)
        return request_mdp
