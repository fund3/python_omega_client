from queue import Queue
import time

import pytest
import zmq

from tes_client.messaging.common_types import RequestHeader
from tes_client.messaging.response_handler import ResponseHandler
from tes_client.communication.response_receiver import ResponseReceiver
from tes_client.communication.request_sender import RequestSender
from tes_client.communication.tes_connection import TesConnection

__TES_ENDPOINT = 'inproc://TES'
__REQUEST_SENDER_ENDPOINT = 'inproc://REQUEST_SENDER'
__RESPONSE_RECEIVER_ENDPOINT = 'inproc://RESPONSE_RECEIVER'
__TES_SOCKET_IDENTITY = b'TES_SOCKET'
__FAKE_ACCESS_TOKEN = 'FakeAccessToken'
__FAKE_REQUEST_HEADER = RequestHeader(client_id=123,
                                      sender_comp_id='987',
                                      access_token=__FAKE_ACCESS_TOKEN,
                                      request_id=100001)


@pytest.fixture(scope="session")
def fake_zmq_context():
    zmq_context = zmq.Context.instance()
    yield zmq_context


@pytest.fixture(scope="module")
def fake_request_sender(fake_zmq_context):
    queue = Queue()
    request_sender = RequestSender(
        zmq_context=fake_zmq_context,
        zmq_endpoint=__REQUEST_SENDER_ENDPOINT,
        outgoing_message_queue=queue,
    )
    request_sender._access_token = __FAKE_ACCESS_TOKEN
    yield request_sender
    request_sender.stop()


@pytest.fixture(scope="module")
def fake_response_receiver(fake_zmq_context):
    response_receiver = ResponseReceiver(
        zmq_context=fake_zmq_context,
        zmq_endpoint=__RESPONSE_RECEIVER_ENDPOINT,
        response_handler=ResponseHandler()
    )
    yield response_receiver
    response_receiver.stop()


@pytest.fixture(scope="module")
def fake_router_socket(fake_zmq_context):
    router_socket = fake_zmq_context.socket(zmq.ROUTER)
    router_socket.bind(__TES_ENDPOINT)
    yield router_socket
    router_socket.close()


@pytest.fixture(scope="module")
def fake_tes_connection(fake_zmq_context,
                        fake_request_sender,
                        fake_response_receiver):
    tes_connection = TesConnection(
        fake_zmq_context,
        __TES_ENDPOINT,
        __REQUEST_SENDER_ENDPOINT,
        __RESPONSE_RECEIVER_ENDPOINT,
        fake_request_sender,
        fake_response_receiver,
        tes_socket_identity=__TES_SOCKET_IDENTITY
    )
    tes_connection.start()
    yield tes_connection
    tes_connection.cleanup()


@pytest.mark.test_id(1)
def test_receive_tes_message(monkeypatch,
                             fake_router_socket,
                             fake_response_receiver,
                             fake_tes_connection):
    collected_message_list = list()

    def mock_handle_message(message):
        collected_message_list.append(message)

    monkeypatch.setattr(fake_response_receiver,
                        '_handle_binary_tes_message',
                        mock_handle_message)

    for x in range(6):
        fake_router_socket.send_multipart(
            [__TES_SOCKET_IDENTITY, b'test'])
    time.sleep(0.1)
    assert len(collected_message_list) == 6


@pytest.mark.test_id(2)
def test_send_tes_message(fake_router_socket,
                          fake_request_sender,
                          fake_tes_connection):
    collected_message_list = list()
    for x in range(6):
        fake_request_sender.send_heartbeat(
            request_header=__FAKE_REQUEST_HEADER)
    time.sleep(0.1)
    for x in range(6):
        collected_message_list.append(fake_router_socket.recv())
    assert len(collected_message_list) == 6
