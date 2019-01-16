import time
from typing import List

# pylint: disable=W0611
import capnp
# pylint: enable=W0611
import pytest
import zmq

# pylint: disable=E0611
# pylint: disable=E0401
import communication_protocol.TradeMessage_capnp as msgs_capnp
# pylint: enable=E0401
# pylint: enable=E0611
from tes_client.messaging.response_handler import ResponseHandler
from tes_client.communication.response_receiver import ResponseReceiver


__FAKE_ROUTER_SOCKET_CONNECTION_STR = 'inproc://FAKE_ROUTER_SOCKET'
__FAKE_DEALER_SOCKET_CONNECTION_STR = 'inproc://FAKE_DEALER_SOCKET'
__RESPONSE_RECEIVER_IDENTITY = b'A'


class FakeResponseHandler(ResponseHandler):
    def __init__(self, message_list):
        self.message_list = message_list
        super().__init__()

    def on_heartbeat(self, client_id: int, sender_comp_id: str):
        self.message_list.append(('heartbeat', client_id, sender_comp_id))

    def on_test_message(self,
                        string: str,
                        client_id: int,
                        sender_comp_id: str):
        self.message_list.append(('test', string, client_id, sender_comp_id))

    def on_system_message(self,
                          error_code: int,
                          message: str,
                          client_id: int,
                          sender_comp_id: str):
        self.message_list.append(('system',
                                  error_code,
                                  message,
                                  client_id,
                                  sender_comp_id))

    def on_logon_ack(self,
                     success: bool,
                     message: str,
                     client_accounts: List[int],
                     client_id: int,
                     sender_comp_id: str):
        self.message_list.append(('logonAck',
                                  success,
                                  message,
                                  client_accounts,
                                  client_id,
                                  sender_comp_id))


@pytest.fixture(scope="session")
def fake_zmq_context():
    zmq_context = zmq.Context.instance()
    yield zmq_context


@pytest.fixture(scope="function")
def fake_response_handler():
    yield FakeResponseHandler(list())


@pytest.fixture(scope="module")
def fake_router_socket(fake_zmq_context):
    router_socket = fake_zmq_context.socket(zmq.ROUTER)
    router_socket.bind(__FAKE_ROUTER_SOCKET_CONNECTION_STR)
    yield router_socket
    router_socket.close()


@pytest.fixture(scope="module")
def fake_dealer_socket(fake_zmq_context):
    dealer_socket = fake_zmq_context.socket(zmq.DEALER)
    dealer_socket.bind(__FAKE_DEALER_SOCKET_CONNECTION_STR)
    yield dealer_socket
    dealer_socket.close()


@pytest.fixture(scope="module")
def fake_response_receiver_from_router(fake_zmq_context, fake_router_socket):
    fake_response_handler = FakeResponseHandler(list())
    response_receiver = ResponseReceiver(
        fake_zmq_context,
        __FAKE_ROUTER_SOCKET_CONNECTION_STR,
        fake_response_handler,
        socket_identity=__RESPONSE_RECEIVER_IDENTITY
    )
    response_receiver.start()
    yield response_receiver
    response_receiver.cleanup()


@pytest.fixture(scope="module")
def fake_response_receiver_from_dealer(fake_zmq_context, fake_dealer_socket):
    fake_response_handler = FakeResponseHandler(list())
    response_receiver = ResponseReceiver(
        fake_zmq_context,
        __FAKE_DEALER_SOCKET_CONNECTION_STR,
        fake_response_handler
    )
    response_receiver.start()
    yield response_receiver
    response_receiver.stop()


@pytest.mark.test_id(1)
def test_message_receiving_from_router(
        monkeypatch, fake_router_socket, fake_response_receiver_from_router):
    collected_message_list = list()

    def mock_handle_message(message):
        collected_message_list.append(message)

    monkeypatch.setattr(fake_response_receiver_from_router,
                        '_handle_binary_tes_message',
                        mock_handle_message)

    for x in range(6):
        fake_router_socket.send_multipart(
            [__RESPONSE_RECEIVER_IDENTITY, b'test'])
    time.sleep(0.1)
    assert len(collected_message_list) == 6
    for x in range(6):
        assert collected_message_list[x] == b'test'


@pytest.mark.test_id(2)
def test_message_receiving_from_dealer(
        monkeypatch, fake_dealer_socket, fake_response_receiver_from_dealer):
    collected_message_list = list()

    def mock_handle_message(message):
        collected_message_list.append(message)

    monkeypatch.setattr(fake_response_receiver_from_dealer,
                        '_handle_binary_tes_message',
                        mock_handle_message)

    for x in range(6):
        fake_dealer_socket.send(b'test')

    time.sleep(0.1)
    assert len(collected_message_list) == 6
    for x in range(6):
        assert collected_message_list[x] == b'test'


@pytest.mark.test_id(3)
def test_heartbeat_handling(fake_response_receiver_from_dealer,
                            fake_response_handler):
    tes_mess = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp = tes_mess.init('type').init('response')
    heartbeat_resp.clientID = 123
    heartbeat_resp.senderCompID = str(987)
    body = heartbeat_resp.init('body')
    body.heartbeat = None

    fake_response_receiver_from_dealer._response_handler = fake_response_handler
    fake_response_receiver_from_dealer._handle_binary_tes_message(
        tes_mess.to_bytes())
    assert fake_response_handler.message_list[0] == (
        'heartbeat', 123, '987')


def test_test_message_handling(fake_response_receiver_from_dealer,
                               fake_response_handler):
    tes_mess = msgs_capnp.TradeMessage.new_message()
    test_response = tes_mess.init('type').init('response')
    test_response.clientID = 123
    test_response.senderCompID = str(987)
    body = test_response.init('body')
    test = body.init('test')
    test.string = 'test_string'

    fake_response_receiver_from_dealer._response_handler = fake_response_handler
    fake_response_receiver_from_dealer._handle_binary_tes_message(
        tes_mess.to_bytes())
    assert fake_response_handler.message_list[0] == ('test', 'test_string',
                                                     123, '987')


def test_system_message_handling(fake_response_receiver_from_dealer,
                                 fake_response_handler):
    tes_mess = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp = tes_mess.init('type').init('response')
    heartbeat_resp.clientID = 123
    heartbeat_resp.senderCompID = str(987)
    body = heartbeat_resp.init('body')
    system = body.init('system')
    account = system.init('accountInfo')
    account.accountID = 100
    system.errorCode = 0
    system.message = ('The Times 03/Jan/2009 Chancellor on brink of second ' +
                      'bailout for banks')

    fake_response_receiver_from_dealer._response_handler = fake_response_handler
    fake_response_receiver_from_dealer._handle_binary_tes_message(
        tes_mess.to_bytes())
    assert fake_response_handler.message_list[0] == (
        'system', system.errorCode, system.message, 123, '987')


def test_logon_ack_handling(fake_response_receiver_from_dealer,
                            fake_response_handler):
    tes_mess = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp = tes_mess.init('type').init('response')
    heartbeat_resp.clientID = 123
    heartbeat_resp.senderCompID = str(987)
    body = heartbeat_resp.init('body')
    logon = body.init('logonAck')
    logon.success = True
    logon.message = ('The Times 03/Jan/2009 Chancellor on brink of second ' +
                     'bailout for banks')
    logon.clientAccounts = [100, 101]

    fake_response_receiver_from_dealer._response_handler = fake_response_handler
    fake_response_receiver_from_dealer._handle_binary_tes_message(
        tes_mess.to_bytes())
    assert fake_response_handler.message_list[0] == (
        'logonAck', logon.success, logon.message, [100, 101], 123, '987')


# TODO: complete mock response handler tests
# TODO: integrate message receiving and handling to perform a full loop
