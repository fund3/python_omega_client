import time
from typing import List

# pylint: disable=W0611
import capnp
# pylint: enable=W0611
import pytest
import zmq

# pylint: disable=E0611
# pylint: disable=E0401
import trading_communication_protocol.TradeMessage_capnp as msgs_capnp
# pylint: enable=E0401
# pylint: enable=E0611
from omega_client.communication.response_receiver import ResponseReceiver
from omega_client.messaging.common_types import LogonAck, SystemMessage
from omega_client.messaging.response_handler import ResponseHandler


__FAKE_ROUTER_SOCKET_ENDPOINT = 'inproc://FAKE_ROUTER_SOCKET'
__FAKE_DEALER_SOCKET_ENDPOINT = 'inproc://FAKE_DEALER_SOCKET'
__RESPONSE_RECEIVER_IDENTITY = b'A'


class FakeResponseHandler(ResponseHandler):
    def __init__(self, message_list):
        self.message_list = message_list
        super().__init__()

    def on_heartbeat(self,
                     client_id: int,
                     sender_comp_id: str,
                     request_id: int):
        self.message_list.append(
            ('heartbeat', client_id, sender_comp_id, request_id))

    def on_test_message(self,
                        string: str,
                        client_id: int,
                        sender_comp_id: str,
                        request_id: int):
        self.message_list.append(
            ('test', string, client_id, sender_comp_id, request_id))

    def on_system_message(self,
                          system_message: SystemMessage,
                          client_id: int,
                          sender_comp_id: str,
                          request_id: int):
        self.message_list.append(('system',
                                  system_message.message.code,
                                  system_message.message.body,
                                  client_id,
                                  sender_comp_id,
                                  request_id))

    def on_logon_ack(self,
                     logon_ack: LogonAck,
                     client_id: int,
                     sender_comp_id: str,
                     request_id: int):
        self.message_list.append(('logonAck',
                                  logon_ack.success,
                                  logon_ack.message.code,
                                  logon_ack.message.body,
                                  [client_account.account_id for client_account
                                   in logon_ack.client_accounts],
                                  client_id,
                                  sender_comp_id,
                                  request_id))


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
    router_socket.bind(__FAKE_ROUTER_SOCKET_ENDPOINT)
    yield router_socket
    router_socket.close()


@pytest.fixture(scope="module")
def fake_dealer_socket(fake_zmq_context):
    dealer_socket = fake_zmq_context.socket(zmq.DEALER)
    dealer_socket.bind(__FAKE_DEALER_SOCKET_ENDPOINT)
    yield dealer_socket
    dealer_socket.close()


@pytest.fixture(scope="module")
def fake_response_receiver_from_router(fake_zmq_context):
    # Creates a response receiver that connects to a ROUTER socket.
    fake_response_handler = FakeResponseHandler(list())
    response_receiver = ResponseReceiver(
        zmq_context=fake_zmq_context,
        zmq_endpoint=__FAKE_ROUTER_SOCKET_ENDPOINT,
        response_handler=fake_response_handler,
        socket_identity=__RESPONSE_RECEIVER_IDENTITY
    )
    response_receiver.start()
    yield response_receiver
    response_receiver.stop()


@pytest.fixture(scope="module")
def fake_response_receiver_from_dealer(fake_zmq_context):
    # Creates a response receiver that connects to a DEALER socket.
    fake_response_handler = FakeResponseHandler(list())
    response_receiver = ResponseReceiver(
        zmq_context=fake_zmq_context,
        zmq_endpoint=__FAKE_DEALER_SOCKET_ENDPOINT,
        response_handler=fake_response_handler
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
                        '_handle_binary_omega_message',
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
                        '_handle_binary_omega_message',
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
    omega_mess = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp = omega_mess.init('type').init('response')
    heartbeat_resp.clientID = 123
    heartbeat_resp.senderCompID = str(987)
    heartbeat_resp.requestID = 100001
    body = heartbeat_resp.init('body')
    body.heartbeat = None

    fake_response_receiver_from_dealer.set_response_handler(
        fake_response_handler)
    fake_response_receiver_from_dealer._handle_binary_omega_message(
        omega_mess.to_bytes())

    assert len(fake_response_handler.message_list) == 1
    assert fake_response_handler.message_list[0] == (
        'heartbeat', 123, '987', 100001)


@pytest.mark.test_id(4)
def test_test_message_handling(fake_response_receiver_from_dealer,
                               fake_response_handler):
    omega_mess = msgs_capnp.TradeMessage.new_message()
    test_response = omega_mess.init('type').init('response')
    test_response.clientID = 123
    test_response.senderCompID = str(987)
    test_response.requestID = 100001
    body = test_response.init('body')
    test = body.init('test')
    test.string = 'test_string'

    fake_response_receiver_from_dealer.set_response_handler(
        fake_response_handler)
    fake_response_receiver_from_dealer._handle_binary_omega_message(
        omega_mess.to_bytes())
    assert len(fake_response_handler.message_list) == 1
    assert fake_response_handler.message_list[0] == ('test', 'test_string',
                                                     123, '987', 100001)


@pytest.mark.test_id(5)
def test_system_message_handling(fake_response_receiver_from_dealer,
                                 fake_response_handler):
    omega_mess = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp = omega_mess.init('type').init('response')
    heartbeat_resp.clientID = 123
    heartbeat_resp.senderCompID = str(987)
    heartbeat_resp.requestID = 100001
    body = heartbeat_resp.init('body')
    system = body.init('system')
    account = system.init('accountInfo')
    account.accountID = 100
    system.message.code = 0
    system.message.body = ('The Times 03/Jan/2009 Chancellor on brink of ' +
                           'second bailout for banks')

    fake_response_receiver_from_dealer.set_response_handler(
        fake_response_handler)
    fake_response_receiver_from_dealer._handle_binary_omega_message(
        omega_mess.to_bytes())
    assert len(fake_response_handler.message_list) == 1
    assert fake_response_handler.message_list[0] == (
        'system', system.message.code, system.message.body, 123, '987', 100001)


@pytest.mark.test_id(6)
def test_logon_ack_handling(fake_response_receiver_from_dealer,
                            fake_response_handler):
    omega_mess = msgs_capnp.TradeMessage.new_message()
    logon_ack_resp = omega_mess.init('type').init('response')
    logon_ack_resp.clientID = 123
    logon_ack_resp.senderCompID = str(987)
    logon_ack_resp.requestID = 100001
    body = logon_ack_resp.init('body')
    logon_ack_capnp = body.init('logonAck')
    logon_ack_capnp.success = True
    logon_ack_capnp.message.code = 0
    logon_ack_capnp.message.body = ('The Times 03/Jan/2009 Chancellor on ' +
                                    'brink of second bailout for banks')
    client_accounts = logon_ack_capnp.init('clientAccounts', 2)
    client_accounts[0].accountID = 100
    client_accounts[1].accountID = 101
    grant = logon_ack_capnp.init('authorizationGrant')
    grant.success = True
    grant.message.body = "Granted"
    grant.accessToken = "AccessToken"
    grant.refreshToken = "refreshToken"
    grant.expireAt = 1551288929.0

    fake_response_receiver_from_dealer.set_response_handler(
        fake_response_handler)
    fake_response_receiver_from_dealer._handle_binary_omega_message(
        omega_mess.to_bytes())
    assert len(fake_response_handler.message_list) == 1
    assert fake_response_handler.message_list[0] == (
        'logonAck',
        logon_ack_capnp.success,
        logon_ack_capnp.message.code,
        logon_ack_capnp.message.body,
        [100, 101],
        123,
        '987',
        100001
    )


# TODO: complete mock response handler tests
# TODO: integrate message receiving and handling to perform a full loop

