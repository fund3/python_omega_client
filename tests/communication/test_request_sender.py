from queue import Queue

import capnp
import pytest
import zmq

import communication_protocol.Exchanges_capnp as exch_capnp
import communication_protocol.TradeMessage_capnp as msgs_capnp
from tes_client.communication.request_sender import RequestSender
from tes_client.messaging.common_types import AccountBalancesReport, \
    AccountCredentials, AccountDataReport, AccountInfo, Balance, \
    CompletedOrdersReport, Exchange, ExchangePropertiesReport, LeverageType, \
    ExecutionReport, OpenPosition, OpenPositionsReport, Order, OrderInfo, \
    OrderStatus, OrderType, Side, SymbolProperties, TimeInForce, \
    WorkingOrdersReport
from tes_client.messaging.message_factory import heartbeat_capnp


TEST_ACCOUNT_CREDS_1 = AccountCredentials(AccountInfo(0), api_key='api_key',
                                          secret_key='secret_key',
                                          passphrase='passphrase')
TEST_TES_CONFIG = {'TES_CONNECTION_STR': 'tcp://127.0.0.1:5555',
                   'CREDENTIALS': [TEST_ACCOUNT_CREDS_1]}

TEST_ZMQ_ENCRYPTION_KEY = b'encryptionkeyencryptionkeyencryptionkeye'

__FAKE_ROUTER_SOCKET_CONNECTION_STR = 'inproc://FAKE_ROUTER_SOCKET'
__FAKE_DEALER_SOCKET_CONNECTION_STR = 'inproc://FAKE_DEALER_SOCKET'
__FAKE_REQUEST_SENDER_CONNECTION_STR = 'inproc://FAKE_REQUEST_SENDER'

# TODO: Integration Testing


@pytest.fixture(scope="session")
def fake_zmq_context():
    zmq_context = zmq.Context.instance()
    yield zmq_context


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
def fake_request_sender_to_router(fake_zmq_context):
    queue = Queue()
    request_sender = RequestSender(
        zmq_context=fake_zmq_context,
        zmq_endpoint=__FAKE_ROUTER_SOCKET_CONNECTION_STR,
        outgoing_message_queue=queue
    )
    request_sender.start()
    yield request_sender
    request_sender.cleanup()


@pytest.fixture(scope="module")
def fake_request_sender_to_dealer(fake_zmq_context):
    queue = Queue()
    request_sender = RequestSender(
        zmq_context=fake_zmq_context,
        zmq_endpoint=__FAKE_DEALER_SOCKET_CONNECTION_STR,
        outgoing_message_queue=queue
    )
    request_sender.start()
    yield request_sender
    request_sender.cleanup()


@pytest.fixture(scope="module")
def fake_request_sender(fake_zmq_context):
    queue = Queue()
    request_sender = RequestSender(
        zmq_context=fake_zmq_context,
        zmq_endpoint=__FAKE_REQUEST_SENDER_CONNECTION_STR,
        outgoing_message_queue=queue
    )
    request_sender._queue_message = lambda message: None
    request_sender.start()
    yield request_sender
    request_sender.cleanup()


@pytest.mark.test_id(1)
def test_message_sending_to_dealer(fake_dealer_socket,
                                   fake_request_sender_to_dealer):
    tes_message, body = heartbeat_capnp(123, '987')
    fake_request_sender_to_dealer.send_heartbeat(123, '987')
    received_message = fake_dealer_socket.recv()
    assert received_message == tes_message.to_bytes()


@pytest.mark.test_id(2)
def test_message_sending_to_router(fake_router_socket,
                                   fake_request_sender_to_router):
    tes_message, body = heartbeat_capnp(123, '987')
    fake_request_sender_to_router.send_heartbeat(123, '987')
    identity, received_message = fake_router_socket.recv_multipart()
    assert received_message == tes_message.to_bytes()


@pytest.mark.test_id(3)
def test_place_order(fake_request_sender):
    order = Order(
        account_info=AccountInfo(account_id=100),
        client_order_id=8675309,
        client_order_link_id='a123',
        symbol='BTC/USD',
        side=Side.buy.name,
        order_type=OrderType.limit.name,
        quantity=1.1,
        price=6000.01,
        time_in_force=TimeInForce.gtc.name,
        leverage_type=LeverageType.none.name
    )
    order = fake_request_sender.place_order(order=order, client_id=0,
                                            sender_comp_id='asdf')
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 100
    assert order.symbol == 'BTC/USD'
    assert order.side == 'buy'
    assert order.orderType == 'limit'
    assert order.quantity == 1.1
    assert order.price == 6000.01
    assert order.timeInForce == 'gtc'


@pytest.mark.test_id(4)
def test_replace_order(fake_request_sender):
    order = fake_request_sender.replace_order(
        account_info=AccountInfo(account_id=100),
        client_id=0, sender_comp_id='asdf',
        order_id='c137', quantity=1.1, order_type=OrderType.limit.name,
        price=6000.01, time_in_force=TimeInForce.gtc.name
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 100
    assert order.orderID == 'c137'
    assert order.orderType == 'limit'
    assert order.quantity == 1.1
    assert order.price == 6000.01
    assert order.timeInForce == 'gtc'


@pytest.mark.test_id(5)
def test_cancel_order(fake_request_sender):
    order = fake_request_sender.cancel_order(
        account_info=AccountInfo(account_id=100), order_id='c137',
        client_id=0, sender_comp_id='asdf'
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 100
    assert order.orderID == 'c137'


@pytest.mark.test_id(6)
def test_request_account_data(fake_request_sender):
    order = fake_request_sender.request_account_data(
        account_info=AccountInfo(account_id=100),
        client_id=0, sender_comp_id='asdf')
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 100


@pytest.mark.test_id(7)
def test_request_account_balances(fake_request_sender):
    order = fake_request_sender.request_account_balances(
        account_info=AccountInfo(account_id=110),
        client_id=0, sender_comp_id='asdf')
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110


@pytest.mark.test_id(8)
def test_request_working_orders(fake_request_sender):
    order = fake_request_sender.request_working_orders(
        account_info=AccountInfo(account_id=110),
        client_id=0, sender_comp_id='asdf'
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110


@pytest.mark.test_id(9)
def test_tes_logon(fake_request_sender):
    creds = [
        AccountCredentials(
            account_info=AccountInfo(account_id=100),
            api_key='fakeApiKey', secret_key='fakeSecret',
            passphrase='fakePassphrase'
        ),
        AccountCredentials(
            account_info=AccountInfo(account_id=110),
            api_key='fakeApiKey', secret_key='fakeSecret',
            passphrase='fakePassphrase'
        ),
        AccountCredentials(
            account_info=AccountInfo(account_id=200),
            api_key='fakeApiKey1', secret_key='fakeSecret1',
            passphrase='fakePassphrase1'
        ),
        AccountCredentials(
            account_info=AccountInfo(account_id=210),
            api_key='fakeApiKey1', secret_key='fakeSecret1',
            passphrase='fakePassphrase1'
        )
    ]
    fake_request_sender._tes_credentials = creds
    logon = fake_request_sender.logon(credentials=creds, client_id=0,
                                      sender_comp_id='asdf')
    assert type(logon) == capnp.lib.capnp._DynamicStructBuilder
    assert logon.credentials[0].accountInfo.accountID == 100
    assert logon.credentials[0].apiKey == 'fakeApiKey'
    assert logon.credentials[0].secretKey == 'fakeSecret'
    assert logon.credentials[0].passphrase == 'fakePassphrase'
    assert logon.credentials[1].accountInfo.accountID == 110
    assert logon.credentials[1].apiKey == 'fakeApiKey'
    assert logon.credentials[1].secretKey == 'fakeSecret'
    assert logon.credentials[1].passphrase == 'fakePassphrase'
    assert logon.credentials[2].accountInfo.accountID == 200
    assert logon.credentials[2].apiKey == 'fakeApiKey1'
    assert logon.credentials[2].secretKey == 'fakeSecret1'
    assert logon.credentials[2].passphrase == 'fakePassphrase1'
    assert logon.credentials[3].accountInfo.accountID == 210
    assert logon.credentials[3].apiKey == 'fakeApiKey1'
    assert logon.credentials[3].secretKey == 'fakeSecret1'
    assert logon.credentials[3].passphrase == 'fakePassphrase1'

    # logon missing passphrase - check for capnp default None
    creds1 = [
        AccountCredentials(
            account_info=AccountInfo(account_id=100),
            api_key='fakeApiKey', secret_key='fakeSecret'
        )
    ]
    fake_request_sender._tes_credentials = creds1
    logon1 = fake_request_sender.logon(credentials=creds1, client_id=0,
                                       sender_comp_id='asdf')
    assert type(logon) == capnp.lib.capnp._DynamicStructBuilder
    assert logon1.credentials[0].accountInfo.accountID == 100
    assert logon1.credentials[0].apiKey == 'fakeApiKey'
    assert logon1.credentials[0].secretKey == 'fakeSecret'
    # capnp default
    assert logon1.credentials[0].passphrase == '<NONE>' or \
        logon1.credentials[0].passphrase == ''

    # logon missing apiKey - Attribute Error
    with pytest.raises(Exception or AttributeError):
        creds2 = [
            AccountCredentials(
                account_info=AccountInfo(account_id=100),
                secret_key='fakeSecret'
            )
        ]
        fake_request_sender._tes_credentials = creds2
        logon2 = fake_request_sender.logon(credentials=creds2, client_id=0,
                                           sender_comp_id='asdf')

    # logon missing apiSecret - Attribute Error
    with pytest.raises(Exception or AttributeError):
        creds3 = [
            AccountCredentials(
                account_info=AccountInfo(account_id=100),
                api_key='fakeApiKey'
            )
        ]
        fake_request_sender._tes_credentials = creds3
        logon3 = fake_request_sender.logon(credentials=creds3, client_id=0,
                                           sender_comp_id='asdf')
    fake_request_sender._tes_credentials = TEST_ACCOUNT_CREDS_1


@pytest.mark.test_id(10)
def test_tes_logoff(fake_request_sender):
    logoff = fake_request_sender.logoff(client_id=0, sender_comp_id='asdf')
    assert type(logoff) == capnp.lib.capnp._DynamicStructBuilder
    assert logoff.logoff is None


@pytest.mark.test_id(11)
def test_tes_heartbeat(fake_request_sender):
    hb = fake_request_sender.send_heartbeat(client_id=0, sender_comp_id='asdf')
    assert type(hb) == capnp.lib.capnp._DynamicStructBuilder
    assert hb.heartbeat is None


@pytest.mark.test_id(12)
def test_request_order_status(fake_request_sender):
    order = fake_request_sender.request_order_status(
        client_id=0, sender_comp_id='asdf',
        account_info=AccountInfo(account_id=110),
        order_id='poiuytrewq123'
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110
    assert order.orderID == 'poiuytrewq123'


@pytest.mark.test_id(13)
def test_request_working_orders(fake_request_sender):
    # test including count and since
    order = fake_request_sender.request_completed_orders(
        client_id=0, sender_comp_id='asdf',
        account_info=AccountInfo(account_id=110), count=2, since=1536267034.
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110
    assert order.count == 2
    assert order.since == pytest.approx(1536267034., rel=1e-2)

    # test including count, not since
    order = fake_request_sender.request_completed_orders(
        client_id=0, sender_comp_id='asdf',
        account_info=AccountInfo(account_id=110), count=2
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110
    assert order.count == 2

    # test including since, not count
    order = fake_request_sender.request_completed_orders(
        client_id=0, sender_comp_id='asdf',
        account_info=AccountInfo(account_id=110), since=1536267034.
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110
    assert order.since == pytest.approx(1536267034., rel=1e-2)

    # test excluding both count and since
    order = fake_request_sender.request_completed_orders(
        client_id=0, sender_comp_id='asdf',
        account_info=AccountInfo(account_id=110)
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110


@pytest.mark.test_id(14)
def test_request_order_mass_status(fake_request_sender):
    # empty order_info_list
    order_info_list = []
    order = fake_request_sender.request_order_mass_status(
        client_id=0, sender_comp_id='asdf',
        account_info=AccountInfo(account_id=110),
        order_info=order_info_list
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110
    assert len(list(order.orderInfo)) == 0

    # filled order_info_list
    order_info_list = [OrderInfo(order_id='poiuy9876')]
    order = fake_request_sender.request_order_mass_status(
        client_id=0, sender_comp_id='asdf',
        account_info=AccountInfo(account_id=110),
        order_info=order_info_list
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110
    assert len(list(order.orderInfo)) == 1
    assert order.orderInfo[0].orderID == 'poiuy9876'


"""
############################################################################

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Margin Support ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

############################################################################
"""


@pytest.mark.test_id(15)
def test_place_order_margin_default(fake_request_sender):
    default_margin_order = Order(
        account_info=AccountInfo(account_id=100),
        client_order_id=9876,
        client_order_link_id='a123',
        symbol='BTC/USD',
        side=Side.buy.name,
        order_type=OrderType.market.name,
        quantity=1.1,
        price=0.0,
        time_in_force=TimeInForce.gtc.name,
        leverage_type=LeverageType.exchangeDefault.name
    )
    # exchange default margin
    order = fake_request_sender.place_order(client_id=0,
                                            sender_comp_id='sndrCmpD',
                                            order=default_margin_order)
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 100
    assert order.symbol == 'BTC/USD'
    assert order.side == 'buy'
    assert order.orderType == 'market'
    assert order.quantity == 1.1
    assert order.price == 0.0
    assert order.timeInForce == 'gtc'
    assert order.leverageType == msgs_capnp.LeverageType.exchangeDefault
    assert order.leverage == 0.0


@pytest.mark.test_id(16)
def test_place_order_margin_custom(fake_request_sender):
    custom_margin_order = Order(
        account_info=AccountInfo(account_id=100),
        client_order_id=9876,
        client_order_link_id='a123',
        symbol='BTC/USD',
        side=Side.buy.name,
        order_type=OrderType.market.name,
        quantity=1.1,
        price=0.0,
        time_in_force=TimeInForce.gtc.name,
        leverage_type=LeverageType.custom.name,
        leverage=2.0
    )
    # custom margin
    order = fake_request_sender.place_order(client_id=0,
                                            sender_comp_id='sndrCmpD',
                                            order=custom_margin_order)
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 100
    assert order.symbol == 'BTC/USD'
    assert order.side == 'buy'
    assert order.orderType == 'market'
    assert order.quantity == 1.1
    assert order.price == 0.0
    assert order.timeInForce == 'gtc'
    assert order.leverageType == msgs_capnp.LeverageType.custom
    assert order.leverage == 2.0


@pytest.mark.test_id(17)
def test_request_open_positions(fake_request_sender):
    open_pos = fake_request_sender.request_open_positions(
        client_id=0, sender_comp_id='asdf',
        account_info=AccountInfo(account_id=110)
    )
    assert type(open_pos) == capnp.lib.capnp._DynamicStructBuilder
    assert open_pos.accountInfo.accountID == 110


@pytest.mark.test_id(18)
def test_request_exchange_properties(fake_request_sender):
    # valid exchange test case
    exch_prop = fake_request_sender.request_exchange_properties(
        client_id=0, sender_comp_id='sndrCmpD',
        exchange='gemini'
    )
    assert type(exch_prop) == capnp.lib.capnp._DynamicStructBuilder
    assert exch_prop.exchange == exch_capnp.Exchange.gemini


@pytest.mark.test_id(19)
def test_request_exchange_properties_invalid_case(fake_request_sender):
    # invalid exchange test case
    exch_prop = fake_request_sender.request_exchange_properties(
        client_id=0, sender_comp_id='sndrCmpD',
        exchange='gdax'
    )
    assert type(exch_prop) == capnp.lib.capnp._DynamicStructBuilder
    assert exch_prop.exchange == exch_capnp.Exchange.undefined