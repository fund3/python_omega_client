import capnp
import pytest
import zmq

import trading_communication_protocol.Exchanges_capnp as exch_capnp
import trading_communication_protocol.TradeMessage_capnp as msgs_capnp

from omega_client.messaging.common_types import AccountBalancesReport, \
    AccountCredentials, AccountDataReport, AccountInfo, Balance, \
    CompletedOrdersReport, Exchange, ExchangePropertiesReport, LeverageType, \
    ExecutionReport, OpenPosition, OpenPositionsReport, Order, OrderInfo, \
    OrderStatus, OrderType, RequestHeader, Side, SymbolProperties, \
    TimeInForce, WorkingOrdersReport
from omega_client.communication.single_client_request_sender import \
    SingleClientRequestSender

TEST_ACCOUNT_CREDS_1 = AccountCredentials(AccountInfo(0), api_key='api_key',
                                          secret_key='secret_key',
                                          passphrase='passphrase')
TEST_OMEGA_CONFIG = {'OMEGA_CONNECTION_STR': 'tcp://127.0.0.1:5555',
                     'CREDENTIALS': [TEST_ACCOUNT_CREDS_1]}

TEST_CLIENT_ID = 123
TEST_SENDER_COMP_ID = str(987)
TEST_ZMQ_ENCRYPTION_KEY = b'encryptionkeyencryptionkeyencryptionkeye'
__FAKE_REQUEST_SENDER_CONNECTION_STR = 'inproc://FAKE_REQUEST_SENDER'
__FAKE_CLIENT_SECRET = ('2B24_ih9IFVdWgxR2sEA3rj0fKlY212Ec_TwTNVCD663ktYb1' +
                        'ABPz4qJy0Ouze6O9vgdueei0XmZ6uGGFM34nw')
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
    request_sender = SingleClientRequestSender(
        zmq_context=fake_zmq_context,
        connection_string=__FAKE_REQUEST_SENDER_CONNECTION_STR,
        client_id=TEST_CLIENT_ID,
        sender_comp_id=TEST_SENDER_COMP_ID
    )
    request_sender._queue_message = lambda message: None
    request_sender._request_header = __FAKE_REQUEST_HEADER
    request_sender.start()
    yield request_sender
    request_sender.cleanup()


@pytest.mark.test_id(1)
def test_place_order(fake_request_sender):
    order = Order(
        account_info=AccountInfo(account_id=100),
        client_order_id=str(8675309),
        client_order_link_id='a123',
        symbol='BTC/USD',
        side=Side.buy.name,
        order_type=OrderType.limit.name,
        quantity=1.1,
        price=6000.01,
        stop_price=0.0,
        time_in_force=TimeInForce.gtc.name,
        expire_at=0.0,
        leverage_type=LeverageType.none.name
    )
    order = fake_request_sender.place_order(order=order)
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 100
    assert order.symbol == 'BTC/USD'
    assert order.side == 'buy'
    assert order.orderType == 'limit'
    assert order.quantity == 1.1
    assert order.price == 6000.01
    assert order.timeInForce == 'gtc'
    assert order.stopPrice == 0.0
    assert order.expireAt == 0.0


@pytest.mark.test_id(2)
def test_replace_order(fake_request_sender):
    order = fake_request_sender.replace_order(
        account_info=AccountInfo(account_id=100),
        order_id='c137', quantity=1.1, order_type=OrderType.limit.name,
        price=6000.01, stop_price=0.0, time_in_force=TimeInForce.gtc.name,
        expire_at=0.0
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 100
    assert order.orderID == 'c137'
    assert order.orderType == 'limit'
    assert order.quantity == 1.1
    assert order.price == 6000.01
    assert order.stopPrice == 0.0
    assert order.timeInForce == 'gtc'
    assert order.expireAt == 0.0


@pytest.mark.test_id(3)
def test_cancel_order(fake_request_sender):
    order = fake_request_sender.cancel_order(
        account_info=AccountInfo(account_id=100), order_id='c137'
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 100
    assert order.orderID == 'c137'


@pytest.mark.test_id(4)
def test_request_account_data(fake_request_sender):
    order = fake_request_sender.request_account_data(
        account_info=AccountInfo(account_id=100))
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 100


@pytest.mark.test_id(5)
def test_request_account_balances(fake_request_sender):
    order = fake_request_sender.request_account_balances(
        account_info=AccountInfo(account_id=110))
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110


@pytest.mark.test_id(6)
def test_request_working_orders(fake_request_sender):
    order = fake_request_sender.request_working_orders(
        account_info=AccountInfo(account_id=110)
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110


@pytest.mark.test_id(7)
def test_omega_logon(fake_request_sender):
    creds = [
        AccountCredentials(
            account_info=AccountInfo(account_id=100 ),
            api_key='fakeApiKey', secret_key='fakeSecret',
            passphrase='fakePassphrase'
        ),
        AccountCredentials(
            account_info=AccountInfo(account_id=110),
            api_key='fakeApiKey', secret_key='fakeSecret',
            passphrase='fakePassphrase'
        ),
        AccountCredentials(
            account_info=AccountInfo(account_id=200 ),
            api_key='fakeApiKey1', secret_key='fakeSecret1',
            passphrase='fakePassphrase1'
        ),
        AccountCredentials(
            account_info=AccountInfo(account_id=210),
            api_key='fakeApiKey1', secret_key='fakeSecret1',
            passphrase='fakePassphrase1'
        )
    ]
    fake_request_sender._omega_credentials = creds
    logon = fake_request_sender.logon(credentials=creds,
                                      client_secret=__FAKE_CLIENT_SECRET)
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
    fake_request_sender._omega_credentials = creds1
    logon1 = fake_request_sender.logon(credentials=creds1,
                                       client_secret=__FAKE_CLIENT_SECRET)
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
        fake_request_sender._omega_credentials = creds2
        logon2 = fake_request_sender.logon(credentials=creds2,
                                           client_secret=__FAKE_CLIENT_SECRET)

    # logon missing apiSecret - Attribute Error
    with pytest.raises(Exception or AttributeError):
        creds3 = [
            AccountCredentials(
                account_info=AccountInfo(account_id=100),
                api_key='fakeApiKey'
            )
        ]
        fake_request_sender._omega_credentials = creds3
        logon3 = fake_request_sender.logon(credentials=creds3,
                                           client_secret=__FAKE_CLIENT_SECRET)
    fake_request_sender._omega_credentials = TEST_ACCOUNT_CREDS_1


@pytest.mark.test_id(8)
def test_omega_logoff(fake_request_sender):
    logoff = fake_request_sender.logoff()
    assert type(logoff) == capnp.lib.capnp._DynamicStructBuilder
    assert logoff.logoff is None


@pytest.mark.test_id(9)
def test_omega_heartbeat(fake_request_sender):
    hb = fake_request_sender.send_heartbeat()
    assert type(hb) == capnp.lib.capnp._DynamicStructBuilder
    assert hb.heartbeat is None


@pytest.mark.test_id(10)
def test_request_order_status(fake_request_sender):
    order = fake_request_sender.request_order_status(
        account_info=AccountInfo(account_id=110),
        order_id='poiuytrewq123'
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110
    assert order.orderID == 'poiuytrewq123'


@pytest.mark.test_id(11)
def test_request_working_orders(fake_request_sender):
    # test including count and since
    order = fake_request_sender.request_completed_orders(
        account_info=AccountInfo(account_id=110), count=2, since=1536267034.
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110
    assert order.count == 2
    assert order.since == pytest.approx(1536267034., rel=1e-2)

    # test including count, not since
    order = fake_request_sender.request_completed_orders(
        account_info=AccountInfo(account_id=110), count=2
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110
    assert order.count == 2

    # test including since, not count
    order = fake_request_sender.request_completed_orders(
        account_info=AccountInfo(account_id=110), since=1536267034.
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110
    assert order.since == pytest.approx(1536267034., rel=1e-2)

    # test excluding both count and since
    order = fake_request_sender.request_completed_orders(
        account_info=AccountInfo(account_id=110)
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110


"""
############################################################################

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Margin Support ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

############################################################################
"""


@pytest.mark.test_id(13)
def test_place_order_margin_default(fake_request_sender):
    default_margin_order = Order(
        account_info=AccountInfo(account_id=100),
        client_order_id=str(9876),
        client_order_link_id='a123',
        symbol='BTC/USD',
        side=Side.buy.name,
        order_type=OrderType.market.name,
        quantity=1.1,
        price=0.0,
        stop_price=0.0,
        time_in_force=TimeInForce.gtc.name,
        expire_at=0.0,
        leverage_type=LeverageType.exchangeDefault.name
    )
    # exchange default margin
    order = fake_request_sender.place_order(order=default_margin_order)
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
    assert order.stopPrice == 0.0
    assert order.expireAt == 0.0


@pytest.mark.test_id(14)
def test_place_order_margin_custom(fake_request_sender):
    custom_margin_order = Order(
        account_info=AccountInfo(account_id=100),
        client_order_id=str(9876),
        client_order_link_id='a123',
        symbol='BTC/USD',
        side=Side.buy.name,
        order_type=OrderType.market.name,
        quantity=1.1,
        price=0.0,
        stop_price=0.0,
        time_in_force=TimeInForce.gtc.name,
        expire_at=0.0,
        leverage_type=LeverageType.custom.name,
        leverage=2.0
    )
    # custom margin
    order = fake_request_sender.place_order(order=custom_margin_order)
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
    assert order.stopPrice == 0.0
    assert order.expireAt == 0.0


@pytest.mark.test_id(15)
def test_request_open_positions(fake_request_sender):
    open_pos = fake_request_sender.request_open_positions(
        account_info=AccountInfo(account_id=110)
    )
    assert type(open_pos) == capnp.lib.capnp._DynamicStructBuilder
    assert open_pos.accountInfo.accountID == 110


@pytest.mark.test_id(16)
def test_request_exchange_properties(fake_request_sender):
    # valid exchange test case
    exch_prop = fake_request_sender.request_exchange_properties(
        exchange='gemini'
    )
    assert type(exch_prop) == capnp.lib.capnp._DynamicStructBuilder
    assert exch_prop.exchange == exch_capnp.Exchange.gemini


@pytest.mark.test_id(17)
def test_request_exchange_properties_invalid_case(fake_request_sender):
    # invalid exchange test case
    exch_prop = fake_request_sender.request_exchange_properties(
        exchange='gdax'
    )
    assert type(exch_prop) == capnp.lib.capnp._DynamicStructBuilder
    assert exch_prop.exchange == exch_capnp.Exchange.undefined
