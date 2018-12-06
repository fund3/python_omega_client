import capnp
import pytest
import zmq

from tes_client.common_types import AccountBalancesReport, \
    AccountCredentials, AccountDataReport, AccountInfo, Balance, \
    CompletedOrdersReport, Exchange, ExchangePropertiesReport, LeverageType, \
    ExecutionReport, OpenPosition, OpenPositionsReport, Order, OrderInfo, \
    OrderStatus, OrderType, Side, SymbolProperties, TimeInForce, \
    WorkingOrdersReport
import communication_protocol.Exchanges_capnp as exch_capnp
import communication_protocol.TradeMessage_capnp as msgs_capnp
from tes_client.single_client_tes_connection import SingleClientTesConnection

TEST_ACCOUNT_CREDS_1 = AccountCredentials(AccountInfo(0), apiKey='api_key',
                                          secretKey='secret_key',
                                          passphrase='passphrase')
TEST_TES_CONFIG = {'TES_CONNECTION_STR': 'tcp://127.0.0.1:5555',
                   'CREDENTIALS': [TEST_ACCOUNT_CREDS_1]}

TEST_CLIENT_ID = 123
TEST_SENDER_COMP_ID = str(987)


@pytest.fixture(scope="module")
def fake_tes_conn():
    # returns an instance of Connection, but doesn't start the thread
    # TODO: Tests are passing because all zmq send calls throw exception
    # and return the expected value.  Should fix in the future.
    zmq_context = zmq.Context.instance()
    tes_conn = SingleClientTesConnection(
        tes_connection_string='tcp://127.0.0.1:5555',
        zmq_context=zmq_context,
        clientID=TEST_CLIENT_ID,
        senderCompID=TEST_SENDER_COMP_ID
    )
    yield tes_conn
    zmq_context.term()


@pytest.mark.test_id(1)
def test_place_order(fake_tes_conn):
    order = Order(
        accountInfo=AccountInfo(accountID=100),
        clientOrderID=8675309,
        clientOrderLinkID='a123',
        symbol='BTC/USD',
        side=Side.buy.name,
        orderType=OrderType.limit.name,
        quantity=1.1,
        price=6000.01,
        timeInForce=TimeInForce.gtc.name,
        leverageType=LeverageType.none.name
    )
    order = fake_tes_conn.place_order(order=order)
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 100
    assert order.symbol == 'BTC/USD'
    assert order.side == 'buy'
    assert order.orderType == 'limit'
    assert order.quantity == 1.1
    assert order.price == 6000.01
    assert order.timeInForce == 'gtc'


@pytest.mark.test_id(2)
def test_replace_order(fake_tes_conn):
    order = fake_tes_conn.replace_order(
        accountInfo=AccountInfo(accountID=100),
        orderID='c137', quantity=1.1, orderType=OrderType.limit.name,
        price=6000.01, timeInForce=TimeInForce.gtc.name
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 100
    assert order.orderID == 'c137'
    assert order.orderType == 'limit'
    assert order.quantity == 1.1
    assert order.price == 6000.01
    assert order.timeInForce == 'gtc'


@pytest.mark.test_id(3)
def test_cancel_order(fake_tes_conn):
    order = fake_tes_conn.cancel_order(
        accountInfo=AccountInfo(accountID=100), orderID='c137'
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 100
    assert order.orderID == 'c137'


@pytest.mark.test_id(4)
def test_request_account_data(fake_tes_conn):
    order = fake_tes_conn.request_account_data(
        accountInfo=AccountInfo(accountID=100))
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 100


@pytest.mark.test_id(5)
def test_request_account_balances(fake_tes_conn):
    order = fake_tes_conn.request_account_balances(
        accountInfo=AccountInfo(accountID=110))
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110


@pytest.mark.test_id(6)
def test_request_working_orders(fake_tes_conn):
    order = fake_tes_conn.request_working_orders(
        accountInfo=AccountInfo(accountID=110)
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110


@pytest.mark.test_id(7)
def test_tes_logon(fake_tes_conn):
    creds = [
        AccountCredentials(
            accountInfo=AccountInfo(accountID=100 ),
            apiKey='fakeApiKey', secretKey='fakeSecret',
            passphrase='fakePassphrase'
        ),
        AccountCredentials(
            accountInfo=AccountInfo(accountID=110),
            apiKey='fakeApiKey', secretKey='fakeSecret',
            passphrase='fakePassphrase'
        ),
        AccountCredentials(
            accountInfo=AccountInfo(accountID=200 ),
            apiKey='fakeApiKey1', secretKey='fakeSecret1',
            passphrase='fakePassphrase1'
        ),
        AccountCredentials(
            accountInfo=AccountInfo(accountID=210),
            apiKey='fakeApiKey1', secretKey='fakeSecret1',
            passphrase='fakePassphrase1'
        )
    ]
    fake_tes_conn._tes_credentials = creds
    logon = fake_tes_conn.logon(credentials=creds)
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
            accountInfo=AccountInfo(accountID=100),
            apiKey='fakeApiKey', secretKey='fakeSecret'
        )
    ]
    fake_tes_conn._tes_credentials = creds1
    logon1 = fake_tes_conn.logon(credentials=creds1)
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
                accountInfo=AccountInfo(accountID=100),
                secretKey='fakeSecret'
            )
        ]
        fake_tes_conn._tes_credentials = creds2
        logon2 = fake_tes_conn.logon(credentials=creds2)

    # logon missing apiSecret - Attribute Error
    with pytest.raises(Exception or AttributeError):
        creds3 = [
            AccountCredentials(
                accountInfo=AccountInfo(accountID=100),
                apiKey='fakeApiKey'
            )
        ]
        fake_tes_conn._tes_credentials = creds3
        logon3 = fake_tes_conn.logon(credentials=creds3)
    fake_tes_conn._tes_credentials = TEST_ACCOUNT_CREDS_1


@pytest.mark.test_id(8)
def test_tes_logoff(fake_tes_conn):
    logoff = fake_tes_conn.logoff()
    assert type(logoff) == capnp.lib.capnp._DynamicStructBuilder
    assert logoff.logoff is None


@pytest.mark.test_id(9)
def test_tes_heartbeat(fake_tes_conn):
    hb = fake_tes_conn.send_heartbeat()
    assert type(hb) == capnp.lib.capnp._DynamicStructBuilder
    assert hb.heartbeat is None


@pytest.mark.test_id(10)
def test_request_order_status(fake_tes_conn):
    order = fake_tes_conn.request_order_status(
        accountInfo=AccountInfo(accountID=110),
        orderID='poiuytrewq123'
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110
    assert order.orderID == 'poiuytrewq123'


@pytest.mark.test_id(11)
def test_request_working_orders(fake_tes_conn):
    # test including count and since
    order = fake_tes_conn.request_completed_orders(
        accountInfo=AccountInfo(accountID=110), count=2, since=1536267034.
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110
    assert order.count == 2
    assert order.since == pytest.approx(1536267034., rel=1e-2)

    # test including count, not since
    order = fake_tes_conn.request_completed_orders(
        accountInfo=AccountInfo(accountID=110), count=2
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110
    assert order.count == 2

    # test including since, not count
    order = fake_tes_conn.request_completed_orders(
        accountInfo=AccountInfo(accountID=110), since=1536267034.
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110
    assert order.since == pytest.approx(1536267034., rel=1e-2)

    # test excluding both count and since
    order = fake_tes_conn.request_completed_orders(
        accountInfo=AccountInfo(accountID=110)
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110


@pytest.mark.test_id(12)
def test_request_order_mass_status(fake_tes_conn):
    # empty order_info_list
    order_info_list = []
    order = fake_tes_conn.request_order_mass_status(
        accountInfo=AccountInfo(accountID=110),
        orderInfo=order_info_list
    )
    assert type(order) == capnp.lib.capnp._DynamicStructBuilder
    assert order.accountInfo.accountID == 110
    assert len(list(order.orderInfo)) == 0

    # filled order_info_list
    order_info_list = [OrderInfo(orderID='poiuy9876')]
    order = fake_tes_conn.request_order_mass_status(
        accountInfo=AccountInfo(accountID=110),
        orderInfo=order_info_list
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


@pytest.mark.test_id(13)
def test_place_order_margin_default(fake_tes_conn):
    default_margin_order = Order(
        accountInfo=AccountInfo(accountID=100),
        clientOrderID=9876,
        clientOrderLinkID='a123',
        symbol='BTC/USD',
        side=Side.buy.name,
        orderType=OrderType.market.name,
        quantity=1.1,
        price=0.0,
        timeInForce=TimeInForce.gtc.name,
        leverageType=LeverageType.exchangeDefault.name
    )
    # exchange default margin
    order = fake_tes_conn.place_order(order=default_margin_order)
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


@pytest.mark.test_id(14)
def test_place_order_margin_custom(fake_tes_conn):
    custom_margin_order = Order(
        accountInfo=AccountInfo(accountID=100),
        clientOrderID=9876,
        clientOrderLinkID='a123',
        symbol='BTC/USD',
        side=Side.buy.name,
        orderType=OrderType.market.name,
        quantity=1.1,
        price=0.0,
        timeInForce=TimeInForce.gtc.name,
        leverageType=LeverageType.custom.name,
        leverage=2.0
    )
    # custom margin
    order = fake_tes_conn.place_order(order=custom_margin_order)
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


@pytest.mark.test_id(15)
def test_request_open_positions(fake_tes_conn):
    open_pos = fake_tes_conn.request_open_positions(
        accountInfo=AccountInfo(accountID=110)
    )
    assert type(open_pos) == capnp.lib.capnp._DynamicStructBuilder
    assert open_pos.accountInfo.accountID == 110


@pytest.mark.test_id(16)
def test_request_exchange_properties(fake_tes_conn):
    # valid exchange test case
    exch_prop = fake_tes_conn.request_exchange_properties(
        exchange='gemini'
    )
    assert type(exch_prop) == capnp.lib.capnp._DynamicStructBuilder
    assert exch_prop.exchange == exch_capnp.Exchange.gemini


@pytest.mark.test_id(17)
def test_request_exchange_properties_invalid_case(fake_tes_conn):
    # invalid exchange test case
    exch_prop = fake_tes_conn.request_exchange_properties(
        exchange='gdax'
    )
    assert type(exch_prop) == capnp.lib.capnp._DynamicStructBuilder
    assert exch_prop.exchange == exch_capnp.Exchange.undefined