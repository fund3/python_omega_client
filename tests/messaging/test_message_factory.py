import math
from typing import List

import capnp
import pytest

from tes_client.messaging.common_types import AccountBalancesReport, \
    AccountCredentials, AccountDataReport, AccountInfo, AuthorizationGrant, \
    Balance, CompletedOrdersReport, Exchange,  \
    ExchangePropertiesReport, ExecutionReport, LeverageType, OpenPosition, \
    Message, OpenPositionsReport, Order, OrderInfo, \
    OrderStatus, OrderType, RequestHeader, Side, \
    SymbolProperties, TimeInForce, WorkingOrdersReport
import communication_protocol.TradeMessage_capnp as msgs_capnp
from tes_client.messaging.message_factory import account_balances_report_py, \
    account_data_report_py, authorization_grant_py, \
    completed_orders_report_py, \
    exchange_properties_report_py, execution_report_py, \
    generate_client_order_id, logoff_ack_py, logon_ack_py, \
    open_positions_report_py, system_message_py, tes_test_message_py, \
    working_orders_report_py, cancel_order_capnp, heartbeat_capnp, \
    logoff_capnp, logon_capnp, place_order_capnp, replace_order_capnp, \
    request_account_balances_capnp, request_account_data_capnp, \
    request_completed_orders_capnp, request_exchange_properties_capnp, \
    request_open_positions_capnp, request_order_mass_status_capnp, \
    request_order_status_capnp, request_server_time_capnp, \
    request_working_orders_capnp,  _determine_order_price, \
    _generate_tes_request

__FAKE_ACCESS_TOKEN = 'FakeAccessToken'
__FAKE_REQUEST_HEADER = RequestHeader(client_id=123,
                                      sender_comp_id='987',
                                      access_token=__FAKE_ACCESS_TOKEN,
                                      request_id=100001)


def get_new_execution_report(body, include_cl_ord_link_id=True):
    # TODO add fees
    er = body.init('executionReport')
    er.orderID = 'c137'
    er.clientOrderID = 123456789000000
    if include_cl_ord_link_id:
        er.clientOrderLinkID = 'a123'
    er.exchangeOrderID = 'c137'
    account = er.init('accountInfo')
    account.accountID = 101
    er.symbol = 'ETH-USD'
    er.side = 'buy'
    er.orderType = 'limit'
    er.quantity = 1.1
    er.price = 512.0
    er.timeInForce = 'gtc'
    er.orderStatus = 'adopted'
    er.filledQuantity = 0.0
    er.avgFillPrice = 0.0
    return er


@pytest.mark.test_id(1)
def test_handle_tes_message_system():
    tes_mess = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp = tes_mess.init('type').init('response')
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
    system_msg = system_message_py(tes_mess.type.response.body.system)
    assert type(system_msg.message.code) == int
    assert type(system_msg.message.body) == str
    assert system_msg.error_code == 0
    assert system_msg.message.body == ('The Times 03/Jan/2009 Chancellor on ' +
                                       'brink of second bailout for banks')


@pytest.mark.test_id(2)
def test_handle_tes_message_logon():
    # logon success
    tes_mess = msgs_capnp.TradeMessage.new_message()
    logon_ack_resp = tes_mess.init('type').init('response')
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
    grant.message.code = 0
    grant.message.body = "Granted"
    grant.accessToken = "AccessToken"
    grant.refreshToken = "refreshToken"
    grant.expireAt = 1551288929.0
    logon_ack = logon_ack_py(tes_mess.type.response.body.logonAck)
    expected_auth_grant = AuthorizationGrant(
        success=True,
        message=Message(body="Granted", code=0),
        access_token="AccessToken",
        refresh_token="refreshToken",
        expire_at=1551288929.0)
    assert type(logon_ack.message.body) == str
    assert type(logon_ack.message.code) == int
    assert logon_ack.message.body == ('The Times 03/Jan/2009 Chancellor on ' +
                                      'brink of second bailout for banks')
    assert type(logon_ack.success) == bool
    assert logon_ack.success
    accts = [acct for acct in logon_ack.client_accounts]
    assert type(accts[0]) == AccountInfo
    assert accts[0].account_id == 100
    assert accts[1].account_id == 101
    assert logon_ack.authorization_grant == expected_auth_grant

    # logon failure
    tes_mess1 = msgs_capnp.TradeMessage.new_message()
    logon_ack_resp1 = tes_mess1.init('type').init('response')
    logon_ack_resp1.clientID = 123
    logon_ack_resp1.senderCompID = str(987)
    logon_ack_resp1.requestID = 100001
    body = logon_ack_resp1.init('body')
    logon_ack_capnp = body.init('logonAck')
    logon_ack_capnp.success = False
    logon_ack_capnp.message.code = 1
    logon_ack_capnp.message.body = 'Jamie Dimon has denied you access'
    client_accounts = logon_ack_capnp.init('clientAccounts', 2)
    client_accounts[0].accountID = 100
    client_accounts[1].accountID = 101
    grant = logon_ack_capnp.init('authorizationGrant')
    grant.success = False
    grant.message = "Authorization failed"
    expected_auth_grant = AuthorizationGrant(
        success=False,
        message=Message(body="Authorization failed", code=1),
        access_token="",
        refresh_token="",
        expire_at=0.0)
    logon_ack = logon_ack_py(tes_mess1.type.response.body.logonAck)
    assert type(logon_ack.message.body) == str
    assert logon_ack.message.body == 'Jamie Dimon has denied you access'
    assert type(logon_ack.success) == bool
    assert not logon_ack.success
    assert logon_ack.authorization_grant == expected_auth_grant

# Test cases where there are no passphrase or other params,
# coupled with the logic change requested in AccountCredentials above.
# i.e. one test with missing passphrase (optional param),
# one test with missing apikey (required param).


@pytest.mark.test_id(3)
def test_handle_tes_message_logoff():
    tes_mess = msgs_capnp.TradeMessage.new_message()
    logoff_resp = tes_mess.init('type').init('response')
    logoff_resp.clientID = 123
    logoff_resp.senderCompID = str(987)
    logoff_resp.requestID = 100001
    body = logoff_resp.init('body')

    # logoff success
    logoff = body.init('logoffAck')
    logoff.success = True
    logoff.message.code = 0
    logoff.message.body = ('The Times 03/Jan/2009 Chancellor on brink of ' +
                           'second bailout for banks')
    logoff_ack = logoff_ack_py(tes_mess.type.response.body.logoffAck)
    assert type(logoff_ack.message.body) == str
    assert logoff_ack.message == ('The Times 03/Jan/2009 Chancellor on ' +
                                  'brink of second bailout for banks')
    assert type(logoff_ack.success) == bool
    assert logoff_ack.success

    tes_mess1 = msgs_capnp.TradeMessage.new_message()
    logoff_resp = tes_mess1.init('type').init('response')
    logoff_resp.clientID = 123
    logoff_resp.senderCompID = str(987)
    logoff_resp.requestID = 100002
    body1 = logoff_resp.init('body')

    # logoff failure
    logoff1 = body1.init('logoffAck')
    logoff1.success = False
    logoff1.message.code = 1
    logoff1.message.body = 'Jamie Dimon has denied you access'
    logoff_ack = logoff_ack_py(tes_mess1.type.response.body.logoffAck)
    assert type(logoff_ack.message.body) == str
    assert logoff_ack.message == 'Jamie Dimon has denied you access'
    assert type(logoff_ack.success) == bool
    assert not logoff_ack.success


@pytest.mark.test_id(4)
def test_handle_tes_message_account_data_report():
    tes_mess = msgs_capnp.TradeMessage.new_message()
    account_data_resp = tes_mess.init('type').init('response')
    account_data_resp.clientID = 123
    account_data_resp.senderCompID = str(987)
    account_data_resp.requestID = 100001
    body = account_data_resp.init('body')
    adr = body.init('accountDataReport')
    account = adr.init('accountInfo')
    account.accountID = 101

    balances = adr.init('balances', 2)
    balances[0].currency = 'BTC'
    balances[0].fullBalance = 1.1
    balances[0].availableBalance = 1.0
    balances[1].currency = 'USD'
    balances[1].fullBalance = 2100.10
    balances[1].availableBalance = 2100.10

    orders = adr.init('orders', 1)
    orders[0].orderID = 'c137'
    orders[0].clientOrderID = 1234
    orders[0].exchangeOrderID = 'asdf1234'
    account0 = orders[0].init('accountInfo')
    account0.accountID = 101
    orders[0].symbol = 'BTC/USD'
    orders[0].side = Side.buy.name
    orders[0].orderType = OrderType.limit.name
    orders[0].quantity = 0.1
    orders[0].price = 10000.0
    orders[0].timeInForce = TimeInForce.gtc.name
    orders[0].orderStatus = OrderStatus.partiallyFilled.name
    orders[0].filledQuantity = 0.20
    orders[0].avgFillPrice = 10000.0
    orders[0].type.statusUpdate = None

    ops = adr.init('openPositions', 1)
    ops[0].symbol = 'ETH/USD'
    ops[0].side = Side.buy.name
    ops[0].quantity = 10.2
    ops[0].initialPrice = 450.3
    ops[0].unrealizedPL = 503.1

    acct_data_report = account_data_report_py(
        account_data_report=tes_mess.type.response.body.accountDataReport)
    assert type(acct_data_report == AccountDataReport)
    assert type(acct_data_report.account_info == AccountInfo)
    assert type(acct_data_report.orders == List[ExecutionReport])
    assert type(acct_data_report.open_positions == List[OpenPosition])
    assert type(acct_data_report.balances == List[Balance])


@pytest.mark.test_id(5)
def test_handle_tes_message_working_orders_report():
    tes_mess = msgs_capnp.TradeMessage.new_message()
    working_orders_resp = tes_mess.init('type').init('response')
    working_orders_resp.clientID = 123
    working_orders_resp.senderCompID = str(987)
    working_orders_resp.requestID = 100001
    body = working_orders_resp.init('body')
    adr = body.init('workingOrdersReport')
    account = adr.init('accountInfo')
    account.accountID = 101

    # TODO
    orders = adr.init('orders', 2)
    orders[0].orderID = 'c137'
    orders[0].clientOrderID = 1234
    orders[0].clientOrderLinkID = 'a123'
    orders[0].exchangeOrderID = 'asdf1234'
    account0 = orders[0].init('accountInfo')
    account0.accountID = 101
    orders[0].symbol = 'BTC/USD'
    orders[0].side = 'buy'
    orders[0].orderType = 'limit'
    orders[0].quantity = 0.1
    orders[0].price = 10000.0
    orders[0].timeInForce = 'gtc'
    orders[0].orderStatus = 'partiallyFilled'
    orders[0].filledQuantity = 0.20
    orders[0].avgFillPrice = 10000.0
    orders[0].type.statusUpdate = None

    # TODO
    orders[1].orderID = 'c138'
    orders[1].clientOrderID = 1235
    orders[1].clientOrderLinkID = 'b123'
    orders[1].exchangeOrderID = 'asdf1235'
    account1 = orders[1].init('accountInfo')
    account1.accountID = 101
    orders[1].symbol = 'BTC/USD'
    orders[1].side = 'buy'
    orders[1].orderType = 'limit'
    orders[1].quantity = 1000.0
    orders[1].price = 10.0
    orders[1].timeInForce = 'gtc'
    orders[1].orderStatus = 'rejected'
    orders[1].filledQuantity = 0.0
    orders[1].avgFillPrice = 0.0
    order_rejected = orders[1].type.init('cancelRejected')
    order_rejected.message = 'way too silly'
    order_rejected.rejectionCode = 1

    wos_reports = working_orders_report_py(
        tes_mess.type.response.body.workingOrdersReport)
    assert type(wos_reports) == WorkingOrdersReport
    assert type(wos_reports.account_info) == AccountInfo

    exec_reports = wos_reports.orders
    assert type(exec_reports) == list
    assert type(exec_reports[0].execution_report_type) == ExecutionReportType
    assert exec_reports[0].execution_report_type.name == 'statusUpdate'
    assert type(exec_reports[1].execution_report_type) == ExecutionReportType
    assert exec_reports[1].execution_report_type.name == 'cancelRejected'
    request_rejected = exec_reports[1].execution_report_type.request_rejected
    assert type(request_rejected) == RequestRejected
    assert request_rejected.message == 'way too silly'
    assert request_rejected.rejection_code == 1


@pytest.mark.test_id(6)
def test_handle_tes_message_account_balances_report():
    tes_mess = msgs_capnp.TradeMessage.new_message()
    account_balances_resp = tes_mess.init('type').init('response')
    account_balances_resp.clientID = 123
    account_balances_resp.senderCompID = str(987)
    account_balances_resp.requestID = 100001
    body = account_balances_resp.init('body')
    adr = body.init('accountDataReport')
    account = adr.init('accountInfo')
    account.accountID = 101
    balances = adr.init('balances', 2)
    balances[0].currency = 'ETH'
    balances[0].fullBalance = 100.1
    balances[0].availableBalance = 97.3
    balances[1].currency = 'USD'
    balances[1].fullBalance = 1005002.02
    balances[1].availableBalance = 915002.02

    acct_bals = account_balances_report_py(
        tes_mess.type.response.body.accountDataReport)
    assert type(acct_bals == AccountBalancesReport)
    assert type(acct_bals.account_info == AccountInfo)
    assert type(acct_bals.balances == List[Balance])

    assert acct_bals.balances[0].currency == 'ETH'
    assert acct_bals.balances[1].currency == 'USD'

    assert acct_bals.balances[0].full_balance == 100.1
    assert acct_bals.balances[0].available_balance == 97.3
    assert acct_bals.balances[1].full_balance == 1005002.02
    assert acct_bals.balances[1].available_balance == 915002.02


@pytest.mark.test_id(7)
def test_handle_tes_message_completed_orders_report():
    tes_mess = msgs_capnp.TradeMessage.new_message()
    completed_orders_resp = tes_mess.init('type').init('response')
    completed_orders_resp.clientID = 123
    completed_orders_resp.senderCompID = str(987)
    completed_orders_resp.requestID = 100001
    body = completed_orders_resp.init('body')
    cor = body.init('completedOrdersReport')
    account = cor.init('accountInfo')
    account.accountID = 101

    orders = cor.init('orders', 2)
    orders[0].orderID = 'c137'
    orders[0].clientOrderID = 1234
    orders[0].exchangeOrderID = 'asdf1234'
    account0 = orders[0].init('accountInfo')
    account0.accountID = 101
    orders[0].symbol = 'BTC/USD'
    orders[0].side = 'buy'
    orders[0].orderType = 'limit'
    orders[0].quantity = 0.1
    orders[0].price = 10000.0
    orders[0].timeInForce = 'gtc'
    orders[0].orderStatus = 'filled'
    orders[0].filledQuantity = 0.20
    orders[0].avgFillPrice = 10000.0
    orders[0].type.statusUpdate = None

    orders[1].orderID = 'c138'
    orders[1].clientOrderID = 1235
    orders[1].exchangeOrderID = 'asdf1235'
    account1 = orders[1].init('accountInfo')
    account1.accountID = 101
    orders[1].symbol = 'BTC/USD'
    orders[1].side = 'buy'
    orders[1].orderType = 'limit'
    orders[1].quantity = 1000.0
    orders[1].price = 10.0
    orders[1].timeInForce = 'gtc'
    orders[1].orderStatus = 'replaced'
    orders[1].filledQuantity = 0.0
    orders[1].avgFillPrice = 0.0
    orders[1].type.statusUpdate = None

    cos_reports = completed_orders_report_py(
        tes_mess.type.response.body.completedOrdersReport)
    assert type(cos_reports) == CompletedOrdersReport
    assert type(cos_reports.account_info) == AccountInfo

    exec_reports = cos_reports.orders
    assert type(exec_reports) == list
    assert type(exec_reports[0].execution_report_type) == ExecutionReportType
    assert exec_reports[0].execution_report_type.name == 'statusUpdate'
    assert type(exec_reports[1].execution_report_type) == ExecutionReportType
    assert exec_reports[1].execution_report_type.name == 'statusUpdate'


@pytest.mark.test_id(8)
def test_handle_tes_message_open_positions_report():
    tes_mess = msgs_capnp.TradeMessage.new_message()
    open_positions_resp = tes_mess.init('type').init('response')
    open_positions_resp.clientID = 123
    open_positions_resp.senderCompID = str(987)
    open_positions_resp.requestID = 100001
    body = open_positions_resp.init('body')
    adr = body.init('openPositionsReport')
    account0 = adr.init('accountInfo')
    account0.accountID = 110
    ops = adr.init('openPositions', 2)
    ops[0].symbol = 'ETH/USD'
    ops[0].side = Side.buy.name
    ops[0].quantity = 10.2
    ops[0].initialPrice = 450.3
    ops[0].unrealizedPL = 503.1
    ops[1].symbol = 'ETH/BTC'
    ops[1].side = Side.sell.name
    ops[1].quantity = 4.9
    ops[1].initialPrice = 0.07
    ops[1].unrealizedPL = -0.003

    open_pos_report = open_positions_report_py(
        tes_mess.type.response.body.openPositionsReport)
    assert type(open_pos_report == OpenPosition)
    assert type(open_pos_report.account_info) == AccountInfo
    assert open_pos_report.account_info.account_id == 110

    open_pos = open_pos_report.open_positions
    # list of list of tuples
    assert open_pos[0].side == 'buy'
    assert open_pos[0].symbol == 'ETH/USD'
    assert open_pos[0].quantity == 10.2
    assert open_pos[0].initial_price == 450.3
    assert open_pos[0].unrealized_pl == 503.1
    assert open_pos[1].side == 'sell'
    assert open_pos[1].symbol == 'ETH/BTC'
    assert open_pos[1].quantity == 4.9
    assert open_pos[1].initial_price == 0.07
    assert open_pos[1].unrealized_pl == -0.003


@pytest.mark.test_id(9)
def test_handle_tes_message_exchange_properties_report():
    # valid test case
    tes_mess = msgs_capnp.TradeMessage.new_message()
    exchange_properties_resp = tes_mess.init('type').init('response')
    exchange_properties_resp.clientID = 123
    exchange_properties_resp.senderCompID = str(987)
    exchange_properties_resp.requestID = 100001
    body = exchange_properties_resp.init('body')
    epr = body.init('exchangePropertiesReport')
    currencies = epr.init('currencies', 3)
    currencies[0] = 'USD'
    currencies[1] = 'BTC'
    currencies[2] = 'ETH'
    sps = epr.init('symbolProperties', 2)
    sps[0].symbol = 'BTC/USD'
    sps[0].pricePrecision = 0.01
    sps[0].quantityPrecision = 0.001
    sps[0].minQuantity = 0.001
    sps[0].maxQuantity = 500.0
    sps[0].marginSupported = False
    sps[0].leverage = [0.0]
    sps[1].symbol = 'ETH/USD'
    sps[1].pricePrecision = 0.01
    sps[1].quantityPrecision = 0.01
    sps[1].minQuantity = 0.01
    sps[1].maxQuantity = 5000.0
    sps[1].marginSupported = True
    sps[1].leverage = [1.0, 1.5, 2.0]
    tifs = epr.init('timeInForces', 2)
    tifs[0] = TimeInForce.undefined.name
    tifs[1] = TimeInForce.gtc.name
    ots = epr.init('orderTypes', 3)
    ots[0] = OrderType.undefined.name
    ots[1] = OrderType.limit.name
    ots[2] = OrderType.market.name

    exch_props_rpt = exchange_properties_report_py(
        tes_mess.type.response.body.exchangePropertiesReport)
    assert type(exch_props_rpt == ExchangePropertiesReport)


@pytest.mark.test_id(10)
def test_on_account_balances():
    tes_mess = msgs_capnp.TradeMessage.new_message()
    account_balances_resp = tes_mess.init('type').init('response')
    account_balances_resp.clientID = 123
    account_balances_resp.senderCompID = str(987)
    account_balances_resp.requestID = 100001
    body = account_balances_resp.init('body')
    adr = body.init('accountBalancesReport')
    account = adr.init('accountInfo')
    account.accountID = 101

    balances = adr.init('balances', 2)
    balances[0].currency = 'BTC'
    balances[0].fullBalance = 1.1
    balances[0].availableBalance = 1.0
    balances[1].currency = 'USD'
    balances[1].fullBalance = 2100.10
    balances[1].availableBalance = 2100.10

    acct_bal_report = account_balances_report_py(
        tes_mess.type.response.body.accountBalancesReport)
    assert type(acct_bal_report == AccountBalancesReport)
    assert type(acct_bal_report.account_info == AccountInfo)
    assert type(acct_bal_report.balances == List[Balance])


@pytest.mark.test_id(11)
def test_handle_tes_message_execution_report():
    # TODO
    # order accepted
    tes_mess = msgs_capnp.TradeMessage.new_message()
    exec_report_resp = tes_mess.init('type').init('response')
    exec_report_resp.clientID = 123
    exec_report_resp.senderCompID = str(987)
    body = exec_report_resp.init('body')
    er = get_new_execution_report(body=body)
    er.type.orderAccepted = None
    er_type = execution_report_py(
        tes_mess.type.response.body.executionReport)
    assert type(er_type == ExecutionReport)
    assert er_type.execution_report_type.name == 'orderAccepted'

    # order rejected
    tes_mess1 = msgs_capnp.TradeMessage.new_message()
    exec_report_resp1 = tes_mess1.init('type').init('response')
    exec_report_resp1.clientID = 123
    exec_report_resp1.senderCompID = str(987)
    body1 = exec_report_resp1.init('body')
    er1 = get_new_execution_report(body=body1, include_cl_ord_link_id=False)
    order_rejected = er1.type.init('orderRejected')
    order_rejected.message = 'too silly'
    order_rejected.rejectionCode = 123
    er_type1 = execution_report_py(tes_mess1.type.response.body.executionReport)
    assert type(er_type1 == ExecutionReport)
    assert er_type1.execution_report_type.name == 'orderRejected'

    # order replaced
    tes_mess2 = msgs_capnp.TradeMessage.new_message()
    exec_report_resp2 = tes_mess2.init('type').init('response')
    exec_report_resp2.clientID = 123
    exec_report_resp2.senderCompID = str(987)
    body2 = exec_report_resp2.init('body')
    er2 = get_new_execution_report(body=body2)
    er2.type.orderReplaced = None
    er_type2 = execution_report_py(tes_mess2.type.response.body.executionReport)
    assert type(er_type2 == ExecutionReport)
    assert er_type2.execution_report_type.name == 'orderReplaced'

    # replace rejected
    tes_mess3 = msgs_capnp.TradeMessage.new_message()
    exec_report_resp3 = tes_mess3.init('type').init('response')
    exec_report_resp3.clientID = 123
    exec_report_resp3.senderCompID = str(987)
    body3 = exec_report_resp3.init('body')
    er3 = get_new_execution_report(body=body3)
    order_rejected = er3.type.init('replaceRejected')
    order_rejected.message = 'way too silly'
    order_rejected.rejectionCode = 321
    er_type3 = execution_report_py(tes_mess3.type.response.body.executionReport)
    assert type(er_type3 == ExecutionReport)
    assert er_type3.execution_report_type.name == 'replaceRejected'

    # order cancelled
    tes_mess4 = msgs_capnp.TradeMessage.new_message()
    exec_report_resp4 = tes_mess4.init('type').init('response')
    exec_report_resp4.clientID = 123
    exec_report_resp4.senderCompID = str(987)
    body4 = exec_report_resp4.init('body')
    er4 = get_new_execution_report(body=body4)
    er4.type.orderCanceled = None
    er_type4 = execution_report_py(tes_mess4.type.response.body.executionReport)
    assert type(er_type4 == ExecutionReport)
    assert er_type4.execution_report_type.name == 'orderCanceled'

    # cancel rejected
    tes_mess5 = msgs_capnp.TradeMessage.new_message()
    exec_report_resp5 = tes_mess5.init('type').init('response')
    exec_report_resp5.clientID = 123
    exec_report_resp5.senderCompID = str(987)
    body5 = exec_report_resp5.init('body')
    er5 = get_new_execution_report(body=body5)
    order_rejected = er5.type.init('cancelRejected')
    order_rejected.message = 'way too silly'
    order_rejected.rejectionCode = 9987
    er_type5 = execution_report_py(tes_mess5.type.response.body.executionReport)
    assert type(er_type5 == ExecutionReport)
    assert er_type5.execution_report_type.name == 'cancelRejected'

    # order filled
    tes_mess6 = msgs_capnp.TradeMessage.new_message()
    exec_report_resp6 = tes_mess6.init('type').init('response')
    exec_report_resp6.clientID = 123
    exec_report_resp6.senderCompID = str(987)
    body6 = exec_report_resp6.init('body')
    er6 = get_new_execution_report(body=body6)
    er6.type.orderFilled = None
    er_type6 = execution_report_py(tes_mess6.type.response.body.executionReport)
    assert type(er_type6 == ExecutionReport)
    assert er_type6.execution_report_type.name == 'orderFilled'

    # status update
    tes_mess7 = msgs_capnp.TradeMessage.new_message()
    exec_report_resp7 = tes_mess7.init('type').init('response')
    exec_report_resp7.clientID = 123
    exec_report_resp7.senderCompID = str(987)
    body7 = exec_report_resp7.init('body')
    er7 = get_new_execution_report(body=body7)
    er7.type.statusUpdate = None
    er_type7 = execution_report_py(tes_mess7.type.response.body.executionReport)
    assert type(er_type7 == ExecutionReport)
    assert er_type7.execution_report_type.name == 'statusUpdate'


@pytest.mark.test_id(12)
def test_determine_order_price():
    # market order
    op = _determine_order_price(order_price=400., order_type='market')
    assert math.isclose(op, 0.0, rel_tol=1e-6)
    # limit order price > MIN_ORDER_PRICE
    op1 = _determine_order_price(order_price=400., order_type='limit')
    assert math.isclose(op1, 400., rel_tol=1e-6)


@pytest.mark.test_id(13)
def test_generate_tes_request():
    body, tes_mess = _generate_tes_request(
        RequestHeader(client_id=0,
                      sender_comp_id='asdf',
                      access_token=__FAKE_ACCESS_TOKEN,
                      request_id=100002)
        )
    # print(body, '\n', tes_mess)
    assert type(body) == capnp.lib.capnp._DynamicStructBuilder
    assert type(tes_mess) == capnp.lib.capnp._DynamicStructBuilder


@pytest.mark.test_id(14)
def test_generate_client_order_id():
    import time
    cl_oid = generate_client_order_id()
    assert type(cl_oid) == int
    # assuming time between generation and testing is < 60 seconds
    assert cl_oid <= int(time.time()*1000000)
    assert cl_oid > int(time.time()*1000000 - 60000000)


@pytest.mark.test_id(15)
def test_handle_tes_message_test():
    tes_mess = msgs_capnp.TradeMessage.new_message()
    test_response = tes_mess.init('type').init('response')
    test_response.clientID = 123
    test_response.senderCompID = str(987)
    test_response.requestID = 100001
    body = test_response.init('body')
    test = body.init('test')
    test.string = 'test_string'
    test_string = tes_test_message_py(
        tes_mess.type.response.body.test)

    assert test_string == 'test_string'


@pytest.mark.test_id(16)
def test_authorization_grant_py():
    auth_grant_capnp = msgs_capnp.AuthorizationGrant.new_message()
    auth_grant_capnp.success = True
    auth_grant_capnp.message.body = 'auth successful'
    auth_grant_capnp.message.code = 0
    auth_grant_capnp.accessToken = 'access_token'
    auth_grant_capnp.refreshToken = 'refresh_token'
    auth_grant_capnp.expireAt = 1549618563.0

    expected_auth_grant = AuthorizationGrant(True,
                                             Message(body='auth successful',
                                                     code=0),
                                             'access_token',
                                             'refresh_token',
                                             1549618563.0)
    assert authorization_grant_py(auth_grant_capnp) == expected_auth_grant


@pytest.mark.test_id(17)
def test_heartbeat_capnp():
    expected_tes_message = msgs_capnp.TradeMessage.new_message()
    heartbeat_req = expected_tes_message.init('type').init('request')
    heartbeat_req.clientID = 123
    heartbeat_req.senderCompID = str(987)
    heartbeat_req.accessToken = __FAKE_ACCESS_TOKEN
    heartbeat_req.requestID = 100001
    body = heartbeat_req.init('body')
    body.heartbeat = None

    actual_tes_message = heartbeat_capnp(__FAKE_REQUEST_HEADER)[0]

    assert actual_tes_message.type.request.clientID == (
            expected_tes_message.type.request.clientID)
    assert actual_tes_message.type.request.senderCompID == (
        expected_tes_message.type.request.senderCompID)
    assert actual_tes_message.type.request.requestID == (
        expected_tes_message.type.request.requestID)
    assert actual_tes_message.type.request.body.heartbeat == (
        expected_tes_message.type.request.body.heartbeat)


@pytest.mark.test_id(18)
def test_request_server_time_capnp():
    expected_tes_message = msgs_capnp.TradeMessage.new_message()
    request_server_time = expected_tes_message.init('type').init('request')
    request_server_time.clientID = 123
    request_server_time.senderCompID = str(987)
    request_server_time.requestID = 100001
    request_server_time.accessToken = __FAKE_ACCESS_TOKEN
    body = request_server_time.init('body')
    body.getServerTime = None

    actual_tes_message = request_server_time_capnp(__FAKE_REQUEST_HEADER)[0]

    assert actual_tes_message.type.request.clientID == (
        expected_tes_message.type.request.clientID)
    assert actual_tes_message.type.request.senderCompID == (
        expected_tes_message.type.request.senderCompID)
    assert actual_tes_message.type.request.requestID == (
        expected_tes_message.type.request.requestID)
    assert actual_tes_message.type.request.body.getServerTime == (
        expected_tes_message.type.request.body.getServerTime)
