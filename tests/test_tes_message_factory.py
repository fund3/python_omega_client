from typing import List

import capnp
import pytest

from tes_client.common_types import AccountBalancesReport, \
    AccountCredentials, AccountDataReport, AccountInfo, Balance, \
    CompletedOrdersReport, Exchange, ExchangePropertiesReport, LeverageType, \
    ExecutionReport, OpenPosition, OpenPositionsReport, Order, OrderInfo, \
    OrderStatus, OrderType, Side, SymbolProperties, TimeInForce, \
    WorkingOrdersReport
import communication_protocol.Exchanges_capnp as exch_capnp
import communication_protocol.TradeMessage_capnp as msgs_capnp
from tes_client.tes_message_factory import build_account_balances_report, \
    build_account_data_report, build_completed_orders_report, \
    build_exchange_properties_report, build_exec_report, build_logoff, \
    build_logon, build_open_positions_report, build_system_message, \
    build_test_message, build_working_orders_report, cancel_order_message, \
    heartbeat_message, logoff_message, logon_message, place_order_message, \
    replace_order_message, request_account_balances_message, \
    request_account_data_message, request_completed_orders, \
    request_exchange_properties_message, request_open_positions_message, \
    request_order_mass_status_message, request_order_status_message, \
    request_working_orders_message


def get_new_execution_report(body, include_cl_ord_link_id=True):
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
    body = heartbeat_resp.init('body')
    system = body.init('system')
    account = system.init('accountInfo')
    account.accountID = 100
    system.errorCode = 0
    system.message = ('The Times 03/Jan/2009 Chancellor on brink of second ' +
                      'bailout for banks')
    error_code, system_msg = build_system_message(
        tes_mess.type.response.body.system)
    assert type(error_code) == int
    assert type(system_msg == str)
    assert error_code == 0
    assert system_msg == ('The Times 03/Jan/2009 Chancellor on brink of ' +
                          'second bailout for banks')


@pytest.mark.test_id(2)
def test_handle_tes_message_logon():
    # logon success
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
    success, msg, accounts = build_logon(tes_mess.type.response.body.logonAck)
    assert type(msg == str)
    assert msg == ('The Times 03/Jan/2009 Chancellor on brink of second ' +
                   'bailout for banks')
    assert type(success) == bool
    assert success
    accts = [acct for acct in accounts]
    assert type(accts[0]) == int
    assert accts == [100, 101]

    # logon failure
    tes_mess1 = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp1 = tes_mess1.init('type').init('response')
    heartbeat_resp1.clientID = 123
    heartbeat_resp1.senderCompID = str(987)
    body1 = heartbeat_resp1.init('body')
    logon1 = body1.init('logonAck')
    logon1.success = False
    logon1.message = 'Jamie Dimon has denied you access'
    logon1.clientAccounts = [100, 101]
    success1, msg1, accounts1 = build_logon(
        tes_mess1.type.response.body.logonAck)
    assert type(msg1 == str)
    assert msg1 == 'Jamie Dimon has denied you access'
    assert type(success1) == bool
    assert not success1

# Test cases where there are no passphrase or other params,
# coupled with the logic change requested in AccountCredentials above.
# i.e. one test with missing passphrase (optional param),
# one test with missing apikey (required param).


@pytest.mark.test_id(3)
def test_handle_tes_message_logoff():
    tes_mess = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp = tes_mess.init('type').init('response')
    heartbeat_resp.clientID = 123
    heartbeat_resp.senderCompID = str(987)
    body = heartbeat_resp.init('body')

    # logoff success
    logoff = body.init('logoffAck')
    logoff.success = True
    logoff.message = ('The Times 03/Jan/2009 Chancellor on brink of second ' +
                      'bailout for banks')
    success, msg = build_logoff(tes_mess.type.response.body.logoffAck)
    assert type(msg == str)
    assert msg == ('The Times 03/Jan/2009 Chancellor on brink of second ' +
                   'bailout for banks')
    assert type(success) == bool
    assert success

    tes_mess1 = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp = tes_mess1.init('type').init('response')
    heartbeat_resp.clientID = 123
    heartbeat_resp.senderCompID = str(987)
    body1 = heartbeat_resp.init('body')

    # logoff failure
    logoff1 = body1.init('logoffAck')
    logoff1.success = False
    logoff1.message = 'Jamie Dimon has denied you access'
    success1, msg1 = build_logoff(tes_mess1.type.response.body.logoffAck)
    assert type(msg1 == str)
    assert msg1 == 'Jamie Dimon has denied you access'
    assert type(success1) == bool
    assert not success1


@pytest.mark.test_id(4)
def test_handle_tes_message_account_data_report():
    tes_mess = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp = tes_mess.init('type').init('response')
    heartbeat_resp.clientID = 123
    heartbeat_resp.senderCompID = str(987)
    body = heartbeat_resp.init('body')
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

    acct_data_report = build_account_data_report(
        accountDataReport=tes_mess.type.response.body.accountDataReport)
    assert type(acct_data_report == AccountDataReport)
    assert type(acct_data_report.accountInfo == AccountInfo)
    assert type(acct_data_report.orders == List[ExecutionReport])
    assert type(acct_data_report.openPositions == List[OpenPosition])
    assert type(acct_data_report.balances == List[Balance])


@pytest.mark.test_id(5)
def test_handle_tes_message_working_orders_report():
    tes_mess = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp = tes_mess.init('type').init('response')
    heartbeat_resp.clientID = 123
    heartbeat_resp.senderCompID = str(987)
    body = heartbeat_resp.init('body')
    adr = body.init('workingOrdersReport')
    account = adr.init('accountInfo')
    account.accountID = 101

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

    wos_reports = build_working_orders_report(
        tes_mess.type.response.body.workingOrdersReport)
    assert type(wos_reports) == WorkingOrdersReport
    assert type(wos_reports.accountInfo) == AccountInfo

    exec_reports = wos_reports.orders
    assert type(exec_reports) == list
    assert type(exec_reports[0].executionReportType) == str
    assert exec_reports[0].executionReportType == 'statusUpdate'
    assert type(exec_reports[1].executionReportType) == str
    assert exec_reports[1].executionReportType == 'cancelRejected'


@pytest.mark.test_id(6)
def test_handle_tes_message_account_balances_report():
    tes_mess = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp = tes_mess.init('type').init('response')
    heartbeat_resp.clientID = 123
    heartbeat_resp.senderCompID = str(987)
    body = heartbeat_resp.init('body')
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

    acct_bals = build_account_balances_report(
        tes_mess.type.response.body.accountDataReport)
    assert type(acct_bals == AccountBalancesReport)
    assert type(acct_bals.accountInfo == AccountInfo)
    assert type(acct_bals.balances == List[Balance])

    assert acct_bals.balances[0].currency == 'ETH'
    assert acct_bals.balances[1].currency == 'USD'

    assert acct_bals.balances[0].fullBalance == 100.1
    assert acct_bals.balances[0].availableBalance == 97.3
    assert acct_bals.balances[1].fullBalance == 1005002.02
    assert acct_bals.balances[1].availableBalance == 915002.02


@pytest.mark.test_id(7)
def test_handle_tes_message_completed_orders_report():
    tes_mess = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp = tes_mess.init('type').init('response')
    heartbeat_resp.clientID = 123
    heartbeat_resp.senderCompID = str(987)
    body = heartbeat_resp.init('body')
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

    cos_reports = build_completed_orders_report(
        tes_mess.type.response.body.completedOrdersReport)
    assert type(cos_reports) == CompletedOrdersReport
    assert type(cos_reports.accountInfo) == AccountInfo

    exec_reports = cos_reports.orders
    assert type(exec_reports) == list
    assert type(exec_reports[0].executionReportType) == str
    assert exec_reports[0].executionReportType == 'statusUpdate'
    assert type(exec_reports[1].executionReportType) == str
    assert exec_reports[1].executionReportType == 'statusUpdate'


@pytest.mark.test_id(8)
def test_handle_tes_message_open_positions_report():
    tes_mess = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp = tes_mess.init('type').init('response')
    heartbeat_resp.clientID = 123
    heartbeat_resp.senderCompID = str(987)
    body = heartbeat_resp.init('body')
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

    open_pos_report = build_open_positions_report(
        tes_mess.type.response.body.openPositionsReport)
    assert type(open_pos_report == OpenPosition)
    assert type(open_pos_report.accountInfo) == AccountInfo
    assert open_pos_report.accountInfo.accountID == 110

    open_pos = open_pos_report.openPositions
    # list of list of tuples
    assert open_pos[0].side == 'buy'
    assert open_pos[0].symbol == 'ETH/USD'
    assert open_pos[0].quantity == 10.2
    assert open_pos[0].initialPrice == 450.3
    assert open_pos[0].unrealizedPL == 503.1
    assert open_pos[1].side == 'sell'
    assert open_pos[1].symbol == 'ETH/BTC'
    assert open_pos[1].quantity == 4.9
    assert open_pos[1].initialPrice == 0.07
    assert open_pos[1].unrealizedPL == -0.003


@pytest.mark.test_id(9)
def test_handle_tes_message_exchange_properties_report():
    # valid test case
    tes_mess = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp = tes_mess.init('type').init('response')
    heartbeat_resp.clientID = 123
    heartbeat_resp.senderCompID = str(987)
    body = heartbeat_resp.init('body')
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

    exch_props_rpt = build_exchange_properties_report(
        tes_mess.type.response.body.exchangePropertiesReport)
    assert type(exch_props_rpt == ExchangePropertiesReport)


@pytest.mark.test_id(10)
def test_on_account_balances():
    tes_mess = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp = tes_mess.init('type').init('response')
    heartbeat_resp.clientID = 123
    heartbeat_resp.senderCompID = str(987)
    body = heartbeat_resp.init('body')
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

    acct_bal_report = build_account_balances_report(
        tes_mess.type.response.body.accountBalancesReport)
    assert type(acct_bal_report == AccountBalancesReport)
    assert type(acct_bal_report.accountInfo == AccountInfo)
    assert type(acct_bal_report.balances == List[Balance])


@pytest.mark.test_id(11)
def test_handle_tes_message_execution_report():
    # order accepted
    tes_mess = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp = tes_mess.init('type').init('response')
    heartbeat_resp.clientID = 123
    heartbeat_resp.senderCompID = str(987)
    body = heartbeat_resp.init('body')
    er = get_new_execution_report(body=body)
    er.type.orderAccepted = None
    er_type = build_exec_report(
        tes_mess.type.response.body.executionReport)
    assert type(er_type == ExecutionReport)
    assert er_type.executionReportType == 'orderAccepted'

    # order rejected
    tes_mess1 = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp1 = tes_mess1.init('type').init('response')
    heartbeat_resp1.clientID = 123
    heartbeat_resp1.senderCompID = str(987)
    body1 = heartbeat_resp1.init('body')
    er1 = get_new_execution_report(body=body1, include_cl_ord_link_id=False)
    order_rejected = er1.type.init('orderRejected')
    order_rejected.message = 'too silly'
    order_rejected.rejectionCode = 123
    er_type1 = build_exec_report(tes_mess1.type.response.body.executionReport)
    assert type(er_type1 == ExecutionReport)
    assert er_type1.executionReportType == 'orderRejected'

    # order replaced
    tes_mess2 = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp2 = tes_mess2.init('type').init('response')
    heartbeat_resp2.clientID = 123
    heartbeat_resp2.senderCompID = str(987)
    body2 = heartbeat_resp2.init('body')
    er2 = get_new_execution_report(body=body2)
    er2.type.orderReplaced = None
    er_type2 = build_exec_report(tes_mess2.type.response.body.executionReport)
    assert type(er_type2 == ExecutionReport)
    assert er_type2.executionReportType == 'orderReplaced'

    # replace rejected
    tes_mess3 = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp3 = tes_mess3.init('type').init('response')
    heartbeat_resp3.clientID = 123
    heartbeat_resp3.senderCompID = str(987)
    body3 = heartbeat_resp3.init('body')
    er3 = get_new_execution_report(body=body3)
    order_rejected = er3.type.init('replaceRejected')
    order_rejected.message = 'way too silly'
    order_rejected.rejectionCode = 321
    er_type3 = build_exec_report(tes_mess3.type.response.body.executionReport)
    assert type(er_type3 == ExecutionReport)
    assert er_type3.executionReportType == 'replaceRejected'

    # order cancelled
    tes_mess4 = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp4 = tes_mess4.init('type').init('response')
    heartbeat_resp4.clientID = 123
    heartbeat_resp4.senderCompID = str(987)
    body4 = heartbeat_resp4.init('body')
    er4 = get_new_execution_report(body=body4)
    er4.type.orderCanceled = None
    er_type4 = build_exec_report(tes_mess4.type.response.body.executionReport)
    assert type(er_type4 == ExecutionReport)
    assert er_type4.executionReportType == 'orderCanceled'

    # cancel rejected
    tes_mess5 = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp5 = tes_mess5.init('type').init('response')
    heartbeat_resp5.clientID = 123
    heartbeat_resp5.senderCompID = str(987)
    body5 = heartbeat_resp5.init('body')
    er5 = get_new_execution_report(body=body5)
    order_rejected = er5.type.init('cancelRejected')
    order_rejected.message = 'way too silly'
    order_rejected.rejectionCode = 9987
    er_type5 = build_exec_report(tes_mess5.type.response.body.executionReport)
    assert type(er_type5 == ExecutionReport)
    assert er_type5.executionReportType == 'cancelRejected'

    # order filled
    tes_mess6 = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp6 = tes_mess6.init('type').init('response')
    heartbeat_resp6.clientID = 123
    heartbeat_resp6.senderCompID = str(987)
    body6 = heartbeat_resp6.init('body')
    er6 = get_new_execution_report(body=body6)
    er6.type.orderFilled = None
    er_type6 = build_exec_report(tes_mess6.type.response.body.executionReport)
    assert type(er_type6 == ExecutionReport)
    assert er_type6.executionReportType == 'orderFilled'

    # status update
    tes_mess7 = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp7 = tes_mess7.init('type').init('response')
    heartbeat_resp7.clientID = 123
    heartbeat_resp7.senderCompID = str(987)
    body7 = heartbeat_resp7.init('body')
    er7 = get_new_execution_report(body=body7)
    er7.type.statusUpdate = None
    er_type7 = build_exec_report(tes_mess7.type.response.body.executionReport)
    assert type(er_type7 == ExecutionReport)
    assert er_type7.executionReportType == 'statusUpdate'
