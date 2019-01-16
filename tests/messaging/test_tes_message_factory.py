import math
from typing import List

import capnp
import pytest

from tes_client.messaging.common_types import AccountBalancesReport, \
    AccountCredentials, AccountDataReport, AccountInfo, Balance, \
    CompletedOrdersReport, Exchange, ExchangePropertiesReport, LeverageType, \
    ExecutionReport, OpenPosition, OpenPositionsReport, Order, OrderInfo, \
    OrderStatus, OrderType, Side, SymbolProperties, TimeInForce, \
    WorkingOrdersReport
import communication_protocol.Exchanges_capnp as exch_capnp
import communication_protocol.TradeMessage_capnp as msgs_capnp
from tes_client.messaging.message_factory import account_balances_report_py, \
    account_data_report_py, completed_orders_report_py, \
    exchange_properties_report_py, execution_report_py, \
    generate_client_order_id, logoff_ack_py, logon_ack_py, \
    open_positions_report_py, system_message_py, tes_test_message_py, \
    working_orders_report_py, cancel_order_capnp, heartbeat_capnp, \
    logoff_capnp, logon_capnp, place_order_capnp, replace_order_capnp, \
    request_account_balances_capnp, request_account_data_capnp, \
    request_completed_orders_capnp, request_exchange_properties_capnp, \
    request_open_positions_capnp, request_order_mass_status_capnp, \
    request_order_status_capnp, request_working_orders_capnp, \
    _determine_order_price, _generate_tes_request


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
    error_code, system_msg = system_message_py(
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
    success, msg, accounts = logon_ack_py(tes_mess.type.response.body.logonAck)
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
    success1, msg1, accounts1 = logon_ack_py(
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
    success, msg = logoff_ack_py(tes_mess.type.response.body.logoffAck)
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
    success1, msg1 = logoff_ack_py(tes_mess1.type.response.body.logoffAck)
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

    wos_reports = working_orders_report_py(
        tes_mess.type.response.body.workingOrdersReport)
    assert type(wos_reports) == WorkingOrdersReport
    assert type(wos_reports.account_info) == AccountInfo

    exec_reports = wos_reports.orders
    assert type(exec_reports) == list
    assert type(exec_reports[0].execution_report_type) == str
    assert exec_reports[0].execution_report_type == 'statusUpdate'
    assert type(exec_reports[1].execution_report_type) == str
    assert exec_reports[1].execution_report_type == 'cancelRejected'


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

    cos_reports = completed_orders_report_py(
        tes_mess.type.response.body.completedOrdersReport)
    assert type(cos_reports) == CompletedOrdersReport
    assert type(cos_reports.account_info) == AccountInfo

    exec_reports = cos_reports.orders
    assert type(exec_reports) == list
    assert type(exec_reports[0].execution_report_type) == str
    assert exec_reports[0].execution_report_type == 'statusUpdate'
    assert type(exec_reports[1].execution_report_type) == str
    assert exec_reports[1].execution_report_type == 'statusUpdate'


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

    exch_props_rpt = exchange_properties_report_py(
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

    acct_bal_report = account_balances_report_py(
        tes_mess.type.response.body.accountBalancesReport)
    assert type(acct_bal_report == AccountBalancesReport)
    assert type(acct_bal_report.account_info == AccountInfo)
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
    er_type = execution_report_py(
        tes_mess.type.response.body.executionReport)
    assert type(er_type == ExecutionReport)
    assert er_type.execution_report_type == 'orderAccepted'

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
    er_type1 = execution_report_py(tes_mess1.type.response.body.executionReport)
    assert type(er_type1 == ExecutionReport)
    assert er_type1.execution_report_type == 'orderRejected'

    # order replaced
    tes_mess2 = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp2 = tes_mess2.init('type').init('response')
    heartbeat_resp2.clientID = 123
    heartbeat_resp2.senderCompID = str(987)
    body2 = heartbeat_resp2.init('body')
    er2 = get_new_execution_report(body=body2)
    er2.type.orderReplaced = None
    er_type2 = execution_report_py(tes_mess2.type.response.body.executionReport)
    assert type(er_type2 == ExecutionReport)
    assert er_type2.execution_report_type == 'orderReplaced'

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
    er_type3 = execution_report_py(tes_mess3.type.response.body.executionReport)
    assert type(er_type3 == ExecutionReport)
    assert er_type3.execution_report_type == 'replaceRejected'

    # order cancelled
    tes_mess4 = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp4 = tes_mess4.init('type').init('response')
    heartbeat_resp4.clientID = 123
    heartbeat_resp4.senderCompID = str(987)
    body4 = heartbeat_resp4.init('body')
    er4 = get_new_execution_report(body=body4)
    er4.type.orderCanceled = None
    er_type4 = execution_report_py(tes_mess4.type.response.body.executionReport)
    assert type(er_type4 == ExecutionReport)
    assert er_type4.execution_report_type == 'orderCanceled'

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
    er_type5 = execution_report_py(tes_mess5.type.response.body.executionReport)
    assert type(er_type5 == ExecutionReport)
    assert er_type5.execution_report_type == 'cancelRejected'

    # order filled
    tes_mess6 = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp6 = tes_mess6.init('type').init('response')
    heartbeat_resp6.clientID = 123
    heartbeat_resp6.senderCompID = str(987)
    body6 = heartbeat_resp6.init('body')
    er6 = get_new_execution_report(body=body6)
    er6.type.orderFilled = None
    er_type6 = execution_report_py(tes_mess6.type.response.body.executionReport)
    assert type(er_type6 == ExecutionReport)
    assert er_type6.execution_report_type == 'orderFilled'

    # status update
    tes_mess7 = msgs_capnp.TradeMessage.new_message()
    heartbeat_resp7 = tes_mess7.init('type').init('response')
    heartbeat_resp7.clientID = 123
    heartbeat_resp7.senderCompID = str(987)
    body7 = heartbeat_resp7.init('body')
    er7 = get_new_execution_report(body=body7)
    er7.type.statusUpdate = None
    er_type7 = execution_report_py(tes_mess7.type.response.body.executionReport)
    assert type(er_type7 == ExecutionReport)
    assert er_type7.execution_report_type == 'statusUpdate'


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
    body, tes_mess = _generate_tes_request(client_id=0, sender_comp_id='asdf')
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
    body = test_response.init('body')
    test = body.init('test')
    test.string = 'test_string'
    test_string = tes_test_message_py(
        tes_mess.type.response.body.test)

    assert test_string == 'test_string'
