import logging
import time
from typing import List
# pylint: disable=W0611
import capnp
# pylint: enable=W0611

from tes_client.common_types import AccountBalancesReport, \
    AccountDataReport, AccountInfo, Balance, \
    CompletedOrdersReport, Exchange, ExchangePropertiesReport, \
    ExecutionReport, OpenPosition, OpenPositionsReport, Order, OrderInfo, \
    OrderType, SymbolProperties, TimeInForce, WorkingOrdersReport
# pylint: disable=E0611
# pylint: disable=E0401
import communication_protocol.Exchanges_capnp as exch_capnp
import communication_protocol.TradeMessage_capnp as msgs_capnp
# pylint: enable=E0611
# pylint: enable=E0401

logger = logging.getLogger(__name__)

# pylint: disable=E1101
EXCHANGE_ENUM_MAPPING = {
    Exchange.poloniex.name: exch_capnp.Exchange.poloniex,
    Exchange.kraken.name: exch_capnp.Exchange.kraken,
    Exchange.gemini.name: exch_capnp.Exchange.gemini,
    Exchange.bitfinex.name: exch_capnp.Exchange.bitfinex,
    Exchange.bittrex.name: exch_capnp.Exchange.bittrex,
    Exchange.binance.name: exch_capnp.Exchange.binance,
    Exchange.coinbasePro.name: exch_capnp.Exchange.coinbasePro,
    Exchange.coinbasePrime.name: exch_capnp.Exchange.coinbasePrime,
    Exchange.bitstamp.name: exch_capnp.Exchange.bitstamp,
    Exchange.itBit.name: exch_capnp.Exchange.itBit
}
# pylint: enable=E1101


def build_test_message(test):
    """
    Builds test message Python object from capnp object.
    :param test: (capnp._DynamicStructBuilder) TestMessage object.
    :return: (str) test message.
    """
    return test.string


def build_system_message(system):
    """
    Builds system message Python object from capnp object.
    :param system: (capnp._DynamicStructBuilder) system message.
    :return: (int) error code, (str) system message.
    """
    return system.errorCode, system.message


def build_logon(logonAck):
    """
    Builds Logon Python object from capnp object.
    :param logonAck: (capnp._DynamicStructBuilder) LogonAck object.
    :return: (bool) success, (str) message, (List[int]) clientAccounts.
    """
    if logonAck.success:
        logger.debug('Logon success', extra={'status': 'logon_success'})
    else:
        logger.debug('Logon failure', extra={'status': 'logon_failure'})
    logger.debug('logon response: ',
                 extra={'logon_response': logonAck.message})
    client_accounts = list([acct for acct in logonAck.clientAccounts])
    logger.debug('client accounts:',
                 extra={'client_accounts': client_accounts})
    # NOTE client accounts are not used for now since they are what's
    #  passed to TES in the 1st place
    return logonAck.success, logonAck.message, client_accounts


def build_logoff(logoffAck):
    """
    Builds Logoff Python object from capnp object.
    :param logoffAck: (capnp._DynamicStructBuilder) LogoffAck object.
    :return: (bool) success, (str) message.
    """
    logoff_success = bool(logoffAck.success)
    logoff_msg = str(logoffAck.message)
    if logoff_success:
        logger.debug('Logoff success.', extra={'status': 'logoff_success'})
    else:
        logger.debug('Logoff failure.', extra={'status': 'logoff_failure'})
    logger.debug('Logoff message.', extra={'logoff_message': logoff_msg})
    return logoff_success, logoff_msg


def build_exec_report(executionReport):
    """
    Builds ExecutionReport Python object from capnp object.
    :param executionReport: (capnp._DynamicStructBuilder) ExecutionReport
        object.
    :return: (ExecutionReport) python object.
    """
    logger.debug('Execution report:',
                 extra={'execution_report': executionReport})
    er_type = executionReport.type.which()

    if er_type == 'orderAccepted':
        logger.debug('Order accepted.', extra={'er_type': str(er_type)})

    elif er_type == 'orderRejected':
        oR = executionReport.type.orderRejected
        logger.debug('Order rejected.',
                     extra={'er_type': str(er_type),
                            'rejection_message': str(oR.message),
                            'rejection_code': str(oR.rejectionCode)})

    elif er_type == 'orderReplaced':
        oR = executionReport.type.orderReplaced
        logger.debug('Order replaced.',
                     extra={'er_type': er_type, 'order_replaced': str(oR)})

    elif er_type == 'replaceRejected':
        rR = executionReport.type.replaceRejected
        logger.debug('Replace rejected.',
                     extra={'er_type': er_type,
                            'rejection_message': str(rR.message),
                            'rejection_code': str(rR.rejectionCode)})

    elif er_type == 'orderCanceled':
        logger.debug('Order cancelled.', extra={'er_type': str(er_type)})

    elif er_type == 'cancelRejected':
        cR = executionReport.type.cancelRejected
        logger.debug('Cancel rejected. ',
                     extra={'er_type': er_type,
                            'rejection_message': str(cR.message),
                            'rejection_code': str(cR.rejectionCode)})

    elif er_type == 'orderFilled':
        logger.debug('Order filled.', extra={'er_type': str(er_type)})

    elif er_type == 'statusUpdate':
        logger.debug('Status update.', extra={'er_type': str(er_type)})

    elif er_type == 'statusUpdateRejected':
        sUR = executionReport.type.statusUpdateRejected
        logger.debug('Status update rejected.',
                     extra={'er_type': str(er_type),
                            'rejection_message': str(sUR.message),
                            'rejection_code': str(sUR.rejectionCode)})
    return build_py_execution_report_from_capnp(executionReport)


def build_account_data_report(accountDataReport):
    """
    Builds AccountDataReport Python object from capnp object, including
    AccountBalances, OpenPositions, and ExecutionReports.
    :param accountDataReport: (capnp._DynamicStructBuilder) AccountDataReport
        object.
    :return: (AccountDataReport) Python class object.
    """
    acct_balances = [build_py_balance_from_capnp(ab) for ab in
                     accountDataReport.balances]
    open_positions = [build_py_open_position_from_capnp(op)
                      for op in accountDataReport.openPositions]
    orders = [build_py_execution_report_from_capnp(er)
              for er in accountDataReport.orders]
    return AccountDataReport(
        accountInfo=build_py_account_info_from_capnp(
            accountDataReport.accountInfo),
        balances=acct_balances,
        openPositions=open_positions,
        orders=orders
    )


def build_account_balances_report(accountBalancesReport):
    """
    Builds AccountBalancesReport Python object from capnp object.
    :param accountBalancesReport: (capnp._DynamicStructBuilder)
        AccountBalancesReport object.
    :return: (AccountBalancesReport) Python class object.
    """
    acct_balances = [build_py_balance_from_capnp(ab) for ab in
                     accountBalancesReport.balances]
    return AccountBalancesReport(
        accountInfo=build_py_account_info_from_capnp(
            accountBalancesReport.accountInfo),
        balances=acct_balances
    )


def build_open_positions_report(openPositionReport):
    """
    Builds OpenPositionReport Python object from capnp object.
    :param openPositionReport: (capnp._DynamicStructBuilder)
        OpenPositionReport object.
    :return: (OpenPositionReport) Python object.
    """
    open_pos = [build_py_open_position_from_capnp(op)
                for op in openPositionReport.openPositions]
    return OpenPositionsReport(
        accountInfo=build_py_account_info_from_capnp(
            openPositionReport.accountInfo),
        openPositions=open_pos
    )


def build_working_orders_report(workingOrdersReport):
    """
    Builds WorkingOrdersReport Python object from capnp object.
    :param workingOrdersReport: (capnp._DynamicStructBuilder)
        WorkingOrdersReport object.
    :return: (WorkingOrdersReport) Python object.
    """
    execution_reports = [build_py_execution_report_from_capnp(er)
                         for er in workingOrdersReport.orders]

    return WorkingOrdersReport(
        accountInfo=build_py_account_info_from_capnp(
            workingOrdersReport.accountInfo),
        orders=execution_reports
    )


def build_completed_orders_report(completedOrdersReport):
    """
    Builds CompletedOrdersReport Python object from capnp object.
    :param completedOrdersReport: (capnp._DynamicStructBuilder)
        CompletedOrdersReport object.
    :return: (CompletedOrdersReport) Python object.
    """
    execution_reports = [build_py_execution_report_from_capnp(er)
                         for er in completedOrdersReport.orders]

    return CompletedOrdersReport(
        accountInfo=build_py_account_info_from_capnp(
            completedOrdersReport.accountInfo),
        orders=execution_reports
    )


def build_exchange_properties_report(exchangePropertiesReport):
    """
    Builds ExchangePropertiesReport Python object from capnp object.
    :param exchangePropertiesReport: (capnp._DynamicStructBuilder)
        ExchangePropertiesReport object.
    :return: (ExchangePropertiesReport) Python object.
    """
    currencies = set(ccy for ccy in exchangePropertiesReport.currencies)
    symbolProperties = {}
    for sp in exchangePropertiesReport.symbolProperties:
        symbolProperties[sp.symbol] = SymbolProperties(
            symbol=sp.symbol,
            pricePrecision=sp.pricePrecision,
            quantityPrecision=sp.quantityPrecision,
            minQuantity=sp.minQuantity,
            maxQuantity=sp.maxQuantity,
            marginSupported=sp.marginSupported,
            leverage=set(lev for lev in sp.leverage)
        )
    timeInForces = set(str(tif)
                       for tif in exchangePropertiesReport.timeInForces)
    orderTypes = set(str(ot)
                     for ot in exchangePropertiesReport.orderTypes)
    return ExchangePropertiesReport(
        exchange=str(exchangePropertiesReport.exchange),
        currencies=currencies,
        symbolProperties=symbolProperties,
        timeInForces=timeInForces,
        orderTypes=orderTypes
    )


def logon_message(clientID: int, senderCompID: str, credentials):
    """
    Generates a capnp Logon message with a specific clientID and set of
    credentials.
    :param credentials: (List[AccountCredentials]) List of exchange
        credentials in the form of AccountCredentials.
    :param clientID: (int) The assigned clientID.
    :param senderCompID: (str) uuid unique to the session the user is on.
    :return: (capnp._DynamicStructBuilder) Logon capnp object.
    """
    tesMessage, body = generate_tes_request(clientID, senderCompID)
    logon = body.init('logon')
    logon.init('credentials', len(credentials))
    logon = set_logon_credentials(logon, credentials)
    return tesMessage, logon


def logoff_message(clientID: int, senderCompID: str):
    """
    Generates a capnp Logoff message with a specific clientID.
    :param clientID: (int) The assigned clientID.
    :param senderCompID: (str) uuid unique to the session the user is on.
    :return: (capnp._DynamicStructBuilder) Logoff capnp object.
    """
    tesMessage, body = generate_tes_request(clientID, senderCompID)
    body.logoff = None
    return tesMessage, body


def heartbeat_message(clientID: int, senderCompID: str):
    """
    Generates a capnp heartbeat message.
    :param clientID: (int) The assigned clientID.
    :param senderCompID: (str) uuid unique to the session the user is on.
    :return: (capnp._DynamicStructBuilder) heartbeat capnp object.
    """
    tesMessage, body = generate_tes_request(clientID, senderCompID)
    body.heartbeat = None
    return tesMessage, body


def place_order_message(clientID: int, senderCompID: str, order: Order):
    """
    Generates a capnp placeOrder message from an Order.
    :param clientID: (int) The assigned clientID.
    :param senderCompID: (str) uuid unique to the session the user is on.
    :param order: (Order) Python object from tes_client.common_types.
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) placeOrder capnp object.
    """
    tesMessage, body = generate_tes_request(clientID, senderCompID)
    placeOrder = body.init('placeOrder')
    acct = placeOrder.init('accountInfo')
    acct.accountID = order.accountInfo.accountID
    placeOrder.clientOrderID = order.clientOrderID
    placeOrder.clientOrderLinkID = order.clientOrderLinkID
    placeOrder.symbol = order.symbol
    placeOrder.side = order.side
    placeOrder.orderType = order.orderType
    placeOrder.quantity = order.quantity
    placeOrder.price = order.price
    placeOrder.timeInForce = order.timeInForce
    placeOrder.leverageType = order.leverageType
    placeOrder.leverage = order.leverage
    return tesMessage, placeOrder


def replace_order_message(clientID: int, senderCompID: str,
                          accountInfo: AccountInfo, orderID: str,
                          # pylint: disable=E1101
                          orderType: str=OrderType.market.name,
                          quantity: float=-1.0, price: float=-1.0,
                          timeInForce: str=TimeInForce.gtc.name
                          # pylint: enable=E1101
                          ):
    """
    Generates a request to TES to replace an order.
    :param clientID: (int) The assigned clientID.
    :param senderCompID: (str) uuid unique to the session the user is on.
    :param accountInfo: (AccountInfo) Account on which to cancel order.
    :param orderID: (str) orderID as returned from the ExecutionReport.
    :param orderType: (OrderType) (OPTIONAL)
    :param quantity: (float) (OPTIONAL)
    :param price: (float) (OPTIONAL)
    :param timeInForce: (TimeInForce) (OPTIONAL)
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) replaceOrder capnp object.
    """
    tesMessage, body = generate_tes_request(clientID, senderCompID)
    replaceOrder = body.init('replaceOrder')
    acct = replaceOrder.init('accountInfo')
    acct.accountID = accountInfo.accountID

    replaceOrder.orderID = orderID
    replaceOrder.orderType = orderType
    replaceOrder.quantity = quantity
    # https://github.com/fund3/tes_python_client/issues/39
    # merge with ExchangePropertiesReport to get more sophisticated
    # price values
    replaceOrder.price = determine_order_price(
        order_price=price, order_type=orderType)
    replaceOrder.timeInForce = timeInForce
    return tesMessage, replaceOrder


def cancel_order_message(clientID: int, senderCompID: str,
                         accountInfo: AccountInfo, orderID: str):
    """
    Generates a capnp cancelOrder message.
    :param clientID: (int) The assigned clientID.
    :param senderCompID: (str) uuid unique to the session the user is on.
    :param accountInfo: (AccountInfo) Account on which to cancel order.
    :param orderID: (str) order_id as returned from the ExecutionReport.
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) cancelOrder capnp object.
    """
    tesMessage, body = generate_tes_request(clientID, senderCompID)
    cancelOrder = body.init('cancelOrder')
    acct = cancelOrder.init('accountInfo')
    acct.accountID = accountInfo.accountID
    cancelOrder.orderID = orderID
    return tesMessage, cancelOrder


def request_account_data_message(clientID: int, senderCompID: str,
                                 accountInfo: AccountInfo):
    """
    Generates a request to TES for full account snapshot including balances,
    open positions, and working orders.
    :param clientID: (int) The assigned clientID.
    :param senderCompID: (str) uuid unique to the session the user is on.
    :param accountInfo: (AccountInfo) Account from which to retrieve data.
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) getAccountData capnp object.
    """
    tesMessage, body = generate_tes_request(clientID, senderCompID)
    getAccountData = body.init('getAccountData')
    acct = getAccountData.init('accountInfo')
    acct.accountID = accountInfo.accountID
    return tesMessage, getAccountData


def request_open_positions_message(clientID: int, senderCompID: str,
                                   accountInfo: AccountInfo):
    """
    Send a request to TES for open positions on an Account.
    :param clientID: (int) The assigned clientID.
    :param senderCompID: (str) uuid unique to the session the user is on.
    :param accountInfo: (AccountInfo) Account from which to retrieve data.
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) getOpenPositions capnp object.
    """
    tesMessage, body = generate_tes_request(clientID, senderCompID)
    getOpenPositions = body.init('getOpenPositions')
    acct = getOpenPositions.init('accountInfo')
    acct.accountID = accountInfo.accountID
    return tesMessage, getOpenPositions


def request_account_balances_message(clientID: int, senderCompID: str,
                                     accountInfo: AccountInfo):
    """
    Generates a request to TES for full account balances snapshot on an
    Account.
    :param clientID: (int) The assigned clientID.
    :param senderCompID: (str) uuid unique to the session the user is on.
    :param accountInfo: (AccountInfo) Account from which to retrieve data.
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) getAccountBalances capnp object.
    """
    tesMessage, body = generate_tes_request(clientID, senderCompID)
    getAccountBalances = body.init('getAccountBalances')
    acct = getAccountBalances.init('accountInfo')
    acct.accountID = accountInfo.accountID
    return tesMessage, getAccountBalances


def request_working_orders_message(clientID: int, senderCompID: str,
                                   accountInfo: AccountInfo):
    """
    Generates a request to TES for all working orders snapshot on an Account.
    :param clientID: (int) The assigned clientID.
    :param senderCompID: (str) uuid unique to the session the user is on.
    :param accountInfo: (AccountInfo) Account from which to retrieve data.
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) getWorkingOrders capnp object.
    """
    tesMessage, body = generate_tes_request(clientID, senderCompID)
    getWorkingOrders = body.init('getWorkingOrders')
    acct = getWorkingOrders.init('accountInfo')
    acct.accountID = accountInfo.accountID
    return tesMessage, getWorkingOrders


def request_order_status_message(clientID: int, senderCompID: str,
                                 accountInfo: AccountInfo, orderID: str):
    """
    Generates a request to TES to request status of a specific order.
    :param clientID: (int) The assigned clientID.
    :param senderCompID: (str) uuid unique to the session the user is on.
    :param accountInfo: (AccountInfo) Account from which to retrieve data.
    :param orderID: (str) The id of the order of interest.
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) getOrderStatus capnp object.
    """
    tesMessage, body = generate_tes_request(clientID, senderCompID)
    getOrderStatus = body.init('getOrderStatus')
    acct = getOrderStatus.init('accountInfo')
    acct.accountID = accountInfo.accountID
    getOrderStatus.orderID = orderID
    return tesMessage, getOrderStatus


def request_completed_orders(clientID: int, senderCompID: str,
                             accountInfo: AccountInfo, count: int=None,
                             since: float=None):
    """
    Generates a request to TES for all completed orders on specified account.
    If both 'count' and 'from_unix' are None, returns orders for last 24h.
    :param clientID: (int) The assigned clientID.
    :param senderCompID: (str) uuid unique to the session the user is on.
    :param accountInfo: (AccountInfo) Account from which to retrieve data.
    :param count: (int) optional, number of returned orders (most recent
        ones).
    :param since: (float) optional, returns all orders from provided unix
        timestamp to present.
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) getCompletedOrders capnp object.
    """
    tesMessage, body = generate_tes_request(clientID, senderCompID)
    getCompletedOrders = body.init('getCompletedOrders')
    acct = getCompletedOrders.init('accountInfo')
    acct.accountID = accountInfo.accountID
    if count is not None:
        getCompletedOrders.count = count
    if since is not None:
        getCompletedOrders.since = since
    return tesMessage, getCompletedOrders


def request_order_mass_status_message(clientID: int, senderCompID: str,
                                      accountInfo: AccountInfo,
                                      orderInfo: List[OrderInfo]):
    """
    Generates a request to TES for status of multiple orders.
    :param clientID: (int) The assigned clientID.
    :param senderCompID: (str) uuid unique to the session the user is on.
    :param accountInfo: (AccountInfo) Account from which to retrieve data.
    :param orderInfo: (List[OrderInfo]) List of orderIDs to get status updates.
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) getOrderMassStatus capnp object.
    """
    tesMessage, body = generate_tes_request(clientID, senderCompID)
    getOrderMassStatus = body.init('getOrderMassStatus')
    acct = getOrderMassStatus.init('accountInfo')
    acct.accountID = accountInfo.accountID
    oi = getOrderMassStatus.init('orderInfo', len(orderInfo))
    for oii in zip(oi, orderInfo):
        oii[0].orderID = oii[1].orderID
    return tesMessage, getOrderMassStatus


def request_exchange_properties_message(clientID: int, senderCompID: str,
                                        exchange: str):
    """
    Generates a request to TES for supported currencies, symbols and their
    associated properties, timeInForces, and orderTypes on an exchange.
    :param clientID: (int) The assigned clientID.
    :param senderCompID: (str) uuid unique to the session the user is on.
    :param exchange: (str) The exchange of interest.
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) getExchangeProperties capnp object.
    """
    tesMessage, body = generate_tes_request(clientID, senderCompID)
    getExchangeProperties = body.init('getExchangeProperties')
    getExchangeProperties.exchange = EXCHANGE_ENUM_MAPPING.get(
        exchange, exch_capnp.Exchange.undefined)
    return tesMessage, getExchangeProperties


def set_logon_credentials(logon, credentials):
    """
    Sets the credentials of a capnp Logon object.
    :param logon: (capnp._DynamicStructBuilder) Logon message.
    :param credentials: (List[AccountCredentials]) List of exchange
        credentials in the form of AccountCredentials.
    :return: (capnp._DynamicStructBuilder) Logon message.
    """
    for credi in zip(logon.credentials, credentials):
        try:
            acct_id = credi[1].accountInfo.accountID
            api_key = credi[1].apiKey
            secret_key = credi[1].secretKey
        except Exception as e:
            logger.error('Missing logon credentials. Account ID, apiKey, '
                         'secretKey are all required', extra={'error': e})
            raise e
        credi[0].accountInfo.accountID = acct_id
        credi[0].apiKey = api_key
        credi[0].secretKey = secret_key
        if credi[1].passphrase is not None:
            credi[0].passphrase = credi[1].passphrase
    return logon


def build_py_account_info_from_capnp(accountInfo):
    """
    Converts a capnp AccountInfo to Python object.
    :param accountInfo: (capnp._DynamicStructBuilder) AccountInfo object.
    :return: (AccountInfo) Populated Python object.
    """
    return AccountInfo(accountID=accountInfo.accountID,
                       exchangeAccountID=accountInfo.exchangeAccountID,
                       accountType=str(accountInfo.accountType),
                       exchangeClientID=accountInfo.exchangeClientID)


def build_py_open_position_from_capnp(openPosition):
    """
    Converts a capnp OpenPosition to Python object.
    :param openPosition: (capnp._DynamicStructBuilder) OpenPosition object.
    :return: (OpenPosition) Populated Python object.
    """
    return OpenPosition(
        symbol=openPosition.symbol,
        side=openPosition.side,
        quantity=openPosition.quantity,
        initialPrice=openPosition.initialPrice,
        unrealizedPL=openPosition.unrealizedPL
    )


def build_py_balance_from_capnp(balance):
    """
    Converts a capnp Balance to Python object.
    :param balance: (capnp._DynamicStructBuilder) Balance object.
    :return: (Balance) Populated Python object.
    """
    return Balance(
        currency=balance.currency,
        fullBalance=balance.fullBalance,
        availableBalance=balance.availableBalance
    )


def build_py_execution_report_from_capnp(executionReport):
    """
    Converts a capnp ExecutionReport to Python object.
    :param executionReport: (capnp._DynamicStructBuilder) ExecutionReport
        object.
    :return: (ExecutionReport) Populated Python object.
    """
    return ExecutionReport(
        orderID=executionReport.orderID,
        clientOrderID=executionReport.clientOrderID,
        clientOrderLinkID=executionReport.clientOrderLinkID,
        exchangeOrderID=executionReport.exchangeOrderID,
        accountInfo=build_py_account_info_from_capnp(
            executionReport.accountInfo),
        symbol=executionReport.symbol,
        side=executionReport.side,
        orderType=executionReport.orderType,
        quantity=executionReport.quantity,
        price=executionReport.price,
        timeInForce=executionReport.timeInForce,
        leverageType=executionReport.leverageType,
        leverage=executionReport.leverage,
        orderStatus=executionReport.orderStatus,
        filledQuantity=executionReport.filledQuantity,
        avgFillPrice=executionReport.avgFillPrice,
        executionReportType=executionReport.type.which(),
        rejectionReason=executionReport.rejectionReason
    )


def determine_rejection_reason(order):
    """
    Determines rejection message by switching on different rejectionReason
    types.
    :param order: (capnp._DynamicStructBuilder) Order object.
    :return: (str) Rejection reason message.
    """
    rejectionType = order.type.which()
    # process rejectionCode
    # https://github.com/fund3/tes_python_client/issues/40
    logger.debug('Determining order rejection reason.',
                 extra={'type': rejectionType})
    if rejectionType == 'orderRejected':
        rejectionMessage = order.type.orderRejected.message
    elif rejectionType == 'replaceRejected':
        rejectionMessage = order.type.replaceRejected.message
    elif rejectionType == 'cancelRejected':
        rejectionMessage = order.type.cancelRejected.message
    elif rejectionType == 'statusUpdateRejected':
        rejectionMessage = order.type.statusUpdateRejected.message
    else:
        rejectionMessage = None
    return rejectionMessage


def determine_order_price(order_price: float, order_type: str):
    """
    TES rejects market orders with a non-zero price, hence this method
    assigns 0.0 as order_price if it receives a market order.
    :param order_price: (float) Desired order price.
    :param order_type: (str) Type of order ie limit, market, etc.
    :return: (float) Properly formatted order price.
    """
    if order_type == 'market':
        return 0.0
    return order_price


def generate_tes_request(clientID: int, senderCompID: str):
    """
    Generates an empty TES request from TradeMessage.capnp.
    :param clientID: (int) The assigned clientID.
    :param senderCompID: (str) uuid unique to the session the user is on.
    :return: (capnp._DynamicStructBuilder) tes_message to be serialized,
             (capnp._DynamicStructBuilder) body (empty, to be filled).

    """
    logger.debug('TES msg request ids.',
                 extra={'clientID': str(clientID),
                        'sender_comp_id': senderCompID})
    tes_message = msgs_capnp.TradeMessage.new_message()
    request = tes_message.init('type').init('request')
    request.clientID = clientID
    request.senderCompID = senderCompID
    body = request.init('body')
    return tes_message, body


def generate_client_order_id():
    """
    :return: (int) Client order_id based on the microsecond timestamp.
    """
    client_order_id = int(time.time()*1000000)
    return client_order_id
