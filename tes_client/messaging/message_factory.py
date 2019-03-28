import logging
import time
from typing import List

# pylint: disable=W0611
import capnp
# pylint: enable=W0611

# pylint: disable=E0611
# pylint: disable=E0401
import communication_protocol.Exchanges_capnp as exch_capnp
import communication_protocol.TradeMessage_capnp as msgs_capnp
# pylint: enable=E0611
# pylint: enable=E0401
from tes_client.messaging.common_types import AccountBalancesReport, \
    AccountCredentials, AccountDataReport, AccountInfo, AuthorizationGrant, \
    Balance, CompletedOrdersReport, Exchange,\
    ExchangePropertiesReport, ExecutionReport, \
    LogoffAck,  LogonAck, Message, OpenPosition, OpenPositionsReport, Order, \
    OrderInfo,  OrderType, RequestHeader, SymbolProperties, \
    SystemMessage, TimeInForce, WorkingOrdersReport

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


def _build_py_message(msg):
    """

    :param msg: (capnp._DynamicStructBuilder) Message object
    :return: Message python object
    """
    return Message(msg.code, msg.body)


def tes_test_message_py(test_message):
    """
    TODO: the naming gets rid of 'test' as a function prefix since pytest
    tests all functions with 'test' as a prefix.  Rename this back to
    test_message_py once we have disabled that behavior.

    Builds test message Python object from capnp object.
    :param test_message: (capnp._DynamicStructBuilder) TestMessage object.
    :return: (str) test message.
    """
    return test_message.string


def system_message_py(system_message):
    """
    Builds system message Python object from capnp object.
    :param system_message: (capnp._DynamicStructBuilder) system message.
    :return: SystemMessage.
    """
    py_message = _build_py_message(system_message.message)
    return SystemMessage(account_info=account_info_py(
                             system_message.accountInfo),
                         message=py_message)


def authorization_grant_py(authorization_grant):
    """
    Builds AuthorizationGrant Python object from capnp object.
    :param authorization_grant: (capnp._DynamicStructBuilder)
        capnp AuthorizationGrant message
    :return: AuthorizationGrant
    """
    py_message = _build_py_message(authorization_grant.message)
    return AuthorizationGrant(success=authorization_grant.success,
                              message=py_message,
                              access_token=authorization_grant.accessToken,
                              refresh_token=authorization_grant.refreshToken,
                              expire_at=authorization_grant.expireAt)


def logon_ack_py(logon_ack):
    """
    Builds LogonAck Python object from capnp object.
    :param logon_ack: (capnp._DynamicStructBuilder) LogonAck object.
    :return: LogonAck
    """
    client_accounts = list([account_info_py(account) for account in
                            logon_ack.clientAccounts])
    py_message = _build_py_message(logon_ack.message)
    return LogonAck(success=logon_ack.success,
                    message=py_message,
                    client_accounts=client_accounts,
                    authorization_grant=authorization_grant_py(
                        logon_ack.authorizationGrant))


def logoff_ack_py(logoff_ack):
    """
    Builds LogoffAck Python object from capnp object.
    :param logoff_ack: (capnp._DynamicStructBuilder) LogoffAck object.
    :return: LogoffAck
    """
    py_message = _build_py_message(logoff_ack.message)
    return LogoffAck(bool(logoff_ack.success), py_message)


def execution_report_py(execution_report):
    """
    Builds ExecutionReport Python object from capnp object.
    :param execution_report: (capnp._DynamicStructBuilder) ExecutionReport
        object.
    :return: (ExecutionReport) python object.
    """
    return _build_py_execution_report_from_capnp(execution_report)


def account_data_report_py(account_data_report):
    """
    Builds AccountDataReport Python object from capnp object, including
    AccountBalances, OpenPositions, and ExecutionReports.
    :param account_data_report: (capnp._DynamicStructBuilder) AccountDataReport
        object.
    :return: (AccountDataReport) Python class object.
    """
    acct_balances = [_build_py_balance_from_capnp(ab) for ab in
                     account_data_report.balances]
    open_positions = [_build_py_open_position_from_capnp(op)
                      for op in account_data_report.openPositions]
    orders = [_build_py_execution_report_from_capnp(er)
              for er in account_data_report.orders]
    return AccountDataReport(
        account_info=account_info_py(account_data_report.accountInfo),
        balances=acct_balances,
        open_positions=open_positions,
        orders=orders
    )


def account_balances_report_py(account_balances_report):
    """
    Builds AccountBalancesReport Python object from capnp object.
    :param account_balances_report: (capnp._DynamicStructBuilder)
        AccountBalancesReport object.
    :return: (AccountBalancesReport) Python class object.
    """
    acct_balances = [_build_py_balance_from_capnp(ab) for ab in
                     account_balances_report.balances]
    return AccountBalancesReport(
        account_info=account_info_py(account_balances_report.accountInfo),
        balances=acct_balances
    )


def open_positions_report_py(open_position_report):
    """
    Builds OpenPositionReport Python object from capnp object.
    :param open_position_report: (capnp._DynamicStructBuilder)
        OpenPositionReport object.
    :return: (OpenPositionReport) Python object.
    """
    open_pos = [_build_py_open_position_from_capnp(op)
                for op in open_position_report.openPositions]
    return OpenPositionsReport(
        account_info=account_info_py(open_position_report.accountInfo),
        open_positions=open_pos
    )


def working_orders_report_py(working_orders_report):
    """
    Builds WorkingOrdersReport Python object from capnp object.
    :param working_orders_report: (capnp._DynamicStructBuilder)
        WorkingOrdersReport object.
    :return: (WorkingOrdersReport) Python object.
    """
    execution_reports = [_build_py_execution_report_from_capnp(er)
                         for er in working_orders_report.orders]

    return WorkingOrdersReport(
        account_info=account_info_py(working_orders_report.accountInfo),
        orders=execution_reports
    )


def completed_orders_report_py(completed_orders_report):
    """
    Builds CompletedOrdersReport Python object from capnp object.
    :param completed_orders_report: (capnp._DynamicStructBuilder)
        CompletedOrdersReport object.
    :return: (CompletedOrdersReport) Python object.
    """
    execution_reports = [_build_py_execution_report_from_capnp(er)
                         for er in completed_orders_report.orders]

    return CompletedOrdersReport(
        account_info=account_info_py(completed_orders_report.accountInfo),
        orders=execution_reports
    )


def exchange_properties_report_py(exchange_properties_report):
    """
    Builds ExchangePropertiesReport Python object from capnp object.
    :param exchange_properties_report: (capnp._DynamicStructBuilder)
        ExchangePropertiesReport object.
    :return: (ExchangePropertiesReport) Python object.
    """
    currencies = set(ccy for ccy in exchange_properties_report.currencies)
    symbol_properties = {}
    for sp in exchange_properties_report.symbolProperties:
        symbol_properties[sp.symbol] = SymbolProperties(
            symbol=sp.symbol,
            price_precision=sp.pricePrecision,
            quantity_precision=sp.quantityPrecision,
            min_quantity=sp.minQuantity,
            max_quantity=sp.maxQuantity,
            margin_supported=sp.marginSupported,
            leverage=set(lev for lev in sp.leverage)
        )
    time_in_forces = set(str(tif)
                         for tif in exchange_properties_report.timeInForces)
    order_types = set(str(ot)
                      for ot in exchange_properties_report.orderTypes)
    return ExchangePropertiesReport(
        exchange=str(exchange_properties_report.exchange),
        currencies=currencies,
        symbol_properties=symbol_properties,
        time_in_forces=time_in_forces,
        order_types=order_types
    )


def logon_capnp(request_header: RequestHeader,
                client_secret: str,
                credentials: List[AccountCredentials]):
    """
    Generates a capnp Logon message with a specific clientID and set of
    credentials.
    :param client_secret: (str) client_secret key assigned by Fund3.
    :param credentials: (List[AccountCredentials]) List of exchange
        credentials in the form of AccountCredentials.
    :param request_header: Header parameter object for requests.
    :return: (capnp._DynamicStructBuilder) Logon capnp object.
    """
    request_header.access_token = ''  # Empty in logon
    tes_message, body = _generate_tes_request(request_header=request_header)
    logon = body.init('logon')
    logon.clientSecret = client_secret
    logon.init('credentials', len(credentials))
    logon = _set_logon_credentials(logon=logon, credentials=credentials)
    return tes_message, logon


def logoff_capnp(request_header: RequestHeader):
    """
    Generates a capnp Logoff message with a specific clientID.
    :param request_header: Header parameter object for requests.
    :return: (capnp._DynamicStructBuilder) Logoff capnp object.
    """
    tes_message, body = _generate_tes_request(request_header=request_header)
    body.logoff = None
    return tes_message, body


def heartbeat_capnp(request_header: RequestHeader):
    """
    Generates a capnp heartbeat message.
    :param request_header: Header parameter object for requests.
    :return: (capnp._DynamicStructBuilder) heartbeat capnp object.
    """
    tes_message, body = _generate_tes_request(request_header=request_header)
    body.heartbeat = None
    return tes_message, body


def request_server_time_capnp(request_header: RequestHeader):
    """
    Generates a capnp getServerTime message.
    :param request_header: Header parameter object for requests.
    :return: (capnp._DynamicStructBuilder) heartbeat capnp object.
    """
    tes_message, body = _generate_tes_request(request_header=request_header)
    body.getServerTime = None
    return tes_message, body


def place_order_capnp(request_header: RequestHeader, order: Order):
    """
    Generates a capnp placeOrder message from an Order.
    :param order: (Order) Python object from tes_client.common_types.
    :param request_header: Header parameter object for requests.
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) placeOrder capnp object.
    """
    tes_message, body = _generate_tes_request(request_header=request_header)
    place_order = body.init('placeSingleOrder')
    acct = place_order.init('accountInfo')
    acct.accountID = order.account_info.account_id
    place_order.clientOrderID = order.client_order_id
    place_order.clientOrderLinkID = order.client_order_link_id
    place_order.symbol = order.symbol
    place_order.side = order.side
    place_order.orderType = order.order_type
    place_order.quantity = order.quantity
    place_order.price = order.price
    place_order.stopPrice = order.stop_price
    place_order.timeInForce = order.time_in_force
    place_order.expireAt = order.expire_at
    place_order.leverageType = order.leverage_type
    place_order.leverage = order.leverage
    return tes_message, place_order


def replace_order_capnp(
        request_header: RequestHeader,
        account_info: AccountInfo,
        order_id: str,
        # pylint: disable=E1101
        order_type: str = OrderType.market.name,
        quantity: float = 0.0,
        price: float = 0.0,
        stop_price: float = 0.0,
        time_in_force: str = TimeInForce.gtc.name,
        # pylint: enable=E1101
        expire_at: float = 0.0
        ):
    """
    Generates a request to TES to replace an order.
    :param account_info: (AccountInfo) Account on which to cancel order.
    :param order_id: (str) orderID as returned from the ExecutionReport.
    :param request_header: Header parameter object for requests.
    :param order_type: (OrderType) (OPTIONAL)
    :param quantity: (float) (OPTIONAL)
    :param price: (float) (OPTIONAL)
    :param stop_price: (float) (OPTIONAL)
    :param time_in_force: (TimeInForce) (OPTIONAL)
    :param expire_at: (float) (OPTIONAL) utc timestamp gtt orders expire at
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) replaceOrder capnp object.
    """
    tes_message, body = _generate_tes_request(request_header=request_header)
    replace_order = body.init('replaceOrder')
    acct = replace_order.init('accountInfo')
    acct.accountID = account_info.account_id

    replace_order.orderID = order_id
    replace_order.orderType = order_type
    replace_order.quantity = quantity
    # https://github.com/fund3/tes_python_client/issues/39
    # merge with ExchangePropertiesReport to get more sophisticated
    # price values
    replace_order.price = _determine_order_price(
        order_price=price, order_type=order_type)
    replace_order.stopPrice = stop_price
    replace_order.timeInForce = time_in_force
    replace_order.expireAt = expire_at
    return tes_message, replace_order


def cancel_order_capnp(
        request_header: RequestHeader,
        account_info: AccountInfo,
        order_id: str):
    """
    Generates a capnp cancelOrder message.
    :param account_info: (AccountInfo) Account on which to cancel order.
    :param order_id: (str) order_id as returned from the ExecutionReport.
    :param request_header: Header parameter object for requests.
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) cancelOrder capnp object.
    """
    tes_message, body = _generate_tes_request(request_header=request_header)
    cancel_order = body.init('cancelOrder')
    acct = cancel_order.init('accountInfo')
    acct.accountID = account_info.account_id
    cancel_order.orderID = order_id
    return tes_message, cancel_order


def cancel_all_orders_capnp(
        request_header: RequestHeader,
        account_info: AccountInfo,
        symbol: str = None,
        side: str = None):
    """
    Generates a capnp CancelAllOrders message.
    :param request_header: Header parameter object for requests.
    :param account_info: (AccountInfo) Account on which to cancel order.
    :param symbol: str (optional)
    :param side: str (optional)
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) cancelOrder capnp object.
    """
    tes_message, body = _generate_tes_request(request_header=request_header)
    cancel_all_orders = body.init('cancelAllOrders')
    acct = cancel_all_orders.init('accountInfo')
    acct.accountID = account_info.account_id
    if symbol:
        cancel_all_orders.symbol = symbol
    if side:
        cancel_all_orders.side = side
    return tes_message, cancel_all_orders


def request_account_data_capnp(
        request_header: RequestHeader,
        account_info: AccountInfo):
    """
    Generates a request to TES for full account snapshot including balances,
    open positions, and working orders.
    :param account_info: (AccountInfo) Account from which to retrieve data.
    :param request_header: Header parameter object for requests.
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) getAccountData capnp object.
    """
    tes_message, body = _generate_tes_request(request_header=request_header)
    get_account_data = body.init('getAccountData')
    acct = get_account_data.init('accountInfo')
    acct.accountID = account_info.account_id
    return tes_message, get_account_data


def request_open_positions_capnp(
        request_header: RequestHeader,
        account_info: AccountInfo):
    """
    Send a request to TES for open positions on an Account.
    :param account_info: (AccountInfo) Account from which to retrieve data.
    :param request_header: Header parameter object for requests.
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) getOpenPositions capnp object.
    """
    tes_message, body = _generate_tes_request(request_header=request_header)
    get_open_positions = body.init('getOpenPositions')
    acct = get_open_positions.init('accountInfo')
    acct.accountID = account_info.account_id
    return tes_message, get_open_positions


def request_account_balances_capnp(
        request_header: RequestHeader,
        account_info: AccountInfo):
    """
    Generates a request to TES for full account balances snapshot on an
    Account.
    :param account_info: (AccountInfo) Account from which to retrieve data.
    :param request_header: Header parameter object for requests.
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) getAccountBalances capnp object.
    """
    tes_message, body = _generate_tes_request(request_header=request_header)
    get_account_balances = body.init('getAccountBalances')
    acct = get_account_balances.init('accountInfo')
    acct.accountID = account_info.account_id
    return tes_message, get_account_balances


def request_working_orders_capnp(
        request_header: RequestHeader,
        account_info: AccountInfo):
    """
    Generates a request to TES for all working orders snapshot on an Account.
    :param account_info: (AccountInfo) Account from which to retrieve data.
    :param request_header: Header parameter object for requests.
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) getWorkingOrders capnp object.
    """
    tes_message, body = _generate_tes_request(request_header=request_header)
    get_working_orders = body.init('getWorkingOrders')
    acct = get_working_orders.init('accountInfo')
    acct.accountID = account_info.account_id
    return tes_message, get_working_orders


def request_order_status_capnp(
        request_header: RequestHeader,
        account_info: AccountInfo,
        order_id: str):
    """
    Generates a request to TES to request status of a specific order.
    :param account_info: (AccountInfo) Account from which to retrieve data.
    :param order_id: (str) The id of the order of interest.
    :param request_header: Header parameter object for requests.
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) getOrderStatus capnp object.
    """
    tes_message, body = _generate_tes_request(request_header=request_header)
    get_order_status = body.init('getOrderStatus')
    acct = get_order_status.init('accountInfo')
    acct.accountID = account_info.account_id
    get_order_status.orderID = order_id
    return tes_message, get_order_status


def request_completed_orders_capnp(
        request_header: RequestHeader,
        account_info: AccountInfo,
        count: int = None,
        since: float = None):
    """
    Generates a request to TES for all completed orders on specified account.
    If both 'count' and 'from_unix' are None, returns orders for last 24h.
    :param account_info: (AccountInfo) Account from which to retrieve data.
    :param request_header: Header parameter object for requests.
    :param count: (int) optional, number of returned orders (most recent ones).
    :param since: (float) optional, returns all orders from provided unix
        timestamp to present.
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) getCompletedOrders capnp object.
    """
    tes_message, body = _generate_tes_request(request_header=request_header)
    get_completed_orders = body.init('getCompletedOrders')
    acct = get_completed_orders.init('accountInfo')
    acct.accountID = account_info.account_id
    if count is not None:
        get_completed_orders.count = count
    if since is not None:
        get_completed_orders.since = since
    return tes_message, get_completed_orders


def request_exchange_properties_capnp(request_header: RequestHeader,
                                      exchange: str):
    """
    Generates a request to TES for supported currencies, symbols and their
    associated properties, timeInForces, and orderTypes on an exchange.
    :param exchange: (str) The exchange of interest.
    :param request_header: Header parameter object for requests.
    :return: (capnp._DynamicStructBuilder) TradeMessage capnp object,
             (capnp._DynamicStructBuilder) getExchangeProperties capnp object.
    """
    tes_message, body = _generate_tes_request(request_header=request_header)
    get_exchange_properties = body.init('getExchangeProperties')
    get_exchange_properties.exchange = EXCHANGE_ENUM_MAPPING.get(
        exchange, exch_capnp.Exchange.undefined)
    return tes_message, get_exchange_properties


def _set_logon_credentials(logon, credentials):
    """
    Sets the credentials of a capnp Logon object.
    :param logon: (capnp._DynamicStructBuilder) Logon message.
    :param credentials: (List[AccountCredentials]) List of exchange
        credentials in the form of AccountCredentials.
    :return: (capnp._DynamicStructBuilder) Logon message.
    """
    for credi in zip(logon.credentials, credentials):
        try:
            acct_id = credi[1].account_info.account_id
            api_key = credi[1].api_key
            secret_key = credi[1].secret_key
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


def account_info_py(account_info):
    """
    Converts a capnp AccountInfo to Python object.
    :param account_info: (capnp._DynamicStructBuilder) AccountInfo object.
    :return: (AccountInfo) Populated Python object.
    """
    return AccountInfo(account_id=account_info.accountID,
                       exchange_account_id=account_info.exchangeAccountID,
                       account_type=str(account_info.accountType),
                       exchange_client_id=account_info.exchangeClientID)


def _build_py_open_position_from_capnp(open_position):
    """
    Converts a capnp OpenPosition to Python object.
    :param open_position: (capnp._DynamicStructBuilder) OpenPosition object.
    :return: (OpenPosition) Populated Python object.
    """
    return OpenPosition(
        symbol=open_position.symbol,
        side=open_position.side,
        quantity=open_position.quantity,
        initial_price=open_position.initialPrice,
        unrealized_pl=open_position.unrealizedPL
    )


def _build_py_balance_from_capnp(balance):
    """
    Converts a capnp Balance to Python object.
    :param balance: (capnp._DynamicStructBuilder) Balance object.
    :return: (Balance) Populated Python object.
    """
    return Balance(
        currency=balance.currency,
        full_balance=balance.fullBalance,
        available_balance=balance.availableBalance
    )


def _build_py_execution_report_from_capnp(execution_report):
    """
    Converts a capnp ExecutionReport to Python object.
    :param execution_report: (capnp._DynamicStructBuilder) ExecutionReport
        object.
    :return: (ExecutionReport) Populated Python object.
    """
    return ExecutionReport(
        order_id=execution_report.orderID,
        client_order_id=execution_report.clientOrderID,
        exchange_order_id=execution_report.exchangeOrderID,
        client_order_link_id=execution_report.clientOrderLinkID,
        account_info=account_info_py(
            execution_report.accountInfo),
        symbol=execution_report.symbol,
        side=execution_report.side,
        order_type=execution_report.orderType,
        quantity=execution_report.quantity,
        price=execution_report.price,
        stop_price=execution_report.stopPrice,
        time_in_force=execution_report.timeInForce,
        expire_at=execution_report.expireAt,
        leverage_type=execution_report.leverageType,
        leverage=execution_report.leverage,
        order_status=execution_report.orderStatus,
        filled_quantity=execution_report.filledQuantity,
        avg_fill_price=execution_report.avgFillPrice,
        fee=execution_report.fee,
        creation_time=execution_report.creationTime,
        submission_time=execution_report.submissionTime,
        completion_time=execution_report.completionTime,
        execution_report_type=execution_report.executionType,
        rejection_reason=_build_py_message(execution_report.rejectionReason)
    )


def _determine_order_price(order_price: float, order_type: str):
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


def _generate_tes_request(request_header: RequestHeader):
    """
    Generates an empty TES request from TradeMessage.capnp.
    :param request_header: Header parameter object for requests.
    :return: (capnp._DynamicStructBuilder) tes_message to be serialized,
             (capnp._DynamicStructBuilder) body (empty, to be filled).
    """
    tes_message = msgs_capnp.TradeMessage.new_message()
    request = tes_message.init('type').init('request')
    request.requestID = request_header.request_id
    request.clientID = request_header.client_id
    request.senderCompID = request_header.sender_comp_id
    request.accessToken = request_header.access_token
    body = request.init('body')
    return tes_message, body


def generate_client_order_id():
    """
    Simple way to generate client_order_id.  The client can generate their
    own unique order id as they wish.
    :return: (str) Client order_id based on the microsecond timestamp.
    """
    client_order_id = str(time.time()*1000000)
    return client_order_id
