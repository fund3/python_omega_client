from enum import Enum, auto
from typing import List, Dict, Set

# pythonized equivalents of what is contained in:
# https://github.com/fund3/communication-protocol/blob/master/TradeMessage.capnp
# make sure to call .name if you want the string representation of an enum
# object when communicating with Omega


class Exchange(Enum):
    """Exchange Names

    https://github.com/fund3/communication-protocol/blob/master/Exchanges.capnp
    """
    undefined = auto()
    poloniex = auto()
    kraken = auto()
    gemini = auto()
    bitfinex = auto()
    bittrex = auto()
    binance = auto()
    coinbasePro = auto()
    coinbasePrime = auto()
    bitstamp = auto()
    itBit = auto()
    okEx = auto()
    hitbtc = auto()


class Side(Enum):
    """Trading Sides

    https://github.com/fund3/CommunicationProtocol/blob/master/TradeMessage.capnp
    """
    undefined = auto()
    buy = auto()
    sell = auto()


class OrderType(Enum):
    """Supported Order Types

    https://github.com/fund3/CommunicationProtocol/blob/master/TradeMessage.capnp
    """
    undefined = auto()
    market = auto()
    limit = auto()
    stopLoss = auto()
    stopLossLimit = auto()
    takeProfit = auto()
    takeProfitLimit = auto()
    trailingStop = auto()
    trailingStopLimit = auto()


class OrderStatus(Enum):
    """Order Status on Exchange

    https://github.com/fund3/CommunicationProtocol/blob/master/TradeMessage.capnp
    """
    undefined = auto()
    received = auto()
    adopted = auto()
    working = auto()
    partiallyFilled = auto()
    filled = auto()
    pendingReplace = auto()
    replaced = auto()
    pendingCancel = auto()
    canceled = auto()
    rejected = auto()
    expired = auto()
    failed = auto()
    deferred = auto()


class TimeInForce(Enum):
    """Order Time In Force

    https://github.com/fund3/CommunicationProtocol/blob/master/TradeMessage.capnp
    """
    undefined = auto()
    gtc = auto()        # Good till cancel
    gtt = auto()        # Good till time
    day = auto()        # Day order
    ioc = auto()        # Immediate or cancel
    fok = auto()        # Fill or kill


class LeverageType(Enum):
    """Leverage Type

    https://github.com/fund3/CommunicationProtocol/blob/master/TradeMessage.capnp
    """
    none = auto()
    exchangeDefault = auto()
    custom = auto()


class AccountType(Enum):
    """Account Type

    https://github.com/fund3/CommunicationProtocol/blob/master/TradeMessage.capnp
    """
    undefined = auto()
    exchange = auto()
    margin = auto()
    combined = auto()


class CommonType:
    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class AccountInfo(CommonType):
    def __init__(self,
                 account_id: int,
                 exchange: str = None,
                 exchange_account_id: str = None,
                 account_type: str = None,
                 exchange_client_id: str = None):
        """
        :param account_id: int id corresponding to an account on an exchange
        Required.
        :param exchange: str exchange in which accountID is contained
        :param exchange_account_id: str account/wallet id, empty in client
            request
        :param account_type: str exchange account type (exchange,
        margin, combined), empty in client request (will replace label)
        :param exchange_client_id: str exchange client (customer) ID,
        empty in client request
        """
        self.account_id = int(account_id)
        self.exchange = str(exchange or '')
        self.exchange_account_id = str(exchange_account_id or '')
        self.account_type = str(account_type or '')
        self.exchange_client_id = str(exchange_client_id or '')


class AccountCredentials(CommonType):
    def __init__(self,
                 account_info: AccountInfo,
                 api_key: str,
                 secret_key: str,
                 passphrase: str = None):
        """
        AccountCredentials object is used for logon
        :param account_info: AccountInfo object containing accountID
        :param api_key: str apiKey for connecting to exchange API
        associated with account_id
        :param secret_key: str secretKey for connecting to exchange API
        associated with account_id
        :param passphrase: str (optional) passphrase for connecting to API
        associated with account_id
        """
        self.account_info = account_info
        self.api_key = str(api_key)
        self.secret_key = str(secret_key)
        self.passphrase = str(passphrase or '')


class Order(CommonType):
    """
    object created for placing a new Order.
    """
    def __init__(self,
                 account_info: AccountInfo,
                 client_order_id: str,
                 symbol: str,
                 side: str,
                 order_type: str,
                 quantity: float,
                 price: float,
                 stop_price: float = 0.0,
                 # pylint: disable=E1101
                 time_in_force: str = TimeInForce.gtc.name,
                 expire_at: float = 0.0,
                 leverage_type: str = LeverageType.none.name,
                 # pylint: enable=E1101
                 leverage: float = 0.0,
                 client_order_link_id: str = None):
        """

        :param account_info: AccountInfo
        :param client_order_id: str orderID generated on the client side
        :param account_info: accountInfo
        :param symbol: str
        :param side: str (see Side enum)
        :param order_type: str (see OrderType enum)
        :param quantity: float
        :param price: float
        :param stop_price: float required for STOP, STOP_LIMIT orders
        :param time_in_force: str (see TimeInForce enum)
        :param expire_at: float utc timestamp gtt order expires at (default 0.0)
        :param leverage_type: str (see LeverageType enum)
        :param leverage: float leverage being used on this specific order
        :param client_order_link_id: str used for identifying strategy (when
        multiple strategies are trading on the same account)
        """
        self.account_info = account_info
        self.client_order_id = str(client_order_id)
        self.symbol = str(symbol)
        self.side = str(side)
        self.order_type = str(order_type)
        self.quantity = float(quantity)
        self.price = float(price)
        self.stop_price = float(stop_price)
        self.time_in_force = str(time_in_force)
        self.expire_at = float(expire_at)
        self.leverage_type = str(leverage_type)
        self.leverage = float(leverage)
        self.client_order_link_id = str(client_order_link_id or '')


class Message(CommonType):
    """
    Message object containing code, body for Logon, Logoff,
    AuthorizationGrant, System Message
    """
    def __init__(self, code: int, body: str = None):
        """

        :param code: int message code
        :param body: str message body
        """
        self.code = int(code)
        self.body = str(body)


class Balance(CommonType):
    def __init__(self,
                 currency: str,
                 full_balance: float,
                 available_balance: float):
        """

        :param currency: str currency pair symbol
        :param full_balance: float
        :param available_balance: float
        """
        self.currency = str(currency)
        self.full_balance = float(full_balance)
        self.available_balance = float(available_balance)


class OpenPosition(CommonType):
    """
    OpenPosition is a glorified immutable dict for easy storage and lookup.
    It is based on the "OpenPosition" struct in:
    https://github.com/fund3/communication-protocol/blob/master/TradeMessage.capnp
    """
    # dict storing the valid values of these types
    # https://github.com/fund3/omega_python_client/issues/38

    def __init__(self,
                 symbol: str,
                 side: str,
                 quantity: float,
                 initial_price: float,
                 unrealized_pl: float):
        """

        :param symbol: str ticker symbol
        :param side: str (see Side enum)
        :param quantity: float
        :param initial_price: float
        :param unrealized_pl: float
        """
        self.symbol = str(symbol)
        self.side = str(side)
        self.quantity = float(quantity)
        self.initial_price = float(initial_price)
        self.unrealized_pl = float(unrealized_pl)


class ExecutionReport(CommonType):
    """
    returned in response to place, modify, cancel, getOrderStatus requests
    """

    def __init__(self,
                 order_id: str,
                 client_order_id: str,
                 exchange_order_id: str,
                 account_info: AccountInfo,
                 symbol: str,
                 side: str,
                 order_type: str,
                 quantity: float,
                 price: float,
                 stop_price: float,
                 time_in_force: str,
                 expire_at: float,
                 leverage_type: str,
                 leverage: float,
                 order_status: str,
                 filled_quantity: float,
                 avg_fill_price: float,
                 fee: float,
                 creation_time: float,
                 submission_time: float,
                 completion_time: float,
                 execution_report_type: str,
                 rejection_reason: Message = None,
                 client_order_link_id: str = None):
        """

        :param order_id: str order_id as assigned by Omega
        :param client_order_id: str orderID generated on the client side
        :param exchange_order_id: str orderID as assigned by Exchange
        :param account_info: accountInfo
        :param symbol: str
        :param side: str (see Side enum)
        :param order_type: str (see OrderType enum)
        :param quantity: float
        :param price: float
        :param stop_price: float
        :param time_in_force: str (see TimeInForce enum)
        :param expire_at: float utc timestamp order expires at
        :param leverage_type: str (see LeverageType enum)
        :param leverage: float leverage being used on this specific order
        :param order_status: str (see OrderStatus enum)
        :param filled_quantity: float amount of quantity which has been filled
        :param avg_fill_price: float average price at which the order has been
        filled thus far
        :param fee: float exchange fee paid for order
        :param creation_time: float unix timestamp when order was created on
            Omega server
        :param submission_time: float unix timestamp when order was received
            by the exchange
        :param completion_time: float unix timestamp when order was completed
        :param execution_report_type: str (see ExecutionReportType enum)
        :param rejection_reason: Message rejectionReason
        :param client_order_link_id: str internal id
        """
        self.order_id = str(order_id)
        self.client_order_id = str(client_order_id)
        self.client_order_link_id = str(client_order_link_id or '')
        self.exchange_order_id = str(exchange_order_id)
        self.account_info = account_info
        self.symbol = str(symbol)
        self.side = str(side)
        self.order_type = str(order_type)
        self.quantity = float(quantity)
        self.price = float(price)
        self.stop_price = float(stop_price)
        self.time_in_force = str(time_in_force)
        self.expire_at = float(expire_at)
        self.leverage_type = str(leverage_type)
        self.leverage = float(leverage)
        self.order_status = str(order_status)
        self.filled_quantity = float(filled_quantity)
        self.avg_fill_price = float(avg_fill_price)
        self.fee = float(fee)
        self.creation_time = float(creation_time)
        self.submission_time = float(submission_time)
        self.completion_time = float(completion_time)
        self.execution_report_type = str(execution_report_type)
        self.rejection_reason = rejection_reason


class AccountDataReport(CommonType):
    def __init__(self,
                 account_info: AccountInfo,
                 balances: List[Balance],
                 open_positions: List[OpenPosition],
                 orders: List[ExecutionReport]):
        """

        :param account_info: accountInfo
        :param balances: List of Balances of all currency pairs on the
        account given in accountInfo
        :param open_positions: List of OpenPosition on the account given in
        accountInfo
        :param orders: List of ExecutionReport of orders which are currently
        active on the account given in accountInfo
        """
        self.account_info = account_info
        self.balances = list(balances)
        self.open_positions = list(open_positions)
        self.orders = list(orders)


class AccountBalancesReport(CommonType):
    def __init__(self,
                 account_info: AccountInfo, balances: List[Balance]):
        """

        :param account_info: AccountInfo
        :param balances: List of Balances of all currency pairs on the
        account given in accountInfo
        """
        self.account_info = account_info
        self.balances = list(balances)


class OpenPositionsReport(CommonType):
    def __init__(self,
                 account_info: AccountInfo,
                 open_positions: List[OpenPosition]):
        """

        :param account_info: AccountInfo
        :param open_positions: List of OpenPosition on the account given in
        accountInfo
        """
        self.account_info = account_info
        self.open_positions = list(open_positions)


class WorkingOrdersReport(CommonType):
    def __init__(self,
                 account_info: AccountInfo, orders: List[ExecutionReport]):
        """

        :param account_info: AccountInfo
        :param orders: List of ExecutionReport of orders which are currently
        active on the account given in accountInfo
        """
        self.account_info = account_info
        self.orders = list(orders)


class CompletedOrdersReport(CommonType):
    def __init__(self, account_info: AccountInfo,
                 orders: List[ExecutionReport]):
        """

        :param account_info: AccountInfo
        :param orders: List of ExecutionReport of orders completed within the
        last 24 hours on the account given in accountInfo
        """
        self.account_info = account_info
        self.orders = list(orders)


class OrderInfo(CommonType):
    def __init__(self,
                 order_id: str,
                 client_order_id: str = None,
                 client_order_link_id: str = None,
                 exchange_order_id: str = None,
                 symbol: str = None):
        """

        :param order_id: int required
        :param client_order_id: str empty in client request
        :param client_order_link_id: str empty in client request
        :param exchange_order_id: str empty in client request
        :param symbol: str empty in client request
        """
        self.order_id = str(order_id)
        self.client_order_id = (str(client_order_id)
                                if client_order_id is not None else None)
        self.client_order_link_id = str(client_order_link_id or '')
        self.exchange_order_id = str(exchange_order_id or '')
        self.symbol = str(symbol or '')


class SymbolProperties(CommonType):
    def __init__(self,
                 symbol: str,
                 price_precision: float,
                 quantity_precision: float,
                 min_quantity: float,
                 max_quantity: float,
                 margin_supported: bool,
                 leverage: Set[float]):
        """
        :param symbol: str
        :param price_precision: float
        :param quantity_precision: float
        :param min_quantity: float
        :param max_quantity: float
        :param margin_supported: bool
        :param leverage: set of float leverages supported for symbol
        """
        self.symbol = str(symbol)
        self.price_precision = float(price_precision)
        self.quantity_precision = float(quantity_precision)
        self.min_quantity = float(min_quantity)
        self.max_quantity = float(max_quantity)
        self.margin_supported = bool(margin_supported)
        self.leverage = set(leverage)


class ExchangePropertiesReport(CommonType):
    def __init__(self,
                 exchange: str,
                 currencies: Set[str],
                 symbol_properties: Dict[str, SymbolProperties],
                 time_in_forces: Set[str],
                 order_types: Set[str]):
        """
        :param exchange: str
        :param currencies: set of str active currencies on exchange
        :param symbol_properties: dict of {symbol: SymbolProperties}
        :param time_in_forces: set of supported TimeInForce across all
        currencies
        :param order_types: set of supported OrderType across all currencies
        """
        self.exchange = str(exchange)
        self.currencies = set(currencies)
        self.symbol_properties = dict(symbol_properties)
        self.time_in_forces = set(time_in_forces)
        self.order_types = set(order_types)


class ReplaceOrder(CommonType):
    def __init__(self,
                 order_id: str,
                 order_type: str = OrderType.market.name,
                 quantity: float = 0.0,
                 price: float = 0.0,
                 time_in_force: str = TimeInForce.gtc.name,
                 expire_at: float = 0.0):
        """
        :param order_type: str (see OrderType enum)
        :param quantity: float
        :param price: float
        :param time_in_force: str (see TimeInForce enum)
        :param expire_at: float (optional) utc timestamp gtt orders expire at
        """
        self.order_id = str(order_id)
        self.order_type = str(order_type)
        self.quantity = float(quantity)
        self.price = float(price)
        self.time_in_force = str(time_in_force)
        self.expire_at = float(expire_at)


class AuthorizationRefresh(CommonType):
    def __init__(self, refresh_token: str):
        """
        :param refresh_token: str refresh token
        """
        self.refresh_token = refresh_token


class AuthorizationGrant(CommonType):
    def __init__(self,
                 success: bool,
                 message: Message,
                 access_token: str,
                 refresh_token: str,
                 expire_at: float):
        """

        :param success: bool
        :param message: Message
        :param access_token: str token used for current Omega session
        :param refresh_token: str token to send to receive a new access_token
        :param expire_at: float utc timestamp at which access_token expires
        """
        self.success = bool(success)
        self.message = message
        self.access_token = str(access_token)
        self.refresh_token = str(refresh_token)
        self.expire_at = float(expire_at)


class LogonAck(CommonType):
    def __init__(self,
                 success: bool,
                 message: Message,
                 client_accounts: List[AccountInfo],
                 authorization_grant: AuthorizationGrant):
        """

        :param success: bool
        :param message: Message
        :param client_accounts: list of client accounts you can trade on
        :param authorization_grant: AuthorizationGrant (see class definition)
        """
        self.success = bool(success)
        self.message = message
        self.client_accounts = client_accounts
        self.authorization_grant = authorization_grant


class LogoffAck(CommonType):
    def __init__(self,
                 success: bool,
                 message: Message):
        """

        :param success: bool
        :param message: Message
        """
        self.success = bool(success)
        self.message = message


class SystemMessage(CommonType):
    def __init__(self,
                 account_info: AccountInfo,
                 message: Message):
        """

        :param account_info: AccountInfo
        :param message: Message
        """
        self.account_info = account_info
        self.message = message


class RequestHeader(CommonType):
    def __init__(self,
                 client_id: int,
                 sender_comp_id: str,
                 access_token: str,
                 request_id: int):
        """
        Header parameter object for requests.
        :param client_id: (int) The assigned client_id.
        :param sender_comp_id: (str) uuid unique to the user session.
        :param access_token: (str) Access token granted by Omega.  Note that
            access_token is ignored in logon.
        :param request_id: (int) sequential, monotonically increasing integer
            request identifier
        """
        self.client_id = client_id
        self.sender_comp_id = sender_comp_id
        self.access_token = access_token
        self.request_id = request_id
