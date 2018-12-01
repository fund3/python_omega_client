from enum import Enum, auto
from typing import List, Dict, Set

# TODO discuss making types "immutable". See:
# https://www.blog.pythonlibrary.org/2014/01/17/how-to-create-immutable-classes-in-python/

# pythonized equivalents of what is contained in:
# https://github.com/fund3/communication-protocol/blob/master/TradeMessage.capnp
# make sure to call .name if you want the string representation of an enum
# object when communicating with TES


class AutoName(Enum):
    """ Enum class with attribute values equal to names

    The attribute values are automatically set to the attribute name.
    """
    def _generate_next_value_(name, start, count, last_values):
        return name


class Exchange(AutoName):
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


class Side(AutoName):
    """Trading Sides

    https://github.com/fund3/CommunicationProtocol/blob/master/TradeMessage.capnp
    """
    undefined = auto()
    buy = auto()
    sell = auto()


class OrderType(AutoName):
    """Supported Order Types

    https://github.com/fund3/CommunicationProtocol/blob/master/TradeMessage.capnp
    """
    undefined = auto()
    market = auto()
    limit = auto()


class OrderStatus(AutoName):
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


class TimeInForce(AutoName):
    """Order Time In Force

    https://github.com/fund3/CommunicationProtocol/blob/master/TradeMessage.capnp
    """
    undefined = auto()
    gtc = auto()        # Good till cancel
    gtt = auto()        # Good till time
    day = auto()        # Day order
    ioc = auto()        # Immediate or cancel
    fok = auto()        # Fill or kill


class LeverageType(AutoName):
    """Leverage Type

    https://github.com/fund3/CommunicationProtocol/blob/master/TradeMessage.capnp
    """
    none = auto()
    exchangeDefault = auto()
    custom = auto()


class AccountType(AutoName):
    """Account Type

    https://github.com/fund3/CommunicationProtocol/blob/master/TradeMessage.capnp
    """
    undefined = auto()
    exchange = auto()
    margin = auto()
    combined = auto()


# TODO make all below objects immutable

class AccountInfo:
    def __init__(self, accountID: int,
                 exchange: str=None,
                 exchangeAccountID: str=None,
                 accountType: str=None,
                 exchangeClientID: str=None):
        """
        :param accountID: int id corresponding to an account on an exchange
        Required.
        :param exchange: str exchange in which accountID is contained
        :param exchangeAccountID: str account/wallet id, empty in client request
        :param accountType: str exchange account type (exchange,
        margin, combined), empty in client request (will replace label)
        :param exchangeClientID: str exchange client (customer) ID,
        empty in client request
        """
        self.accountID = int(accountID)
        self.exchange = str(exchange or '')
        self.exchangeAccountID = str(exchangeAccountID or '')
        self.accountType = str(accountType or '')
        self.exchangeClientID = str(exchangeClientID or '')

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class AccountCredentials:
    def __init__(self, accountInfo: AccountInfo, apiKey: str, secretKey: str,
                 passphrase: str=None):
        """
        AccountCredentials object is used for logon
        :param accountInfo: AccountInfo object containing accountID
        :param apiKey: str apiKey for connecting to exchange API
        associated with accountID
        :param secretKey: str secretKey for connecting to exchange API
        associated with accountID
        :param passphrase: str (optional) passphrase for connecting to API
        associated with accountID
        """
        self.accountInfo = accountInfo
        self.apiKey = str(apiKey)
        self.secretKey = str(secretKey)
        self.passphrase = str(passphrase or '')

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class Order:
    """
    object created for placing a new Order.
    """
    def __init__(self, accountInfo: AccountInfo,
                 clientOrderID: int,
                 symbol: str,
                 side: Side,
                 orderType: OrderType,
                 quantity: float,
                 price: float,
                 timeInForce: str = TimeInForce.gtc.name,
                 leverageType: str = LeverageType.none.name,
                 leverage: float = 0.0,
                 clientOrderLinkID: str = None):
        """

        :param accountInfo: AccountInfo
        :param clientOrderID: int orderID generated on the client side
        :param accountInfo: accountInfo
        :param symbol: str
        :param side: str (see Side enum)
        :param orderType: str (see OrderType enum)
        :param quantity: float
        :param price: float
        :param timeInForce: str (see TimeInForce enum)
        :param leverageType: str (see LeverageType enum)
        :param leverage: float leverage being used on this specific order
        :param clientOrderLinkID: str used for identifying strategy (when
        multiple strategies are trading on the same account)
        """
        self.accountInfo = accountInfo
        self.clientOrderID = int(clientOrderID)
        self.clientOrderLinkID = str(clientOrderLinkID or '')
        self.symbol = str(symbol)
        self.side = str(side.name)
        self.orderType = str(orderType.name)
        self.quantity = float(quantity)
        self.price = float(price)
        self.timeInForce = str(timeInForce)
        self.leverageType = str(leverageType)
        self.leverage = float(leverage)

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class RequestRejected:
    def __init__(self, message: str=None):
        """

        :param message: str rejection reason
        """
        self.message = str(message or '')

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class Balance:
    def __init__(self, currency: str, fullBalance: float,
                 availableBalance: float):
        """

        :param currency: str currency pair symbol
        :param fullBalance: float
        :param availableBalance: float
        """
        self.currency = str(currency)
        self.fullBalance = float(fullBalance)
        self.availableBalance = float(availableBalance)

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class OpenPosition:
    """
    OpenPosition is a glorified immutable dict for easy storage and lookup.
    It is based on the "OpenPosition" struct in:
    https://github.com/fund3/communication-protocol/blob/master/TradeMessage.capnp
    """
    # TODO dict storing the valid values of these types
    def __init__(self,
                 symbol: str,
                 side: str,
                 quantity: float,
                 initialPrice: float,
                 unrealizedPL: float):
        """

        :param symbol: str ticker symbol
        :param side: str (see Side enum)
        :param quantity: float
        :param initialPrice: float
        :param unrealizedPL: float
        """
        self.symbol = str(symbol)
        self.side = str(side)
        self.quantity = float(quantity)
        self.initialPrice = float(initialPrice)
        self.unrealizedPL = float(unrealizedPL)

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class ExecutionReportType(AutoName):
    """Execution Report Type"""
    orderAccepted = auto()
    orderRejected = auto()
    orderReplaced = auto()
    replaceRejected = auto()
    orderCanceled = auto()
    cancelRejected = auto()
    orderFilled = auto()
    statusUpdate = auto()
    statusUpdateRejected = auto()


class ExecutionReport:
    """
    returned in response to place, modify, cancel, getOrderStatus requests
    """
    def __init__(self, orderID: str,
                 clientOrderID: int,
                 exchangeOrderID: str,
                 accountInfo: AccountInfo,
                 symbol: str,
                 side: str,
                 orderType: str,
                 quantity: float,
                 price: float,
                 timeInForce: str,
                 leverageType: str,
                 leverage: float,
                 orderStatus: str,
                 filledQuantity: float,
                 avgFillPrice: float,
                 executionReportType: str,
                 rejectionReason: str=None,
                 clientOrderLinkID: str=None):
        """

        :param orderID: str orderID as assigned by TES
        :param clientOrderID: int orderID generated on the client side
        :param clientOrderLinkID: str internal id used for
        :param exchangeOrderID: str orderID as assigned by Exchange
        :param accountInfo: accountInfo
        :param symbol: str
        :param side: str (see Side enum)
        :param orderType: str (see OrderType enum)
        :param quantity: float
        :param price: float
        :param timeInForce: str (see TimeInForce enum)
        :param leverageType: str (see LeverageType enum)
        :param leverage: float leverage being used on this specific order
        :param orderStatus: str (see OrderStatus enum)
        :param filledQuantity: float amount of quantity which has been filled
        :param avgFillPrice: float average price at which the order has been
        filled thus far
        :param executionReportType: str (see ExecutionReportType enum)
        :param rejectionReason: str rejectionReason
        """
        self.orderID = str(orderID)
        self.clientOrderID = int(clientOrderID)
        self.clientOrderLinkID = str(clientOrderLinkID or '')
        self.exchangeOrderID = str(exchangeOrderID)
        self.accountInfo = accountInfo
        self.symbol = str(symbol)
        self.side = Side[side].name
        self.orderType = OrderType[orderType].name
        self.quantity = float(quantity)
        self.price = float(price)
        self.timeInForce = TimeInForce[timeInForce].name
        self.leverageType = LeverageType[leverageType].name
        self.leverage = float(leverage)
        self.orderStatus = str(orderStatus)
        self.filledQuantity = float(filledQuantity)
        self.avgFillPrice = float(avgFillPrice)
        self.executionReportType = str(executionReportType)
        self.rejectionReason = str(rejectionReason) or ''

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class AccountDataReport:
    def __init__(self, accountInfo: AccountInfo,
                 balances: List[Balance],
                 openPositions: List[OpenPosition],
                 orders: List[ExecutionReport]):
        """

        :param accountInfo: accountInfo
        :param balances: List of Balances of all currency pairs on the
        account given in accountInfo
        :param openPositions: List of OpenPosition on the account given in
        accountInfo
        :param orders: List of ExecutionReport of orders which are currently
        active on the account given in accountInfo
        """
        self.accountInfo = accountInfo
        self.balances = list(balances)
        self.openPositions = list(openPositions)
        self.orders = list(orders)

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class AccountBalancesReport:
    def __init__(self, accountInfo: AccountInfo, balances: List[Balance]):
        """

        :param accountInfo: AccountInfo
        :param balances: List of Balances of all currency pairs on the
        account given in accountInfo
        """
        self.accountInfo = accountInfo
        self.balances = list(balances)

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class OpenPositionsReport:
    def __init__(self, accountInfo: AccountInfo,
                 openPositions: List[OpenPosition]):
        """

        :param accountInfo: AccountInfo
        :param openPositions: List of OpenPosition on the account given in
        accountInfo
        """
        self.accountInfo = accountInfo
        self.openPositions = list(openPositions)

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class WorkingOrdersReport:
    def __init__(self, accountInfo: AccountInfo, orders: List[ExecutionReport]):
        """

        :param accountInfo: AccountInfo
        :param orders: List of ExecutionReport of orders which are currently
        active on the account given in accountInfo
        """
        self.accountInfo = accountInfo
        self.orders = list(orders)

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class CompletedOrdersReport:
    def __init__(self, accountInfo: AccountInfo, orders: List[ExecutionReport]):
        """

        :param accountInfo: AccountInfo
        :param exchange: str
        :param orders: List of ExecutionReport of orders completed within the
        last 24 hours on the account given in accountInfo
        """
        self.accountInfo = accountInfo
        self.orders = list(orders)

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class OrderInfo:
    def __init__(self, orderID: str,
                 clientOrderID: int=None,
                 clientOrderLinkID: str=None,
                 exchangeOrderID: str=None,
                 symbol: str=None):
        """

        :param orderID: int required
        :param clientOrderID: int empty in client request
        :param clientOrderLinkID: str empty in client request
        :param exchangeOrderID: str empty in client request
        :param symbol: str empty in client request
        """
        self.orderID = str(orderID)
        self.clientOrderID = int(clientOrderID) if clientOrderID is not None \
            else None
        self.clientOrderLinkID = str(clientOrderLinkID or '')
        self.exchangeOrderID = str(exchangeOrderID or '')
        self.symbol = str(symbol or '')

    def __str__(self):
        return str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class SymbolProperties:
    def __init__(self, symbol: str,
                 pricePrecision: float,
                 quantityPrecision: float,
                 minQuantity: float,
                 maxQuantity: float,
                 marginSupported: bool,
                 leverage: Set[float]):
        """
        :param symbol: str
        :param pricePrecision: float
        :param quantityPrecision: float
        :param minQuantity: float
        :param maxQuantity: float
        :param marginSupported: bool
        :param leverage: set of float leverages supported for symbol
        """
        self.symbol = str(symbol)
        self.pricePrecision = float(pricePrecision)
        self.quantityPrecision = float(quantityPrecision)
        self.minQuantity = float(minQuantity)
        self.maxQuantity = float(maxQuantity)
        self.marginSupported = bool(marginSupported)
        self.leverage = set(leverage)


class ExchangePropertiesReport:
    def __init__(self, exchange: str,
                 currencies: Set[str],
                 symbolProperties: Dict[str, SymbolProperties],
                 timeInForces: Set[str],
                 orderTypes: Set[str]):
        """
        :param exchange: str
        :param currencies: set of str active currencies on exchange
        :param symbolProperties: dict of {symbol: SymbolProperties}
        :param timeInForces: set of supported TimeInForce across all currencies
        :param orderTypes: set of supported OrderType across all currencies
        """
        self.exchange = str(exchange)
        self.currencies = set(currencies)
        self.symbolProperties = dict(symbolProperties)
        self.timeInForces = set(timeInForces)
        self.orderTypes = set(orderTypes)


class ReplaceOrder:
    def __init__(self, orderID: str,
                 orderType: str=OrderType.market.name,
                 quantity: float=-1.0,
                 price: float=-1.0,
                 timeInForce: str=TimeInForce.gtc.name):
        """
        :param orderType: str (see OrderType enum)
        :param quantity: float
        :param price: float
        :param timeInForce: str (see TimeInForce enum)
        """
        self.orderID = str(orderID)
        self.orderType = str(orderType)
        self.quantity = float(quantity)
        self.price = float(price)
        self.timeInForce = str(timeInForce)
