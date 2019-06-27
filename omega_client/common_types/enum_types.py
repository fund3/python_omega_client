from enum import Enum, auto


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
    hitBTC = auto()


class Side(Enum):
    """Trading Sides

    https://github.com/fund3/OmegaProtocol/blob/master/TradeMessage.capnp
    """
    undefined = auto()
    buy = auto()
    sell = auto()


class OrderClass(Enum):
    """Order Class

    https://github.com/fund3/OmegaProtocol/blob/master/TradeMessage.capnp
    """
    simple = auto()
    compound = auto()


class OrderType(Enum):
    """Supported Order Types

    https://github.com/fund3/OmegaProtocol/blob/master/TradeMessage.capnp
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

    https://github.com/fund3/OmegaProtocol/blob/master/TradeMessage.capnp
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
    pendingUpdate = auto()


class TimeInForce(Enum):
    """Order Time In Force

    https://github.com/fund3/OmegaProtocol/blob/master/TradeMessage.capnp
    """
    undefined = auto()
    gtc = auto()        # Good till cancel
    gtt = auto()        # Good till time
    day = auto()        # Day order
    ioc = auto()        # Immediate or cancel
    fok = auto()        # Fill or kill


class LeverageType(Enum):
    """Leverage Type

    https://github.com/fund3/OmegaProtocol/blob/master/TradeMessage.capnp
    """
    none = auto()
    exchangeDefault = auto()
    custom = auto()


class AccountType(Enum):
    """Account Type

    https://github.com/fund3/OmegaProtocol/blob/master/TradeMessage.capnp
    """
    undefined = auto()
    exchange = auto()
    margin = auto()
    combined = auto()


class ContigentType(Enum):
    """Contigent Type

    https://github.com/fund3/OmegaProtocol/blob/master/TradeMessage.capnp
    """
    none = auto()
    batch = auto()  # Batch (list of orders each independent of the other)
    oco = auto()    # Order cancel other
    opo = auto()    # Order place order(s)


class Channel(Enum):
    """
    Channels available for Market Data Subscription
    https://github.com/fund3/OmegaProtocol/blob/master/MarketDataMessage2.capnp
    """
    ticker = auto()
    orderbook = auto()  # level 2 orderbook data
