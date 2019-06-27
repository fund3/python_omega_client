from typing import List

from omega_client.common_types.common_type import CommonType

# pythonized equivalents of what is contained in:
# https://github.com/fund3/OmegaProtocol/blob/master/MarketDataMessage.capnp and
# https://github.com/fund3/OmegaProtocol/blob/master/MarketDataMessage2.capnp
# make sure to call .name if you want the string representation of an enum
# object when communicating with Omega


class MDHeader(CommonType):
    def __init__(self, client_id: int, sender_comp_id: str = None):
        """
        MarketData Header
        :param client_id: (int) The assigned client_id.
        :param sender_comp_id: (str) uuid unique to the user session.
        """
        self.client_id = client_id
        self.sender_comp_id = sender_comp_id


class MarketDataEntry(CommonType):
    def __init__(self, price: float, quantity: float):
        """
        Data Structure for Level 2 orderbook data entries
        :param price: (float) a discrete price for which there is a non-zero
        "quantity" of limit orders open on the orderbook
        :param quantity: (float) positive amount of open orders available at
        "price"
        """
        self.price = price
        self.quantity = quantity


class TickerData(CommonType):
    def __init__(self,
                 exchange: str,
                 symbol: str,
                 side: str,
                 price: float,
                 quantity: float,
                 timestamp: float):
        """
        Data Structure for stream of ticks for a given symbol on an exchange
        :param exchange: (str) exchange as formatted in Exchanges.capnp i.e.
        kraken
        :param symbol: (str) i.e. BTC/USD
        :param side: (str) i.e. buy
        :param price: (float) price of last closed trade of symbol on exchange
        :param quantity: (float) quantity of last closed trade of symbol on
        exchange
        :param timestamp: (float) unix timestamp seconds since 1/1/1970
        """
        self.exchange = exchange
        self.symbol = symbol
        self.side = side
        self.price = price
        self.quantity = quantity
        self.timestamp = timestamp


class OrderbookData(CommonType):
    def __init__(self,
                 exchange: str,
                 symbol: str,
                 bids: List[MarketDataEntry],
                 asks: List[MarketDataEntry],
                 timestamp: float):
        """
        Data Structure for the Level 2 Orderbook of "symbol" on "exchange"
        :param exchange: (str) exchange as formatted in Exchanges.capnp i.e.
        kraken
        :param symbol: (str) i.e. BTC/USD
        :param bids: (List[MarketDataEntry]) ordered list of limit buy orders
        :param asks: (List[MarketDataEntry]) ordered list of limit sell orders
        :param timestamp: (float) unix timestamp seconds since 1/1/1970
        """
        self.exchange = exchange
        self.symbol = symbol
        self.bids = bids
        self.asks = asks
        self.timestamp = timestamp


class MDSystemMessage(CommonType):
    def __init__(self, code: int, body: str = None):
        """
        Data Structure for system messages from MarketDataProvider
        :param code: (int) system message code
        :param body: (str) message
        """
        self.code = code
        self.body = body
