from typing import List, Union

from omega_client.common_types.common_type import CommonType
from omega_client.common_types.enum_types import SubscriptionType, MDEAction, \
    MDEType

# pythonized equivalents of what is contained in:
# https://github.com/fund3/OmegaProtocol/blob/master/MarketDataMessage.capnp and
# https://github.com/fund3/OmegaProtocol/blob/master/MarketDataMessage2.capnp
# make sure to call .name if you want the string representation of an enum
# object when communicating with Omega


class PairId(CommonType):
    def __init__(self, exchange: str, symbol: str):
        """
        Struct representation of a trading pair
        :param exchange: (str) name of exchange i.e. kraken
        :param symbol: (str) pair in format BASE/QUOTE i.e. BTC/USD
        """
        self.exchange = exchange
        self.symbol = symbol


class MarketDataRequest(CommonType):
    def __init__(self,
                 pair_ids: List[PairId],
                 entry_types,
                 depth: int,
                 subscription_type: SubscriptionType):
        """
        http://www.fixwiki.org/fixwiki/MarketDataRequest/FIX.5.0SP2%2B

        :param pair_ids:
        :param entry_types: (List[Union[MarketDataRequest, MarketDataSnapshot,
                                        MarketDataIncrementalRefresh]])

        :param depth: (int) 0 = full L2 orderbook, 1 = top of book (L1);
        http://www.fixwiki.org/fixwiki/MarketDepth
        :param subscription_type: (SubscriptionType)
        http://www.fixwiki.org/fixwiki/SubscriptionRequestType
        """
        self.pair_ids = pair_ids
        self.entry_types = entry_types
        self.depth = depth
        self.subscription_type = subscription_type


class MarketDataEntry(CommonType):
    def __init__(self,
                 event_timestamp: float,
                 action: MDEAction,
                 mde_type: MDEType,
                 price: float,
                 size: float,
                 position: int,
                 side: str,
                 trade_id: str):
        """
        https://github.com/fund3/OmegaProtocol/blob/master/MarketDataMessage.capnp

        :param event_timestamp: (float)
        :param action: (MDEAction)
        :param mde_type: (MDEType)
        :param price: (float)
        :param size: (float)
        :param position: (int) optional, position in orderbook, empty if the
        entry is not an orderbook update
        :param side: (str)
        :param trade_id: (str) optional
        """
        self.event_timestamp = event_timestamp
        self.action = action
        self.mde_type = mde_type
        self.price = price
        self.size = size
        self.position = position
        self.side = side
        self.trade_id = trade_id


class EntriesById(CommonType):
    def __init__(self, pair_id: PairId, entries: List[MarketDataEntry]):
        """

        :param pair_id:
        :param entries:
        """
        self.pair_id = pair_id
        self.entries = entries


class MarketDataSnapshot(CommonType):
    def __init__(self, timestamp: float, entries_by_id_list: List[EntriesById]):
        """
        http://fixwiki.org/fixwiki/MarketDataSnapshotFullRefresh/FIX.5.0SP2%2Bol

        :param timestamp: (float) unix timestamp seconds since 1/1/1970
        :param entries_by_id_list:
        """
        self.timestamp = timestamp
        self.entries_by_id_list = entries_by_id_list


class MarketDataIncrementalRefresh(CommonType):
    def __init__(self, timestamp: float, entries_by_id_list: List[EntriesById]):
        """
        http://fixwiki.org/fixwiki/MarketDataIncrementalRefresh/FIX.5.0SP2%2B

        :param timestamp: (float) unix timestamp seconds since 1/1/1970
        :param entries_by_id_list:
        """
        self.timestamp = timestamp
        self.entries_by_id_list = entries_by_id_list


class MarketDataMessage(CommonType):
    def __init__(self,
                 timestamp: float,
                 request_type: Union[MarketDataRequest, MarketDataSnapshot,
                                     MarketDataIncrementalRefresh],
                 sequence_number: int = None,
                 request_id: int = None):
        """

        :param timestamp: (float) unix timestamp seconds since 1/1/1970
        :param request_type:
        :param sequence_number:
        :param request_id:
        """
        self.timestamp = timestamp
        self.request_type = request_type
        self.sequence_number = sequence_number
        self.request_id = request_id


class MDHeader(CommonType):
    def __init__(self, client_id: int, sender_comp_id: str = None):
        """
        MarketData Header
        :param client_id: (int) The assigned client_id.
        :param sender_comp_id: (str) uuid unique to the user session.
        """
        self.client_id = client_id
        self.sender_comp_id = sender_comp_id


class L2OrderbookMarketDataEntry(CommonType):
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
                 bids: List[L2OrderbookMarketDataEntry],
                 asks: List[L2OrderbookMarketDataEntry],
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
