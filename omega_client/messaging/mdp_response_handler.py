from abc import abstractmethod
import logging

from omega_client.common_types.market_data_structs import MDSystemMessage, \
    OrderbookData,TickerData
from omega_client.messaging.response_unpacker import unpack_response

logger = logging.getLogger(__name__)


class MDPResponseHandler:
    ###########################################################################
    #                                                                         #
    # ~~~~~~~~~~~~~~~~~~~~~~ Incoming MDP Omega Messages ~~~~~~~~~~~~~~~~~~~~ #
    #                                                                         #
    ###########################################################################
    def __init__(self):
        self._command_dispatcher = {
            'ticker': self.on_ticker_data,
            'orderbookSnapshot': self.on_orderbook_snapshot,
            'orderbookUpdate': self.on_orderbook_update,
            'systemMessage': self.on_system_message
        }

    def handle_response(self, response_type, response):
        self._command_dispatcher[response_type](
            *unpack_response(response_type, response))

    @abstractmethod
    def on_ticker_data(self,
                       client_id: int,
                       sender_comp_id: str,
                       ticker_data: TickerData):
        """
        Override in subclass to handle Omega MDP Ticker Data
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        :param ticker_data: (TickerData) latest "tick"
        """

    @abstractmethod
    def on_orderbook_snapshot(self,
                              client_id: int,
                              sender_comp_id: str,
                              orderbook_snapshot: OrderbookData):
        """
        Override in subclass to handle Omega MDP Ticker Data
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        :param orderbook_snapshot: (OrderbookData) snapshot latest L2 orderbook
        """

    @abstractmethod
    def on_orderbook_update(self,
                            client_id: int,
                            sender_comp_id: str,
                            orderbook_update: OrderbookData):
        """
        Override in subclass to handle Omega MDP Ticker Data
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        :param orderbook_update: (OrderbookData) updates in L2 orderbook
        """

    @abstractmethod
    def on_system_message(self,
                          client_id: int,
                          sender_comp_id: str,
                          system_message: MDSystemMessage):
        """
        Override in subclass to handle Omega MDP Ticker Data
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        :param system_message: (MDSystemMessage) system message from Omega MDP
        """
