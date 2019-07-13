import logging

from omega_client.examples.single_client_session_refresher import \
    SingleClientSessionRefresher
from omega_client.messaging.mdp_response_handler import MDPResponseHandler
from omega_client.messaging.response_handler import ResponseHandler
from omega_client.common_types.market_data_structs import *
from omega_client.common_types.trading_structs import *

logger = logging.getLogger(__name__)


class PrintingResponseHandler(ResponseHandler):
    def __init__(self, session_refresher: SingleClientSessionRefresher = None):
        """
        Example class to print all responses and automatically refresh sessions.
        """
        super().__init__()
        self.session_refresher = session_refresher

    def on_server_time(self, server_time: float, client_id: int,
                       sender_comp_id: str, request_id: int):
        pass

    def on_system_message(self, system_message: SystemMessage, client_id: int,
                          sender_comp_id: str, request_id: int):
        pass

    def on_account_data(self, report: AccountDataReport, client_id: int,
                        sender_comp_id: str, request_id: int):
        pass

    def on_account_balances(self, report: AccountBalancesReport, client_id: int,
                            sender_comp_id: str, request_id: int):
        pass

    def on_open_positions(self, report: OpenPositionsReport, client_id: int,
                          sender_comp_id: str, request_id: int):
        pass

    def on_working_orders_report(self, report: WorkingOrdersReport,
                                 client_id: int, sender_comp_id: str,
                                 request_id: int):
        pass

    def on_completed_orders_report(self, report: CompletedOrdersReport,
                                   client_id, sender_comp_id, request_id: int):
        pass

    def on_exchange_properties_report(self, report: ExchangePropertiesReport,
                                      client_id, sender_comp_id,
                                      request_id: int):
        pass

    def on_test_message(self, string: str, client_id: int, sender_comp_id: str,
                        request_id: int):
        pass

    def on_heartbeat(self,
                     client_id: int,
                     sender_comp_id: str,
                     request_id: int):
        print({'message': 'Heartbeat received!',
               'client_id': client_id,
               'sender_comp_id': sender_comp_id,
               'request_id': request_id})

    def on_logon_ack(self,
                     logon_ack: LogonAck,
                     client_id: int,
                     sender_comp_id: str,
                     request_id: int):
        print(
            {'success': logon_ack.success,
             'message_body': logon_ack.message.body,
             'message_code': logon_ack.message.code,
             'client_accounts': logon_ack.client_accounts,
             'auth_grant_success': logon_ack.authorization_grant.success,
             'auth_grant_msg_body': logon_ack.authorization_grant.message.body,
             'auth_grant_msg_code': logon_ack.authorization_grant.message.code,
             'auth_grant_accessToken':
                 logon_ack.authorization_grant.access_token,
             'auth_grant_refreshToken':
                 logon_ack.authorization_grant.refresh_token,
             'auth_grant_expireAt':
                 logon_ack.authorization_grant.expire_at,
             'client_id': client_id,
             'sender_comp_id': sender_comp_id,
             'request_id': request_id}
        )
        # start session refresher thread
        self.session_refresher.update_token(logon_ack.authorization_grant)
        self.session_refresher.start()

    def on_authorization_grant(self,
                               authorization_grant: AuthorizationGrant,
                               client_id,
                               sender_comp_id,
                               request_id: int):
        print(
            {'auth_grant_success': authorization_grant.success,
             'auth_grant_message_body': authorization_grant.message.body,
             'auth_grant_message_code': authorization_grant.message.code,
             'auth_grant_access_token': authorization_grant.access_token,
             'auth_grant_refresh_token': authorization_grant.refresh_token,
             'auth_grant_expire_at': authorization_grant.expire_at,
             'client_id': client_id,
             'sender_comp_id': sender_comp_id,
             'request_id': request_id}
        )
        # update session_refresher
        self.session_refresher.update_token(authorization_grant)

    def on_logoff_ack(self,
                      logoff_ack: LogoffAck,
                      client_id: int,
                      sender_comp_id: str,
                      request_id: int):
        print({'success': logoff_ack.success,
               'message_body': logoff_ack.message.body,
               'message_code': logoff_ack.message.code,
               'client_id': client_id,
               'sender_comp_id': sender_comp_id})

    def on_exec_report(self, report: ExecutionReport,
                       client_id: int,
                       sender_comp_id: str,
                       request_id: int):
        print({'message': 'Order executed!', 'report': report})


"""
========================= MDP Response Handler =================================
"""


class PrintingMDPResponseHandler(MDPResponseHandler):
    def __init__(self):
        """
        Example class to print all responses and automatically refresh sessions.
        """
        super().__init__()

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
        print('client_id: ', client_id)
        print('sender_comp_id: ', sender_comp_id)
        print('ticker_data: ', ticker_data.__str__())

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
        print('client_id: ', client_id)
        print('sender_comp_id: ', sender_comp_id)
        print('orderbook_snapshot: ', orderbook_snapshot.__str__())

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
        print('client_id: ', client_id)
        print('sender_comp_id: ', sender_comp_id)
        print('orderbook_update: ', orderbook_update.__str__())

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
        print('client_id: ', client_id)
        print('sender_comp_id: ', sender_comp_id)
        print('system_message: ', system_message.__str__())
