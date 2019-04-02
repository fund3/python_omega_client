from datetime import datetime as dt
import logging

from tes_client.communication.request_sender import RequestSender
from tes_client.messaging.response_handler import ResponseHandler
from tes_client.messaging.common_types import *

import time
from threading import Event, Thread


logger = logging.getLogger(__name__)


class SessionRefresher(Thread):
    def __init__(self,
                 request_sender: RequestSender,
                 client_id: int,
                 sender_comp_id: str):
        super().__init__()
        self.access_token = None
        self.refresh_token = None
        self.token_expire_time = None
        self.request_sender = request_sender

        self.client_id = client_id
        self.sender_comp_id = sender_comp_id
        self.request_id = 0

        self.waiting_for_new_token = False

        self._is_running = Event()

    def is_running(self):
        """
        Return True if the thread is running, False otherwise.
        """
        return self._is_running.is_set()

    def run(self):
        """
        Threaded implementation of automatic session refresh main loop

        """
        self._is_running.set()
        while self.is_running():
            # sleep until 10 seconds before the token expires
            time_until_session_refresh = (self.token_expire_time -
                                          dt.utcnow().timestamp() - 10.)
            print('SessionRefresher sleeping {} seconds'.format(
                time_until_session_refresh))
            time.sleep(seconds=time_until_session_refresh)

            # increment the request_id
            self.request_id += 1

            # send the authorization refresh request to Omega
            self.request_sender.request_authorization_refresh(
                request_header=RequestHeader(
                    client_id=self.client_id,
                    sender_comp_id=self.sender_comp_id,
                    access_token=self.access_token,
                    request_id=self.request_id
                ),
                auth_refresh=AuthorizationRefresh(
                    refresh_token=self.refresh_token)
            )

            self.waiting_for_new_token = True

            # poll every second waiting for AuthorizationGrant Response to
            # update token
            while self.waiting_for_new_token:
                print('SessionRefresher waiting 1 second for new token')
                time.sleep(1)

        return

    def stop(self):
        """
        Clear the _is_running Event, which terminates the refresh loop.
        """
        self._is_running.clear()

    def update_token(self, auth_grant: AuthorizationGrant):
        if auth_grant.success:
            self.access_token = str(auth_grant.access_token)
            self.refresh_token = str(auth_grant.refresh_token)
            self.token_expire_time = float(auth_grant.expire_at)
            self.waiting_for_new_token = False
            print('SessionRefresher successfully updated access token')
            return True
        print('SessionRefresher failed to successfully update access token. '
              'Stopping.')
        self.stop()
        return False


class PrintingResponseHandler(ResponseHandler):
    def __init__(self, session_refresher: SessionRefresher = None):
        """
        Example class to print all responses and automatically refresh sessions
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
