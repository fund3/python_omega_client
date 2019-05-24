from datetime import datetime as dt
import logging
from threading import Timer

from omega_client.messaging.common_types import AccountBalancesReport, \
    AccountCredentials, AccountDataReport, AuthorizationGrant, \
    AuthorizationRefresh, CompletedOrdersReport, ExchangePropertiesReport, \
    ExecutionReport, LogoffAck, LogonAck, OpenPositionsReport, SystemMessage,\
    WorkingOrdersReport
from omega_client.messaging.response_handler import ResponseHandler

logger = logging.getLogger(__name__)


class SingleClientResponseHandler(ResponseHandler):
    ###########################################################################
    #                                                                         #
    # ~~~~~~~~~~~~~~~~~~~~~~~~~ Incoming OmegaMessages ~~~~~~~~~~~~~~~~~~~~~~ #
    #                                                                         #
    ###########################################################################
    def __init__(self):
        self._command_dispatcher = {
            'heartbeat': self.on_heartbeat,
            'test': self.on_test_message,
            'serverTime': self.on_server_time,
            'system': self.on_system_message,
            'logonAck': self._on_logon_ack,
            'logoffAck': self.on_logoff_ack,
            'executionReport': self.on_exec_report,
            'accountDataReport': self.on_account_data,
            'workingOrdersReport': self.on_working_orders_report,
            'accountBalancesReport': self.on_account_balances,
            'openPositionsReport': self.on_open_positions,
            'completedOrdersReport': self.on_completed_orders_report,
            'exchangePropertiesReport': self.on_exchange_properties_report,
            'authorizationGrant': self._on_authorization_grant
        }
        self._request_sender = None
        self._refresh_token = None

    def set_request_sender(self, request_sender):
        self._request_sender = request_sender

    def _on_logon_ack(self,
                      logon_ack: LogonAck,
                      client_id: int,
                      sender_comp_id: str,
                      request_id: int):
        """
        Internal
        :param logon_ack: (LogonAck) LogonAck message from Omega.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        :param request_id: (int) request_id which requested this response
        :return:
        """
        if (logon_ack and logon_ack.success and logon_ack.authorization_grant
            and logon_ack.authorization_grant.success
        ):
            self._request_sender.set_access_token(
                logon_ack.authorization_grant.access_token)
            self._refresh_token = logon_ack.authorization_grant.refresh_token
            self._send_authorization_refresh()
        else:
            if not logon_ack.success:
                logger.error('logon_ack error: ',
                             extra={'message': logon_ack.message})
            if not logon_ack.authorization_grant.success:
                logger.error(
                    'authorization_grant error: ',
                    extra={'message': logon_ack.authorization_grant.message})
        self.on_logon_ack(
            logon_ack=logon_ack,
            client_id=client_id,
            sender_comp_id=sender_comp_id,
            request_id=request_id)

    def _on_authorization_grant(self,
                                authorization_grant: AuthorizationGrant,
                                client_id: int,
                                sender_comp_id: str,
                                request_id: int):
        """
        Override in subclass to handle Omega AuthorizationGrant response.
        :param authorization_grant: AuthorizationGrant python object
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        :param request_id: (int) request_id which requested this response
        """
        if authorization_grant and authorization_grant.success:
            self._request_sender.set_access_token(
                authorization_grant.access_token)
            self._refresh_token = authorization_grant.refresh_token
            self._token_expire_time = authorization_grant.expire_at
            time_until_session_refresh = (self._token_expire_time -
                                          dt.utcnow().timestamp() - 30.)
            Timer(time_until_session_refresh,
                  self._send_authorization_refresh).start()
        else:
            if not authorization_grant.success:
                logger.error(
                    'authorization_grant error: ',
                    extra={'message': authorization_grant.message})
        self.on_authorization_grant(
            authorization_grant=authorization_grant,
            client_id=client_id,
            sender_comp_id=sender_comp_id,
            request_id=request_id)

    def _send_authorization_refresh(self):
        self._request_sender.request_authorization_refresh(
            auth_refresh=AuthorizationRefresh(
                refresh_token=self._refresh_token)
        )
