from abc import abstractmethod
import logging
from typing import List

from tes_client.messaging.common_types import AccountBalancesReport, \
    AccountCredentials, AccountDataReport, AuthorizationGrant, \
    CompletedOrdersReport, ExchangePropertiesReport, ExecutionReport, LogoffAck, \
    LogonAck, OpenPositionsReport, SystemMessage, WorkingOrdersReport
from tes_client.messaging.response_unpacker import unpack_response

logger = logging.getLogger(__name__)


class ResponseHandler:
    ###########################################################################
    #                                                                         #
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~ Incoming TESMessages ~~~~~~~~~~~~~~~~~~~~~~~ #
    #                                                                         #
    ###########################################################################
    def __init__(self):
        self._command_dispatcher = {
            'heartbeat': self.on_heartbeat,
            'test': self.on_test_message,
            'serverTime': self.on_server_time,
            'system': self.on_system_message,
            'logonAck': self.on_logon_ack,
            'logoffAck': self.on_logoff_ack,
            'executionReport': self.on_exec_report,
            'accountDataReport': self.on_account_data,
            'workingOrdersReport': self.on_working_orders_report,
            'accountBalancesReport': self.on_account_balances,
            'openPositionsReport': self.on_open_positions,
            'completedOrdersReport': self.on_completed_orders_report,
            'exchangePropertiesReport': self.on_exchange_properties_report,
            'authorizationGrant': self.on_authorization_grant
        }

    def handle_response(self, response_type, response):
        self._command_dispatcher[response_type](
            *unpack_response(response_type, response))

    @abstractmethod
    def on_heartbeat(self,
                     client_id: int,
                     sender_comp_id: str,
                     request_id: int):
        """
        Override in subclass to handle Omega heartbeat response.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        :param request_id: (int) request_id which requested this response
        """

    @abstractmethod
    def on_test_message(self,
                        string: str,
                        client_id: int,
                        sender_comp_id: str,
                        request_id: int):
        """
        Override in subclass to handle Omega test message response.
        :param string: (str) Test message from Omega.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        :param request_id: (int) request_id which requested this response
        """

    @abstractmethod
    def on_server_time(self,
                       server_time: float,
                       client_id: int,
                       sender_comp_id: str,
                       request_id: int):
        """
        Override in subclass to handle Omega test message response.
        :param server_time: (float) Server time from Omega.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        :param request_id: (int) request_id which requested this response
        """

    @abstractmethod
    def on_system_message(self,
                          system_message: SystemMessage,
                          client_id: int,
                          sender_comp_id: str,
                          request_id: int):
        """
        Override in subclass to handle Omega system message response.
        :param system_message: (SystemMessage) The system message from Omega.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        :param request_id: (int) request_id which requested this response
        """

    @abstractmethod
    def on_logon_ack(self,
                     logon_ack: LogonAck,
                     client_id: int,
                     sender_comp_id: str,
                     request_id: int):
        """
        Override in subclass to handle Omega logonAck response.
        :param logon_ack: (LogonAck) LogonAck message from Omega.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        :param request_id: (int) request_id which requested this response
        """

    @abstractmethod
    def on_logoff_ack(self,
                      logoff_ack: LogoffAck,
                      client_id: int,
                      sender_comp_id: str,
                      request_id: int):
        """
        Override in subclass to handle Omega logoffAck response.
        :param logoff_ack: (LogoffAck) LogoffAck from Omega.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        :param request_id: (int) request_id which requested this response
        """

    @abstractmethod
    def on_exec_report(self,
                       report: ExecutionReport,
                       client_id: int,
                       sender_comp_id: str,
                       request_id: int):
        """
        Override in subclass to handle Omega ExecutionReport response.
        :param report: ExecutionReport python object.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        :param request_id: (int) request_id which requested this response
        """

    @abstractmethod
    def on_account_data(self,
                        report: AccountDataReport,
                        client_id: int,
                        sender_comp_id: str,
                        request_id: int):
        """
        Override in subclass to handle Omega AccountDataReport response.
        :param report: AccountDataReport Python object.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        :param request_id: (int) request_id which requested this response
        """

    @abstractmethod
    def on_account_balances(self,
                            report: AccountBalancesReport,
                            client_id: int,
                            sender_comp_id: str,
                            request_id: int):
        """
        Override in subclass to handle Omega AccountBalancesReport response.
        :param report: AccountBalancesReport Python object.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        :param request_id: (int) request_id which requested this response
        """

    @abstractmethod
    def on_open_positions(self,
                          report: OpenPositionsReport,
                          client_id: int,
                          sender_comp_id: str,
                          request_id: int):
        """
        Override in subclass to handle Omega OpenPositionsReport response.
        :param report: OpenPositionReport Python object.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        :param request_id: (int) request_id which requested this response
        """

    @abstractmethod
    def on_working_orders_report(self,
                                 report: WorkingOrdersReport,
                                 client_id: int,
                                 sender_comp_id: str,
                                 request_id: int):
        """
        Override in subclass to handle Omega WorkingOrdersReport response.
        :param report: WorkingOrdersReport Python object.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        :param request_id: (int) request_id which requested this response
        """

    @abstractmethod
    def on_completed_orders_report(self,
                                   report: CompletedOrdersReport,
                                   client_id,
                                   sender_comp_id,
                                   request_id: int):
        """
        Override in subclass to handle Omega CompletedOrdersReport response.
        :param report: CompletedOrdersReport Python object.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        :param request_id: (int) request_id which requested this response
        """

    @abstractmethod
    def on_exchange_properties_report(self,
                                      report: ExchangePropertiesReport,
                                      client_id,
                                      sender_comp_id,
                                      request_id: int):
        """
        Override in subclass to handle Omega ExchangePropertiesReport response.
        :param report: ExchangePropertiesReport Python object.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        :param request_id: (int) request_id which requested this response
        """

    @abstractmethod
    def on_authorization_grant(self,
                               authorization_grant: AuthorizationGrant,
                               client_id,
                               sender_comp_id,
                               request_id: int):
        """
        Override in subclass to handle Omega AuthorizationGrant response.
        :param authorization_grant: AuthorizationGrant python object
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        :param request_id: (int) request_id which requested this response
        """
