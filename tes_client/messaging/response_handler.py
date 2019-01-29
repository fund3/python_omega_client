from abc import abstractmethod
import logging
from typing import List

from tes_client.messaging.common_types import AccountBalancesReport, \
    AccountCredentials, AccountDataReport, CompletedOrdersReport, \
    ExchangePropertiesReport, ExecutionReport, OpenPositionsReport, \
    WorkingOrdersReport
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
            'system': self.on_system_message,
            'logonAck': self.on_logon_ack,
            'logoffAck': self.on_logoff_ack,
            'executionReport': self.on_exec_report,
            'accountDataReport': self.on_account_data,
            'workingOrdersReport': self.on_working_orders_report,
            'accountBalancesReport': self.on_account_balances,
            'openPositionsReport': self.on_open_positions,
            'completedOrdersReport': self.on_completed_orders_report,
            'exchangePropertiesReport': self.on_exchange_properties_report
        }

    def handle_response(self, response_type, response):
        self._command_dispatcher[response_type](
            *unpack_response(response_type, response))

    @abstractmethod
    def on_heartbeat(self, client_id: int, sender_comp_id: str):
        """
        Override in subclass to handle TES heartbeat response.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        """

    @abstractmethod
    def on_test_message(self,
                        string: str,
                        client_id: int,
                        sender_comp_id: str):
        """
        Override in subclass to handle TES test message response.
        :param string: (str) Test message from TES.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        """

    @abstractmethod
    def on_system_message(self,
                          error_code: int,
                          message: str,
                          client_id: int,
                          sender_comp_id: str):
        """
        Override in subclass to handle TES system message response.
        :param error_code: (int) The error_code from TES.
        :param message: (str) The error message from TES.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        """

    @abstractmethod
    def on_logon_ack(self,
                     success: bool,
                     message: str,
                     client_accounts: List[int],
                     client_id: int,
                     sender_comp_id: str):
        """
        Override in subclass to handle TES logonAck response.
        :param success: (bool) True if logon is successful, False otherwise.
        :param message: (str) Logon message from TES.
        :param client_accounts: (List[int]) A list of *all* account_ids that are
            logged on in the current logon request, including accounts that are
            logged on in previous logon requests.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        """

    @abstractmethod
    def on_logoff_ack(self,
                      success: bool,
                      message: str,
                      client_id: int,
                      sender_comp_id: str):
        """
        Override in subclass to handle TES logoffAck response.
        :param success: (bool) If True, logoff is successful, False otherwise.
        :param message: (str) Logoff message from TES.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        """

    @abstractmethod
    def on_exec_report(self,
                       report: ExecutionReport,
                       client_id: int,
                       sender_comp_id: str):
        """
        Override in subclass to handle TES ExecutionReport response.
        :param report: ExecutionReport python object.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        """

    @abstractmethod
    def on_account_data(self,
                        report: AccountDataReport,
                        client_id: int,
                        sender_comp_id: str):
        """
        Override in subclass to handle TES AccountDataReport response.
        :param report: AccountDataReport Python object.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        """

    @abstractmethod
    def on_account_balances(self,
                            report: AccountBalancesReport,
                            client_id: int,
                            sender_comp_id: str):
        """
        Override in subclass to handle TES AccountBalancesReport response.
        :param report: AccountBalancesReport Python object.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        """

    @abstractmethod
    def on_open_positions(self,
                          report: OpenPositionsReport,
                          client_id: int,
                          sender_comp_id: str):
        """
        Override in subclass to handle TES OpenPositionsReport response.
        :param report: OpenPositionReport Python object.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        """

    @abstractmethod
    def on_working_orders_report(self,
                                 report: WorkingOrdersReport,
                                 client_id: int,
                                 sender_comp_id: str):
        """
        Override in subclass to handle TES WorkingOrdersReport response.
        :param report: WorkingOrdersReport Python object.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        """

    @abstractmethod
    def on_completed_orders_report(self,
                                   report: CompletedOrdersReport,
                                   client_id,
                                   sender_comp_id):
        """
        Override in subclass to handle TES CompletedOrdersReport response.
        :param report: CompletedOrdersReport Python object.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        """

    @abstractmethod
    def on_exchange_properties_report(self,
                                      report: ExchangePropertiesReport,
                                      client_id,
                                      sender_comp_id):
        """
        Override in subclass to handle TES ExchangePropertiesReport response.
        :param report: ExchangePropertiesReport Python object.
        :param client_id: (int) client_id of the response.
        :param sender_comp_id: (str) sender_comp_id of the response.
        """
