import logging
from typing import List

from tes_client.messaging.response_handler import ResponseHandler
from tes_client.messaging.common_types import ExecutionReport

logger = logging.getLogger(__name__)


class PrintingResponseHandler(ResponseHandler):
    def on_heartbeat(self, client_id: int, sender_comp_id: str):
        print(client_id, sender_comp_id)

    def on_logon_ack(self,
                     success: bool,
                     message: str,
                     client_accounts: List[int],
                     client_id: int,
                     sender_comp_id: str):
        print(success, message, client_accounts, client_id, sender_comp_id)

    def on_logoff_ack(self,
                      success: bool,
                      message: str,
                      client_id: int,
                      sender_comp_id: str):
        print(success, message, client_id, sender_comp_id)

    def on_exec_report(self, report: ExecutionReport,
                       client_id: int,
                       sender_comp_id: str):
        print('Order executed!')
        print(report.symbol)
        print(report.side)
        print(report.quantity)
