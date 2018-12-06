from threading import Event
from typing import List
import uuid

import zmq

from tes_client.common_types import AccountCredentials, AccountInfo, \
    ExecutionReport, LeverageType, Order, OrderType, Side, TimeInForce
from tes_client.single_client_tes_connection import SingleClientTesConnection


class ExampleConnection(SingleClientTesConnection):
    def __init__(self, tes_connection_string: str,
                 zmq_context: zmq.Context, clientID: int, senderCompID: str):
        self.logged_on = Event()
        self.order_accepted = Event()
        super().__init__(tes_connection_string, zmq_context, clientID,
                         senderCompID)

    def on_exec_report(self, clientID: int, senderCompID: str,
                       report: ExecutionReport):
        print('Order executed!')
        print(report.symbol)
        print(report.side)
        print(report.quantity)
        if report.type.which() == 'orderAccepted':
            self.order_accepted.set()

    def on_logon_ack(self, clientID: int, senderCompID: str, success: bool,
                     message: str, clientAccounts: List[int]):
        if success:
            self.logged_on.set()


def main():
    # Setting up required dummy variables.
    zmq_context = zmq.Context.instance()
    tes_connection_string = 'tcp://0.0.0.0/9999'
    clientID = 1
    senderCompID = str(uuid.uuid4())
    apiKey = 'apiKey'
    secretKey = 'secretKey'
    credentials = AccountCredentials(AccountInfo(clientID), apiKey, secretKey)

    tes_conn = ExampleConnection(
        tes_connection_string, zmq_context, clientID, senderCompID)
    tes_conn.start()
    tes_conn.running.wait()
    tes_conn.logon(credentials)
    tes_conn.logged_on.wait()
    order = Order(
        accountInfo=AccountInfo(accountID=100, label='default'),
        clientOrderID=1,  # ID generated by client to keep track of the order
        clientOrderLinkID='test_model',  # A str to identify and group orders
        symbol='BTC/USD',
        side=Side.buy.name,
        orderType=OrderType.limit.name,
        quantity=1.1,
        price=6000.01,
        timeInForce=TimeInForce.gtc.name,
        leverageType=LeverageType.none.name
    )
    tes_conn.place_order(order)
    tes_conn.order_accepted.wait()
    tes_conn.logoff()


if __name__ == '__main__':
    main()