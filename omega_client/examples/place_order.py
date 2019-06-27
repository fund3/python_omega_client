import time
import uuid

from omega_client.communication.omega_connection import \
    configure_single_client_omega_connection
from omega_client.common_types.trading_structs import (AccountCredentials,
                                                       AccountInfo, LeverageType, Order, OrderType, Side, TimeInForce)
from omega_client.messaging.printing_response_handler import \
    PrintingResponseHandler

OMEGA_ENDPOINT = "tcp://0.0.0.0:9999"
OMEGA_SERVER_KEY = "omega_server_key"


def main():
    client_id = 1
    sender_comp_id = str(uuid.uuid4())
    omega_connection, request_sender, response_receiver = (
        configure_single_client_omega_connection(OMEGA_ENDPOINT,
                                                 OMEGA_SERVER_KEY,
                                                 client_id,
                                                 sender_comp_id,
                                                 PrintingResponseHandler()))

    omega_connection.start()
    omega_connection.wait_until_running()

    account_id = 2
    api_key = "api_key"
    secret_key = "secret_key"
    passphrase = "passphrase"
    credentials = AccountCredentials(AccountInfo(account_id), api_key,
                                     secret_key, passphrase)

    request_sender.logon([credentials])
    request_sender.send_heartbeat()
    order = Order(
        account_info=AccountInfo(account_id=account_id),
        # ID generated by client to keep track of the order
        client_order_id=str(123),
        client_order_link_id='test',  # A str to identify and group orders
        symbol='ETH/USD',
        side=Side.sell.name,
        order_type=OrderType.market.name,  # Optional param
        quantity=1.1,
        price=0.0,
        time_in_force=TimeInForce.gtc.name,
        leverage_type=LeverageType.none.name
    )
    request_sender.place_order(order)
    time.sleep(2)
    request_sender.logoff()
    time.sleep(2)
    omega_connection.cleanup()


if __name__ == '__main__':
    main()
