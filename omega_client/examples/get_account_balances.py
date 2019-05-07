import time
import uuid

from omega_client.communication.omega_connection import \
    configure_single_client_omega_connection
from omega_client.messaging.common_types import (AccountCredentials,
    AccountInfo, LeverageType, Order, OrderType, Side, TimeInForce)
from omega_client.messaging.printing_response_handler import \
    PrintingResponseHandler

OMEGA_ENDPOINT = "tcp://0.0.0.0:9999"
OMEGA_SERVER_KEY = "omega_server_key"


"""
In this example, a single client with 2 accounts logs onto Omega and 
requests account balances for both accounts. 

You can use the same template for requesting open positions, working orders, 
account data, and completed orders. 
"""


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

    account_id0 = 100
    account_id1 = 101

    api_key0 = "api_key"
    secret_key0 = "secret_key"
    passphrase0 = "passphrase"
    api_key1 = "api_key1"
    secret_key1 = "secret_key1"

    account_info0 = AccountInfo(account_id0)
    account_info1 = AccountInfo(account_id1)

    credentials0 = AccountCredentials(account_info0,
                                      api_key0,
                                      secret_key0,
                                      passphrase0)

    # account1 doesn't have a passphrase
    credentials1 = AccountCredentials(account_info1, api_key1, secret_key1)

    request_sender.logon([credentials0, credentials1])
    request_sender.send_heartbeat()

    request_sender.request_account_balances(account_info=account_info0)
    request_sender.request_account_balances(account_info=account_info1)
    time.sleep(2)
    request_sender.logoff()
    time.sleep(2)
    omega_connection.cleanup()


if __name__ == '__main__':
    main()
