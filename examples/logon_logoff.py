import time
import uuid

from tes_client.communication.tes_connection import \
    configure_single_client_tes_connection
from tes_client.messaging.common_types import AccountCredentials, AccountInfo
from tes_client.messaging.printing_response_handler import \
    PrintingResponseHandler

TES_ENDPOINT = "tcp://0.0.0.0:9999"
TES_SERVER_KEY = "tes_server_key"
DISTRIBUTED_CLIENTS = False


def main():
    # The client_id is the id assigned by Fund3.  It is unique
    # per client and one client can have multiple accounts (denoted by
    # account_id).
    client_id = 1
    # Sender comp id is a uuid unique to each machine a client uses to identify
    # the machine and to route the appropriate responses back to the machine
    # that sent the request.
    sender_comp_id = str(uuid.uuid4())

    # configure_single_client_tes_connection sets up a default TesConnection
    # with one default client_id
    # The ResponseHandler is a command dispatch callback class.  Basically,
    # when the response is received from Omega, TesConnection will route it to
    # ResponseReceiver, and the type of the response will be determined.
    # Each type of response will trigger a specific function that can be
    # overridden in a child class of ResponseHandler for client designated
    # action upon receiving a certain type of response.  E.g. updating
    # internal order status when ExecutionReport is received, updating
    # balance when balance is received etc.
    # See tes_client.messaging.response_handler and
    # tes_client.messaging.printing_response_handler (example child class
    # that just prints everything).
    tes_connection, request_sender, response_receiver = (
        configure_single_client_tes_connection(
            omega_endpoint=TES_ENDPOINT,
            omega_server_key=TES_SERVER_KEY,
            client_id=client_id,
            sender_comp_id=sender_comp_id,
            response_handler=PrintingResponseHandler()))
    # Starting the TesConnection thread.
    tes_connection.start()
    # Waiting for the TesConnection to be set up.
    tes_connection.wait_until_running()

    # Account id is assigned by Fund3 and is unique per exchange account.
    account_id = 2
    # exchange API credentials
    api_key = "api_key"
    secret_key = "secret_key"
    passphrase = "passphrase"  # Optional, only for certain exchanges.
    # Set up AccountCredentials object.
    credentials = AccountCredentials(account_info=AccountInfo(account_id),
                                     api_key=api_key,
                                     secret_key=secret_key,
                                     passphrase=passphrase)
    # Send logon message
    request_sender.logon([credentials])
    time.sleep(2)
    request_sender.send_heartbeat()
    request_sender.logoff()
    time.sleep(2)
    tes_connection.cleanup()


if __name__ == '__main__':
    main()
