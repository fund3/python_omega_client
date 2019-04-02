import time
import uuid

from omega_client.communication.omega_connection import \
    configure_single_client_omega_connection
from omega_client.messaging.common_types import AccountCredentials, AccountInfo
from omega_client.messaging.printing_response_handler import \
    PrintingResponseHandler

OMEGA_ENDPOINT = "tcp://0.0.0.0:9999"
OMEGA_SERVER_KEY = "omega_server_key"
DISTRIBUTED_CLIENTS = False


def main():
    client_id = 1
    # sender_comp_id is a unique identifier for a omega_client.  Omega supports the
    # use case of multiple omega_clients sending messages with the same
    # client_id, hence a sender_comp_id is needed to distinguish the machine
    # and client in the middle of a request and response communication.
    # Clients would have to manage their own client_id and sender_comp_id.
    client_id_machine_dict = dict()
    # The simplest approach is to generate a uuid per machine per clientId as
    # the senderCompId and store them in a dictionary.
    # Feel free to generate uuids with an approach that fits your use case,
    # but this is the recommended way by Python:
    if DISTRIBUTED_CLIENTS:
        # Unique uuid across different machines, taking into account the MAC
        # address
        sender_comp_id = str(uuid.uuid1())
    else:
        # Random uuid
        sender_comp_id = str(uuid.uuid4())
    client_id_machine_dict[client_id] = sender_comp_id

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
    passphrase = "passphrase"  # Optional, only for certain exchanges
    credentials = AccountCredentials(AccountInfo(account_id), api_key,
                                     secret_key, passphrase)

    request_sender.logon([credentials])
    request_sender.send_heartbeat()
    request_sender.logoff()
    time.sleep(2)
    omega_connection.cleanup()


if __name__ == '__main__':
    main()
