import time
import uuid

from omega_client.common_types.enum_types import Channel, Exchange
from omega_client.common_types.market_data_structs import MDHeader
from omega_client.communication.mdp_connection import \
    configure_single_client_mdp_connection
from omega_client.messaging.printing_response_handler import \
    PrintingMDPResponseHandler

OMEGA_ENDPOINT = "tcp://0.0.0.0:9999"
OMEGA_SERVER_KEY = "omega_server_key"
DISTRIBUTED_CLIENTS = False


def main():
    # The client_id is the id assigned by Fund3.  It is unique
    # per client and one client can have multiple accounts (denoted by
    # account_id).
    client_id = 12345
    # Sender comp id is a uuid unique to each machine a client uses to identify
    # the machine and to route the appropriate responses back to the machine
    # that sent the request.
    sender_comp_id = str(uuid.uuid4())
    md_header = MDHeader(client_id=client_id, sender_comp_id=sender_comp_id)

    # configure_single_client_omega_connection sets up a default
    # MDPOmegaConnection with one default client_id
    # The MDPResponseHandler is a command dispatch callback class.  Basically,
    # when the response is received from Omega, MDPOmegaConnection will route
    # it to MDPResponseReceiver, and the type of the response will be
    # determined. Each type of response will trigger a specific function that
    # can be overridden in a child class of ResponseHandler for client
    # designated action upon receiving a certain type of response.  E.g.
    # updating internal orderbook data structures

    # See omega_client.messaging.mdp_response_handler and
    # omega_client.messaging.printing_response_handler (example child class
    # that just prints everything).
    omega_mdp_connection, mdp_request_sender, mdp_response_receiver = (
        configure_single_client_mdp_connection(
            omega_endpoint=OMEGA_ENDPOINT,
            omega_server_key=OMEGA_SERVER_KEY,
            mdp_response_handler=PrintingMDPResponseHandler()))
    # Starting the OmegaConnection thread.
    omega_mdp_connection.start()
    # Waiting for the MDPOmegaConnection to be set up.
    omega_mdp_connection.wait_until_running()

    # subscribe to ETH/USD L2 orderbook with depth 10 on coinbasePrime
    omega_mdp_connection.request_mdp(
        request_header=md_header,
        channels=[Channel.orderbook.name],
        exchange=Exchange.coinbasePrime.name,
        symbols=['ETH/USD'],
        market_depth=10,
        is_subscribe=True
    )

    # wait and observe orderbook data coming in
    time.sleep(60)

    # unsubscribe
    omega_mdp_connection.request_mdp(
        request_header=md_header,
        channels=[Channel.orderbook.name],
        exchange=Exchange.coinbasePrime.name,
        symbols=['ETH/USD'],
        market_depth=10,
        is_subscribe=False
    )

    # stop and cleanup
    mdp_request_sender.logoff()
    time.sleep(2)
    omega_mdp_connection.cleanup()


if __name__ == '__main__':
    main()
