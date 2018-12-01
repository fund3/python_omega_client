"""
senderCompId is a unique identifier for a tes_client.  TES supports the use
case of multiple tes_clients sending messages with the same client_id, hence a
senderCompId is needed to distinguish the machine and client in the middle of a
request and response communication.
Clients would have to manage their own clientId and senderCompId.
"""
import uuid

from tes_client import TesConnection

DISTRIBUTED_CLIENTS = False


def main():
    clientId_machine_dict = dict()
    clientId1 = 1
    # The simplest approach is to generate a uuid per machine per clientId as
    # the senderCompId and stored in a dictionary.
    # Feel free to generate uuids with an approach that fits your use case,
    # but this is the recommended way by Python:
    if DISTRIBUTED_CLIENTS:
        # Unique uuid across different machines, taking into account the MAC
        # address
        senderCompId1 = uuid.uuid1()
    else:
        # Random uuid
        senderCompId1 = uuid.uuid4()
    clientId_machine_dict[clientId1] = senderCompId1
    tes = TesConnection()
    # Assuming client is logged on
    tes.send_heartbeat(clientID=clientId1, senderCompID=senderCompId1)


if __name__ == '__main__':
    main()