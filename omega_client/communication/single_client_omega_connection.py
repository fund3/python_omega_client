import logging
from typing import List, Union

import zmq

from omega_client.communication.omega_connection import OmegaConnection, \
    REQUEST_SENDER_ENDPOINT, RESPONSE_RECEIVER_ENDPOINT
from omega_client.communication.response_receiver import ResponseReceiver
from omega_client.communication.single_client_request_sender import \
    SingleClientRequestSender
from omega_client.messaging.common_types import AccountBalancesReport, \
    AccountCredentials, AccountDataReport, AccountInfo, \
    AuthorizationRefresh, ExchangePropertiesReport, \
    ExecutionReport, OpenPositionsReport, Order, OrderInfo, \
    OrderType, TimeInForce, WorkingOrdersReport, Batch, OCO, OPO
from omega_client.messaging.single_client_response_handler import \
    SingleClientResponseHandler

logger = logging.getLogger(__name__)


class SingleClientOmegaConnection:
    """
    Abstract wrapper for OmegaConnection so that single client request calls
    can be simplified from the outside.
    """
    def __init__(self, omega_connection: OmegaConnection,
                 request_sender: SingleClientRequestSender):
        assert isinstance(request_sender,
                          SingleClientRequestSender)
        self._omega_connection = omega_connection
        self._request_sender = request_sender

    ##########################################################################
    #                                                                        #
    # ~~~~~~~~~~~~~~~~~~~~~ Wrapper for Request Sender ~~~~~~~~~~~~~~~~~~~~~ #
    #                                                                        #
    ##########################################################################
    def logon(self,
              client_secret: str,
              credentials: List[AccountCredentials]):
        """
        Logon to Omega for a specific client_id and set of credentials.
        :param client_secret: (str) client_secret key assigned by Fund3.
        :param credentials: (List[AccountCredentials]) List of exchange
            credentials in the form of AccountCredentials.
        :return: (capnp._DynamicStructBuilder) Logon capnp object.
        """
        return self._request_sender.logon(
            client_secret=client_secret,
            credentials=credentials
        )

    def logoff(self):
        """
        Logoff Omega for a specific client_id.
        :return: (capnp._DynamicStructBuilder) Logoff capnp object.
        """
        return self._request_sender.logoff()

    def send_heartbeat(self):
        """
        Sends a heartbeat to Omega for maintaining and verifying connection.
        Only clients that are logged on will receive heartbeat back from
        Omega.
        :return: (capnp._DynamicStructBuilder) heartbeat capnp object.
        """
        return self._request_sender.send_heartbeat()

    def request_server_time(self):
        """
        Request Omega server time for syncing client and server timestamps.
        :return: (capnp._DynamicStructBuilder) request_server_time capnp
        object.
        """
        return self._request_sender.request_server_time()

    def place_order(self, order: Order):
        """
        Sends a request to Omega to place an order.
        :param order: (Order) Python object containing all required fields.
        :return: (capnp._DynamicStructBuilder) place_order capnp object.
        """
        return self._request_sender.place_order(order=order)

    def place_contingent_order(self,
                               contingent_order: Union[Batch, OPO, OCO]):
        """
        Sends a request to Omega to place a contingent order.
        :param contingent_order: (Batch, OPO, or OCO) python object
        :return: (capnp._DynamicStructBuilder) placeContingentOrder capnp
        object.
        """
        return self._request_sender.place_contingent_order(
            contingent_order=contingent_order)

    def replace_order(self,
                      account_info: AccountInfo,
                      order_id: str,
                      # pylint: disable=E1101
                      order_type: str = OrderType.undefined.name,
                      quantity: float = 0.0,
                      price: float = 0.0,
                      stop_price: float = 0.0,
                      time_in_force: str = TimeInForce.gtc.name,
                      # pylint: enable=E1101
                      expire_at: float = 0.0):
        """
        Sends a request to Omega to replace an order.
        :param account_info: (AccountInfo) Account on which to cancel order.
        :param order_id: (str) order_id as returned from the
        ExecutionReport.
        :param order_type: (OrderType) (optional)
        :param quantity: (float) (optional)
        :param price: (float) (optional)
        :param stop_price: (float) (optional)
        :param time_in_force: (TimeInForce) (optional)
        :param expire_at: (float) (optional)
        :return: (capnp._DynamicStructBuilder) replaceOrder capnp object.
        """
        return self._request_sender.replace_order(
            account_info=account_info,
            order_id=order_id,
            order_type=order_type,
            quantity=quantity,
            price=price,
            stop_price=stop_price,
            time_in_force=time_in_force,
            expire_at=expire_at
        )

    def cancel_order(self,
                     account_info: AccountInfo,
                     order_id: str):
        """
        Sends a request to Omega to cancel an order.
        :param account_info: (AccountInfo) Account on which to cancel order.
        :param order_id: (str) order_id as returned from the
        ExecutionReport.
        :return: (capnp._DynamicStructBuilder) cancel_order object.
        """
        return self._request_sender.cancel_order(
            account_info=account_info,
            order_id=order_id
        )

    def cancel_all_orders(self,
                          account_info: AccountInfo,
                          symbol: str = None,
                          side: str = None):
        """
        Sends a request to Omega to cancel an order.
        :param account_info: (AccountInfo) Account on which to cancel order.
        :param symbol: (str) (optional)
        :param side: (str) (optional)
        :return (capnp._DynamicStructBuilder) cancel_all_orders object.
        """
        return self._request_sender.cancel_all_orders(
            account_info=account_info,
            symbol=symbol,
            side=side
        )

    def request_account_data(self,
                             account_info: AccountInfo):
        """
        Sends a request to Omega for full account snapshot including
        balances,
        open positions, and working orders on specified account.
        :param account_info: (AccountInfo) Account from which to retrieve
        data.
        :return: (capnp._DynamicStructBuilder) request_account_data capnp
            object.
        """
        return self._request_sender.request_account_data(
            account_info=account_info
        )

    def request_open_positions(self,
                               account_info: AccountInfo):
        """
        Sends a request to Omega for open positions on an Account.
        :param account_info: (AccountInfo) Account from which to retrieve
        data.
        :return: (capnp._DynamicStructBuilder) request_open_positions capnp
            object.
        """
        return self._request_sender.request_open_positions(
            account_info=account_info
        )

    def request_account_balances(self,
                                 account_info: AccountInfo):
        """
        Sends a request to Omega for full account balances snapshot on an
        Account.
        :param account_info: (AccountInfo) Account from which to retrieve
        data.
        :return: (capnp._DynamicStructBuilder) request_account_balances
        capnp
            object.
        """
        return self._request_sender.request_account_balances(
            account_info=account_info
        )

    def request_working_orders(self,
                               account_info: AccountInfo):
        """
        Sends a request to Omega for all working orders snapshot on an
        Account.
        :param account_info: (AccountInfo) Account from which to retrieve
        data.
        :return: (capnp._DynamicStructBuilder) request_working_orders capnp
            object.
        """
        return self._request_sender.request_working_orders(
            account_info=account_info
        )

    def request_order_status(self,
                             account_info: AccountInfo,
                             order_id: str):
        """
        Sends a request to Omega to request status of a specific order.
        :param account_info: (AccountInfo) Account from which to retrieve
        data.
        :param order_id: (str) The id of the order of interest.
        :return: (capnp._DynamicStructBuilder) request_order_status capnp
            object.
        """
        return self._request_sender.request_order_status(
            account_info=account_info,
            order_id=order_id
        )

    def request_completed_orders(self,
                                 account_info: AccountInfo,
                                 count: int = None,
                                 since: float = None):
        """
        Sends a request to Omega for all completed orders on specified
        account.  If both 'count' and 'from_unix' are None, returns orders
        for last 24h.
        :param account_info: (AccountInfo) Account from which to retrieve
        data.
        :param count: (int) optional, number of returned orders (most recent
            ones).
        :param since: (float) optional, returns all orders from provided
        unix
            timestamp to present.
        :return: (capnp._DynamicStructBuilder) request_completed_orders
        capnp
            object.
        """
        return self._request_sender.request_completed_orders(
            account_info=account_info,
            count=count,
            since=since
        )

    def request_exchange_properties(self,
                                    exchange: str):
        """
        Sends a request to Omega for supported currencies, symbols and their
        associated properties, timeInForces, and orderTypes on an exchange.
        :param exchange: (str) The exchange of interest.
        :return: (capnp._DynamicStructBuilder)
        request_exchange_properties capnp
            object.
        """
        return self._request_sender.request_exchange_properties(
            exchange=exchange
        )

    def request_authorization_refresh(self,
                                      auth_refresh: AuthorizationRefresh):
        """
        Sends a request to Omega to refresh the session
        :param auth_refresh: AuthorizationRefresh python object
        :return: (capnp._DynamicStructBuilder) authorization_refresh capnp
            object.
        """
        return self._request_sender.request_authorization_refresh(
            auth_refresh=auth_refresh
        )


def configure_single_client_omega_connection(
        zmq_context: zmq.Context,
        omega_endpoint: str,
        omega_server_key: str,
        client_id: int,
        sender_comp_id: str,
        response_handler: SingleClientResponseHandler):
    """
    Set up a OmegaConnection that comes with request_sender and
    response_receiver.  Sets the default client_id and sender_comp_id for
    the request sender.
    Note that each machine should be assigned a unique sender_comp_id even
    when the client_id is the same.
    :param omega_endpoint: (str) The zmq endpoint to connect to Omega.
    :param omega_server_key: (str) The public key of the Omega server.
    :param client_id: (int) The client id assigned by Fund3.
    :param sender_comp_id: (str) str representation of a unique Python uuid.
    :param response_handler: (ResponseHandler) The handler object that will
        be called in a callback function when omega_connection receives a
        message.
    :return: omega_connection
    """
    request_sender = SingleClientRequestSender(
        zmq_context=zmq_context,
        zmq_endpoint=REQUEST_SENDER_ENDPOINT,
        client_id=client_id,
        sender_comp_id=sender_comp_id)
    response_handler.set_request_sender(request_sender=request_sender)
    response_receiver = ResponseReceiver(
        zmq_context=zmq_context,
        zmq_endpoint=RESPONSE_RECEIVER_ENDPOINT,
        response_handler=response_handler)
    omega_connection = OmegaConnection(
        zmq_context=zmq_context,
        omega_endpoint=omega_endpoint,
        request_sender_endpoint=REQUEST_SENDER_ENDPOINT,
        response_receiver_endpoint=RESPONSE_RECEIVER_ENDPOINT,
        request_sender=request_sender,
        response_receiver=response_receiver,
        server_zmq_encryption_key=omega_server_key)
    single_client_omega_connection = SingleClientOmegaConnection(
        omega_connection=omega_connection,
        request_sender=request_sender
    )
    return single_client_omega_connection
