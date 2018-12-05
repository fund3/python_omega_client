from abc import abstractmethod
import logging
import time
from threading import Event, Thread
from typing import List

import capnp
import zmq

from tes_client.common_types import AccountBalancesReport, \
    AccountCredentials, AccountDataReport, AccountInfo, Balance, \
    CompletedOrdersReport, Exchange, ExchangePropertiesReport, \
    ExecutionReport, OpenPosition, OpenPositionsReport, Order, OrderInfo, \
    OrderType, SymbolProperties, TimeInForce, WorkingOrdersReport
import communication_protocol.TradeMessage_capnp as msgs_capnp
from tes_client.tes_message_factory import build_account_balances_report, \
    build_account_data_report, build_completed_orders_report, \
    build_exchange_properties_report, build_exec_report, build_logoff, \
    build_logon, build_open_positions_report, build_system_message, \
    build_test_message, build_working_orders_report, cancel_order_message, \
    heartbeat_message, logoff_message, logon_message, place_order_message, \
    replace_order_message, request_account_balances_message, \
    request_account_data_message, request_completed_orders, \
    request_exchange_properties_message, request_open_positions_message, \
    request_order_mass_status_message, request_order_status_message, \
    request_working_orders_message

logger = logging.getLogger(__name__)


class TesConnection(Thread):
    """
    Base TesConnection class that abstracts out ZMQ connection, capn-proto
    parsing, and communication with the TES.
    Actions like Placing Orders, Requesting Account Balances and passing
    their associated responses from TES as callbacks are handled by this class.
    Also handles heartbeats to maintain connection.

    Attributes:
        _tes_connection_string: (str) of ip address, port for connecting to
        TES, in the form of a zmq connection str 'protocol://interface:port',
        e.g. 'tcp://0.0.0.0:9999'
        _zmq_context: (zmq.Context) Required to create sockets. It is
            recommended that one application use one shared zmq context for
            all sockets.
        _tes_connection_socket: (zmq.Socket) The primary socket that sends
            and receives messages from TES.
        _TES_POLLING_TIMEOUT_MILLI: (int) The polling timeout for
            _tes_connection_socket.
        running:
    """
    def __init__(self,
                 tes_connection_string: str,
                 zmq_context: zmq.Context,
                 tes_polling_timeout_milli: int=1000,
                 name: str='TesConnection'):
        """
        :param tes_polling_timeout_milli: int millisecond TES polling
        interval. Leave as default unless specifically instructed to change.
        :param name: str name of the thread (used for debugging)
        """
        assert tes_connection_string
        assert zmq_context is not None
        self._tes_connection_string = tes_connection_string
        self._zmq_context = zmq_context
        self._tes_connection_socket = None
        self._TES_POLLING_TIMEOUT_MILLI = tes_polling_timeout_milli
        super().__init__(name=name)
        self.running = Event()

    def _send(self, tes_mess):
        if self._tes_connection_socket is None:
            logger.error('Uninitialized TES zmq connection socket.')
            return

        self._tes_connection_socket.send(tes_mess.to_bytes())
    """
    ############################################################################
    
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Thread Methods ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    ############################################################################
    """

    def stop(self):
        logger.debug('Stopping engine..')
        self.running.clear()
        logger.debug('..done.')

    def cleanup(self):
        if self._tes_connection_socket:
            self._tes_connection_socket.close()

    def run(self):
        logger.debug('Creating TES DEALER socket:',
                     extra={'action': 'creating_socket',
                            'socket_type': 'zmq.DEALER'})
        self._tes_connection_socket = self._zmq_context.socket(zmq.DEALER)
        logger.debug('Connecting to TES socket:',
                     extra={'action': 'connect_to_tes',
                            'connection_string': self._tes_connection_string})
        self._tes_connection_socket.connect(self._tes_connection_string)
        poller = zmq.Poller()
        poller.register(self._tes_connection_socket, zmq.POLLIN)

        logger.debug('Zmq poller registered.  Waiting for message execution '
                     'responses.', extra={'polling_interval':
                                          self._TES_POLLING_TIMEOUT_MILLI})
        self.running.set()
        while self.running.is_set():
            socks = dict(poller.poll(self._TES_POLLING_TIMEOUT_MILLI))
            if socks.get(self._tes_connection_socket) == zmq.POLLIN:
                message = self._tes_connection_socket.recv()
                self._handle_tes_message(message)
        time.sleep(2.)
        self.cleanup()

    """
    ############################################################################
    
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Incoming TESMessages ~~~~~~~~~~~~~~~~~~~~~~~~~~

    ############################################################################
    """
    def _handle_tes_message(self, binary_msg):
        """
        Callback when a tes_message is received and passed to an appriopriate
        event handler method.
        :param binary_msg: (capnp._DynamicStructBuilder) The received
            tesMessage.
        """
        logger.debug('Received TESMessage..')
        try:
            tes_msg = msgs_capnp.TradeMessage.from_bytes(binary_msg)
            response = tes_msg.type.response
        except (Exception, TypeError) as e:
            logger.error('Exception in decoding message',
                         extra={'exception': repr(e)})
            return
        which = response.body.which()

        if which == 'heartbeat':
            logger.debug('Received heartbeat ack from TES.',
                         extra={'type': 'heartbeat_ack'})
            self.on_heartbeat(response.clientID, response.senderCompID)
        elif which == 'test':
            test = response.body.test
            logger.debug('Test Message:',
                         extra={'test_message': test.string})
            self.on_test_message(build_test_message(test),
                                 response.clientID,
                                 response.senderCompID)
        elif which == 'system':
            system = response.body.system
            logger.debug('System Message:',
                         extra={'error_code': system.errorCode,
                                'system_message': system.message})
            self.on_system_message(*build_system_message(system), 
                                   response.clientID,
                                   response.senderCompID)
        elif which == 'logonAck':
            logon_ack = response.body.logonAck
            logger.debug('LogonAck Message:',
                         extra={'clientID': response.clientID,
                                'senderCompID': response.senderCompID})
            self.on_logon_ack(*build_logon(logon_ack),
                              response.clientID,
                              response.senderCompID)
        elif which == 'logoffAck':
            logoff_ack = response.body.logoffAck
            logger.debug('LogoffAck Message:',
                         extra={'clientID': response.clientID,
                                'senderCompID': response.senderCompID})
            self.on_logoff_ack(*build_logoff(logoff_ack),
                               response.clientID,
                               response.senderCompID)
        elif which == 'executionReport':
            execution_report = response.body.executionReport
            logger.debug('Received executionReport Message:',
                         extra={'clientID': response.clientID,
                                'senderCompID': response.senderCompID})
            self.on_exec_report(build_exec_report(execution_report),
                                response.clientID,
                                response.senderCompID)
        elif which == 'accountDataReport':
            acct_data_report = response.body.accountDataReport
            logger.debug('Received account data report.',
                         extra={'type': 'account_data_report',
                                'acct_data_report': str(acct_data_report)})
            self.on_account_data(build_account_data_report(acct_data_report),
                                 response.clientID,
                                 response.senderCompID)
        elif which == 'workingOrdersReport':
            working_orders_report = response.body.workingOrdersReport
            logger.debug('Received working orders report.',
                         extra={'type': 'working_orders_report',
                                'working_orders_report': str(
                                   working_orders_report)})
            self.on_working_orders_report(
                build_working_orders_report(working_orders_report),
                response.clientID,
                response.senderCompID)
        elif which == 'accountBalancesReport':
            acct_bals_report = response.body.accountBalancesReport
            logger.debug('Received exchange account balances.',
                         extra={'type': 'account_balance_report',
                               'acct_bals_report': str(acct_bals_report)})
            self.on_account_balances(
                build_account_balances_report(acct_bals_report),
                response.clientID,
                response.senderCompID)
        elif which == 'openPositionsReport':
            open_pos_report = response.body.openPositionsReport
            logger.debug('Received open positions report.',
                         extra={'type': 'open_positions_report',
                                'open_positions_report': str(open_pos_report)})
            self.on_open_positions(
                build_open_positions_report(open_pos_report),
                response.clientID,
                response.senderCompID)
        elif which == 'completedOrdersReport':
            completed_orders_report = response.body.completedOrdersReport
            logger.debug('Received completed orders report.',
                         extra={'type': 'completed_orders_report',
                                'completed_orders_report': str(
                                   completed_orders_report)})
            self.on_completed_orders_report(
                build_completed_orders_report(completed_orders_report),
                response.clientID,
                response.senderCompID)
        elif which == 'exchangePropertiesReport':
            exchange_properties_report = response.body.exchangePropertiesReport
            logger.debug('Received exchange properties report.',
                         extra={'type': 'exchange_properties_report',
                                'exchange_properties_report': str(
                                   exchange_properties_report)})
            self.on_exchange_properties_report(
                build_exchange_properties_report(exchange_properties_report),
                response.clientID,
                response.senderCompID)

    @abstractmethod
    def on_heartbeat(self, clientID: int, senderCompID: str):
        """
        Override in subclass to handle TES heartbeat response.
        :param clientID: (int) clientID of the response.
        :param senderCompID: (str) senderCompID of the response.
        """

    @abstractmethod
    def on_test_message(self, string: str, clientID: int, senderCompID: str):
        """
        Override in subclass to handle TES test message response.
        :param string: (str) Test message from TES.
        :param clientID: (int) clientID of the response.
        :param senderCompID: (str) senderCompID of the response.
        """

    @abstractmethod
    def on_system_message(self, errorCode: int, 
                          message: str, 
                          clientID: int, 
                          senderCompID: str):
        """
        Override in subclass to handle TES system message response.
        :param errorCode: (int) The errorCode from TES.
        :param message: (str) The error message from TES.
        :param clientID: (int) clientID of the response.
        :param senderCompID: (str) senderCompID of the response.
        """

    @abstractmethod
    def on_logon_ack(self, success: bool,
                     message: str, 
                     clientAccounts: List[int], 
                     clientID: int, 
                     senderCompID: str):
        """
        Override in subclass to handle TES logonAck response.
        :param success: (bool) True if logon is successful, False otherwise.
        :param message: (str) Logon message from TES.
        :param clientAccounts: (List[int]) A list of *all* accountIDs that are
            logged on in the current logon request, including accounts that are
            logged on in previous logon requests.
        :param clientID: (int) clientID of the response.
        :param senderCompID: (str) senderCompID of the response.
        """

    @abstractmethod
    def on_logoff_ack(self, success: bool, 
                      message: str, 
                      clientID: int, 
                      senderCompID: str):
        """
        Override in subclass to handle TES logoffAck response.
        :param success: (bool) If True, logoff is successful, False otherwise.
        :param message: (str) Logoff message from TES.
        :param clientID: (int) clientID of the response.
        :param senderCompID: (str) senderCompID of the response.
        """

    @abstractmethod
    def on_exec_report(self, report: ExecutionReport, 
                       clientID: int, 
                       senderCompID: str):
        """
        Override in subclass to handle TES ExecutionReport response.
        :param report: ExecutionReport python object.
        :param clientID: (int) clientID of the response.
        :param senderCompID: (str) senderCompID of the response.
        """

    @abstractmethod
    def on_account_data(self, report: AccountDataReport,
                        clientID: int, 
                        senderCompID: str):
        """
        Override in subclass to handle TES AccountDataReport response.
        :param report: AccountDataReport Python object.
        :param clientID: (int) clientID of the response.
        :param senderCompID: (str) senderCompID of the response.
        """

    @abstractmethod
    def on_account_balances(self, report: AccountBalancesReport,
                            clientID: int, 
                            senderCompID: str):
        """
        Override in subclass to handle TES AccountBalancesReport response.
        :param report: AccountBalancesReport Python object.
        :param clientID: (int) clientID of the response.
        :param senderCompID: (str) senderCompID of the response.
        """

    @abstractmethod
    def on_open_positions(self, report: OpenPositionsReport,
                          clientID: int, 
                          senderCompID: str):
        """
        Override in subclass to handle TES OpenPositionsReport response.
        :param report: OpenPositionReport Python object.
        :param clientID: (int) clientID of the response.
        :param senderCompID: (str) senderCompID of the response.
        """

    @abstractmethod
    def on_working_orders_report(self, report: WorkingOrdersReport,
                                 clientID: int, 
                                 senderCompID: str):
        """
        Override in subclass to handle TES WorkingOrdersReport response.
        :param report: WorkingOrdersReport Python object.
        :param clientID: (int) clientID of the response.
        :param senderCompID: (str) senderCompID of the response.
        """

    @abstractmethod
    def on_completed_orders_report(self, report: CompletedOrdersReport,
                                   clientID, 
                                   senderCompID):
        """
        Override in subclass to handle TES CompletedOrdersReport response.
        :param report: CompletedOrdersReport Python object.
        :param clientID: (int) clientID of the response.
        :param senderCompID: (str) senderCompID of the response.
        """

    @abstractmethod
    def on_exchange_properties_report(self, report: ExchangePropertiesReport,
                                      clientID, 
                                      senderCompID):
        """
        Override in subclass to handle TES ExchangePropertiesReport response.
        :param report: ExchangePropertiesReport Python object.
        :param clientID: (int) clientID of the response.
        :param senderCompID: (str) senderCompID of the response.
        """

    """
    ############################################################################
    
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~ Outgoing TESMessages ~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ----------------- Public Methods to be called by client -------------------
    
    ############################################################################
    """
    def logon(self, credentials: List[AccountCredentials], clientID: int,
              senderCompID: str):
        """
        Logon to TES for a specific clientID and set of credentials.
        :param clientID: (int) The assigned clientID.
        :param senderCompID: (str) uuid unique to the session the user is on.
            It is recommended to use Python3 built-in uuid.uuid1() to avoid
            collision when running tes_connection on multiple machines.
        :param credentials: (List[AccountCredentials]) List of exchange
            credentials in the form of AccountCredentials.
        :return: (capnp._DynamicStructBuilder) Logon capnp object.
        """
        tesMessage, logon = logon_message(clientID, senderCompID, credentials)
        logger.debug('Sending TES logon msg.',
                     extra={'clientID': clientID,
                            'senderCompID': senderCompID})
        self._send(tesMessage)
        return logon

    def logoff(self, clientID: int, senderCompID: str):
        """
        Logoff TES for a specific clientID.
        :param clientID: (int) The assigned clientID.
        :param senderCompID: (str) uuid unique to the session the user is on.
        :return: (capnp._DynamicStructBuilder) Logoff capnp object.
        """
        tesMessage, body = logoff_message(clientID, senderCompID)
        logger.debug('Sending logoff',
                     extra={'clientID': clientID,
                            'senderCompID': senderCompID})
        self._send(tesMessage)
        return body

    def send_heartbeat(self, clientID: int, senderCompID: str):
        """
        Sends a heartbeat to TES for maintaining and verifying connection.
        Only clients that are logged on will receive heartbeat back from TES.
        :param clientID: (int) The assigned clientID.
        :param senderCompID: (str) uuid unique to the session the user is on.
        :return: (capnp._DynamicStructBuilder) heartbeat capnp object.
        """
        tesMessage, body = heartbeat_message(clientID, senderCompID)
        logger.debug('Sending heartbeat.',
                     extra={'clientID': clientID,
                            'senderCompID': senderCompID})
        self._send(tesMessage)
        return body

    def place_order(self, order: Order, clientID: int, senderCompID: str):
        """
        Sends a request to TES to place an order.
        :param clientID: (int) The assigned clientID.
        :param senderCompID: (str) uuid unique to the session the user is on.
        :param order: (Order) Python object containing all required fields.
        :return: (capnp._DynamicStructBuilder) placeOrder capnp object.
        """
        tesMessage, placeOrder = place_order_message(
            clientID, senderCompID, order)
        logger.debug('Sending Place Order.',
                     extra={'accountID': placeOrder.accountInfo.accountID,
                            'clientOrderID': order.clientOrderID,
                            'clientOrderLinkID': order.clientOrderLinkID,
                            'symbol': order.symbol,
                            'side': order.side,
                            'orderType': order.orderType,
                            'quantity': str(order.quantity),
                            'price': str(order.price),
                            'timeInForce': order.timeInForce,
                            'leverageType': order.leverageType,
                            'leverage': str(order.leverage)})
        self._send(tesMessage)
        return placeOrder

    def replace_order(self, accountInfo: AccountInfo, orderID: str,
                      clientID: int, senderCompID: str,
                      orderType: str=OrderType.market.name,
                      quantity: float=-1.0, price: float=-1.0,
                      timeInForce: str=TimeInForce.gtc.name):
        """
        Sends a request to TES to replace an order.
        :param clientID: (int) The assigned clientID.
        :param senderCompID: (str) uuid unique to the session the user is on.
        :param accountInfo: (AccountInfo) Account on which to cancel order.
        :param orderID: (str) order_id as returned from the ExecutionReport.
        :param orderType: (OrderType) (OPTIONAL)
        :param quantity: (float) (OPTIONAL)
        :param price: (float) (OPTIONAL)
        :param timeInForce: (TimeInForce) (OPTIONAL)
        :return: (capnp._DynamicStructBuilder) replaceOrder capnp object.
        """
        tesMessage, replaceOrder = replace_order_message(
            clientID, senderCompID, accountInfo, orderID, orderType,
            quantity, price, timeInForce)
        logger.debug('Sending Replace Order ',
                     extra={'orderID': orderID,
                            'orderType': orderType,
                            'quantity': str(quantity),
                            'price': str(price),
                            'timeInForce': timeInForce})
        self._send(tesMessage)
        return replaceOrder

    def cancel_order(self, accountInfo: AccountInfo, orderID: str,
                     clientID: int, senderCompID: str):
        """
        Sends a request to TES to cancel an order.
        :param clientID: (int) The assigned clientID.
        :param senderCompID: (str) uuid unique to the session the user is on.
        :param accountInfo: (AccountInfo) Account on which to cancel order.
        :param orderID: (str) order_id as returned from the ExecutionReport.
        :return: (capnp._DynamicStructBuilder) cancelOrder object.
        """
        tesMessage, cancelOrder = cancel_order_message(
            clientID, senderCompID, accountInfo, orderID)
        logger.debug('Cancelling Order.', extra={'order_id': orderID})
        self._send(tesMessage)
        return cancelOrder

    def request_account_data(self, accountInfo: AccountInfo, clientID: int,
                             senderCompID: str):
        """
        Sends a request to TES for full account snapshot including balances,
        open positions, and working orders on specified exchange.
        :param clientID: (int) The assigned clientID.
        :param senderCompID: (str) uuid unique to the session the user is on.
        :param accountInfo: (AccountInfo) Account from which to retrieve data.
        :return: (capnp._DynamicStructBuilder) getAccountData capnp object.
        """
        tesMessage, getAccountData = request_account_data_message(
            clientID, senderCompID, accountInfo)
        logger.debug('Requesting Account Data.',
                     extra={'accountID': str(accountInfo.accountID)})
        self._send(tesMessage)
        return getAccountData

    def request_open_positions(self, accountInfo: AccountInfo, clientID: int,
                               senderCompID: str):
        """
        Sends a request to TES for open positions on an Account.
        :param clientID: (int) The assigned clientID.
        :param senderCompID: (str) uuid unique to the session the user is on.
        :param accountInfo: (AccountInfo) Account from which to retrieve data.
        :return: (capnp._DynamicStructBuilder) getOpenPositions capnp object.
        """
        tesMessage, getOpenPositions = request_open_positions_message(
            clientID, senderCompID, accountInfo)
        logger.debug('Requesting Open Positions',
                     extra={'accountID': str(accountInfo.accountID)})
        self._send(tesMessage)
        return getOpenPositions

    def request_account_balances(self, accountInfo: AccountInfo,
                                 clientID: int, senderCompID: str):
        """
        Sends a request to TES for full account balances snapshot on an Account.
        :param clientID: (int) The assigned clientID.
        :param senderCompID: (str) uuid unique to the session the user is on.
        :param accountInfo: (AccountInfo) Account from which to retrieve data.
        :return: (capnp._DynamicStructBuilder) getAccountBalances capnp object.
        """
        tesMessage, getAccountBalances = request_account_balances_message(
            clientID, senderCompID, accountInfo)
        logger.debug('Requesting Account Balances',
                     extra={'accountID': str(accountInfo.accountID)})
        self._send(tesMessage)
        return getAccountBalances

    def request_working_orders(self, accountInfo: AccountInfo, clientID: int,
                               senderCompID: str):
        """
        Sends a request to TES for all working orders snapshot on an Account.
        :param clientID: (int) The assigned clientID.
        :param senderCompID: (str) uuid unique to the session the user is on.
        :param accountInfo: (AccountInfo) Account from which to retrieve data.
        :return: (capnp._DynamicStructBuilder) getWorkingOrders capnp object.
        """
        tesMessage, getWorkingOrders = request_working_orders_message(
            clientID, senderCompID, accountInfo)
        logger.debug('Requesting Working Orders.',
                     extra={'accountID': str(accountInfo.accountID)})
        self._send(tesMessage)
        return getWorkingOrders

    def request_order_status(self, accountInfo: AccountInfo, orderID: str,
                             clientID: int, senderCompID: str):
        """
        Sends a request to TES to request status of a specific order.
        :param clientID: (int) The assigned clientID.
        :param senderCompID: (str) uuid unique to the session the user is on.
        :param accountInfo: (AccountInfo) Account from which to retrieve data.
        :param orderID: (str) The id of the order of interest.
        :return: (capnp._DynamicStructBuilder) getOrderStatus capnp object.
        """
        tesMessage, getOrderStatus = request_order_status_message(
            clientID, senderCompID, accountInfo, orderID)
        logger.debug('Requesting Order Status.',
                     extra={'accountID': str(accountInfo.accountID),
                            'orderID': orderID})
        self._send(tesMessage)
        return getOrderStatus

    def request_completed_orders(self, accountInfo: AccountInfo,
                                 clientID: int, senderCompID: str,
                                 count: int=None, since: float=None):
        """
        Sends a request to TES for all completed orders on specified account.
        If both 'count' and 'from_unix' are None, returns orders for last 24h.
        :param clientID: (int) The assigned clientID.
        :param senderCompID: (str) uuid unique to the session the user is on.
        :param accountInfo: (AccountInfo) Account from which to retrieve data.
        :param count: (int) optional, number of returned orders (most recent
            ones).
        :param since: (float) optional, returns all orders from provided unix
            timestamp to present.
        :return: (capnp._DynamicStructBuilder) getCompletedOrders capnp object.
        """
        tesMessage, getCompletedOrders = request_completed_orders(
            clientID, senderCompID, accountInfo, count, since)
        logger.debug('Requesting Working Orders.',
                     extra={'accountID': str(accountInfo.accountID)})
        self._send(tesMessage)
        return getCompletedOrders

    def request_order_mass_status(self, accountInfo: AccountInfo,
                                  orderInfo: List[OrderInfo], clientID: int,
                                  senderCompID: str):
        """
        Sends a request to TES for status of multiple orders.
        :param clientID: (int) The assigned clientID.
        :param senderCompID: (str) uuid unique to the session the user is on.
        :param accountInfo: (AccountInfo) Account from which to retrieve data.
        :param orderInfo: (List[OrderInfo]) List of orderIDs to get status
            updates.
        :return: (capnp._DynamicStructBuilder) getOrderMassStatus capnp object.
        """
        tesMessage, getOrderMassStatus = request_order_mass_status_message(
            clientID, senderCompID, accountInfo, orderInfo)
        logger.debug('Requesting Mass Order Status.',
                     extra={'accountID': str(accountInfo.accountID),
                            'orderIDs': str([oid.orderID for oid in
                                             orderInfo])})
        self._send(tesMessage)
        return getOrderMassStatus

    def request_exchange_properties(self, exchange: str, clientID: int,
                                    senderCompID: str):
        """
        Sends a request to TES for supported currencies, symbols and their
        associated properties, timeInForces, and orderTypes on an exchange.
        :param clientID: (int) The assigned clientID.
        :param senderCompID: (str) uuid unique to the session the user is on.
        :param exchange: (str) The exchange of interest.
        :return: (capnp._DynamicStructBuilder) getExchangeProperties capnp
            object.
        """
        tesMessage, getExchangeProperties = request_exchange_properties_message(
            clientID, senderCompID, exchange)
        logger.debug('Requesting Exchange Properties.',
                     extra={'exchange': exchange})
        self._send(tesMessage)
        return getExchangeProperties
