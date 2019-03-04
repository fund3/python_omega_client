from tes_client.messaging.message_factory import account_balances_report_py, \
    account_data_report_py, completed_orders_report_py, \
    exchange_properties_report_py, execution_report_py, logoff_ack_py, \
    logon_ack_py, open_positions_report_py, system_message_py, \
    tes_test_message_py, working_orders_report_py


# TODO add requestID
def _heartbeat_message_unpacker(response):
    return response.clientID, response.senderCompID, response.requestID


def _test_message_unpacker(response):
    return (
        tes_test_message_py(response.body.test),
        response.clientID,
        response.senderCompID,
        response.requestID
    )


def _server_time_message_unpacker(response):
    return (
        response.body.serverTime,
        response.clientID,
        response.senderCompID,
        response.requestID
    )


def _system_message_unpacker(response):
    return (
        system_message_py(response.body.system),
        response.clientID,
        response.senderCompID,
        response.requestID
    )


def _logon_ack_message_unpacker(response):
    return (
        logon_ack_py(response.body.logonAck),
        response.clientID,
        response.senderCompID,
        response.requestID
    )


def _logoff_ack_message_unpacker(response):
    return (
        logoff_ack_py(response.body.logoffAck),
        response.clientID,
        response.senderCompID,
        response.requestID
    )


def _execution_report_message_unpacker(response):
    return (
        execution_report_py(response.body.executionReport),
        response.clientID,
        response.senderCompID,
        response.requestID
    )


def _account_data_report_message_unpacker(response):
    return (
        account_data_report_py(response.body.accountDataReport),
        response.clientID,
        response.senderCompID,
        response.requestID
    )


def _working_orders_report_message_unpacker(response):
    return (
        working_orders_report_py(response.body.workingOrdersReport),
        response.clientID,
        response.senderCompID,
        response.requestID
    )


def _account_balances_report_message_unpacker(response):
    return (
        account_balances_report_py(response.body.accountBalancesReport),
        response.clientID,
        response.senderCompID,
        response.requestID
    )


def _open_positions_report_message_unpacker(response):
    return (
        open_positions_report_py(response.body.openPositionsReport),
        response.clientID,
        response.senderCompID,
        response.requestID
    )


def _completed_orders_report_message_unpacker(response):
    return (
        completed_orders_report_py(response.body.completedOrdersReport),
        response.clientID,
        response.senderCompID,
        response.requestID
    )


def _exchange_properties_report_message_unpacker(response):
    return (
        exchange_properties_report_py(
            response.body.exchangePropertiesReport),
        response.clientID,
        response.senderCompID,
        response.requestID
    )


_tes_response_unpacker = {
    'heartbeat': _heartbeat_message_unpacker,
    'test': _test_message_unpacker,
    'serverTime': _server_time_message_unpacker,
    'system': _system_message_unpacker,
    'logonAck': _logon_ack_message_unpacker,
    'logoffAck': _logoff_ack_message_unpacker,
    'executionReport': _execution_report_message_unpacker,
    'accountDataReport': _account_data_report_message_unpacker,
    'workingOrdersReport': _working_orders_report_message_unpacker,
    'accountBalancesReport': _account_balances_report_message_unpacker,
    'openPositionsReport': _open_positions_report_message_unpacker,
    'completedOrdersReport': _completed_orders_report_message_unpacker,
    'exchangePropertiesReport': _exchange_properties_report_message_unpacker
}


def unpack_response(response_type, response):
    return _tes_response_unpacker[response_type](response)
