from omega_client.messaging.message_factory import orderbook_snapshot_py, \
    orderbook_update_py, md_system_message_py, ticker_data_py


def _ticker_data_unpacker(response):
    return ticker_data_py(response.ticker)


def _orderbook_snapshot_unpacker(response):
    return orderbook_snapshot_py(response.orderbookSnapshot)


def _orderbook_update_unpacker(response):
    return orderbook_update_py(response.orderbookUpdate)


def _system_message_unpacker(response):
    return md_system_message_py(response.systemMessage)


_omega_response_unpacker = {
    'ticker': _ticker_data_unpacker,
    'orderbookSnapshot': _orderbook_snapshot_unpacker,
    'orderbookUpdate': _orderbook_update_unpacker,
    'systemMessage': _system_message_unpacker
}


def unpack_response(response_type, response):
    return _omega_response_unpacker[response_type](response)
