import time
from datetime import datetime as dt
from threading import Thread, Event

from omega_client.communication.request_sender import RequestSender
from omega_client.messaging.common_types import RequestHeader, \
    AuthorizationRefresh, AuthorizationGrant


class SingleClientSessionRefresher(Thread):
    def __init__(self,
                 request_sender: RequestSender,
                 client_id: int,
                 sender_comp_id: str):
        """
        This is an example class used for refreshing the session of a single
        client (1 client_id). If you have multiple users, you need to develop
        your a method on your own for refreshing the session for multiple users.
        This class is used by the logon_logoff.py example.

        :param request_sender: RequestSender class used for sending messages
        to Omega
        :param client_id: int client_id
        :param sender_comp_id: str
        """
        super().__init__()
        self.access_token = None
        self.refresh_token = None
        self.token_expire_time = None
        self.request_sender = request_sender

        self.client_id = client_id
        self.sender_comp_id = sender_comp_id
        self.request_id = 0

        self.waiting_for_new_token = False

        self._is_running = Event()

    def is_running(self):
        """
        Return True if the thread is running, False otherwise.
        """
        return self._is_running.is_set()

    def run(self):
        """
        Threaded implementation of automatic session refresh main loop
        for a single client.
        """
        self._is_running.set()
        while self.is_running():
            # sleep until 10 seconds before the token expires
            time_until_session_refresh = (self.token_expire_time -
                                          dt.utcnow().timestamp() - 10.)
            print('SessionRefresher sleeping {} seconds'.format(
                time_until_session_refresh))
            time.sleep(seconds=time_until_session_refresh)

            # increment the request_id
            self.request_id += 1

            # send the authorization refresh request to Omega
            self.request_sender.request_authorization_refresh(
                request_header=RequestHeader(
                    client_id=self.client_id,
                    sender_comp_id=self.sender_comp_id,
                    access_token=self.access_token,
                    request_id=self.request_id
                ),
                auth_refresh=AuthorizationRefresh(
                    refresh_token=self.refresh_token)
            )

            self.waiting_for_new_token = True

            # poll every second waiting for AuthorizationGrant Response to
            # update token
            while self.waiting_for_new_token:
                print('SessionRefresher waiting 1 second for new token')
                time.sleep(1)

        return

    def stop(self):
        """
        Clear the _is_running Event, which terminates the refresh loop.
        """
        self._is_running.clear()

    def update_token(self, auth_grant: AuthorizationGrant):
        if auth_grant.success:
            self.access_token = str(auth_grant.access_token)
            self.refresh_token = str(auth_grant.refresh_token)
            self.token_expire_time = float(auth_grant.expire_at)
            self.waiting_for_new_token = False
            print('SessionRefresher successfully updated access token')
            return True
        print('SessionRefresher failed to successfully update access token. '
              'Stopping.')
        self.stop()
        return False
