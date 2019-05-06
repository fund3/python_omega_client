Real Usage
**********

There are 4 classes which should be implemented/overridden by the end user:
1. `OmegaConnection`: the main thread that handles all communication between the client and Omega. There should only be one instance of `OmegaConnection`.
2. `RequestSender`: the thread that sends requests to Omega via `OmegaConnection`. There should be a unique `RequestSender` thread for each client.
3. `ResponseHandler`: event driven class to handle responses from Omega.
4. `SessionRefresher`: Thread to refresh session
