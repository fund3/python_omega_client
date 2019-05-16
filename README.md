# Omega Python Client
API Wrapper for connecting to Fund3 Omega Order Execution and Management System

## Requirements
1. gcc 4.8+
2. Python3.5+
3. Access to `python_omega_client` and `TradingCommunicationProtocol` Fund3 repos.
4. A valid SSH Key on the machine or cloned python_omega_client repo.
5. For now we assume there is a symlink pointing Python versions to `python3`
and `pip3`, but that will be changed in the future to detect python versions.


## Installation

See Dockerfiles in `docker` directory.  Note that AWS Linux 1 and AWS Linux 2
contains instructions to install the library on respective AMIs on AWS.  You
will need to install gcc 4.8+ on AWS Linux 1 for the installation to work. The 
Dockerfiles are using `root` user by default.
Python3.5 and Python3.6 installations are identical as long as `python3` and
`pip3` are used.  Do not use sudo for these commands unless the user version 
of Python3 is the same as the root version of Python3 and you know what you 
are doing since you may run into issues with symlink etc.

Alternative ways:
1. Clone the repo and run `python3 setup.py install` or `pip3 install .` in the
root directory of the repo.
2. Alternatively, if you have SSH key on your machine, you can do
```
pip3 install git+ssh://git@github.com/fund3/python_omega_client.git
```
3. If you want to manually input credentials or use credentials saved locally on git,
you can do:
```
pip3 install git+https://github.com/fund3/python_omega_client.git
```

## Using Docker Containers

From the root directory of the repo, do
`docker build -f docker/python3.6/Dockerfile .`
Replace python3.6 with the directory name that you are using.

## Example Usage
After modifying credentials and IDs in heartbeat.py, you should be able to
connect with `python3 examples/heartbeat.py`.
The expected output prints one of each of Logon, Heartbeat and Logoff messages.

## Troubleshoot
If, for some reason, `pip3 install` was not successful because there was no
capnproto installed, do this and install with pip3 again:
```
curl -O https://capnproto.org/capnproto-c++-0.7.0.tar.gz
tar zxf capnproto-c++-0.7.0.tar.gz
cd capnproto-c++-0.7.0
./configure
make -j6 check
sudo make install
```

## Getting Started
1. Install the python_omega_client package using the above instructions.
2. See documentation on `common_types.py`: https://omega-client.readthedocs
.io/omega_client.messaging.html#module-omega_client.messaging.common_types for the definitions of the types used throughout the package.
3. Familiarize yourself with the 4 classes mentioned in the "Real Usage" section
below and understand how they work together.
4. Try out some of the examples in the "Examples" section below.
5. Override the 4 classes enumerated in the "Real Usage" section in your
application.

## Real Usage

There are 4 classes which should be implemented/overridden by the end user:
1. `OmegaConnection`: the main thread that handles all communication
between the client and Omega. There should only be one instance of
`OmegaConnection`.
2. `RequestSender`: the thread that sends requests to Omega via
`OmegaConnection`. There should be a unique `RequestSender` thread for each
client.
3. `ResponseHandler`: event driven class to handle responses from Omega.
4. `SessionRefresher`: Thread to refresh session

## Examples

See the `examples/` directory.
Heartbeat: https://omega-client.readthedocs.io/_modules/omega_client/examples/heartbeat.html#main
Logon, Logoff: https://omega-client.readthedocs.io/_modules/omega_client/examples/logon_logoff.html#main
Place Order: https://omega-client.readthedocs.io/_modules/omega_client/examples/place_order.html#main
Single Client Session Refresher: https://omega-client.readthedocs.io/_modules/omega_client/examples/single_client_session_refresher.html#SingleClientSessionRefresher
Printing Response Handler: https://omega-client.readthedocs.io/omega_client.messaging.html#module-omega_client.messaging.printing_response_handler

## Documentation

For documentation on specific classes and methods, see:
- `communication` module, which includes `omega_connection.py`, `request_sender.py`,
`response_receiver.py`, `single_client_request_sender.py`: https://omega-client.readthedocs.io/omega_client.communication.html#module-omega_client.communication
- `messaging` module, which includes `common_types.py`, `response_handler.py`,
`printing_response_handler.py`: https://omega-client.readthedocs.io/omega_client.messaging.html#omega-client-messaging-package
