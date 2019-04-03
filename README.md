# Omega Python Client
API Wrapper for connecting to Fund3 Trade Execution System

## Requirements
1. gcc 4.8+
2. Python3.5+
3. Access to `python_omega_client` and `CommunicationProtocol` Fund3 repos.
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
curl -O https://capnproto.org/capnproto-c++-0.6.1.tar.gz
tar zxf capnproto-c++-0.6.1.tar.gz
cd capnproto-c++-0.6.1
./configure
make -j6 check
sudo make install
```
