Installation
************

See Dockerfiles in `docker` directory.  Note that AWS Linux 1 and AWS Linux 2
contains instructions to install the library on respective AMIs on AWS.  You
will need to install gcc 4.8+ on AWS Linux 1 for the installation to work. The
Dockerfiles are using `root` user by default.
Python3.5 and Python3.6 installations are identical as long as `python3` and
`pip3` are used.  Do not use sudo for these commands unless the user version
of Python3 is the same as the root version of Python3 and you know what you
are doing since you may run into issues with symlink etc.

Alternative ways
================
1. Clone the repo and run ``python3 setup.py install`` or ``pip3 install .`` in the root directory of the repo.
2. Alternatively, if you have SSH key on your machine, you can do ``pip3 install git+ssh://git@github.com/fund3/python_omega_client.git``
3. If you want to manually input credentials or use credentials saved locally on git,
you can do: ``pip3 install git+https://github.com/fund3/python_omega_client.git``
