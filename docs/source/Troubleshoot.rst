Troubleshoot
************

If, for some reason, `pip3 install` was not successful because there was no
capnproto installed, do this and install with pip3 again:
.. code::
    curl -O https://capnproto.org/capnproto-c++-0.7.0.tar.gz
    tar zxf capnproto-c++-0.7.0.tar.gz
    cd capnproto-c++-0.7.0
    ./configure
    make -j6 check
    sudo make install

