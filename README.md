pyhlld
=========

pyhlld provides a Python client library to interface with
hlld servers.

Features
--------


* Provides a simple API for using hlld
* Command pipelining to reduce latency


Install
-------

Download and install from source:

    python setup.py install

Example
------

Using pyhlld is very simple:

    from pyhlld import HlldClient

    # Create a client to a local hlld server, default port
    client = HlldClient("localhost")

    # Get or create the foobar set
    foobar = client.create_set("foobar")

    # Set a key and check the size
    foobar.add("Test Key!")
    assert foobar.info()["size"] == 1

Using pipelining is straightforward as well:

    from pyhlld import HlldClient

    # Create a client to a local hlld server, default port
    client = HlldClient("localhost")

    # Get or create the pipe set
    pipe = client.create_set("pipe")

    # Chain multiple add commands
    results = pipe.add("foo").add("bar").add("baz").execute()
    assert results[0]
    assert results[1]
    assert results[2]

