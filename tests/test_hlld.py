"""
Unit Tests for the pyhlld module.

Currently, most of these tests require a fresh instance of
hlld to be valid and pass, and are almost rediculously simple.
"""

import unittest
import time
import random
from pyhlld import HlldClient

class TestHlld(unittest.TestCase):

    """TestCase class for pyhlld."""

    testID = None

    def setUp(self):
        """Setup the tests."""
        # Create a client to a local hlld server, default port
        self.client = HlldClient('localhost')
        self.testID = "%d.%d" % (time.time(),
            random.randint(1000, 1000000))

    def test_simple(self):
        """Test the most basic functionality of pyhlld."""
        # Get or create the foobar set
        foobar = self.client.create_set("foobar")

        # Set a key and check the size
        foobar.add("Test Key!")
        assert int(foobar.info()["size"]) == 1

    def test_pipeline(self):
        # Get or create the pipe set
        pipe = self.client.create_set("pipe").pipeline()

        # Chain multiple add commands
        results = pipe.add("foo").add("bar").add("baz").execute()
        assert results[0]
        assert results[1]
        assert results[2]

if __name__ == '__main__':
    unittest.main()
