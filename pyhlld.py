"""
This module implements a client for the hlld server.
"""
__all__ = ["HlldError", "HlldConnection", "HlldClient", "HlldSet"]
__version__ = "0.1.0"
import logging
import socket
import errno
import hashlib


class HlldError(Exception):
    "Root of exceptions from the client library"
    pass


class HlldConnection(object):
    "Provides a convenient interface to server connections"
    def __init__(self, server, timeout, attempts=3):
        """
        Creates a new Hlld Connection.

        :Parameters:
            - server: Provided as a string, either as "host" or "host:port" or "host:port:udpport".
                      Uses the default port of 4553 if none is provided.
            - timeout: The socket timeout to use.
            - attempts (optional): Maximum retry attempts on errors. Defaults to 3.
        """
        # Parse the host/port
        parts = server.split(":", 1)
        if len(parts) == 2:
            host, port = parts[0], int(parts[1])
        else:
            host, port = parts[0], 4553

        self.server = (host, port)
        self.timeout = timeout
        self.sock = None
        self.fh = None
        self.attempts = attempts
        self.logger = logging.getLogger("pyhlld.HlldConnection.%s.%d" % self.server)

    def _create_socket(self):
        "Creates a new socket, tries to connect to the server"
        # Connect the socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(self.timeout)
        s.connect(self.server)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        # Set no delay if possible
        if hasattr(socket, "TCP_NODELAY"):
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.fh = None
        return s

    def send(self, cmd):
        "Sends a command with out the newline to the server"
        if self.sock is None:
            self.sock = self._create_socket()
        sent = False
        for attempt in xrange(self.attempts):
            try:
                self.sock.sendall(cmd + "\n")
                sent = True
                break
            except socket.error, e:
                self.logger.exception("Failed to send command to hlld server! Attempt: %d" % attempt)
                if e[0] in (errno.ECONNRESET, errno.ECONNREFUSED, errno.EAGAIN, errno.EHOSTUNREACH, errno.EPIPE):
                    self.sock = self._create_socket()
                else:
                    raise

        if not sent:
            self.logger.critical("Failed to send command to hlld server after %d attempts!" % self.attempts)
            raise EnvironmentError("Cannot contact hlld server!")

    def read(self):
        "Returns a single line from the file"
        if self.sock is None:
            self.sock = self._create_socket()
        if not self.fh:
            self.fh = self.sock.makefile()
        read = self.fh.readline().rstrip("\r\n")
        return read

    def readblock(self, start="START", end="END"):
        """
        Reads a response block from the server. The servers
        responses are between `start` and `end` which can be
        optionally provided. Returns an array of the lines within
        the block.
        """
        lines = []
        first = self.read()
        if first != start:
            raise HlldError("Did not get block start (%s)! Got '%s'!" % (start, first))
        while True:
            line = self.read()
            if line == end:
                break
            lines.append(line)
        return lines

    def send_and_receive(self, cmd):
        """
        Convenience wrapper around `send` and `read`. Sends a command,
        and reads the response, performing a retry if necessary.
        """
        done = False
        for attempt in xrange(self.attempts):
            try:
                self.send(cmd)
                return self.read()
            except socket.error, e:
                self.logger.exception("Failed to send command to hlld server! Attempt: %d" % attempt)
                if e[0] in (errno.ECONNRESET, errno.ECONNREFUSED, errno.EAGAIN, errno.EHOSTUNREACH, errno.EPIPE):
                    self.sock = self._create_socket()
                else:
                    raise

        if not done:
            self.logger.critical("Failed to send command to hlld server after %d attempts!" % self.attempts)
            raise EnvironmentError("Cannot contact hlld server!")

    def response_block_to_dict(self):
        """
        Convenience wrapper around `readblock` to convert a block
        output into a dictionary by splitting on spaces, and using the
        first column as the key, and the remainder as the value.
        """
        resp_lines = self.readblock()
        return dict(tuple(l.split(" ", 1)) for l in resp_lines)


class HlldClient(object):
    "Provides a client abstraction around the hlld interface."
    def __init__(self, server, timeout=None, hash_keys=False):
        """
        Creates a new hlld client.

        :Parameters:
            - server : A server string, provided as "host" or "host:port".
            - timeout: (Optional) A socket timeout to use, defaults to no timeout.
            - hash_keys: (Optional) Should keys be hashed before sending to hlld. Defaults to False.
        """
        self.server = server
        self.timeout = timeout
        self.server_conn = None
        self.hash_keys = hash_keys

    def _server_connection(self):
        "Returns a connection to the server, tries to cache connections."
        if self.server_conn:
            return self.server_conn
        else:
            self.server_conn = HlldConnection(self.server, self.timeout)
            return self.server_conn

    def create_set(self, name, precision=None, eps=None, in_memory=False):
        """
        Creates a new set on the hlld server and returns a HlldSet
        to interface with it. This will return a HlldSet object attached
        to the set if the set already exists.

        :Parameters:
            - name : The name of the new set
            - precision (optional) : The precision of the HyperLogLog
            - eps (optional) : The upper bound on variance of the HyperLogLog
            - in_memory (optional) : If True, specified that the set should be created
              in memory only.
        """
        if precision and eps:
            raise ValueError("Cannot provide both precision and epsilon!")
        conn = self._server_connection()
        cmd = "create %s" % name
        if precision:
            cmd += " precision=%d" % precision
        if eps:
            cmd += " eps=%f" % eps
        if in_memory:
            cmd += " in_memory=1"
        conn.send(cmd)
        resp = conn.read()
        if resp == "Done":
            return HlldSet(conn, name, self.hash_keys)
        elif resp == "Exists":
            return self[name]
        else:
            raise HlldError("Got response: %s" % resp)

    def __getitem__(self, name):
        "Gets a HlldSet object based on the name."
        conn = self._server_connection()
        return HlldSet(conn, name, self.hash_keys)

    def list_sets(self):
        """
        Lists all the available sets.
        Returns a dictionary of {set_name : set_info}.
        """
        responses = {}
        conn = self._server_connection()
        conn.send("list")
        resp = conn.readblock()

        for line in resp:
            name, info = line.split(" ", 1)
            info = info.split(" ")
            info_dict = {
                "eps": float(info[0]),
                "precision": int(info[1]),
                "bytes": int(info[2]),
                "size": int(info[3])
            }
            responses[name] = info_dict

        return responses

    def flush(self):
        "Instructs all servers to flush to disk"
        # Send the flush
        conn = self._server_connection()
        conn.send("flush")
        resp = conn.read()
        if resp != "Done":
            raise HlldError("Got response: '%s'" % resp)


class HlldSet(object):
    "Provides an interface to a single Hlld set"
    def __init__(self, conn, name, hash_keys=False):
        """
        Creates a new HlldSet object.

        :Parameters:
            - conn : The connection to use
            - name : The name of the set
            - hash_keys : Should the keys be hashed client side
        """
        self.conn = conn
        self.name = name
        self.hash_keys = hash_keys

    def _get_key(self, key):
        """
        Returns the key we should send to the server
        """
        if self.hash_keys:
            return hashlib.sha1(key).hexdigest()
        return key

    def add(self, key):
        """
        Adds a new key to the set. No return value.
        """
        resp = self.conn.send_and_receive("s %s %s" % (self.name, self._get_key(key)))
        if resp != "Done":
            raise HlldError("Got response: %s" % resp)

    def bulk(self, keys):
        "Performs a bulk set command, adds multiple keys in the set"
        command = ("b %s " % self.name) + " ".join([self._get_key(k) for k in keys])
        resp = self.conn.send_and_receive(command)
        if resp != "Done":
            raise HlldError("Got response: %s" % resp)

    def drop(self):
        "Deletes the set from the server. This is permanent"
        resp = self.conn.send_and_receive("drop %s" % (self.name))
        if resp != "Done":
            raise HlldError("Got response: %s" % resp)

    def close(self):
        """
        Closes the set on the server.
        """
        resp = self.conn.send_and_receive("close %s" % (self.name))
        if resp != "Done":
            raise HlldError("Got response: %s" % resp)

    def clear(self):
        """
        Clears the set on the server.
        """
        resp = self.conn.send_and_receive("clear %s" % (self.name))
        if resp != "Done":
            raise HlldError("Got response: %s" % resp)

    def __len__(self):
        "Returns the count of items in the set."
        info = self.info()
        return int(info["size"])

    def info(self):
        "Returns the info dictionary about the set."
        self.conn.send("info %s" % (self.name))
        return self.conn.response_block_to_dict()

    def flush(self):
        "Forces the set to flush to disk"
        resp = self.conn.send_and_receive("flush %s" % (self.name))
        if resp != "Done":
            raise HlldError("Got response: %s" % resp)

    def pipeline(self):
        "Creates a HlldPipeline for pipelining multiple queries"
        return HlldPipeline(self.conn, self.name, self.hash_keys)


class HlldPipeline(object):
    "Provides an interface to a single Hlld set"
    def __init__(self, conn, name, hash_keys=False):
        """
        Creates a new HlldPipeline object.

        :Parameters:
            - conn : The connection to use
            - name : The name of the set
            - hash_keys : Should the keys be hashed client side
        """
        self.conn = conn
        self.name = name
        self.hash_keys = hash_keys
        self.buf = []

    def _get_key(self, key):
        """
        Returns the key we should send to the server
        """
        if self.hash_keys:
            return hashlib.sha1(key).hexdigest()
        return key

    def add(self, key):
        """
        Adds a new key to the set. No return value.
        """
        self.buf.append(("add", "s %s %s" % (self.name, self._get_key(key))))
        return self

    def bulk(self, keys):
        "Performs a bulk set command, adds multiple keys in the set"
        command = ("b %s " % self.name) + " ".join([self._get_key(k) for k in keys])
        self.buf.append(("bulk", command))
        return self

    def drop(self):
        "Deletes the set from the server. This is permanent"
        self.buf.append(("drop", "drop %s" % (self.name)))
        return self

    def close(self):
        """
        Closes the set on the server.
        """
        self.buf.append(("close", "close %s" % (self.name)))
        return self

    def clear(self):
        """
        Clears the set on the server.
        """
        self.buf.append(("clear", "clear %s" % (self.name)))
        return self

    def info(self):
        "Returns the info dictionary about the set."
        self.buf.append(("info", "info %s" % (self.name)))
        return self

    def flush(self):
        "Forces the set to flush to disk"
        self.buf.append(("flush", "flush %s" % (self.name)))
        return self

    def merge(self, pipeline):
        """
        Merges this pipeline with another pipeline. Commands from the
        other pipeline are appended to the commands of this pipeline.
        """
        self.buf.extend(pipeline.buf)
        return self

    def execute(self):
        """
        Executes the pipelined commands. All commands are sent to
        the server in the order issued, and responses are returned
        in appropriate order.
        """
        # Send each command
        buf = self.buf
        self.buf = []
        for name, cmd in buf:
            self.conn.send(cmd)

        # Get the responses
        all_resp = []
        for name, cmd in buf:
            if name in ("add", "bulk", "drop", "close", "clear", "flush"):
                resp = self.conn.read()
                if resp == "Done":
                    all_resp.append(True)
                else:
                    all_resp.append(HlldError("Got response: %s" % resp))

            elif name == "info":
                try:
                    resp = self.conn.response_block_to_dict()
                    all_resp.append(resp)
                except HlldError, e:
                    all_resp.append(e)
            else:
                raise Exception("Unknown command! Command: %s" % name)

        return all_resp

