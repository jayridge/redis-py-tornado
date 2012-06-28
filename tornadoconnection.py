import os
import socket
import greenlet
import logging
from tornado import iostream
import hiredis
from itertools import chain, imap
from redis.exceptions import (
                              RedisError,
                              ConnectionError,
                              ResponseError,
                              InvalidResponse,
                              AuthenticationError
                              )


class TornadoHiredisParser(object):
    "Parser class for connections using Hiredis"
    def __init__(self):
        pass
    
    def __del__(self):
        try:
            self.on_disconnect()
        except:
            pass
    
    def on_connect(self, connection):
        self.connection = connection
        kwargs = {
            'protocolError': InvalidResponse,
            'replyError': ResponseError,
        }
        if connection.decode_responses:
            kwargs['encoding'] = connection.encoding
        self._reader = hiredis.Reader(**kwargs)
    
    def on_disconnect(self):
        self.connection = None
        self._reader = None
    
    def read_response(self):
        def noop():
            pass
        
        if not self._reader:
            raise ConnectionError("Socket closed on remote end")
        response = self._reader.gets()
        while response is False:
            gr = greenlet.getcurrent()
            def on_read(bytes):
                logging.debug("on_read: %r" % bytes)
                gr.switch(bytes)

            self.connection.stream.read_until("\r\n", on_read)
            buffer = gr.parent.switch()
            if not buffer:
                raise ConnectionError("Socket closed on remote end")
            self._reader.feed(buffer)
            # proactively, but not conclusively, check if more data is in the
            # buffer. if the data received doesn't end with \n, there's more.
            if not buffer.endswith('\n'):
                continue
            response = self._reader.gets()
        return response


class TornadoConnection(object):
    "Manages TCP communication to and from a Redis server"
    def __init__(self, host='localhost', port=6379, db=0, password=None,
                 socket_timeout=None, encoding='utf-8',
                 encoding_errors='strict', decode_responses=False,
                 parser_class=TornadoHiredisParser):
        self.pid = os.getpid()
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.socket_timeout = socket_timeout
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.decode_responses = decode_responses
        self.stream = None
        self._sock = None
        self._parser = parser_class()
    
    def __del__(self):
        try:
            self.disconnect()
        except:
            pass
    
    def connect(self):
        "Connects to the Redis server if not already connected"
        if self.stream and not self.stream.closed():
            return
        try:
            self._connect()
        except socket.error, e:
            raise ConnectionError(self._error_message(e))

    def _connect(self):
        "Create a TCP socket connection"
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        sock.settimeout(self.socket_timeout)
        sock.connect((self.host, self.port))
        self.stream = iostream.IOStream(sock)
        self.on_connect()
    
    def _error_message(self, exception):
        # args for socket.error can either be (errno, "message")
        # or just "message"
        if len(exception.args) == 1:
            return "Error connecting to %s:%s. %s." % \
                (self.host, self.port, exception.args[0])
        else:
            return "Error %s connecting %s:%s. %s." % \
                (exception.args[0], self.host, self.port, exception.args[1])
    
    def on_connect(self):
        "Initialize the connection, authenticate and select a database"
        self._parser.on_connect(self)
        self.stream.set_close_callback(self._parser.on_disconnect)
        
        # if a password is specified, authenticate
        if self.password:
            self.send_command('AUTH', self.password)
            response = self.read_response()
            if response != 'OK':
                raise AuthenticationError('Invalid Password')
        
        # if a database is specified, switch to it
        if self.db:
            self.send_command('SELECT', self.db)
            response = self.read_response()
            if response != 'OK':
                raise ConnectionError('Invalid Database')
    
    def disconnect(self):
        "Disconnects from the Redis server"
        self._parser.on_disconnect()
        if self.stream and not self.stream.closed():
            self.stream.close()
    
    def send_packed_command(self, command):
        "Send an already packed command to the Redis server"
        if not self.stream or self.stream.closed():
            self.connect()
        gr = greenlet.getcurrent()
        def on_write():
            gr.switch()
        self.stream.write(command, on_write)
        gr.parent.switch()
        
    def send_command(self, *args):
        "Pack and send a command to the Redis server"
        self.send_packed_command(self.pack_command(*args))

    def read_response(self):
        "Read the response from a previously sent command"
        try:
            response = self._parser.read_response()
            logging.debug("read response: %r" % response)
        except:
            self.disconnect()
            raise
        if response.__class__ == ResponseError:
            raise response
        return response
    
    def encode(self, value):
        "Return a bytestring representation of the value"
        if isinstance(value, unicode):
            return value.encode(self.encoding, self.encoding_errors)
        return str(value)
    
    def pack_command(self, *args):
        "Pack a series of arguments into a value Redis command"
        command = ['$%s\r\n%s\r\n' % (len(enc_value), enc_value)
                   for enc_value in imap(self.encode, args)]
        return '*%s\r\n%s' % (len(command), ''.join(command))


