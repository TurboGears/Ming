import os
import logging

import pymongo
import gevent
from gevent.queue import Queue, Empty
from gevent.local import local

class AsyncConnection(pymongo.Connection):

    def __init__(self, *args, **kwargs):
        super(AsyncConnection, self).__init__(_connect=False, *args, **kwargs)
        self._Connection__pool = AsyncPool(
            self._Connection__connect,
            self._Connection__max_pool_size)
        self._Connection__find_master()

    def __repr__(self):
        if len(self._Connection__nodes) == 1:
            return "Connection(%r, %r)" % (self._Connection__host, self._Connection__port)
        else:
            return "Connection(%r)" % ["%s:%d" % n for n in self._Connection__nodes]

    def disconnect(self):
        self._Connection__pool = AsyncPool(
            self._Connection__connect,
            self._Connection__max_pool_size)
        self._Connection__host = None
        self._Connection__port = None
        
class AsyncPool(object):

    __slots__ = ["sockets", "socket_factory", "pool_size", "log", "local", "pid" ]

    def __init__(self, socket_factory, pool_size):
        self.pid = os.getpid()
        self.pool_size = pool_size
        self.socket_factory = socket_factory
        self.sockets = Queue()
        self.log = logging.getLogger('ming.async.AsyncPool')
        self.local = local()

    def _get_sock(self):
        return getattr(self.local, 'sock', None)
    def _set_sock(self, value):
        self.local.sock = value
    sock = property(_get_sock, _set_sock)

    def socket(self):
        pid = os.getpid()

        if pid != self.pid:
            self.sock = None
            self.sockets = Queue()
            self.pid = pid

        if self.sock is not None:
            self.log.debug('Return existing socket to greenlet %s', gevent.getcurrent() )
            return self.sock
        gl = gevent.getcurrent()
        try:
            self.sock = self.sockets.get_nowait()
            self.log.debug('Checkout socket %s to greenlet %s',
                           self.sock, gl )
        except Empty:
            self.sock = self.socket_factory()
            self.log.debug('Create socket in greenlet %s', gl)
        self.sock.last_greenlet = gl
        return self.sock

    def return_socket(self):
        if self.sock is None:
            self.log.debug('No socket to return from greenlet %s', gevent.getcurrent() )
            return
        if self.sockets.qsize() < self.pool_size:
            gl = gevent.getcurrent()
            self.log.debug('Checkin socket %s from greenlet %s',
                           self.sock, gl)
            self.sockets.put(self.sock)
            self.sock = None
        else:
            self.log.debug('Close socket in greenlet %s', gevent.getcurrent() )
            self.sock.close()
            self.sock = None
        self.local.sock = None
