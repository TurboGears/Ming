import logging

import pymongo

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

    __slots__ = ["sockets", "socket_factory", "pool_size", "log" ]
    local = local()

    def __init__(self, socket_factory, pool_size):
        self.pool_size = pool_size
        self.socket_factory = socket_factory
        self.sockets = Queue()
        self.log = logging.getLogger('ming.async.AsyncPool')

    def socket(self):
        if getattr(self.local, 'sock', None) is not None:
            self.log.debug('Return existing socket')
            return self.local.sock
        try:
            self.local.sock = self.sockets.get_nowait()
            self.log.debug('Checkout socket')
        except Empty:
            self.local.sock = self.socket_factory()
            self.log.debug('Create socket')
        return self.local.sock

    def return_socket(self):
        if self.local.sock is None:
            self.log.debug('No socket to return')
            return
        if self.sockets.qsize() < self.pool_size:
            self.log.debug('Return socket')
            self.sockets.put(self.local.sock)
        else:
            self.log.debug('Close socket')
            self.local.sock.close()
        self.local.sock = None
