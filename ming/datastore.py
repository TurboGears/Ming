from __future__ import with_statement
import time
import logging

from threading import Lock

from pymongo.connection import Connection
from pymongo.master_slave_connection import MasterSlaveConnection

from . import mim


class Engine(object):
    '''Proxy for a pymongo connection, providing some url parsing'''

    def __init__(self, master='mongodb://localhost:27017/', slave=None,
                 connect_retry=3, **connect_args):
        self._log = logging.getLogger(__name__)
        self._conn = None
        self._lock = Lock()
        self._connect_retry = connect_retry
        self._connect_args = connect_args
        self.configure(master, slave)

    def __repr__(self):
        return 'Engine(master=%r, slave=%r, **%r)' % (
            self.master_args,
            self.slave_args,
            self._connect_args)

    def configure(self, master='mongodb://localhost:27017/', slave=None):
        if master and master.startswith('mim://'):
            if slave:
                self._log.warning('Master/slave not supported with mim://')
                slave = None
            self._conn = mim.Connection.get()
        self.master_args = master
        self.slave_args = slave
        assert self.master_args or self.slave_args, \
            'You must specify either a master or a slave'
        if self.master_args and self.slave_args:
            hosts = self.slave_args[len('mongodb://'):]
            self._slave_hosts = ['mongodb://' + host for host in hosts.split(',') ]
        else:
            self._slave_hosts = []

    @property
    def conn(self):
        for attempt in xrange(self._connect_retry+1):
            if self._conn is not None: break
            with self._lock:
                if self._connect() is None:
                    time.sleep(1)
        return self._conn

    def _connect(self):
        self._conn = None
        master = None
        slaves = []
        try:
            if self.master_args:
                try:
                    master = Connection(self.master_args, **self._connect_args)
                except:
                    self._log.exception(
                        'Error connecting to master: %s', self.master_args)
            if self.slave_args and master:
                slaves = []
                for host in self._slave_hosts:
                    try:
                        slaves.append(Connection(
                                host, slave_okay=True, **self._connect_args))
                    except:
                        self._log.exception(
                            'Error connecting to slave: %s', host)
            if master:
                if slaves:
                    self._conn = MasterSlaveConnection(master, slaves)
                else:
                    self._conn = master
            else:
                self._conn = Connection(self.slave_args, slave_okay=True, **self._connect_args)
        except:
            self._log.exception('Cannot connect to %s %s' % (self.master_args, self.slave_args))
        return self._conn

class DataStore(object):
    '''Engine bound to a particular database'''

    def __init__(self, master=None, slave=None, connect_retry=3,
                 bind=None, database=None, **connect_args):
        '''
        :param master: connection URL(s) - mongodb://host:port[,host:port]
        :type master: string
        :param slave: like master, but slave(s)
        :type slave: string
        :param connect_retry: retry this many times (with 1-second sleep) when a Connection cannot be established
        :type connect_retry: int
        :param bind: instead of master and slave params, use an existing ming.datastore.Engine
        :param database: database name
        :param connect_args: arguments passed along to pymongo.Connect() such as network_timeout
        '''
        if bind is None:
            master=master or 'mongodb://localhost:27017/'
            bind = Engine(master, slave, connect_retry, **connect_args)
        self.bind = bind
        self.database = database

    def __repr__(self):
        return 'DataStore(%r, %s)' % (self.bind, self.database)

    @property
    def conn(self):
        return self.bind.conn

    @property
    def db(self):
        return getattr(self.bind.conn, self.database, None)

class ShardedDataStore(object):
    _lock = Lock()
    _engines = {}

    @classmethod
    def get(cls, uri, database):
        with cls._lock:
            engine = cls._engines.get(uri)
            if not engine:
                engine = cls._engines[uri] = Engine(uri)
            return DataStore(bind=engine, database=database)
