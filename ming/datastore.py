from __future__ import with_statement
import time
import logging

from threading import Lock

from pymongo.connection import Connection
from pymongo.master_slave_connection import MasterSlaveConnection

from .utils import parse_uri
from . import mim

log = logging.getLogger(__name__)

class Engine(object):
    '''Proxy for a pymongo connection, providing some url parsing'''

    def __init__(self, master='mongo://localhost:27017/', slave=None,
                 connect_retry=3):
        self._conn = None
        self._lock = Lock()
        self._connect_retry = connect_retry
        self.configure(master, slave)

    def configure(self, master='mongo://localhost:27017/gutenberg', slave=None):
        log.disabled = 0 # @%#$@ logging fileconfig disables our logger
        if isinstance(master, basestring):
            master = [ master ]
        if isinstance(slave, basestring):
            slave = [ slave ]
        if master is None: master = []
        if slave is None: slave = []
        self.master_args = [ parse_uri(s) for s in master if s ]
        self.slave_args = [ parse_uri(s) for s in slave if s ]
        if len(self.master_args) > 2:
            log.warning(
                'Only two masters are supported at present, you specified %r',
                master)
            self.master_args = self.master_args[:2]
        if len(self.master_args) > 1 and self.slave_args:
            log.warning(
                'Master/slave is not supported with replica pairs')
            self.slave_args = []
        one_url = (self.master_args+self.slave_args)[0]
        self.scheme = one_url['scheme']
        if one_url['scheme'] == 'mim':
            self._conn = mim.Connection.get()
        for a in self.master_args + self.slave_args:
            assert a['scheme'] == self.scheme

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
        network_timeout = self.master_args[0]['query'].get('network_timeout')
        if network_timeout is not None:
            network_timeout = float(network_timeout)
        try:
            if len(self.master_args) > 1:
                try:
                    self._conn = Connection(
                        [ '%s:%s' % (ma.get('host'), ma.get('port'))
                          for ma in self.master_args ],
                        network_timeout=network_timeout)
                except TypeError:
                    self._conn = Connection.paired(
                        *[ (ma.get('host'), ma.get('port'))
                           for ma in self.master_args],
                         network_timeout=network_timeout)
            else:
                if self.master_args:
                    try:
                        master = Connection(str(self.master_args[0]['host']), int(self.master_args[0]['port']),
                                            network_timeout=network_timeout)
                    except:
                        if self.slave_args:
                            log.exception('Cannot connect to master: %s will use slave: %s' % (self.master_args, self.slave_args))
                            # and continue... to use the slave only
                            master = None
                        else:
                            raise
                else:
                    log.info('No master connection specified, using slaves only: %s' % self.slave_args)
                    master = None

                if self.slave_args:
                    slave = []
                    for a in self.slave_args:
                        network_timeout = a['query'].get('network_timeout')
                        if network_timeout is not None:
                            network_timeout = float(network_timeout)
                        slave.append(
                            Connection(str(a['host']), int(a['port']),
                                       slave_okay=True,
                                       network_timeout=network_timeout,
                                      )
                        )
                    if master:
                        self._conn = MasterSlaveConnection(master, slave)
                    else:
                        self._conn = slave[0]

                else:
                    self._conn = master
        except:
            log.exception('Cannot connect to %s %s' % (self.master_args, self.slave_args))
        return self._conn

class DataStore(object):
    '''Engine bound to a particular database'''

    def __init__(self, master=None, slave=None, connect_retry=3,
                 bind=None, database=None):
        if bind is None:
            master=master or 'mongo://localhost:27017/gutenberg'
            bind = Engine(master, slave, connect_retry)
            one_url = (bind.master_args+bind.slave_args)[0]
            database = one_url['path'][1:]
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
    def get(cls, uri):
        args = parse_uri(uri)
        key = (args['scheme'], args['host'], args['port'])
        with cls._lock:
            engine = cls._engines.get(key)
            if not engine:
                engine = cls._engines[key] = Engine(uri)
            return DataStore(bind=engine, database=args['path'][1:])

