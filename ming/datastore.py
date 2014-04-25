from __future__ import with_statement
import time
import logging
import six
from six.moves import urllib
from threading import Lock

try:
    import gevent
except ImportError:
    gevent = None

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

from . import mim
from . import exc

def create_engine(*args, **kwargs):
    '''Wrapper for creating Engines, setting the connection class
    appropriately'''
    use_class = kwargs.pop('use_class', None)
    connect_retry = kwargs.pop('connect_retry', 3)
    sleep = kwargs.pop('sleep', None)
    auto_ensure_indexes = kwargs.pop('auto_ensure_indexes', True)
    if use_class is None:
        if args and args[0].startswith('mim:'):
            use_class = lambda *a, **kw: mim.Connection.get()
            args = args[1:]
        else:
            use_class = MongoClient
    if sleep is None:
        if kwargs.get('use_greenlets', False):
            sleep = gevent.sleep
        else:
            sleep = time.sleep
    return Engine(use_class, args, kwargs, connect_retry, sleep, auto_ensure_indexes)

def create_datastore(uri, **kwargs):
    '''Wrapper for creating DataStores, setting the connection class

    Keyword args:

    - bind - a ming.datastore.Engine instance
    - authenticate - a dict { name:str, password:str } with auth info

    All other keyword args are passed along to create_engine()

    The following are equivalent:

    - create_datastore('mongodb://localhost:27017/foo')
    - create_datastore('foo')
    - create_datastore('foo', bind=create_engine())
    '''
    auth = kwargs.pop('authenticate', None)
    bind = kwargs.pop('bind', None)

    if bind and kwargs:
        raise exc.MingConfigError(
            "Unrecognized kwarg(s) when supplying bind: %s" %
            (kwargs.keys(),))

    parts = urllib.parse.urlsplit(uri)

    # Create the engine (if necessary)
    if parts.scheme:
        if bind: raise exc.MingConfigError("bind not allowed with full URI")
        bind_uri = parts._replace(
            netloc=parts.netloc.split('@')[-1],
            path='/').geturl()
        bind = create_engine(bind_uri, **kwargs)

    # extract the auth information
    if parts.username:
        if auth: raise exc.MingConfigError(
            "auth info supplied in uri and kwargs")
        auth = dict(
            name=parts.username, password=parts.password)

    # extract the database
    database = parts.path
    if database.startswith('/'): database = database[1:]

    if bind is None: bind = create_engine(**kwargs)

    return DataStore(bind, database, authenticate=auth)

class Engine(object):
    '''Engine is a thin proxy wrapper around pymongo (or mim) connection objects,
    providing for lazily creating the actual connection.'''

    def __init__(self, Connection,
                 conn_args, conn_kwargs, connect_retry, sleep, auto_ensure_indexes):
        self._Connection = Connection
        self._conn_args = conn_args
        self._conn_kwargs = conn_kwargs
        self._connect_retry = connect_retry
        self._sleep = sleep
        self._auto_ensure_indexes = auto_ensure_indexes
        self._log = logging.getLogger(__name__)
        self._conn = None
        self._lock = Lock()

    def __repr__(self): # pragma no cover
        return '<Engine %r>' % self._conn

    def __getattr__(self, name):
        if name == 'conn': raise AttributeError(name)
        return getattr(self.conn, name)

    def __getitem__(self, name):
        return self.conn[name]

    @property
    def conn(self):
        if self._conn is None: self.connect()
        return self._conn

    def connect(self):
        for x in six.moves.xrange(self._connect_retry+1):
            try:
                with self._lock:
                    if self._conn is None:
                        self._conn = self._Connection(
                            *self._conn_args, **self._conn_kwargs)
                    else:
                        return self._conn
            except ConnectionFailure:
                if x < self._connect_retry:
                    self._log.exception('Error connecting (#%d)', x)
                    self._sleep(1)
                else:
                    raise

class DataStore(object):

    def __init__(self, bind, name, authenticate=None):
        self.bind = bind
        self.name = name
        self._authenticate = authenticate
        self._db = None

    def __repr__(self): # pragma no cover
        return '<DataStore %r>' % self._db

    def __getattr__(self, name):
        if name == 'db': raise AttributeError(name)
        return getattr(self.db, name)

    @property
    def conn(self):
        '''For backward-compatibility'''
        return self.connection

    @property
    def db(self):
        if self._db is None:
            if self.bind is None: raise AttributeError
            self._db = self.bind[self.name]
            if self._authenticate:
                self._db.authenticate(**self._authenticate)
        return self._db

