# coding=utf-8
from __future__ import with_statement
import time
import logging
import six
from six.moves import urllib
from threading import Lock
from typing import Union

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, InvalidURI
from pymongo.uri_parser import parse_uri

from . import mim
from . import exc

Conn = Union[mim.Connection, MongoClient]


def create_engine(*args, **kwargs):
    """Creates a new :class:`.Engine` instance.

    According to the provided url schema ``mongodb://`` or ``mim://``
    it creates a MongoDB connection or an in-memory database.

    All the provided keyword arguments are passed to :class:`.Engine`.
    """
    use_class = kwargs.pop('use_class', None)
    connect_retry = kwargs.pop('connect_retry', 3)
    auto_ensure_indexes = kwargs.pop('auto_ensure_indexes', True)
    if use_class is None:
        if args and args[0].startswith('mim:'):
            use_class = lambda *a, **kw: mim.Connection.get()
            args = args[1:]
        else:
            use_class = MongoClient
    return Engine(use_class, args, kwargs, connect_retry, auto_ensure_indexes)


def create_datastore(uri, **kwargs):
    """Creates a new :class:`.DataStore` for the database identified by ``uri``.

    ``uri`` is a mongodb url in the form ``mongodb://username:password@address:port/dbname``,
    it can also be used to connect to a *replica set* by specifying multiple mongodb
    addresses separated by comma set
    ``mongodb://localhost:27018,localhost:27019,localhost:27020/dbname?replicaSet=rsname``.

    Optional Keyword args:

    - bind - a :class:`ming.datastore.Engine` instance

    All other keyword args are passed along to :meth:`create_engine`.

    The following are equivalent:

    - create_datastore('mongodb://localhost:27017/foo')
    - create_datastore('foo')
    - create_datastore('foo', bind=create_engine())
    """
    bind = kwargs.pop('bind', None)

    if bind and kwargs:
        raise exc.MingConfigError(
            "Unrecognized kwarg(s) when supplying bind: %s" %
            (kwargs.keys(),))

    try:
        database = parse_uri(uri)["database"]
    except InvalidURI:
        urlparts = urllib.parse.urlsplit(uri)
        database = urlparts.path
        if not urlparts.scheme:
            # provided uri is invalid for PyMongo and for Ming
            uri = None

    # extract the database
    if database.startswith("/"):
        database = database[1:]

    if uri:
        # User provided a valid connection URL.
        if bind:
            raise exc.MingConfigError("bind not allowed with full URI")
        bind = create_engine(uri, **kwargs)

    if bind is None:
        # Couldn't parse a valid connection endpoint,
        # Create engine without connection.
        bind = create_engine(**kwargs)

    return DataStore(bind, database)


class Engine(object):
    """Engine represents the connection to a MongoDB (or in-memory database).

    The ``Engine`` class lazily creates the connection the firs time it's
    actually accessed.
    """

    def __init__(self, Connection,
                 conn_args, conn_kwargs, connect_retry, auto_ensure_indexes, _sleep=time.sleep):
        self._Connection = Connection
        self._conn_args = conn_args
        self._conn_kwargs = conn_kwargs
        self._connect_retry = connect_retry
        self._sleep = _sleep
        self._auto_ensure_indexes = auto_ensure_indexes
        self._log = logging.getLogger(__name__)
        self._conn = None
        self._lock = Lock()

    def __repr__(self): # pragma no cover
        return '<Engine %r>' % self._conn

    def __getattr__(self, name):
        """Get the ``name`` database through this connection."""
        if name == 'conn':
            raise AttributeError(name)
        return getattr(self.conn, name)

    def __getitem__(self, name):
        return self.conn[name]

    @property
    def conn(self) -> Conn:
        """This is the pymongo connection itself."""
        if self._conn is None: self.connect()
        return self._conn

    def connect(self):
        """Actually opens the connection to MongoDB.

        This is usually done automatically when accessing
        a database for the first time through the engine.
        """
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
    """Represents a Database on a specific MongoDB Instance.

    DataStore keeps track of a specific database on a
    MongoDB Instance, Cluster or ReplicaSet. The database
    is represented by its name while MongoDB is represented
    by an :class:`.Engine` instance.

    DataStores are usually created using the
    :func:`.create_datastore` function.
    """

    def __init__(self, bind, name, authenticate=None):
        self.bind = bind
        self.name = name
        self._authenticate = authenticate
        self._db = None

    def __repr__(self): # pragma no cover
        return '<DataStore %r>' % self._db

    def __getattr__(self, name):
        """Get the ``name`` collection on this database."""
        if name == 'db':
            raise AttributeError(name)
        return getattr(self.db, name)

    @property
    def conn(self):
        return self.bind.conn

    @property
    def db(self):
        """This is the database on MongoDB.

        Accessing this property returns the pymongo db,
        untracked by Ming.
        """
        if self._db is None:
            if self.bind is None:
                raise ValueError('Trying to access db of an unconnected DataStore')

            self._db = self.bind[self.name]
        return self._db
