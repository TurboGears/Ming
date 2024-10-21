from __future__ import annotations

import time
from contextlib import closing
import logging
from threading import Lock
from typing import Union, TYPE_CHECKING
import urllib
import warnings
import weakref

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.encryption import ClientEncryption, Algorithm
from pymongo.errors import ConnectionFailure, InvalidURI, EncryptionError
from pymongo.uri_parser import parse_uri
from pymongocrypt.errors import MongoCryptError

from ming.utils import LazyProperty

from . import mim
from . import exc

if TYPE_CHECKING:
    from . import encryption

Conn = Union[mim.Connection, MongoClient]


def create_engine(*args, **kwargs) -> Engine:
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


def create_datastore(uri, **kwargs) -> DataStore:
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

    encryption_config: encryption.EncryptionConfig = kwargs.pop('encryption', None)

    if uri:
        # User provided a valid connection URL.
        if bind:
            raise exc.MingConfigError("bind not allowed with full URI")
        bind = create_engine(uri, **kwargs)

    if bind is None:
        # Couldn't parse a valid connection endpoint,
        # Create engine without connection.
        bind = create_engine(**kwargs)


    return DataStore(bind, database, encryption_config)

class Engine:
    """Engine represents the connection to a MongoDB (or in-memory database).

    The ``Engine`` class lazily creates the connection the first time it's
    accessed.
    """

    @staticmethod
    def _cleanup_conn(client, *args, **kwargs):
        if getattr(client, 'close', None) is not None:
            client.close()

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
        for x in range(self._connect_retry+1):
            try:
                with self._lock:
                    if self._conn is None:
                        # NOTE: Runs MongoClient/EncryptionClient
                        conn = self._Connection(
                            *self._conn_args, **self._conn_kwargs)
                        weakref.finalize(self, Engine._cleanup_conn, conn)
                        self._conn = conn
                    else:
                        return self._conn
            except ConnectionFailure:
                if x < self._connect_retry:
                    self._log.exception('Error connecting (#%d)', x)
                    self._sleep(1)
                else:
                    raise


class DataStore:
    """Represents a Database on a specific MongoDB Instance.

    DataStore keeps track of a specific database on a
    MongoDB Instance, Cluster or ReplicaSet. The database
    is represented by its name while MongoDB is represented
    by an :class:`.Engine` instance.

    DataStores are usually created using the
    :func:`.create_datastore` function.
    """

    def __init__(self, bind: Engine, name: str, encryption_config: encryption.EncryptionConfig = None):
        self.bind = bind
        self.name = name
        self._encryption_config = encryption_config
        self._db = None

    def __repr__(self): # pragma no cover
        return '<DataStore %r>' % self._db

    def __getattr__(self, name):
        """Get the ``name`` collection on this database."""
        if name == 'db':
            raise AttributeError(name)
        return getattr(self.db, name)

    @property
    def conn(self) -> Conn:
        return self.bind.conn

    @property
    def db(self) -> Database:
        """This is the database on MongoDB.

        Accessing this property returns the pymongo db,
        untracked by Ming.
        """
        if self._db is None:
            if self.bind is None:
                raise ValueError('Trying to access db of an unconnected DataStore')

            self._db = self.bind[self.name]
        return self._db

    @property
    def encryption(self) -> encryption.EncryptionConfig | None:
        return self._encryption_config

    @LazyProperty
    def encryptor(self) -> ClientEncryption:
        """Creates and returns a :class:`pymongo.encryption.ClientEncryption` instance for the given ming datastore. It uses this to handle encryption/decryption using pymongo's native routines.
        """
        encryption = ClientEncryption(self.encryption.kms_providers, self.encryption.key_vault_namespace,
                                      self.conn, self.conn.codec_options)
        return encryption

    def make_data_key(self):
        """Mongodb's Client Side Field Level Encryption (CSFLE) requires a data key to be present in the key vault collection. This ensures that the key vault collection is properly indexed and that a data key is present for each provider.
        """
        # index recommended by mongodb docs:
        key_vault_db_name, key_vault_coll_name = self.encryption.key_vault_namespace.split('.')
        key_vault_coll = self.conn[key_vault_db_name][key_vault_coll_name]
        key_vault_coll.create_index("keyAltNames", unique=True,
                                    partialFilterExpression={"keyAltNames": {"$exists": True}})

        for provider, options in self.encryption.provider_options.items():
            self.encryptor.create_data_key(provider, **options)

    def encr(self, s: str | None, _first_attempt=True, provider='local') -> bytes | None:
        """Encrypts a string using the encryption configuration of the ming datastore that this class is bound to.
        Most of the time, you won't need to call this directly, as it is used by the :meth:`ming.encryption.EncryptedDocumentMixin.encrypt_some_fields` method.
        """
        if s is None:
            return None
        try:
            key_alt_name = self.encryption._get_key_alt_name(provider)
            return self.encryptor.encrypt(s, Algorithm.AEAD_AES_256_CBC_HMAC_SHA_512_Deterministic,
                                     key_alt_name=key_alt_name)
        except (EncryptionError, MongoCryptError) as e:
            if _first_attempt and 'not all keys requested were satisfied' in str(e):
                self.make_data_key()
                return self.encr(s, _first_attempt=False)
            else:
                raise

    def decr(self, b: bytes | None) -> str | None:
        """Decrypts a string using the encryption configuration of the ming datastore that this class is bound to.
        """
        if b is None:
            return None
        return self.encryptor.decrypt(b)
