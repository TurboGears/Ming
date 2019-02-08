import sys
from unittest import TestCase, main

from mock import patch, Mock
from pymongo.errors import ConnectionFailure

import ming
from ming import Session
from ming import mim
from ming import create_datastore, create_engine
from ming.datastore import Engine
from ming.exc import MingConfigError


class DummyConnection(object):
    def __init__(*args, **kwargs):
        pass


class TestEngineConnection(TestCase):
    @patch('ming.datastore.MongoClient', spec=True)
    def test_normal(self, MockConnection):
        from pymongo import MongoClient
        result = create_engine('master')
        conn = result.connect()
        assert isinstance(conn, MongoClient)

    @patch('ming.datastore.MongoClient', spec=True)
    def test_get_db(self, MockConnection):
        from pymongo import MongoClient
        result = create_engine('master')
        conn = result.connect()
        assert isinstance(conn, MongoClient)
        self.assertEqual(conn.read_preference, result.read_preference)


class TestConnectionFailure(TestCase):

    def test_connect(self):
        failures = [ 0 ]
        def Connection(*a,**kw):
            failures[0] += 1
            raise ConnectionFailure()
        engine = Engine(Connection, (), {}, 17, True,
                        _sleep=lambda x:None)
        self.assertRaises(ConnectionFailure, engine.connect)
        self.assertEqual(failures[0], 18)


class TestEngineMim(TestCase):

    def test_mim(self):
        with patch('ming.datastore.mim.Connection', spec=True) as Connection:
            result = create_engine('mim:///')
            conn = result.connect()
            assert conn is Connection.get()


class TestReplicaSet(TestCase):

    @patch('ming.datastore.MongoClient', spec=True)
    def test_replica_set(self, MockConn):
        from pymongo import MongoClient
        result = create_engine(
            'mongodb://localhost:23,localhost:27017,localhost:999/',
            replicaSet='foo')
        conn = result.connect()
        assert isinstance(conn, MongoClient)


class TestDatastore(TestCase):
    def setUp(self):
        self.patcher_conn = patch('ming.datastore.MongoClient')
        self.MockConn = self.patcher_conn.start()

    def tearDown(self):
        self.patcher_conn.stop()

    def test_one_uri(self):
        self._check_datastore(
            create_datastore('mongodb://localhost/test_db'),
            'test_db')

    def test_engine_with_name(self):
        self._check_datastore(
            create_datastore('test_db', bind=create_engine('master')),
            'test_db')

    def test_database_only(self):
        self._check_datastore(
            create_datastore('test_db'),
            'test_db')

    @patch('ming.datastore.MongoClient', spec=True)
    def test_replica_set(self, MockConn):
        from pymongo import MongoClient
        result = create_datastore(
            'mongodb://localhost:23,localhost:27017,localhost:999/test_db',
            replicaSet='foo')
        print(result.bind._conn_args[0])
        assert result.bind._conn_args[0].startswith('mongodb://localhost:23,localhost:27017,localhost:999')

    @patch('ming.datastore.MongoClient', spec=True)
    def test_configure_no_formencode(self, Connection):
        with patch.dict(sys.modules, {"formencode": None}):
            self.assertRaises(
                MingConfigError,
                ming.configure,
                **{
                    "ming.main.uri": "mongodb://localhost:27017/test_db",
                    "ming.main.connect_retry": 1,
                    "ming.main.tz_aware": False,
                }
            )

    @patch('ming.datastore.MongoClient', spec=True)
    def test_configure_no_formencode_variabledecode(self, Connection):
        with patch.dict(sys.modules, {"formencode.variabledecode": None}):
            self.assertRaises(
                MingConfigError,
                ming.configure,
                **{
                    "ming.main.uri": "mongodb://localhost:27017/test_db",
                    "ming.main.connect_retry": 1,
                    "ming.main.tz_aware": False,
                }
            )

    @patch('ming.datastore.MongoClient', spec=True)
    def test_configure(self, Connection):
        ming.configure(**{
                'ming.main.uri':'mongodb://localhost:27017/test_db',
                'ming.main.connect_retry': 1,
                'ming.main.tz_aware': False,
                })
        session = Session.by_name('main')
        assert session.bind.conn is not None
        assert session.bind.db is not None
        assert session.bind.bind._auto_ensure_indexes
        args, kwargs = Connection.call_args
        assert 'database' not in kwargs

    @patch('ming.datastore.MongoClient', spec=True)
    def test_configure_with_database(self, Connection):
        ming.configure(
            **{
                "ming.main.uri": "mongodb://localhost:27017/test_db",
                "ming.main.database": "another_test_db",
                "ming.main.connect_retry": 1,
                "ming.main.tz_aware": False,
            }
        )
        session = Session.by_name("main")
        assert session.bind.conn is not None
        assert session.bind.db is not None
        assert session.bind.bind._auto_ensure_indexes
        args, kwargs = Connection.call_args
        assert "database" in kwargs

    @patch('ming.datastore.MongoClient', spec=True)
    def test_configure_auto_ensure_indexes(self, Connection):
        ming.configure(**{
                'ming.main.uri':'mongodb://localhost:27017/test_db',
                'ming.main.connect_retry': 1,
                'ming.main.tz_aware': False,
                'ming.main.auto_ensure_indexes': 'False',
                })
        session = Session.by_name('main')
        assert session.bind.conn is not None
        assert session.bind.db is not None
        assert not session.bind.bind._auto_ensure_indexes
        args, kwargs = Connection.call_args
        assert 'database' not in kwargs

    @patch('ming.datastore.MongoClient', spec=True)
    def test_configure_optional_params(self, Connection):
        ming.configure(**{
                'ming.main.uri':'mongodb://localhost:27017/test_db',
                'ming.main.replicaSet': 'foobar',
                'ming.main.w': 2,
                'ming.main.ssl': True,
                })
        session = Session.by_name('main')
        assert session.bind.conn is not None
        assert session.bind.db is not None

    def test_no_kwargs_with_bind(self):
        self.assertRaises(
            ming.exc.MingConfigError,
            create_datastore,
            'test_db', bind=create_engine('master'), replicaSet='foo')

    def test_mim_ds(self):
        ds = create_datastore('mim:///test_db')
        conn = ds.bind.connect()
        assert conn is mim.Connection.get()

    def test_create_datastore_bind_not_allowed(self):
        self.assertRaises(
            ming.exc.MingConfigError,
            create_datastore,
            'mim://test_db', bind=create_engine('master'))

    def _check_datastore(self, ds, db_name):
        assert ds.db is self.MockConn()[db_name]
        assert ds.name == db_name


if __name__ == '__main__':
    main()
