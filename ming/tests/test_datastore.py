from unittest import TestCase, main

from mock import patch, Mock
from pymongo.errors import ConnectionFailure

import ming
from ming import Session, Field, Document
from ming import create_datastore, create_engine
from ming.datastore import Engine
from ming import schema as S

class DummyConnection(object):
    def __init__(*args, **kwargs): pass

class TestEngineConnection(TestCase):

    @patch('ming.datastore.Connection', spec=True)
    def test_normal(self, MockConnection):
        from pymongo import Connection
        result = create_engine('master')
        conn = result.connect()
        assert isinstance(conn, Connection)

    @patch('ming.datastore.Connection', spec=True)
    def test_get_db(self, MockConnection):
        from pymongo import Connection
        result = create_engine('master')
        conn = result.connect()
        assert isinstance(conn, Connection)
        self.assertEqual(conn.read_preference, result.read_preference)

    @patch('ming.datastore.Connection', spec=True)
    @patch('ming.datastore.gevent')
    def test_greenlets(self, gevent, MockConnection):
        from pymongo import Connection
        result = create_engine('master', use_greenlets=True)
        conn = result.connect()
        assert isinstance(conn, Connection)

class TestConnectionFailure(TestCase):

    def test_connect(self):
        failures = [ 0 ]
        def Connection(*a,**kw):
            failures[0] += 1
            raise ConnectionFailure()
        engine = Engine(Connection, (), {}, 17, lambda x:None)
        self.assertRaises(ConnectionFailure, engine.connect)
        self.assertEqual(failures[0], 17)

class TestEngineMim(TestCase):
    
    def test_mim(self):
        from ming.mim import Connection
        with patch('ming.datastore.mim.Connection', spec=True):
            result = create_engine('mim:///')
            conn = result.connect()
            assert isinstance(conn, Connection)

class TestMasterSlave(TestCase):
    
    @patch('ming.datastore.Connection', spec=True)
    @patch('ming.datastore.MasterSlaveConnection', spec=True)
    def test_with_strings(self, MockMasterSlaveConnection, MockConnection):
        from pymongo.master_slave_connection import MasterSlaveConnection
        result = create_engine(
            'master', slaves=['slave1', 'slave2'])
        conn = result.connect()
        assert isinstance(conn, MasterSlaveConnection)
        mock_conn = MockConnection()
        MockMasterSlaveConnection.assert_called_with(
            mock_conn, [mock_conn, mock_conn],
            document_class=dict, tz_aware=False)

    @patch('ming.datastore.Connection', spec=True)
    @patch('ming.datastore.MasterSlaveConnection', spec=True)
    def test_with_connections(self, MockMasterSlaveConnection, MockConnection):
        from pymongo.master_slave_connection import MasterSlaveConnection
        master = Mock()
        slaves = [ Mock(), Mock() ]
        result = create_engine(master, slaves=slaves)
        conn = result.connect()
        assert isinstance(conn, MasterSlaveConnection)
        MockMasterSlaveConnection.assert_called_with(
            master, slaves,
            document_class=dict, tz_aware=False)

class TestReplicaSet(TestCase):

    @patch('ming.datastore.Connection', spec=True)
    @patch('ming.datastore.ReplicaSetConnection', spec=True)
    def test_replica_set(self, MockRSConn, MockConn):
        from pymongo import ReplicaSetConnection
        result = create_engine(
            'mongodb://localhost:23,localhost:27017,localhost:999/',
            replicaSet='foo')
        conn = result.connect()
        assert isinstance(conn, ReplicaSetConnection)

class TestDatastore(TestCase):

    def setUp(self):
        self.patcher_conn = patch('ming.datastore.Connection')
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

    def test_with_auth_in_uri(self):
        ds = create_datastore('mongodb://user:pass@server/test_db')
        self._check_datastore(ds, 'test_db')
        self.assertEqual(
            ds._authenticate,
            dict(name='user', password='pass'))

    def test_configure(self):
        ming.configure(**{
                'ming.main.uri':'mongodb://localhost:27017/test_db',
                'ming.main.network_timeout':'0.1',
                'ming.main.connect_retry': 1,
                'ming.main.tz_aware': False,
                })
        session = Session.by_name('main')
        assert session.bind.conn is not None
        assert session.bind.db is not None

    def test_no_kwargs_with_bind(self):
        self.assertRaises(
            ming.exc.MingConfigError,
            create_datastore,
            'test_db', bind=create_engine('master'), replicaSet='foo')

    def test_no_double_auth(self):
        self.assertRaises(
            ming.exc.MingConfigError,
            create_datastore,
            'mongodb://user:pass@server/test_db',
            authenticate=dict(name='user', password='pass'))

    def _check_datastore(self, ds, db_name):
        assert ds.db is self.MockConn()[db_name]
        assert ds.name == db_name

if __name__ == '__main__':
    main()
