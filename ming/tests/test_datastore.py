from unittest import TestCase, main

from mock import patch

import ming
from ming import Session, Field, Document
from ming import datastore as DS
from ming import schema as S

CONNECT_ARGS = dict(
    network_timeout=0.1)

class TestEngine(TestCase):

    def test_master_slave(self):
        ms = DS.Engine(
            'mongodb://localhost:27017/',
            'mongodb://localhost:27017/',
            **CONNECT_ARGS)
        assert ms.conn is not None

    def test_master_slave_failover(self):
        ms = DS.Engine(
            'mongodb://localhost:23/',
            'mongodb://localhost:27017/',
            **CONNECT_ARGS)
        assert ms.conn is not None

    def test_replica_set(self):
        ms = DS.Engine(
            'mongodb://localhost:23,localhost:27017,localhost:999/',
            **CONNECT_ARGS)
        assert ms.conn is not None

    def test_replica_set_slaves(self):
        ms = DS.Engine(
            'mongodb://localhost:23,localhost:27017/',
            'mongodb://localhost:999/',
            **CONNECT_ARGS)
        assert ms.conn is not None

    def test_slave_only(self):
        ms = DS.Engine(
            None, 'mongodb://localhost:27017/',
            **CONNECT_ARGS)
        assert ms.conn is not None

class TestDatastore(TestCase):

    def test_basic(self):
        ds = DS.DataStore(
            'mongodb://localhost:27017',
            database='test_db',
            **CONNECT_ARGS)
        assert ds.conn is not None
        assert ds.db is not None

    def test_configure(self):
        ming.configure(**{
                'ming.main.master':'mongodb://localhost:27017/',
                'ming.main.database':'test_db',
                'ming.main.network_timeout':'0.1',
                'ming.main.connect_retry': 1,
                'ming.main.tz_aware': False,
                })
        session = Session.by_name('main')
        assert session.bind.conn is not None
        assert session.bind.db is not None

if __name__ == '__main__':
    main()
