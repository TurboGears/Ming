from unittest import TestCase, main

from mock import patch

import ming
from ming import Session, Field, Document
from ming import datastore as DS
from ming import schema as S

class TestDatastore(TestCase):

    def setUp(self):
        class TestDoc(Document):
            class __mongometa__:
                name='test_doc'
                session = Session.by_name('main')
            _id=Field(S.ObjectId, if_missing=None)
            a=Field(S.Int, if_missing=None)
            b=Field(S.Object, dict(a=S.Int(if_missing=None)))
        config = {
            'ming.main.master':'mongo://localhost:27017/test_db' }
        ming.configure(**config)
        self.session = TestDoc.__mongometa__.session

    def test_basic(self):
        self.assert_(repr(self.session.bind), 'DataStore(master=[{')
        self.session.bind.conn

    def test_master_slave(self):
        ms = DS.DataStore(master='mongo://localhost:27017/test_db?network_timeout=5',
                          slave='mongo://localhost:27017/test_db?network_timeout=5')
        self.assert_(ms.conn is not None)
        self.assert_(ms.db is not None)
    
    def test_master_slave_failover(self):
        ms = DS.DataStore(master='mongo://localhost:23/test_db',
                          slave='mongo://localhost:27017/test_db')
        ms.conn # should failover to slave-only
        ms.db
        ms_fail = DS.DataStore(master='mongo://localhost:23/test_db')
        self.assert_(ms_fail.conn is None)
    
    @patch('pymongo.connection.Connection.paired')
    def test_replica_pair(self, paired):
        ms = DS.DataStore(master=['mongo://localhost:23/test_db',
                                  'mongo://localhost:27017/test_db'])
        self.assert_(ms.conn is not None)
        paired.assert_called_with(('localhost',23), ('localhost',27017))
        self.assert_(ms.db is not None)
        ms_fail = DS.DataStore(master='mongo://localhost:23/test_db')
        self.assert_(ms_fail.conn is None)
        
    def test_3masters(self):
        ms = DS.DataStore(master=['mongo://localhost:23/test_db',
                                  'mongo://localhost:27017/test_db',
                                  'mongo://localhost:999/test_db',
                                  ])
        self.assert_(ms.conn is not None)
        self.assertEqual(len(ms.master_args), 2)
        
    def test_replica_pair_slaves(self):
        ms = DS.DataStore(master=['mongo://localhost:23/test_db',
                                  'mongo://localhost:27017/test_db'],
                          slave='mongo://localhost:999/test_db')
        self.assert_(ms.conn is not None)
        self.assertEqual(len(ms.slave_args), 0)
        
    def test_slave_only(self):
        ms = DS.DataStore(master = None,
                          slave = 'mongo://localhost:27017/test_db')
        self.assert_(ms.conn is not None)
        self.assert_(ms.db is not None)
        

if __name__ == '__main__':
    main()

