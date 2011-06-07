from unittest import TestCase

from ming import datastore as DS

class TestDatastore(TestCase):

    def setUp(self):
        self.bind = DS.DataStore(master='mim:///', database='testdb')
        self.bind.conn.drop_all()
        self.bind.db.coll.insert({'_id':'foo', 'a':2, 'c':[1,2,3]})

    def test_exists(self):
        f = self.bind.db.coll.find
        assert 1 == f(dict(a={'$exists':True})).count()
        assert 0 == f(dict(a={'$exists':False})).count()
        assert 0 == f(dict(b={'$exists':True})).count()
        assert 1 == f(dict(b={'$exists':False})).count()

    def test_all(self):
        f = self.bind.db.coll.find
        assert 1 == f(dict(c={'$all':[1,2]})).count()
        assert 1 == f(dict(c={'$all':[1,2,3]})).count()
        assert 0 == f(dict(c={'$all':[2,3,4]})).count()
        assert 1 == f(dict(c={'$all':[]})).count()
        
        
