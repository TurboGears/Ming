from unittest import TestCase

from ming import datastore as DS

class TestDatastore(TestCase):

    def setUp(self):
        self.bind = DS.DataStore(master='mim:///', database='testdb')
        self.bind.conn.drop_all()
        self.bind.db.coll.insert({'_id':'foo', 'a':2})

    def test_exists(self):
        assert 1 == self.bind.db.coll.find(dict(
                a={'$exists':True})).count()
        assert 0 ==  self.bind.db.coll.find(dict(
                a={'$exists':False})).count()
        assert 0 ==  self.bind.db.coll.find(dict(
                b={'$exists':True})).count()
        assert 1 ==  self.bind.db.coll.find(dict(
                b={'$exists':False})).count()
