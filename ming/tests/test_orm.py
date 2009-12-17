from unittest import TestCase

from ming import schema as S
from ming import datastore as DS
from ming.orm.ormsession import ORMSession
from ming.orm.property import FieldProperty
from ming.orm.mapped_class import MappedClass
from ming.orm.base import state

class TestBasicMapping(TestCase):
    
    def setUp(self):
        self.datastore = DS.DataStore(
            master='mongo://localhost:27017/test_db')
        self.session = ORMSession(self.datastore)
        class Basic(MappedClass):
            class __mongometa__:
                name='basic'
                session = self.session
            _id = FieldProperty(S.ObjectId)
            a = FieldProperty(int)
            b = FieldProperty([int])
            c = FieldProperty(dict(
                    d=int, e=int))
        self.Basic = Basic
        self.session.impl.remove(self.Basic, {})

    def tearDown(self):
        self.session.clear()

    def test_create(self):
        doc = self.Basic()
        assert state(doc).status == 'new'
        self.session.flush()
        assert state(doc).status == 'clean'
        doc.a = 5
        assert state(doc).status == 'dirty'
        self.session.flush()
        assert state(doc).status == 'clean'

    def test_query(self):
        doc = self.Basic(a=1, b=[2,3], c=dict(d=4, e=5))
        self.session.flush()
        q = self.Basic.query.find(dict(a=1))
        self.assertEqual(q.count(), 1)
        doc.a = 5
        self.session.flush()
        q = self.Basic.query.find(dict(a=1))
        self.assertEqual(q.count(), 0)
        
