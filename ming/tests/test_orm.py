from unittest import TestCase

from ming import schema as S
from ming import datastore as DS
from ming.orm.ormsession import ORMSession
from ming.orm.property import FieldProperty, RelationProperty, ForeignIdProperty
from ming.orm.mapped_class import MappedClass
from ming.orm.base import state

class TestRelation(TestCase):

    def setUp(self):
        self.datastore = DS.DataStore(
            master='mongo://localhost:27017/test_db')
        self.session = ORMSession(bind=self.datastore)
        class Parent(MappedClass):
            class __mongometa__:
                name='parent'
                session = self.session
            _id = FieldProperty(int)
            children = RelationProperty('Child')
        class Child(MappedClass):
            class __mongometa__:
                name='child'
                session = self.session
            _id = FieldProperty(int)
            parent_id = ForeignIdProperty('Parent')
            parent = RelationProperty('Parent')
        MappedClass.compile_all()
        self.Parent = Parent
        self.Child = Child
        self.session.impl.remove(self.Parent, {})
        self.session.impl.remove(self.Child, {})

    def tearDown(self):
        self.session.impl.remove(self.Parent, {})
        self.session.impl.remove(self.Child, {})
        self.session.clear()

    def test_parent(self):
        parent = self.Parent(_id=1)
        children = [ self.Child(_id=i, parent_id=1) for i in range(5) ]
        self.session.flush()
        self.session.clear()
        parent = self.Parent.query.get(_id=1)
        self.assertEqual(len(parent.children), 5)

    def test_readonly(self):
        parent = self.Parent(_id=1)
        children = [ self.Child(_id=i, parent_id=1) for i in range(5) ]
        self.session.flush()
        self.session.clear()
        parent = self.Parent.query.get(_id=1)
        def clearchildren():
            parent.children = []
        def setchild():
            parent.children[0] = children[0]
        self.assertRaises(TypeError, clearchildren)
        self.assertRaises(TypeError, parent.children.append, children[0])
        self.assertRaises(TypeError, setchild)

class TestBasicMapping(TestCase):
    
    def setUp(self):
        self.datastore = DS.DataStore(
            master='mongo://localhost:27017/test_db')
        self.session = ORMSession(bind=self.datastore)
        class Basic(MappedClass):
            class __mongometa__:
                name='basic'
                session = self.session
            _id = FieldProperty(S.ObjectId)
            a = FieldProperty(int)
            b = FieldProperty([int])
            c = FieldProperty(dict(
                    d=int, e=int))
        MappedClass.compile_all()
        self.Basic = Basic
        self.session.impl.remove(self.Basic, {})

    def tearDown(self):
        self.session.clear()
        self.session.impl.remove(self.Basic, {})

    def test_create(self):
        doc = self.Basic()
        assert state(doc).status == 'new'
        self.session.flush()
        assert state(doc).status == 'clean'
        doc.a = 5
        assert state(doc).status == 'dirty'
        self.session.flush()
        assert state(doc).status == 'clean'
        c = doc.c
        c.e = 5
        assert state(doc).status == 'dirty'

    def test_query(self):
        doc = self.Basic(a=1, b=[2,3], c=dict(d=4, e=5))
        self.session.flush()
        q = self.Basic.query.find(dict(a=1))
        self.assertEqual(q.count(), 1)
        doc.a = 5
        self.session.flush()
        q = self.Basic.query.find(dict(a=1))
        self.assertEqual(q.count(), 0)
        
