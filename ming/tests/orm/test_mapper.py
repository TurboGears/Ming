from unittest import TestCase

from mock import Mock

from ming import datastore as DS
from ming import schema as S
from ming import collection, Field, Session
from ming.base import Object
from ming.orm import ORMSession, mapper, state, Mapper
from ming.orm import ForeignIdProperty, RelationProperty
from ming.orm.icollection import InstrumentedList, InstrumentedObj

class TestBasicMapping(TestCase):
    
    def setUp(self):
        self.datastore = DS.DataStore(
            'mim:///', database='test_db')
        session = Session(bind=self.datastore)
        self.session = ORMSession(session)
        basic = collection(
            'basic', session,
            Field('_id', S.ObjectId),
            Field('a', int),
            Field('b', [int]),
            Field('c', dict(
                    d=int, e=int)))
        class Basic(object):
            pass                    
        self.session.mapper(Basic, basic)
        self.basic = basic
        self.Basic = Basic

    def tearDown(self):
        self.session.clear()
        self.datastore.conn.drop_all()

    def test_disable_instrument(self):
        # Put a doc in the DB
        self.Basic(a=1, b=[2,3], c=dict(d=4, e=5))
        self.session.flush()
        # Load back with instrumentation
        self.session.clear()
        obj = self.Basic.query.find().options(instrument=True).first()
        self.assertEqual(type(obj.b), InstrumentedList)
        self.assertEqual(type(obj.c), InstrumentedObj)
        # Load back without instrumentation
        self.session.clear()
        obj = self.Basic.query.find().options(instrument=False).first()
        self.assertEqual(type(obj.b), list)
        self.assertEqual(type(obj.c), Object)

    def test_enable_instrument(self):
        session = Session(bind=self.datastore)
        basic1 = collection(
            'basic1', session,
            Field('_id', S.ObjectId),
            Field('a', int),
            Field('b', [int]),
            Field('c', dict(
                    d=int, e=int)))
        class Basic1(object):
            pass                    
        self.session.mapper(Basic1, basic1, options=dict(instrument=False))
        # Put a doc in the DB
        Basic1(a=1, b=[2,3], c=dict(d=4, e=5))
        self.session.flush()
        # Load back with instrumentation
        self.session.clear()
        obj = Basic1.query.find().options(instrument=True).first()
        self.assertEqual(type(obj.b), InstrumentedList)
        self.assertEqual(type(obj.c), InstrumentedObj)
        # Load back without instrumentation
        self.session.clear()
        obj = Basic1.query.find().options(instrument=False).first()
        self.assertEqual(type(obj.b), list)
        self.assertEqual(type(obj.c), Object)

    def test_repr(self):
        doc = self.Basic(a=1, b=[2,3], c=dict(d=4,e=5))
        sdoc = repr(doc)
        assert 'a=1' in sdoc, sdoc
        assert 'b=I[2, 3]' in sdoc, sdoc
        assert "c=I{'e': 5, 'd': 4}" in sdoc, sdoc

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
        assert state(doc).status == 'dirty', state(doc).status
        assert repr(state(doc)).startswith('<ObjectState')

    def test_mapped_object(self):
        doc = self.Basic(a=1, b=[2,3], c=dict(d=4, e=5))
        self.assertEqual(doc.a, doc['a'])
        self.assertRaises(AttributeError, getattr, doc, 'foo')
        self.assertRaises(KeyError, doc.__getitem__, 'foo')
        doc['a'] = 5
        self.assertEqual(doc.a, doc['a'])
        self.assertEqual(doc.a, 5)
        self.assert_('a' in doc)
        doc.delete()

    def test_mapper(self):
        m = mapper(self.Basic)
        assert repr(m) == '<Mapper Basic:basic>'
        self.datastore.db.basic.insert(dict(
                a=1, b=[2,3], c=dict(d=4, e=5), f='unknown'))
        print list(self.datastore.db.basic.find())
        obj = self.Basic.query.find().options(instrument=False).first()
        print obj
        q = self.Basic.query.find()
        self.assertEqual(q.count(), 1)
        m.remove({})
        q = self.Basic.query.find()
        self.assertEqual(q.count(), 0)

    def test_query(self):
        doc = self.Basic(a=1, b=[2,3], c=dict(d=4, e=5))
        self.session.flush()
        q = self.Basic.query.find(dict(a=1))
        self.assertEqual(q.count(), 1)
        doc.a = 5
        self.session.flush()
        q = self.Basic.query.find(dict(a=1))
        self.assertEqual(q.count(), 0)
        self.assertEqual(doc.query.find(dict(a=1)).count(), 0)
        self.assertEqual(doc.query.find(dict(b=doc.b)).count(), 1)
        doc = self.Basic.query.get(a=5)
        self.assert_(doc is not None)
        self.Basic.query.remove({})
        self.assertEqual(self.Basic.query.find().count(), 0)

    def test_delete(self):
        doc = self.Basic(a=1, b=[2,3], c=dict(d=4, e=5))
        self.session.flush()
        q = self.Basic.query.find()
        self.assertEqual(q.count(), 1)
        doc.delete()
        q = self.Basic.query.find()
        self.assertEqual(q.count(), 1)
        self.session.flush()
        q = self.Basic.query.find()
        self.assertEqual(q.count(), 0)
        doc = self.Basic(a=1, b=[2,3], c=dict(d=4, e=5))
        self.session.flush()
        q = self.Basic.query.find()
        self.assertEqual(q.count(), 1)
        
    def test_imap(self):
        doc = self.Basic(a=1, b=[2,3], c=dict(d=4, e=5))
        self.session.flush()
        doc1 = self.Basic.query.get(_id=doc._id)
        self.assert_(doc is doc1)
        self.session.expunge(doc)
        doc1 = self.Basic.query.get(_id=doc._id)
        self.assert_(doc is not doc1)
        self.session.expunge(doc)
        self.session.expunge(doc)
        self.session.expunge(doc)
        
        
class TestRelation(TestCase):
    def setUp(self):
        self.datastore = DS.DataStore(
            'mim:///', database='test_db')
        session = Session(bind=self.datastore)
        self.session = ORMSession(session)
        class Parent(object): pass
        class Child(object): pass
        parent = collection(
            'parent', session,
            Field('_id', int))
        child = collection(
            'child', session,
            Field('_id', int),
            Field('parent_id', int))
        mapper(Parent, parent, self.session, properties=dict(
                children=RelationProperty(Child)))
        mapper(Child, child, self.session, properties=dict(
                parent_id=ForeignIdProperty(Parent),
                parent = RelationProperty(Parent)))
        self.Parent = Parent
        self.Child = Child

    def tearDown(self):
        self.session.clear()
        self.datastore.conn.drop_all()

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

class TestPolymorphic(TestCase):

    def setUp(self):
        self.datastore = DS.DataStore(
            'mim:///', database='test_db')
        session = Session(bind=self.datastore)
        self.session = ORMSession(session)
        base = collection(
            'test_doc', session,
            Field('_id', S.ObjectId),
            Field('type', str, if_missing='base'),
            Field('a', int),
            polymorphic_on='type',
            polymorphic_identity='base')
        derived = collection(
            base, 
            Field('type', str, if_missing='derived'),
            Field('b', int),
            polymorphic_identity='derived')
        class Base(object): pass
        class Derived(Base): pass
        mapper(Base, base, self.session)
        mapper(Derived, derived, self.session)
        self.Base = Base
        self.Derived = Derived

    def test_polymorphic(self):
        self.Base(a=1)
        self.Derived(a=2,b=2)
        self.session.flush()
        self.session.clear()
        q = self.Base.query.find()
        r = sorted(q.all())
        assert r[0].__class__ is self.Base
        assert r[1].__class__ is self.Derived

