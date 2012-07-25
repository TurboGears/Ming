from unittest import TestCase

from ming import schema as S
from ming import create_datastore
from ming import Session
from ming.odm import ODMSession, Mapper
from ming.odm import FieldProperty, RelationProperty, ForeignIdProperty
from ming.odm import FieldPropertyWithMissingNone
from ming.odm.declarative import MappedClass
from ming.odm import state, mapper
from ming.odm import MapperExtension, SessionExtension

class TestIndex(TestCase):

    def test_string_index(self):
        class Test(MappedClass):
            class __mongometa__:
                indexes = [ 'abc' ]
            _id = FieldProperty(S.Int)
            abc=FieldProperty(S.Int, if_missing=None)
        mgr = mapper(Test).collection.m
        assert len(mgr.indexes) == 1, mgr.indexes

class TestRelation(TestCase):

    def setUp(self):
        self.datastore = create_datastore('mim:///test_db')
        self.session = ODMSession(bind=self.datastore)
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
        Mapper.compile_all()
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

class TestBasicMapperExtension(TestCase):
    def setUp(self):
        self.datastore = create_datastore('mim:///test_db')
        self.session = ODMSession(bind=self.datastore)
        class BasicMapperExtension(MapperExtension):
            def after_insert(self, instance, state, session):
                assert 'clean'==state.status
            def before_insert(self, instance, state, session):
                assert 'new'==state.status
            def before_update(self, instance, state, session):
                assert 'dirty'==state.status
            def after_update(self, instance, state, session):
                assert 'clean'==state.status
        class Basic(MappedClass):
            class __mongometa__:
                name='basic'
                session = self.session
                extensions = [BasicMapperExtension, MapperExtension]
            _id = FieldProperty(S.ObjectId)
            a = FieldProperty(int)
            b = FieldProperty([int])
            c = FieldProperty(dict(
                    d=int, e=int))
        Mapper.compile_all()
        self.Basic = Basic
        self.session.remove(self.Basic)

    def tearDown(self):
        self.session.clear()
        self.datastore.conn.drop_all()

    def test_mapper_extension(self):
        doc = self.Basic()
        doc.a = 5
        self.session.flush()
        doc.a = 6
        self.session.flush()

class TestBasicMapping(TestCase):
    
    def setUp(self):
        self.datastore = create_datastore('mim:///test_db')
        self.session = ODMSession(bind=self.datastore)
        class Basic(MappedClass):
            class __mongometa__:
                name='basic'
                session = self.session
            _id = FieldProperty(S.ObjectId)
            a = FieldProperty(int)
            b = FieldProperty([int])
            c = FieldProperty(dict(
                    d=int, e=int))
            d = FieldPropertyWithMissingNone(str, if_missing=S.Missing)
            e = FieldProperty(str, if_missing=S.Missing)
        Mapper.compile_all()
        self.Basic = Basic
        self.session.remove(self.Basic)

    def tearDown(self):
        self.session.clear()
        self.datastore.conn.drop_all()

    def test_repr(self):
        doc = self.Basic(a=1, b=[2,3], c=dict(d=4, e=5))
        repr(self.session)

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
        assert repr(state(doc)).startswith('<ObjectState')

    def test_mapped_object(self):
        doc = self.Basic(a=1, b=[2,3], c=dict(d=4, e=5))
        self.assertEqual(doc.a, doc['a'])
        self.assertEqual(doc.d, None)
        self.assertRaises(AttributeError, getattr, doc, 'e')
        self.assertRaises(AttributeError, getattr, doc, 'foo')
        self.assertRaises(KeyError, doc.__getitem__, 'foo')

        doc['d'] = 'test'
        self.assertEqual(doc.d, doc['d'])
        doc['e'] = 'test'
        self.assertEqual(doc.e, doc['e'])
        del doc.d
        self.assertEqual(doc.d, None)
        del doc.e
        self.assertRaises(AttributeError, getattr, doc, 'e')

        doc['a'] = 5
        self.assertEqual(doc.a, doc['a'])
        self.assertEqual(doc.a, 5)
        self.assert_('a' in doc)
        doc.delete()

    def test_mapper(self):
        m = mapper(self.Basic)
        self.assertEqual(repr(m), '<Mapper Basic:basic>')
        doc = self.Basic(a=1, b=[2,3], c=dict(d=4, e=5))
        self.session.flush()
        q = self.Basic.query.find()
        self.assertEqual(q.count(), 1)
        self.session.remove(self.Basic, {})
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
        
        
class TestPolymorphic(TestCase):

    def setUp(self):
        self.datastore = create_datastore('mim:///test_db')
        self.doc_session = Session(self.datastore)
        self.odm_session = ODMSession(self.doc_session)
        class Base(MappedClass):
            class __mongometa__:
                name='test_doc'
                session = self.odm_session
                polymorphic_on='type'
                polymorphic_identity='base'
            _id = FieldProperty(S.ObjectId)
            type=FieldProperty(str, if_missing='base')
            a=FieldProperty(int)
        class Derived(Base):
            class __mongometa__:
                polymorphic_identity='derived'
            type=FieldProperty(str, if_missing='derived')
            b=FieldProperty(int)
        Mapper.compile_all()
        self.Base = Base
        self.Derived = Derived

    def test_polymorphic(self):
        self.Base(a=1)
        self.odm_session.flush()
        self.Derived(a=2,b=2)
        self.odm_session.flush()
        self.odm_session.clear()
        q = self.Base.query.find()
        r = sorted(q.all())
        assert r[0].__class__ is self.Base
        assert r[1].__class__ is self.Derived

