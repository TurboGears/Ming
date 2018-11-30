from unittest import TestCase

from mock import Mock, patch

from ming import create_datastore
from ming import schema as S
from ming import collection, Field, Session
from ming.base import Object
from ming.odm import ODMSession, mapper, state, Mapper, session
from ming.odm import ForeignIdProperty, RelationProperty
from ming.odm.icollection import InstrumentedList, InstrumentedObj


class TestOrphanObjects(TestCase):

    def setUp(self):
        self.datastore = create_datastore('mim:///test_db')
        session = Session(bind=self.datastore)
        self.session = ODMSession(session)
        basic = collection('basic', session)
        class Basic(object):
            pass
        self.session.mapper(Basic, basic)
        self.basic = basic
        self.Basic = Basic

    def tearDown(self):
        self.session.clear()
        self.datastore.conn.clear_all()
        self.datastore.conn.drop_all()

    def test_orphan_object(self):
        obj = self.Basic()
        assert session(obj) is self.session
        self.session.clear()
        assert session(obj) is None


class TestWithNoFields(TestCase):

    def setUp(self):
        self.datastore = create_datastore('mim:///test_db')
        session = Session(bind=self.datastore)
        self.session = ODMSession(session)
        basic = collection('basic', session)
        class Basic(object):
            pass
        self.session.mapper(Basic, basic)
        self.basic = basic
        self.Basic = Basic

    def tearDown(self):
        self.session.clear()
        self.datastore.conn.drop_all()

    def test_query(self):
        self.basic(dict(a=1)).m.insert()
        doc = self.Basic.query.get(a=1)

class TestBasicMapping(TestCase):

    def setUp(self):
        self.datastore = create_datastore('mim:///test_db')
        session = Session(bind=self.datastore)
        self.session = ODMSession(session)
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

    def test_set_to_same(self):
        obj = self.Basic(a=1)
        assert state(obj).status == 'new'
        self.session.flush()
        assert state(obj).status == 'clean'
        obj.a = 1
        assert state(obj).status == 'clean'

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
        doc = self.Basic(a=1, b=[2,3], c=dict(d=4, e=5))
        sdoc = repr(doc)
        # sometimes there's a line break in the repr
        sdoc_cleaned = sdoc.replace('\n ', '')
        assert 'a=1' in sdoc_cleaned, sdoc_cleaned
        assert 'b=I[2, 3]' in sdoc_cleaned, sdoc_cleaned
        # order is not guaranteed with dictionaries
        assert "c=I{'d': 4, 'e': 5}" in sdoc_cleaned or \
            "c=I{'e': 5, 'd': 4}" in sdoc_cleaned, sdoc_cleaned

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
        obj = self.Basic.query.find().options(instrument=False).first()
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
        self.assertEqual(doc.query.find(dict(b=doc.b)).count(), 1)
        doc = self.Basic.query.get(a=5)
        self.assert_(doc is not None)
        self.Basic.query.remove({})
        self.assertEqual(self.Basic.query.find().count(), 0)

    def test_query_fields(self):
        doc = self.Basic(a=1, b=[2,3], c=dict(d=4, e=5))
        self.session.flush()
        self.session.clear()

        q = self.Basic.query.find(dict(a=1), projection=['b'])
        self.assertEqual(q.count(), 1)

        o = q.first()
        self.assertEqual(o.b, [2,3])
        self.assertEqual(o.a, None)
        o.b = [4,5]
        self.session.flush()
        self.session.clear()

        q = self.Basic.query.find(dict(a=1))
        self.assertEqual(q.count(), 1)

        o = q.first()
        self.assertEqual(o.a, 1)
        self.assertEqual(o.b, [4, 5])
        self.assertEqual(o.c, dict(d=4, e=5))

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

    @patch('ming.mim.Collection.aggregate')
    def test_aggregate(self, pymongo_aggregate):
        self.Basic.query.aggregate([])
        assert pymongo_aggregate.called

    @patch('ming.mim.Collection.map_reduce')
    def test_map_reduce(self, mim_map_reduce):
        self.Basic.query.map_reduce('...', '...', {})
        assert mim_map_reduce.called

    @patch('ming.mim.Collection.distinct')
    def test_distinct(self, mim_distinct):
        self.Basic.query.distinct('field')
        assert mim_distinct.called

    @patch('ming.mim.Cursor.distinct')
    def test_cursor_distinct(self, mim_distinct):
        self.Basic.query.find({'a': 'b'}).distinct('field')
        assert mim_distinct.called

    @patch('pymongo.collection.Collection.inline_map_reduce')
    def test_inline_map_reduce(self, pymongo_inline_map_reduce):
        self.Basic.query.inline_map_reduce()
        assert pymongo_inline_map_reduce.called

    @patch('pymongo.collection.Collection.group')
    def test_group(self, pymongo_group):
        self.Basic.query.group()
        assert pymongo_group.called


class TestRelation(TestCase):
    def setUp(self):
        self.datastore = create_datastore('mim:///test_db')
        session = Session(bind=self.datastore)
        self.session = ODMSession(session)
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

    def test_instrumented_readonly(self):
        parent = self.Parent(_id=1)
        children = [ self.Child(_id=i, parent_id=1) for i in range(5) ]
        self.session.flush()
        self.session.clear()
        parent = self.Parent.query.get(_id=1)
        self.assertRaises(TypeError, parent.children.append, children[0])

class TestPolymorphic(TestCase):

    def setUp(self):
        self.datastore = create_datastore('mim:///test_db')
        session = Session(bind=self.datastore)
        self.session = ODMSession(session)
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
        r = [x.__class__ for x in q]
        self.assertEqual(2, len(r))
        self.assertTrue(self.Base in r)
        self.assertTrue(self.Derived in r)


