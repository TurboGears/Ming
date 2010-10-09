from unittest import TestCase

from mock import Mock

from ming import schema as S
from ming import datastore as DS
from ming import Session
from ming.orm import ORMSession
from ming.orm import FieldProperty, RelationProperty, ForeignIdProperty
from ming.orm import MappedClass
from ming.orm import state, mapper
from ming.orm.icollection import instrument, deinstrument, InstrumentedObj

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
        self.assertRaises(AttributeError, getattr, doc, 'foo')
        self.assertRaises(KeyError, doc.__getitem__, 'foo')
        doc['a'] = 5
        self.assertEqual(doc.a, doc['a'])
        self.assertEqual(doc.a, 5)
        self.assert_('a' in doc)
        doc.delete()

    def test_mapper(self):
        m = mapper(self.Basic)
        self.assert_(repr(m).startswith('<Mapper for '))
        doc = self.Basic(a=1, b=[2,3], c=dict(d=4, e=5))
        self.session.flush()
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
        
        
class TestICollection(TestCase):

    def setUp(self):
        self.obj = dict(
            a=[ 1,2,3 ])
        self.tracker = Mock()
        self.iobj = instrument(self.obj, self.tracker)
        self.obj1 = deinstrument(self.iobj)
        self.list = [ 1,2,3 ]
        self.ilist = instrument(self.list, self.tracker)
        self.list1 = deinstrument(self.ilist)
        class Child(InstrumentedObj):
            attr = 5
        self.Child = Child

    def test_instrument(self):
        self.assertNotEqual(type(self.iobj), dict)
        self.assertNotEqual(type(self.iobj['a']), list)
        self.assertNotEqual(type(self.ilist), list)
        self.assertEqual(type(self.obj1), dict)
        self.assertEqual(type(self.obj1['a']), list)
        self.assertEqual(type(self.list1), list)
        self.assertEqual(self.obj1, self.iobj.deinstrumented_clone())
        self.assertEqual(self.list1, deinstrument(self.ilist))

    def test_derived(self):
        ch = self.Child(self.tracker)
        ch.attr = 10
        self.assertEqual(ch.attr, 10)
        self.assertRaises(KeyError, ch.__getitem__, 'attr')

    def test_iobj(self):
        self.iobj['b'] = 5
        self.tracker.added_item.assert_called_with(5)
        self.iobj['b'] = 10
        self.tracker.added_item.assert_called_with(10)
        self.tracker.removed_item.assert_called_with(5)
        del self.iobj['b']
        self.tracker.removed_item.assert_called_with(10)
        self.assertEqual(self.iobj.a, [1,2,3])
        self.assertRaises(AttributeError, getattr, self.iobj, 'b')
        self.iobj.b = '5'
        self.iobj.pop('b')
        self.tracker.removed_item.assert_called_with('5')
        self.iobj.popitem()
        self.tracker.removed_item.assert_called_with([1,2,3])
        self.iobj.clear()
        self.tracker.cleared.assert_called_with()
        self.iobj.update(dict(a=5, b=6),
                         c=7, d=8)
        self.assertEqual(self.iobj, dict(a=5, b=6, c=7, d=8))
        self.iobj.replace(dict(a=5, b=6))
        self.assertEqual(self.iobj, dict(a=5, b=6))

    def test_ilist(self):
        self.ilist[0] = 5
        self.assertEqual(self.ilist[0], 5)
        self.tracker.removed_item.assert_called_with(1)
        self.tracker.added_item.assert_called_with(5)
        self.ilist[:2] = [1,2,3]
        self.tracker.removed_item.assert_called_with(2)
        self.tracker.added_item.assert_called_with(2)
        self.assertEqual(self.ilist, [1,2,3,3])
        del self.ilist[0]
        self.tracker.removed_item.assert_called_with(1)
        self.assertEqual(self.ilist, [2,3,3])
        del self.ilist[:1]
        self.tracker.removed_item.assert_called_with(2)
        self.assertEqual(self.ilist, [3,3])
        self.ilist += self.list
        self.tracker.added_item.assert_called_with(3)
        self.assertEqual(self.ilist, [3,3,1,2,3])
        self.ilist *= 2
        self.tracker.added_item.assert_called_with(3)
        self.assertEqual(self.ilist, [3,3,1,2,3] * 2)
        self.ilist *= 0
        self.tracker.removed_item.assert_called_with(3)
        self.assertEqual(self.ilist, [])
        self.ilist.insert(0, 1)
        self.tracker.added_item.assert_called_with(1)
        self.ilist.insert(0, 2)
        self.tracker.added_item.assert_called_with(2)
        self.assertEqual(self.ilist, [2, 1])
        self.assertEqual(self.ilist.pop(), 1)
        self.tracker.removed_item.assert_called_with(1)
        self.ilist.replace([1,2,3,4])
        self.ilist.remove(2)
        self.assertEqual(self.ilist, [1,3,4])
        self.tracker.removed_item.assert_called_with(2)
        self.assertRaises(ValueError, self.ilist.remove, 22)
        self.assertEqual(self.ilist.pop(0), 1)
        self.tracker.removed_item.assert_called_with(1)
        
        
class TestPolymorphic(TestCase):

    def setUp(self):
        self.bind = DS.DataStore(master='mim:///')
        self.doc_session = Session(self.bind)
        self.orm_session = ORMSession(self.doc_session)
        class Base(MappedClass):
            class __mongometa__:
                name='test_doc'
                session = self.orm_session
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
        MappedClass.compile_all()
        self.Base = Base
        self.Derived = Derived

    def test_polymorphic(self):
        self.Base(a=1)
        self.Derived(a=2,b=2)
        self.orm_session.flush()
        self.orm_session.clear()
        q = self.Base.query.find()
        r = sorted(q.all())
        assert r[0].__class__ is self.Base
        assert r[1].__class__ is self.Derived

