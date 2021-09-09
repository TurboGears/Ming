import sys
from collections import defaultdict
from unittest import TestCase, SkipTest

from ming import schema as S
from ming import create_datastore
from ming import Session
from ming.exc import MingException
from ming.odm import ODMSession, Mapper
from ming.odm import FieldProperty, RelationProperty, ForeignIdProperty
from ming.odm import FieldPropertyWithMissingNone
from ming.odm.declarative import MappedClass
from ming.odm import state, mapper
from ming.odm import MapperExtension, SessionExtension
from ming.odm.odmsession import ODMCursor
import bson

class TestIndex(TestCase):

    def test_string_index(self):
        class Test(MappedClass):
            class __mongometa__:
                indexes = [ 'abc' ]
            _id = FieldProperty(S.Int)
            abc=FieldProperty(S.Int, if_missing=None)
        mgr = mapper(Test).collection.m
        assert len(mgr.indexes) == 1, mgr.indexes

class TestMapping(TestCase):
    DATASTORE = 'mim:///test_db'

    def setUp(self):
        Mapper._mapper_by_classname.clear()
        self.datastore = create_datastore(self.DATASTORE)
        self.session = ODMSession(bind=self.datastore)
        
    def tearDown(self):
        self.session.clear()
        try:
            self.datastore.conn.drop_all()
        except TypeError:
            self.datastore.conn.drop_database(self.datastore.db)
        Mapper._mapper_by_classname.clear()
      
    def test_with_mixins(self):
        class Mixin1(object):
            def __init__(self):
                pass
            
            def dosomething(self):
                pass
            
        class Mixin2(object):
            def domore(self):
                pass
        
        class User(MappedClass, Mixin1, Mixin2):
            class __mongometa__:
                name = "userswithmixin"
                session = self.session
                
            _id = FieldProperty(S.ObjectId)
            username = FieldProperty(str)
            
        u = User(_id=None, username="anonymous")
        self.session.flush()
        
        u2 = User.query.find({"username": "anonymous"}).first()
        assert u._id == u2._id

    def test_with_init_subclass(self):
        if sys.version_info[0:2] < (3, 6):
            raise SkipTest('__init_subclass__ not supported before python 3.6')

        class User(MappedClass):
            class __mongometa__:
                name = "users_with_init_subclass"
                session = self.session

            _id = FieldProperty(S.ObjectId)
            username = FieldProperty(str)

            def __init_subclass__(cls, constant=1, **kwargs):
                super().__init_subclass__(**kwargs)
                cls.prop = constant

        class B(User, constant=2):
            pass

        b = B()
        assert b.prop == 2

    def test_delete_super(self):
        class User(MappedClass):
            class __mongometa__:
                name = "user_with_custom_delete"
                session = self.session

            _id = FieldProperty(S.ObjectId)
            username = FieldProperty(str)
            
            def delete(self):
                super(User, self).delete()

        u = User(_id=None, username="anonymous")
        self.session.flush()
        u.delete()
        self.session.flush()

        assert not User.query.find({"username": "anonymous"}).count()


class TestMappingReal(TestMapping):
    DATASTORE = "mongodb://localhost/ming_tests?serverSelectionTimeoutMS=100"

        
class TestRelation(TestCase):
    DATASTORE = 'mim:///test_db'

    def setUp(self):
        Mapper._mapper_by_classname.clear()
        self.datastore = create_datastore(self.DATASTORE)
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
        try:
            self.datastore.conn.drop_all()
        except TypeError:
            self.datastore.conn.drop_database(self.datastore.db)

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

    def test_writable(self):
        parent = self.Parent(_id=1)
        children = [ self.Child(_id=i, parent_id=1) for i in range(5) ]
        other_parent = self.Parent(_id=2)
        self.session.flush()
        self.session.clear()

        child = self.Child.query.get(_id=4)
        child.parent = other_parent
        self.session.flush()
        self.session.clear()

        parent1 = self.Parent.query.get(_id=1)
        self.assertEqual(len(parent1.children), 4)

        parent2 = self.Parent.query.get(_id=2)
        self.assertEqual(len(parent2.children), 1)

    def test_writable_backref(self):
        parent = self.Parent(_id=1)
        children = [ self.Child(_id=i, parent_id=1) for i in range(5) ]
        self.session.flush()
        self.session.clear()

        parent = self.Parent.query.get(_id=1)
        parent.children = parent.children[:4]
        self.session.flush()
        self.session.clear()

        parent = self.Parent.query.get(_id=1)
        self.assertEqual(len(parent.children), 4)

    def test_nullable_relationship(self):
        parent = self.Parent(_id=1)
        children = [ self.Child(_id=i, parent_id=1) for i in range(5) ]
        self.session.flush()
        self.session.clear()

        child = self.Child.query.get(_id=0)
        child.parent = None
        self.session.flush()
        self.session.clear()

        parent = self.Parent.query.get(_id=1)
        self.assertEqual(len(parent.children), 4)

        child = self.Child.query.get(_id=0)
        self.assertEqual(child.parent, None)
        self.assertEqual(child.parent_id, None)

    def test_nullable_foreignid(self):
        parent = self.Parent(_id=1)
        children = [ self.Child(_id=i, parent_id=1) for i in range(5) ]
        self.session.flush()
        self.session.clear()

        child = self.Child.query.get(_id=0)
        child.parent_id = None
        self.session.flush()
        self.session.clear()

        child = self.Child.query.get(_id=0)
        self.assertEqual(child.parent_id, None)
        self.assertEqual(child.parent, None)

        parent = self.Parent.query.get(_id=1)
        self.assertEqual(len(parent.children), 4)


class TestRealMongoRelation(TestRelation):
    DATASTORE = "mongodb://localhost/ming_tests?serverSelectionTimeoutMS=100"


class TestManyToManyListRelation(TestCase):

    def setUp(self):
        Mapper._mapper_by_classname.clear()
        self.datastore = create_datastore('mim:///test_db')
        self.session = ODMSession(bind=self.datastore)
        class Parent(MappedClass):
            class __mongometa__:
                name='parent'
                session = self.session
            _id = FieldProperty(int)
            children = RelationProperty('Child')
            _children = ForeignIdProperty('Child', uselist=True)
        class Child(MappedClass):
            class __mongometa__:
                name='child'
                session = self.session
            _id = FieldProperty(int)
            parents = RelationProperty('Parent')
        Mapper.compile_all()
        self.Parent = Parent
        self.Child = Child

    def tearDown(self):
        self.session.clear()
        self.datastore.conn.drop_all()

    def test_compiled_field(self):
        self.assertEqual(self.Parent._children.field.type, [self.Child._id.field.type])

    def test_parent(self):
        children = [ self.Child(_id=i) for i in range(5) ]
        parent = self.Parent(_id=1)
        parent._children = [c._id for c in children]
        other_parent = self.Parent(_id=2)
        other_parent._children = [c._id for c in children[:2]]
        self.session.flush()
        self.session.clear()

        parent = self.Parent.query.get(_id=1)
        self.assertEqual(len(parent.children), 5)
        self.session.clear()

        child = self.Child.query.get(_id=0)
        self.assertEqual(len(child.parents), 2)

    def test_instrumented_readonly(self):
        children = [ self.Child(_id=i) for i in range(5) ]
        parent = self.Parent(_id=1)
        parent._children = [c._id for c in children]
        self.session.flush()
        self.session.clear()

        parent = self.Parent.query.get(_id=1)
        self.assertRaises(TypeError, parent.children.append, children[0])

    def test_writable(self):
        children = [ self.Child(_id=i) for i in range(5) ]
        parent = self.Parent(_id=1)
        parent._children = [c._id for c in children]
        self.session.flush()
        self.session.clear()

        parent = self.Parent.query.get(_id=1)
        parent.children = parent.children + [self.Child(_id=5)]
        self.session.flush()
        self.session.clear()

        parent = self.Parent.query.get(_id=1)
        self.assertEqual(len(parent.children), 6)
        self.session.clear()

        child = self.Child.query.get(_id=5)
        self.assertEqual(len(child.parents), 1)

        parent = self.Parent.query.get(_id=1)
        parent.children = []
        self.session.flush()
        self.session.clear()

        parent = self.Parent.query.get(_id=1)
        self.assertEqual(len(parent.children), 0)
        self.session.clear()

    def test_writable_backref(self):
        children = [ self.Child(_id=i) for i in range(5) ]
        parent = self.Parent(_id=1)
        parent._children = [c._id for c in children]
        other_parent = self.Parent(_id=2)
        other_parent._children = [c._id for c in children[:2]]
        self.session.flush()
        self.session.clear()

        child = self.Child.query.get(_id=4)
        self.assertEqual(len(child.parents), 1)
        child.parents = child.parents + [other_parent]
        self.session.flush()
        self.session.clear()

        child = self.Child.query.get(_id=4)
        self.assertEqual(len(child.parents), 2)
        self.session.clear()

        child = self.Child.query.get(_id=4)
        child.parents = [parent]
        self.session.flush()
        self.session.clear()

        child = self.Child.query.get(_id=4)
        self.assertEqual(len(child.parents), 1)
        self.session.clear()

class TestManyToManyListReverseRelation(TestCase):

    def setUp(self):
        Mapper._mapper_by_classname.clear()
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
            parents = RelationProperty('Parent')
            _parents = ForeignIdProperty('Parent', uselist=True)
        Mapper.compile_all()
        self.Parent = Parent
        self.Child = Child

    def tearDown(self):
        self.session.clear()
        self.datastore.conn.drop_all()

    def test_compiled_field(self):
        self.assertEqual(self.Child._parents.field.type, [self.Parent._id.field.type])

    def test_parent(self):
        parent = self.Parent(_id=1)
        other_parent = self.Parent(_id=2)

        children = [ self.Child(_id=i, _parents=[parent._id]) for i in range(5) ]
        for c in children[:2]:
            c._parents.append(other_parent._id)
        self.session.flush()
        self.session.clear()

        parent = self.Parent.query.get(_id=1)
        self.assertEqual(len(parent.children), 5)
        self.session.clear()

        child = self.Child.query.get(_id=0)
        self.assertEqual(len(child.parents), 2)


class TestManyToManyListCyclic(TestCase):

    def setUp(self):
        Mapper._mapper_by_classname.clear()
        self.datastore = create_datastore('mim:///test_db')
        self.session = ODMSession(bind=self.datastore)

        class TestCollection(MappedClass):
            class __mongometa__:
                name='test_collection'
                session = self.session
            _id = FieldProperty(int)

            children = RelationProperty('TestCollection')
            _children = ForeignIdProperty('TestCollection', uselist=True)
            parents = RelationProperty('TestCollection', via=('_children', False))

        Mapper.compile_all()
        self.TestCollection = TestCollection

    def tearDown(self):
        self.session.clear()
        self.datastore.conn.drop_all()

    def test_compiled_field(self):
        self.assertEqual(self.TestCollection._children.field.type, [self.TestCollection._id.field.type])

    def test_cyclic(self):
        children = [ self.TestCollection(_id=i) for i in range(10, 15) ]
        parent = self.TestCollection(_id=1)
        parent._children = [c._id for c in children]
        other_parent = self.TestCollection(_id=2)
        other_parent._children = [c._id for c in children[:2]]
        self.session.flush()
        self.session.clear()

        parent = self.TestCollection.query.get(_id=1)
        self.assertEqual(len(parent.children), 5)
        self.session.clear()

        child = self.TestCollection.query.get(_id=10)
        self.assertEqual(len(child.parents), 2)


class TestRelationWithNone(TestCase):

    def setUp(self):
        Mapper._mapper_by_classname.clear()
        self.datastore = create_datastore('mim:///test_db')
        self.session = ODMSession(bind=self.datastore)
        class GrandParent(MappedClass):
            class __mongometa__:
                name='grand_parent'
                session=self.session
            _id = FieldProperty(int)
        class Parent(MappedClass):
            class __mongometa__:
                name='parent'
                session = self.session
            _id = FieldProperty(int)
            grandparent_id = ForeignIdProperty('GrandParent')
            grandparent = RelationProperty('GrandParent')
            children = RelationProperty('Child')
        class Child(MappedClass):
            class __mongometa__:
                name='child'
                session = self.session
            _id = FieldProperty(int)
            parent_id = ForeignIdProperty('Parent', allow_none=True)
            parent = RelationProperty('Parent')
        Mapper.compile_all()
        self.GrandParent = GrandParent
        self.Parent = Parent
        self.Child = Child

    def tearDown(self):
        self.session.clear()
        self.datastore.conn.drop_all()

    def test_none_allowed(self):
        parent = self.Parent(_id=1)
        child = self.Child(_id=1, parent_id=parent._id)
        none_parent = self.Parent(_id=None)
        none_child = self.Child(_id=2, parent_id=None)
        self.session.flush()
        self.session.clear()

        child = self.Child.query.get(_id=1)
        parent = child.parent
        self.assertEqual(parent._id, 1)

        none_child = self.Child.query.get(_id=2)
        none_parent = none_child.parent
        self.assertNotEqual(none_parent, None)
        self.assertEqual(none_parent._id, None)

    def test_none_not_allowed(self):
        grandparent = self.GrandParent(_id=1)
        parent = self.Parent(_id=1, grandparent_id=grandparent._id)
        none_grandparent = self.GrandParent(_id=None)
        none_parent = self.Parent(_id=2, grandparent_id=None)
        self.session.flush()
        self.session.clear()

        parent = self.Parent.query.get(_id=1)
        grandparent = parent.grandparent
        self.assertEqual(grandparent._id, 1)

        none_parent = self.Parent.query.get(_id=2)
        none_grandparent = none_parent.grandparent
        self.assertEqual(none_grandparent, None)


class ObjectIdRelationship(TestCase):
    def setUp(self):
        Mapper._mapper_by_classname.clear()
        
        self.datastore = create_datastore('mim:///test_db')
        self.session = ODMSession(bind=self.datastore)
        class Parent(MappedClass):
            class __mongometa__:
                name='parent'
                session = self.session
            _id = FieldProperty(S.ObjectId)
            children = ForeignIdProperty('Child', uselist=True)
            field_with_default_id = ForeignIdProperty(
                'Child',
                uselist=True,
                if_missing=lambda:[bson.ObjectId('deadbeefdeadbeefdeadbeef')])
            field_with_default = RelationProperty('Child', 'field_with_default_id')
        class Child(MappedClass):
            class __mongometa__:
                name='child'
                session = self.session
            _id = FieldProperty(S.ObjectId)
            parent_id = ForeignIdProperty(Parent)
            field_with_default_id = ForeignIdProperty(
                Parent,
                if_missing=lambda:bson.ObjectId('deadbeefdeadbeefdeadbeef'))
            field_with_default = RelationProperty('Parent', 'field_with_default_id')
        
        Mapper.compile_all()
        self.Parent = Parent
        self.Child = Child

    def tearDown(self):
        self.session.clear()
        self.datastore.conn.drop_all()

    def test_empty_relationship(self):
        child = self.Child()
        self.session.flush()
        self.assertIsNone(child.parent_id)

    def test_empty_list_relationship(self):
        parent = self.Parent()
        self.session.flush()
        self.assertEqual(parent.children, [])

    def test_default_relationship(self):
        parent = self.Parent(_id=bson.ObjectId('deadbeefdeadbeefdeadbeef'))
        child = self.Child()
        self.session.flush()
        self.assertEqual(child.field_with_default, parent)

    def test_default_list_relationship(self):
        child = self.Child(_id=bson.ObjectId('deadbeefdeadbeefdeadbeef'))
        parent = self.Parent()
        self.session.flush()
        self.assertEqual(parent.field_with_default, [child])


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
    DATASTORE = 'mim:///test_db'

    def setUp(self):
        self.datastore = create_datastore(self.DATASTORE)
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
        try:
            self.datastore.conn.drop_all()
        except TypeError:
            self.datastore.conn.drop_database(self.datastore.db)

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


class TestRealBasicMapping(TestBasicMapping):
    DATASTORE = "mongodb://localhost/test_ming?serverSelectionTimeoutMS=100"


class TestPolymorphic(TestCase):

    def setUp(self):
        Mapper._mapper_by_classname.clear()
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
        r = [x.__class__ for x in q]
        self.assertEqual(2, len(r))
        self.assertTrue(self.Base in r)
        self.assertTrue(self.Derived in r)


class TestODMCursor(TestCase):

    def test_bool_exc(self):
        session = None
        class Base(MappedClass):
            pass
        cls = Base
        mongo_cursor = None
        cursor = ODMCursor(session, cls, mongo_cursor)
        self.assertRaises(MingException, lambda: bool(cursor))


class TestHooks(TestCase):

    def setUp(self):
        Mapper._mapper_by_classname.clear()
        self.datastore = create_datastore('mim:///test_db')
        self.session = ODMSession(bind=self.datastore)
        self.hooks_called = defaultdict(list)
        tc = self
        class Basic(MappedClass):
            class __mongometa__:
                name = 'hook'
                session = self.session
                def before_save(instance):
                    tc.hooks_called['before_save'].append(instance)

            _id = FieldProperty(S.ObjectId)
            a = FieldProperty(int)
        Mapper.compile_all()
        self.Basic = Basic
        self.session.remove(self.Basic)

    def test_hook_base(self):
        doc = self.Basic()
        doc.a = 5
        self.session.flush()
        self.assertEqual(self.hooks_called['before_save'],
                         [
                             {'_id': doc._id, 'a': doc.a}
                         ])
