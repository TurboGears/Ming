from unittest import TestCase
from collections import defaultdict

import mock
import pymongo
import six
from pymongo.errors import AutoReconnect

from ming.base import Cursor
from ming.declarative import Document
from ming.metadata import Field, Index
from ming import schema as S
from ming.session import Session
from ming.exc import MingException

def mock_datastore():
    def mock_collection():
        c = mock.Mock()
        c.find_one = mock.Mock(return_value={})
        return c
    ds = mock.Mock()
    ds.db = defaultdict(mock_collection)
    return ds

class TestIndex(TestCase):

    def test_string_index(self):
        class TestDoc(Document):
            class __mongometa__:
                indexes = [ 'abc' ]
            _id = Field(S.Int)
            abc=Field(S.Int, if_missing=None)
        assert len(TestDoc.m.indexes) == 1, TestDoc.m.indexes


class TestRenameField(TestCase):

    def setUp(self):
        self.MockSession = mock.Mock()
        self.MockSession.db = mock.MagicMock()
        class TestDoc(Document):
            class __mongometa__:
                name='test_doc'
                session = self.MockSession
            _a = Field('a', S.Int, if_missing=S.Missing)
            @property
            def a(self):
                native = getattr(self, '_a', None)
                if native < 10: return native
                return 10
        self.TestDoc = TestDoc

    def test_rename_field(self):
        doc = self.TestDoc.make(dict(a=5))
        assert doc == { 'a': 5 }
        assert doc.a == 5
        assert doc._a == 5
        doc = self.TestDoc.make(dict(a=50))
        assert doc == { 'a': 50 }
        assert doc.a == 10
        assert doc._a == 50

class TestDocument(TestCase):

    def setUp(self):
        self.MockSession = mock.Mock()
        self.MockSession.db = mock.MagicMock()
        class TestDoc(Document):
            class __mongometa__:
                name='test_doc'
                session = self.MockSession
                indexes = [ ('a',) ]
            a=Field(S.Int, if_missing=None)
            b=Field(S.Object(dict(a=S.Int(if_missing=None))))
        class TestDocNoSchema(Document):
            class __mongometa__:
                name='test_doc'
                session = self.MockSession
        self.TestDoc = TestDoc
        self.TestDocNoSchema = TestDocNoSchema

    def test_field(self):
        doc = self.TestDoc(dict(a=1, b=dict(a=5)))
        self.assertEqual(doc.a, 1)
        self.assertEqual(doc.b, dict(a=5))
        doc.a = 5
        self.assertEqual(doc, dict(a=5, b=dict(a=5)))
        del doc.a
        self.assertEqual(doc, dict(b=dict(a=5)))
        self.assertRaises(AttributeError, getattr, doc, 'c')
        self.assertRaises(AttributeError, getattr, doc, 'a')

    def test_no_schema(self):
        doc = self.TestDocNoSchema.make(dict(a=5, b=6))
        self.assertEqual(doc.a, 5)
        self.assertEqual(doc.b, 6)

    def test_mgr_with_session(self):
        other_session = mock.Mock()
        self.assertEqual(self.TestDoc.m.with_session(other_session).session, other_session)

    def test_mgr_cls_proxies(self):
        self.TestDoc.m.find(dict(a=5), a=5)
        self.MockSession.find.assert_called_with(self.TestDoc, dict(a=5), a=5)

    def test_mgr_inst_proxies(self):
        doc = self.TestDoc.make(dict(a=5))
        doc.m.save(1,2,a=5)
        self.MockSession.save.assert_called_with(doc, 1,2,a=5)

    def test_mgr_ensure_indexes(self):
        self.TestDoc.m.ensure_indexes()
        self.MockSession.ensure_indexes.assert_called_with(self.TestDoc)

    def test_instance_remove(self):
        # remove operates on a whole collection

        self.TestDoc.m.remove()
        self.MockSession.remove.assert_called_with(self.TestDoc)

        doc = self.TestDoc.make(dict(a=5))
        self.assertRaises(AttributeError, getattr, doc.m, 'remove')


    def test_migrate(self):
        doc = self.TestDoc.make(dict(a=5))
        self.MockSession.find = mock.Mock(return_value=[doc])
        self.TestDoc.m.migrate()
        self.MockSession.find.assert_called_with(self.TestDoc)
        self.MockSession.save.assert_called_with(doc)

class TestIndexes(TestCase):

    def setUp(self):
        self.maxDiff = None
        self.MockSession = mock.Mock()
        self.MockSession.db = mock.MagicMock()
        class MyDoc(Document):
            class __mongometa__:
                session = self.MockSession
                name = 'test_some_indexes'
                indexes = [
                    ('test1', 'test2'),
                ]
                unique_indexes = [
                    ('test1',),
                ]
                custom_indexes = [
                    dict(fields=('test7',), unique=True, sparse=True),
                    dict(fields=('test8',), unique=False, sparse=True),
                    dict(fields=('test9',), expireAfterSeconds=5, name='TESTINDEX9')
                ]
                schema = dict(
                    _id = S.ObjectId,
                    test1 = str,
                    test2 = str,
                    test3 = int,
                    test7 = str,
                    test8 = int,
                )
        self.MyDoc = MyDoc

    def test_ensure_indexes(self):
        # make sure the manager constructor calls ensure_index with the right stuff
        self.MyDoc.m
        collection = self.MockSession.db[self.MyDoc.m.collection_name]
        ensure_index = collection.ensure_index
        args = ensure_index.call_args_list
        for a in args:
            print(a)
        indexes = [
            ( ([ ('test1', pymongo.ASCENDING), ('test2', pymongo.ASCENDING) ],),
              dict(unique=False, sparse=False, background=True) ),
            ( ([ ('test1', pymongo.ASCENDING) ], ),
              dict(unique=True, sparse=False, background=True) ),
            ( ( [ ('test7', pymongo.ASCENDING) ],),
              dict(unique=True, sparse=True, background=True) ),
            ( ( [ ('test8', pymongo.ASCENDING) ],),
              dict(unique=False, sparse=True, background=True) ) ]
        for i in indexes:
            self.assert_(i in args, args)

    def test_ensure_indexes_custom_options(self):
        self.MyDoc.m
        collection = self.MockSession.db[self.MyDoc.m.collection_name]
        ensure_index = collection.ensure_index
        args = ensure_index.call_args_list

        custom_named_index = None
        for index in self.MyDoc.m.indexes:
            if index.name == 'TESTINDEX9':
                custom_named_index = index
                break
        self.assert_(custom_named_index is not None, self.MyDoc.m.indexes)

        custom_index = ( ([ ('test9', pymongo.ASCENDING) ],),
                         dict(unique=False, sparse=False, background=True,
                              expireAfterSeconds=5, name='TESTINDEX9') )
        self.assert_(custom_index in args, args)

    def test_ensure_indexes_slave(self):
        # on a slave, an error will be thrown, but it should be swallowed
        collection = self.MockSession.db[self.MyDoc.__mongometa__.name]
        ensure_index = collection.ensure_index
        ensure_index.side_effect = AutoReconnect('not master')
        self.MyDoc.m
        assert ensure_index.called

        # don't keep trying after it failed once
        self.MyDoc.m
        assert ensure_index.call_count == 1, ensure_index.call_args_list

    def test_auto_ensure_indexes_option(self):
        ensure_index = self.MockSession.db[self.MyDoc.__mongometa__.name].ensure_index
        self.MockSession.bind.bind._auto_ensure_indexes = False
        self.MyDoc.m
        assert not ensure_index.called

    def test_ensure_indexes_other_error(self):
        # same as above, but no swallowing
        collection = self.MockSession.db[self.MyDoc.__mongometa__.name]
        ensure_index = collection.ensure_index
        ensure_index.side_effect = AutoReconnect('blah blah')

        self.assertRaises(AutoReconnect, lambda: self.MyDoc.m)
        assert ensure_index.called

    def test_index_inheritance_child_none(self):
        class MyChild(self.MyDoc):
            class __mongometa__:
                pass

        self.assertEqual(MyChild.__mongometa__.indexes,
                         self.MyDoc.__mongometa__.indexes)
        self.assertEqual(MyChild.__mongometa__.unique_indexes,
                         self.MyDoc.__mongometa__.unique_indexes)
        self.assertEqual(MyChild.__mongometa__.custom_indexes,
                         self.MyDoc.__mongometa__.custom_indexes)

    def test_index_inheritance_both(self):
        class MyChild(self.MyDoc):
            class __mongometa__:
                indexes = [
                    ('test3',),
                ]
                unique_indexes = [
                    ('test4',),
                ]
                custom_indexes = []
        class MyGrandChild(MyChild):
            class __mongometa__:
                indexes = [
                    ('test5',),
                ]
                unique_indexes = [
                    ('test6',),
                ]

        self.assertEqual(
            list(MyGrandChild.m.indexes),
            [ Index('test1', 'test2'),
              Index('test1', unique=True),
              Index('test7', unique=True, sparse=True),
              Index('test8', unique=False, sparse=True),
              Index('test9', expireAfterSeconds=5, name='TESTINDEX9'),
              Index('test3'),
              Index('test4', unique=True),
              Index('test5'),
              Index('test6', unique=True) ])

    def test_index_inheritance_neither(self):
        class NoIndexDoc(Document):
            class __mongometa__:
                session = Session()
                name = 'test123'
                schema = dict(
                    _id = S.ObjectId,
                    test1 = str,
                    test2 = str,
                    test3 = int,
                )
        class StillNone(NoIndexDoc):
            class __mongometa__:
                pass

        self.assertEqual(list(StillNone.m.indexes), [])

    def test_index_inheritance_parent_none(self):
        class NoIndexDoc(Document):
            class __mongometa__:
                session = Session()
                name = 'test123'
                schema = dict(
                    _id = S.ObjectId,
                    test1 = str,
                    test2 = str,
                    test3 = int,
                )
        class AddSome(NoIndexDoc):
            class __mongometa__:
                indexes = [
                    ('foo',),
                ]
                unique_indexes = [
                    ('bar',),
                ]

        self.assertEqual(
            list(AddSome.m.indexes),
            [ Index('foo'), Index('bar', unique=True) ])

class TestCursor(TestCase):

    def setUp(self):
        self.MockSession = mock.Mock()
        self.MockSession.db = mock.MagicMock()
        class IteratorMock(mock.Mock):
            def __init__(self, base_iter):
                super(IteratorMock, self).__init__()
                self._base_iter = base_iter
            def __iter__(self):
                return self
            def next(self):
                return next(self._base_iter)
            __next__ = next
        class TestDoc(Document):
            class __mongometa__:
                name='test_doc'
                session = self.MockSession
            a=Field(int)
            b=Field(S.Object(dict(a=int)))
        self.TestDoc = TestDoc
        mongo_cursor = IteratorMock(iter([ {}, {}, {} ]))
        mongo_cursor.count = mock.Mock(return_value=3)
        mongo_cursor.limit = mock.Mock(return_value=mongo_cursor)
        mongo_cursor.hint = mock.Mock(return_value=mongo_cursor)
        mongo_cursor.skip = mock.Mock(return_value=mongo_cursor)
        mongo_cursor.sort = mock.Mock(return_value=mongo_cursor)
        self.cursor = Cursor(TestDoc, mongo_cursor)

    def test_cursor(self):
        obj = dict(a=None, b=dict(a=None))
        self.assertEqual(self.cursor.count(), 3)
        self.assertEqual(self.cursor.next(), obj)
        self.cursor.limit(100)
        self.cursor.skip(10)
        self.cursor.hint('foo')
        self.cursor.sort('a')
        self.assertEqual(self.cursor.all(), [obj,obj])
        self.cursor.cursor.limit.assert_called_with(100)
        self.cursor.cursor.skip.assert_called_with(10)
        self.cursor.cursor.hint.assert_called_with('foo')
        self.cursor.cursor.sort.assert_called_with('a')
        self.assertRaises(MingException, lambda: bool(self.cursor))

    def test_first(self):
        obj = dict(a=None, b=dict(a=None))
        self.assertEqual(self.cursor.first(), obj)
        self.assertEqual(self.cursor.first(), obj)
        self.assertEqual(self.cursor.first(), obj)
        self.assertEqual(self.cursor.first(), None)

    def test_one_full(self):
        self.assertRaises(ValueError, self.cursor.one)

    def test_one_empty(self):
        self.cursor.all()
        self.assertRaises(ValueError, self.cursor.one)

    def test_one_ok(self):
        self.cursor.next()
        self.cursor.next()
        obj = dict(a=None, b=dict(a=None))
        self.assertEqual(self.cursor.one(), obj)

class TestPolymorphic(TestCase):

    def setUp(self):
        self.MockSession = mock.Mock()
        self.MockSession.db = mock.MagicMock()
        class Base(Document):
            class __mongometa__:
                name='test_doc'
                session = self.MockSession
                polymorphic_registry={}
                polymorphic_on='type'
                polymorphic_identity='base'
            type=Field(str)
            a=Field(int)
        class Derived(Base):
            class __mongometa__:
                name='test_doc'
                session = self.MockSession
                polymorphic_identity='derived'
            b=Field(int)
        self.Base = Base
        self.Derived = Derived

    def test_polymorphic(self):
        self.assertEqual(self.Base.make(dict(type='base')),
                         dict(type='base', a=None))
        self.assertEqual(self.Base.make(dict(type='derived')),
                         dict(type='derived', a=None, b=None))


class TestHooks(TestCase):

    def setUp(self):
        from ming.session import Session
        self.bind = mock_datastore()
        self.session = Session(self.bind)
        self.hooks_called = defaultdict(list)
        tc = self
        class Base(Document):
            class __mongometa__:
                name='test_doc'
                session = self.session
                polymorphic_registry={}
                polymorphic_on='type'
                polymorphic_identity='base'
                def before_save(instance):
                    tc.hooks_called['before_save'].append(instance)
            _id=Field(int)
            type=Field(str)
            a=Field(int)
        class Derived(Base):
            class __mongometa__:
                name='test_doc'
                session = self.session
                polymorphic_identity='derived'
            b=Field(int)
        self.Base = Base
        self.Derived = Derived

    def test_hook_base(self):
        b = self.Base(dict(_id=1, a=5))
        b.m.save()
        self.assertEqual(self.hooks_called['before_save'], [b])
        d = self.Derived(dict(a=5, b=6))
        d.m.save()
        self.assertEqual(self.hooks_called['before_save'], [b, d])

class TestMigration(TestCase):

    def setUp(self):
        self.MockSession = mock.Mock()
        self.MockSession.db = mock.MagicMock()

        class TestDoc(Document):
            class __mongometa__:
                name='test_doc'
                session = self.MockSession
            version=Field(1)
            a=Field(int)

        class TestDoc(Document):
            class __mongometa__:
                name='test_doc'
                session = self.MockSession
                version_of = TestDoc
                def migrate(old_doc):
                    return dict(old_doc, b=42, version=2)
            version=Field(2)
            a=Field(int)
            b=Field(int, required=True)
        self.TestDoc = TestDoc

    def testMigration(self):
        self.assertEqual(self.TestDoc.make(dict(version=1, a=5)),
                         dict(version=2, a=5, b=42))

