'''
Test the new functional syntax for collection definition
'''

from unittest import TestCase
from collections import defaultdict

import mock
import pymongo
import six

from ming import Session, Field, Index, Cursor, collection
from ming import schema as S

def mock_datastore():
    ds = mock.Mock()
    ds.db = defaultdict(mock_collection)
    return ds

def mock_collection():
    c = mock.Mock()
    c.find_one = mock.Mock(return_value={})
    return c

class TestDocument(TestCase):

    def setUp(self):
        self.MockSession = mock.Mock()
        self.MockSession.db = mock.MagicMock()
        self.TestDoc = collection(
            'test_doc', self.MockSession,
            Field('a', int, if_missing=None, index=True),
            Field('b', S.Object, if_missing=dict(a=None)))
        self.TestDocNoSchema = collection(
            'test_doc', self.MockSession)

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

    def test_field_missing(self):
        doc = self.TestDoc.make(dict(a=1))
        self.assertIsNotNone(doc.b)
        self.assertEqual(doc.b['a'], None)

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
        self.MockSession = mock.Mock()
        self.MockSession.db = mock.MagicMock()
        self.MyDoc = collection(
            'test_some_indexes', self.MockSession,
            Field('_id', S.ObjectId),
            Field('test1', str, index=True, unique=True),
            Field('test2', str),
            Field('test3', str),
            Index('test2'),
            Index('test1', 'test2', direction=pymongo.DESCENDING))

    @mock.patch('ming.session.Session.ensure_index')
    def test_ensure_indexes(self, ensure_index):
        # make sure the manager constructor calls ensure_index with the right
        # stuff
        self.MyDoc.m
        collection = self.MockSession.db[self.MyDoc.m.collection_name]
        ensure_index = collection.ensure_index
        args = ensure_index.call_args_list
        indexes = [
            ( ([ ('test1', pymongo.DESCENDING), ('test2', pymongo.DESCENDING) ],),
              dict(unique=False, sparse=False, background=True) ),
            ( ([ ('test1', pymongo.ASCENDING) ], ),
              dict(unique=True, sparse=False, background=True) ), ]
        for i in indexes:
            self.assert_(i in args, args)


    @mock.patch('ming.session.Session.ensure_index')
    def test_ensure_indexes_slave(self, ensure_index):
        # on a slave, an error will be thrown, but it should be swallowed
        self.MyDoc.m
        collection = self.MockSession.db[self.MyDoc.m.collection_name]
        ensure_index = collection.ensure_index
        assert ensure_index.called

    def test_index_inheritance_child_none(self):
        MyChild = collection(self.MyDoc, collection_name='my_child')

        self.assertEqual(
            list(MyChild.m.indexes),
            list(self.MyDoc.m.indexes))

    def test_index_inheritance_both(self):
        MyChild = collection(
            self.MyDoc,
            Index('test3'),
            Index('test4', unique=True),
            collection_name='my_child')
        MyGrandChild = collection(
            MyChild,
            Index('test5'),
            Index('test6', unique=True),
            collection_name='my_grand_child')

        self.assertEqual(
            list(MyGrandChild.m.indexes),
            [ Index('test1', unique=True),
              Index('test2'),
              Index(('test1', -1), ('test2', -1)),
              Index('test3'),
              Index('test4', unique=True),
              Index('test5'),
              Index('test6', unique=True) ])

    def test_index_inheritance_neither(self):
        NoIndexDoc = collection(
            'test123', self.MockSession,
            Field('_id', S.ObjectId),
            Field('test1', str),
            Field('test2', str),
            Field('test3', str))
        StillNone = collection(NoIndexDoc)

        self.assertEqual(list(StillNone.m.indexes), [])

    def test_index_inheritance_parent_none(self):
        NoIndexDoc = collection(
            'test123', self.MockSession,
            Field('_id', S.ObjectId),
            Field('test1', str),
            Field('test2', str),
            Field('test3', str))
        AddSome = collection(
            NoIndexDoc,
            Index('foo'),
            Index('bar', unique=True))

        self.assertEqual(list(AddSome.m.indexes),
                         [ Index('foo'), Index('bar', unique=True) ])

class TestCursor(TestCase):

    def setUp(self):
        class IteratorMock(mock.Mock):
            def __init__(self, base_iter):
                super(IteratorMock, self).__init__()
                self._base_iter = base_iter
            def __iter__(self):
                return self
            def next(self):
                return next(self._base_iter)
            __next__ = next

        self.MockSession = mock.Mock()
        self.MockSession.db = mock.MagicMock()
        self.TestDoc = collection(
            'test_doc', self.MockSession,
            Field('a', int),
            Field('b', dict(a=int)))

        mongo_cursor = IteratorMock(iter([ {}, {}, {} ]))
        mongo_cursor.count = mock.Mock(return_value=3)
        mongo_cursor.limit = mock.Mock(return_value=mongo_cursor)
        mongo_cursor.hint = mock.Mock(return_value=mongo_cursor)
        mongo_cursor.skip = mock.Mock(return_value=mongo_cursor)
        mongo_cursor.sort = mock.Mock(return_value=mongo_cursor)
        self.cursor = Cursor(self.TestDoc, mongo_cursor)

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
        self.Base = collection(
            'test_doc', self.MockSession,
            Field('type', str),
            Field('a', int),
            polymorphic_on='type',
            polymorphic_identity='base')
        self.Derived = collection(
            self.Base,
            Field('b', int),
            polymorphic_identity='derived')

    def test_polymorphic(self):
        self.assertEqual(self.Base.make(dict(type='base')),
                         dict(type='base', a=None))
        self.assertEqual(self.Base.make(dict(type='derived')),
                         dict(type='derived', a=None, b=None))


class TestHooks(TestCase):

    def setUp(self):
        self.bind = mock_datastore()
        self.session = Session(self.bind)
        self.hooks_called = defaultdict(list)
        self.Base = collection(
            'test_doc', self.session,
            Field('_id', int),
            Field('type', str),
            Field('a', int),
            polymorphic_on='type',
            polymorphic_identity='base',
            before_save = lambda inst: (
                self.hooks_called['before_save'].append(inst)))
        self.Derived = collection(
            self.Base,
            Field('b', int),
            polymorphic_identity='derived')

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
        TestDoc_old = collection(
            'test_doc', self.MockSession,
            Field('version', 1),
            Field('a', int))
        self.TestDoc = collection(
            'test_doc', self.MockSession,
            Field('version', 2),
            Field('a', int),
            Field('b', int, required=True),
            version_of=TestDoc_old,
            migrate=lambda old_doc: dict(old_doc, b=42 ,version=2))

    def testMigration(self):
        self.assertEqual(self.TestDoc.make(dict(version=1, a=5)),
                         dict(version=2, a=5, b=42))


