from collections import defaultdict
from unittest import TestCase

import mock
import pymongo

import ming
from ming import Document, Field
from ming.relation import relation
from ming import schema as S
from ming import datastore as DS
from ming.session import Session

def mock_datastore():
    ds = mock.Mock()
    ds.db = defaultdict(mock_collection)
    return ds

def mock_collection():
    c = mock.Mock()
    c.find_one = mock.Mock(return_value={})
    return c


class TestSession(TestCase):

    def setUp(self):
        self.datastore = DS.DataStore(
            master='mongo://localhost:27017/test_db')
        self.session = Session(self.datastore, autoflush=False)
        class Parent(Document):
            class __mongometa__:
                name='parent'
                session = self.session
            _id=Field(S.ObjectId)
            children = relation('Child', backref='parent', via='parent_id')
        class Child(Document):
            class __mongometa__:
                name='child'
                session = self.session
            _id=Field(S.ObjectId)
            parent_id = Field(S.ObjectId)
        self.Parent = Parent
        self.Child = Child
        Document.fixup_backrefs()

    def test_basic(self):
        parent = self.Parent.make({})
        self.session.uow.flush()
        children = [ self.Child.make(dict(parent_id=parent._id))
                     for i in range(5) ]
        self.session.uow.flush()
        for child in parent.children:
            assert child.parent is parent
class TestSessionUnitOfWork(TestCase):

    def setUp(self):
        self.bind = mock_datastore()
        self.session = Session(self.bind, autoflush=False)
        class TestDoc(Document):
            class __mongometa__:
                name='test_doc'
                session = self.session
                indexes = [ ('b','c') ]
                unique_indexes = [ ('cc'), ]
            _id=Field(S.ObjectId, if_missing=None)
            a=Field(S.Int, if_missing=None)
            b=Field(S.Object, dict(a=S.Int(if_missing=None)))
            c=Field([int])
            cc=Field(dict(dd=int, ee=int))
        self.TestDoc = TestDoc

    def testCreateNew(self):
        doc = self.TestDoc({})
        assert 'a' not in doc
        self.session.uow.flush()
        assert 'a' in doc

    def testSameObject(self):
        doc = self.TestDoc(dict(
                _id=pymongo.bson.ObjectId()))
        self.session.uow.flush()
        doc2 = self.TestDoc.m.get(_id=doc._id)
        assert doc is doc2

    def testObjectState(self):
        doc = self.TestDoc(dict(
                _id=pymongo.bson.ObjectId()))
        assert doc in list(self.session.uow.new)
        self.session.uow.flush()
        assert doc in list(self.session.uow.clean)
        doc['a'] = 5
        assert doc in list(self.session.uow.dirty)
        self.session.uow.flush()
        assert doc in list(self.session.uow.clean)
        doc.pop('a')
        assert doc in list(self.session.uow.dirty)
        self.session.uow.flush()
        assert doc in list(self.session.uow.clean)
        doc.popitem()
        assert doc in list(self.session.uow.dirty)
        self.session.uow.flush()
        assert doc in list(self.session.uow.clean)
        doc.m.delete()
        assert doc in list(self.session.uow.deleted)
        self.session.uow.flush()
        assert doc not in list(self.session.uow.clean)
        self.session.clear()
        assert doc not in list(self.session.uow.clean)
        self.session.uow.save_clean(doc)
        assert doc in list(self.session.uow.clean)
        repr(self.session.uow)
        doc.clear()
        assert doc in list(self.session.uow.dirty)
        self.session.uow.flush()

    def testSubobjectState(self):
        doc = self.TestDoc(dict(
                _id=pymongo.bson.ObjectId()))
        assert doc in list(self.session.uow.new)
        self.session.uow.flush()
        assert doc in list(self.session.uow.clean)
        doc.cc.dd = 5
        assert doc in list(self.session.uow.dirty)
        self.session.uow.flush()
        assert doc in list(self.session.uow.clean)
        doc.c.append(5)
        assert doc in list(self.session.uow.dirty)
        self.session.uow.flush()
        assert doc in list(self.session.uow.clean)
        

