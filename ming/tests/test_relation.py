from collections import defaultdict
from unittest import TestCase

import mock

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
