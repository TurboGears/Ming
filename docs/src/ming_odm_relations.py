#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Clear the class names in case MappedClasses are declared in another example
from ming.odm import Mapper
Mapper._mapper_by_classname.clear()

from ming import create_datastore
from ming.odm import ThreadLocalODMSession
session = ThreadLocalODMSession(bind=create_datastore('mim:///odm_tutorial'))

from ming import schema
from ming.odm import MappedClass
from ming.odm import FieldProperty, ForeignIdProperty, RelationProperty

class WikiPage(MappedClass):
    class __mongometa__:
        session = session
        name = 'wiki_page'

    _id = FieldProperty(schema.ObjectId)
    title = FieldProperty(schema.String(required=True))
    text = FieldProperty(schema.String(if_missing=''))

    comments = RelationProperty('WikiComment')

class WikiComment(MappedClass):
    class __mongometa__:
        session = session
        name = 'wiki_comment'

    _id = FieldProperty(schema.ObjectId)
    page_id = ForeignIdProperty('WikiPage')
    text = FieldProperty(schema.String(if_missing=''))

    page = RelationProperty('WikiPage')

class Parent(MappedClass):
    class __mongometa__:
        name='parent'
        session = session

    _id = FieldProperty(schema.ObjectId)
    name = FieldProperty(schema.String(required=True))
    _children = ForeignIdProperty('Child', uselist=True)

    children = RelationProperty('Child')

class Child(MappedClass):
    class __mongometa__:
        name='child'
        session = session

    _id = FieldProperty(schema.ObjectId)
    name = FieldProperty(schema.String(required=True))

    parents = RelationProperty('Parent')

Parent.query.remove({})
Child.query.remove({})
WikiPage.query.remove({})
WikiComment.query.remove({})

#{compileall
from ming.odm import Mapper
Mapper.compile_all()
#}


def snippet1_1():
    WikiPage(title='MyFirstPage', text='This is my first page!!!')
    session.flush()
    session.clear()

    # Get a page for which to add the comments
    wp = WikiPage.query.get(title='MyFirstPage')

    # Create some comments
    WikiComment(page_id=wp._id, text='A comment')
    WikiComment(page_id=wp._id, text='Another comment')
    session.flush()

def snippet1_2():
    session.clear()
    # Load the original page
    wp = WikiPage.query.get(title='MyFirstPage')

    # View its comments
    wp.comments
    wp.comments[0].page is wp

def snippet1_3():
    session.clear()
    # Load the original page
    wp = WikiPage.query.get(title='MyFirstPage')
    len(wp.comments)

    wk = WikiComment(text='A comment')
    wk.page = wp
    session.flush()

    # Refresh the page to see the relation change even on its side.
    wp = session.refresh(wp)
    len(wp.comments)

def snippet1_4():
    session.clear()
    # Load the original page
    wp = WikiPage.query.get(title='MyFirstPage')
    len(wp.comments)

    wk = WikiComment(text='A comment')
    wp.comments = wp.comments + [wk]
    session.flush()

    # Refresh the page to see the relation change even on its side.
    wp = session.refresh(wp)
    len(wp.comments)
    # Refresh the comment to see the relation change even on its side.
    wk = session.refresh(wk)
    wk.page is wp


def snippet2_1():
    session.clear()
    # Create a bunch of Parents and Children
    p1, p2 = Parent(name='p1'), Parent(name='p2')
    c1, c2 = Child(name='c1'), Child(name='c2')

    # Relate them through their relationship
    p1.children = [c1, c2]
    c2.parents = [p1, p2]
    session.flush()

    # Fetch them back to see if the relations are correct
    session.clear()
    p1, p2 = Parent.query.find({}).sort('name').all()
    c1, c2 = Child.query.find({}).sort('name').all()

    len(c1.parents)  # c1 was only assigned to p1
    len(c2.parents)  # c2 was assigned both to p1 and p2
    len(p1.children) == 2 and c1 in p1.children
    len(p2.children) == 1 and p2.children[0] is c2