=======================
Ming ORM Layer Tutorial
=======================

.. [[[cog from cog_utils import interact]]]
.. [[[end]]]

Introduction
------------

In addition to the basic interface documented in :doc:`tour`, Ming provides a
higher-level abstraction for modeling objects in MongoDB.  This higher-layer
abstraction will be referred in this document as the ORM since it is implemented
in the "spirit" of object-relational mappers such as `SQLAlchemy
<http://sqlalchemy.org>`_.

The ORM provides several features beyond those provided by the basic Ming
modules:

unit-of-work pattern
    Using ORM-enabled sessions allows you to operate on in-memory objects
    exclusively until you are ready to "flush" all changes to the database.
    Although MongoDB doesn't provide transactions, the unit-of-work (UOW) pattern
    provides some of the benefits of transactions by delaying writes until you
    are fairly certain nothing is going to go wrong.

identity map
    In base Ming, each query returns unique document objects even if those
    document objects refer to the same document in the database.  The identity
    map in Ming ensures that when two queries each return results that correspond
    to the same document in the database, the queries will return the same Python
    object as well.

relations between objects
    Although MongoDB is non-relational, it is still useful to represent
    relationships between documents in the database.  The ORM layer in Ming
    provides the ability to model one-to-many relationships between documents as
    straightforward properties on Python objects.

The ORM Session
---------------

In basic Ming, the session is only responsible for attaching model classes to the
actual MongoDB datastore.  In the ORM, however, the session is responsible for
this as well as maintaining the unit of work, identity map, and relations between
objects.  The ORM session itself is not designed to be thread-safe, so Ming
provides a thread-local version of the session for safe operation in a
multithreaded environment.  We will be using the thread-local session for this
tutorial:

.. include:: src/ming_orm_tutorial.py
   :literal:
   :start-after: #{initial-imports
   :end-before: #}

The code above creates a thread-local ORM-aware session that we will use when
defining our model classes.  

Mapping Classes
---------------

In base Ming, the mapped classes were descended from the :class:`ming.base.Document`
class, itself a subclass of :class:`dict`.  In the ORM layer, mapped classes are
descended from :class:`ming.orm.mapped_class.MappedClass`, which is *not*
descended from :class:`dict`, to emphasize the difference between a mapped class
(which may contain relations to other objects) and a MongoDB document (which may
not).  For this tutorial, we will be modifying the Wiki example in :doc:`tour` to
use the ORM.  First, we need a few more imports:

.. include:: src/ming_orm_tutorial.py
   :literal:
   :start-after: #{orm-imports
   :end-before: #}

Now, we can define our model:

.. literalinclude:: src/ming_orm_tutorial.py
   :pyobject: WikiPage

At the end of the model file, you should call `compile_all()` on the
`MappedClass` to ensure that Ming has full information on all mapped classes:

.. include:: src/ming_orm_tutorial.py
   :literal:
   :start-after: #{compileall
   :end-before: #}

The only real differences here are that rather than inheriting from
:class:`ming.base.Document`, we are inheriting from
:class:`ming.orm.mapped_class.MappedClass`, and rather than using a
:class:`ming.base.Field`, we are using a
:class:`ming.orm.property.FieldProperty`.  (You might alwo notice the
`RelationProperty`; we will cover in :ref:`relation`.)  

Once we have the boilerplate out of
the way, we can create instances of the `WikiPage` as any other Python class.  One
thing to notice is that we don't explicitly call the `save()` method on the
`WikiPage`; that will be called for us automatically when we `flush()` the session:

.. [[[cog interact('ming_orm_tutorial', 1) ]]]

>>> wp = WikiPage(title='FirstPage',
...               text='This is my first page')
>>> wp
<WikiPage text='This is my first page'
  _id=ObjectId('4e126629fc26a23cf5000001')
  title='FirstPage'>
>>> session
TLProxy of <session>
  <UnitOfWork>
    <new>
      <WikiPage text='This is my first page'
          _id=ObjectId('4e126629fc26a23cf5000001')
          title='FirstPage'>
    <clean>
    <dirty>
    <deleted>
  <imap (1)>
    WikiPage : 4e126629fc26a23cf5000001 => <WikiPage text='This is my first page'
        _id=ObjectId('4e126629fc26a23cf5000001')
        title='FirstPage'>
>>> session.flush()
>>> session
TLProxy of <session>
  <UnitOfWork>
    <new>
    <clean>
      <WikiPage text='This is my first page'
          _id=ObjectId('4e126629fc26a23cf5000001')
          title='FirstPage'>
    <dirty>
    <deleted>
  <imap (1)>
    WikiPage : 4e126629fc26a23cf5000001 => <WikiPage text='This is my first page'
        _id=ObjectId('4e126629fc26a23cf5000001')
        title='FirstPage'>
>>> session.clear()
>>> session
TLProxy of <session>
  <UnitOfWork>
    <new>
    <clean>
    <dirty>
    <deleted>
  <imap (0)>

.. [[[end]]]

Once we have a `WikiPage` in the database, we can retrieve it using the `.query`
attribute, modify it, and flush the modified object out to the database:

.. [[[cog interact('ming_orm_tutorial', 2)]]]

>>> wp = WikiPage.query.get(title='FirstPage')
>>> session
TLProxy of <session>
  <UnitOfWork>
    <new>
    <clean>
      <WikiPage text=u'This is my first page'
          _id=ObjectId('4e126629fc26a23cf5000001')
          title=u'FirstPage'>
    <dirty>
    <deleted>
  <imap (1)>
    WikiPage : 4e126629fc26a23cf5000001 => <WikiPage text=u'This is my first page'
        _id=ObjectId('4e126629fc26a23cf5000001')
        title=u'FirstPage'>
>>> 
>>> # Verify the IdentityMap keeps only one copy of the object
>>> wp2 = WikiPage.query.get(title='FirstPage')
>>> wp is wp2
True
>>> 
>>> # Modify the object in memory
>>> wp.title = 'MyFirstPage'
>>> 
>>> # Notice that the object has been marked dirty
>>> session
TLProxy of <session>
  <UnitOfWork>
    <new>
    <clean>
    <dirty>
      <WikiPage text=u'This is my first page'
          _id=ObjectId('4e126629fc26a23cf5000001')
          title='MyFirstPage'>
    <deleted>
  <imap (1)>
    WikiPage : 4e126629fc26a23cf5000001 => <WikiPage text=u'This is my first page'
        _id=ObjectId('4e126629fc26a23cf5000001')
        title='MyFirstPage'>
>>> wp
<WikiPage text=u'This is my first page'
  _id=ObjectId('4e126629fc26a23cf5000001')
  title='MyFirstPage'>
>>> session.flush()
>>> 
>>> # We can also delete objects
>>> wp = WikiPage.query.get(title='MyFirstPage')
>>> wp.delete()
>>> session
TLProxy of <session>
  <UnitOfWork>
    <new>
    <clean>
    <dirty>
    <deleted>
      <WikiPage text=u'This is my first page'
          _id=ObjectId('4e126629fc26a23cf5000001')
          title='MyFirstPage'>
  <imap (1)>
    WikiPage : 4e126629fc26a23cf5000001 => <WikiPage text=u'This is my first page'
        _id=ObjectId('4e126629fc26a23cf5000001')
        title='MyFirstPage'>
>>> # Rather than flushing, we'll keep the object
>>> #   around and just clear the session instead
>>> session.clear()

.. [[[end]]]

.. _relation:

Relating Classes
----------------

The real power of the ORM comes in being able to view related classes.  Suppose
we wish to represent comments on a `WikiPage`:

.. literalinclude:: src/ming_orm_tutorial.py
   :pyobject: WikiComment

Here, we have defined a `ForeignIdProperty` `page_id` to reference the original
`Wikipage`.  This tells Ming to create a field in `WikiComment` that represents a
"foreign key" into the `WikiPage._id` field.  This sets up a one-to-many
relationship between `WikiPage` and `WikiComment`.  In order to actually use the
relationship, however, we must use the `RelationProperty` to reference the
related class.  In this case, we will use the property `page` to access the page
about which this comment refers.  To actually use these classes, we need to
create some comments:

.. [[[cog interact('ming_orm_tutorial', 3) ]]]

>>> wp = WikiPage.query.get(title='MyFirstPage')
>>> # Create some comments
>>> WikiComment(page_id=wp._id,
...             text='A comment')
<WikiComment text='A comment'
  _id=ObjectId('4e126629fc26a23cf5000004')
  page_id=ObjectId('4e126629fc26a23cf5000001')>
>>> WikiComment(page_id=wp._id,
...             text='Another comment')
<WikiComment text='Another comment'
  _id=ObjectId('4e126629fc26a23cf5000005')
  page_id=ObjectId('4e126629fc26a23cf5000001')>
>>> session.flush()
>>> session.clear()
>>> # Load the original page
>>> wp = WikiPage.query.get(title='MyFirstPage')
>>> session
TLProxy of <session>
  <UnitOfWork>
    <new>
    <clean>
      <WikiPage text=u'This is my first page'
          _id=ObjectId('4e126629fc26a23cf5000001')
          title=u'MyFirstPage'>
    <dirty>
    <deleted>
  <imap (1)>
    WikiPage : 4e126629fc26a23cf5000001 => <WikiPage text=u'This is my first page'
        _id=ObjectId('4e126629fc26a23cf5000001')
        title=u'MyFirstPage'>
>>> # View its comments
>>> wp.comments
I[<WikiComment text=u'A comment'
  _id=ObjectId('4e126629fc26a23cf5000004')
  page_id=ObjectId('4e126629fc26a23cf5000001')>, <WikiComment text=u'Another comment'
  _id=ObjectId('4e126629fc26a23cf5000005')
  page_id=ObjectId('4e126629fc26a23cf5000001')>]
>>> session
TLProxy of <session>
  <UnitOfWork>
    <new>
    <clean>
      <WikiPage text=u'This is my first page'
          _id=ObjectId('4e126629fc26a23cf5000001')
          title=u'MyFirstPage'>
      <WikiComment text=u'A comment'
          _id=ObjectId('4e126629fc26a23cf5000004')
          page_id=ObjectId('4e126629fc26a23cf5000001')>
      <WikiComment text=u'Another comment'
          _id=ObjectId('4e126629fc26a23cf5000005')
          page_id=ObjectId('4e126629fc26a23cf5000001')>
    <dirty>
    <deleted>
  <imap (3)>
    WikiPage : 4e126629fc26a23cf5000001 => <WikiPage text=u'This is my first page'
        _id=ObjectId('4e126629fc26a23cf5000001')
        title=u'MyFirstPage'>
    WikiComment : 4e126629fc26a23cf5000004 => <WikiComment text=u'A comment'
        _id=ObjectId('4e126629fc26a23cf5000004')
        page_id=ObjectId('4e126629fc26a23cf5000001')>
    WikiComment : 4e126629fc26a23cf5000005 => <WikiComment text=u'Another comment'
        _id=ObjectId('4e126629fc26a23cf5000005')
        page_id=ObjectId('4e126629fc26a23cf5000001')>
>>> wp.comments[0].page
<WikiPage text=u'This is my first page'
  _id=ObjectId('4e126629fc26a23cf5000001')
  title=u'MyFirstPage'>
>>> wp.comments[0].page is wp
True

.. [[[end]]]

And voilà, you have related objects.  Note that at present the relations between
objects are read-only, so if you want to make or break a relationship, you must
do it by setting the `ForeignIdProperty`.  

Querying the ORM
----------------

You've already seen how to retrieve single objects from the ORM using the
`query.get()` method on `MappedClass` objects.  You can also perform regular Ming
queries using the `query.find()` method:

.. [[[cog interact('ming_orm_tutorial', 4) ]]]

>>> wp = WikiPage.query.get(title='MyFirstPage')
>>> results = WikiComment.query.find(dict(page_id=wp._id))
>>> list(results)
[<WikiComment text=u'A comment'
  _id=ObjectId('4e126629fc26a23cf5000004')
  page_id=ObjectId('4e126629fc26a23cf5000001')>, <WikiComment text=u'Another comment'
  _id=ObjectId('4e126629fc26a23cf5000005')
  page_id=ObjectId('4e126629fc26a23cf5000001')>]

.. [[[end]]]

Dropping Down Below the ORM
---------------------------

You can also access the underlying Ming `Document` and (non-ORM) `Session` by
using some helper functions, so all the power of basic Ming (and MongoDB) is
accessible at all times:

.. [[[cog interact('ming_orm_tutorial', 5) ]]]

>>> from ming.orm import mapper
>>> m = mapper(WikiPage)
>>> # m.collection is the 'base' Ming document class
>>> m.collection
<class 'ming.metadata.Document<wiki_page>'>
>>> # Retrieve the 'base' Ming session
>>> session.impl
<ming.session.Session object at 0x25b4810>

.. [[[end]]]

