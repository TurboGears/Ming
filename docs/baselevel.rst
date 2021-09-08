.. _ming_baselevel:

=====================
Ming Foundation Layer
=====================

MongoDB and Ming
================

MongoDB_ is a high-performance schemaless database that allows you to store and
retrieve JSON-like documents.  MongoDB stores these documents in collections,
which are analogous to SQL tables.  Because MongoDB is schemaless, there are no
guarantees given to the database client of the format of the data that may be
returned from a query; you can put any kind of document into a collection that
you want.

While this dynamic behavior is handy in a rapid development environment where you
might delete and re-create the database many times a day, it starts to be a
problem when you *need* to make guarantees of the type of data in a collection
(because you code depends on it).  The goal of Ming is to allow you to specify
the schema for your data in Python code and then develop in confidence, knowing
the format of data you get from a query.

As Ming is heavily inspired by `SQLAlchemy`_ its Object Document Mapper layer
is mapped over a foundation layer (through the :class:`.Mapper` class) that
provides the basic validation and connection management features over dictionaries.

Connecting to the Database
==========================

Ming manages your connection to the MongoDB database using an object known as a
:class:`.DataStore`.  The DataStore is actually just a thin wrapper around a pymongo_
Database object which is used by :class:`ming.Session` to perform the actual
queries::

    from ming import Session
    session = Session(create_datastore('mongodb://localhost:27017/tutorial'))

.. note::

    Note that :class:`.Session` is thread-safe by itself as it stores no data,
    differently from the base :class:`.ODMSession` class which requires to be
    used through :class:`ThreadLocalODMSession` to be thread safe.

Collection objects
==================

.. sidebar:: Declarative versus Imperative

   There are two styles to define models in Ming at foundation layer: declarative and
   imperative. Both styles are available at the document level (which this tutorial covers).
   Which you end up using is mostly a matter of personal style.
   The declarative style actually predated the imperative style, and the main author
   of Ming uses both styles interchangeably in application programming based on which
   seems more convenient for the task at hand.

Now that that boilerplate is out of the way, we can actually start writing our
models.  We will start with a model representing a WikiPage. We can do that in
"imperative" mode as follows:

.. code-block:: python

    from ming import collection, Field, schema

    WikiPage = collection('wiki_page', session,
        Field('_id', schema.ObjectId),
        Field('title', schema.String),
        Field('text', schema.String)
    )

Here, we define a `WikiPage` Python class with three fields, `_id`, `title`, and
`text`.  We also bind the `WikiPage` to the `session` we defined earlier and to
the `wiki_page` name.

If you prefer a "declarative" style, you can also declare a collection by subclassing
the :class:`.Document` class::

    from ming import Field, schema
    from ming.declarative import Document

    class WikiPage(Document):

        class __mongometa__:
            session = session
            name = 'wiki_page'

        _id = Field(schema.ObjectId)
        title = Field(schema.String)
        text = Field(schema.String)

Here, rather than use the `collection()` function, we are defining the class
directly, grouping some of the metadata used by ming into a `__mongometa__` class
in order to reduce namespace conflicts. Note that we don't have to provide the
name of our various `Field` instances as strings here since they already have
names implied by their names as class attributes. If we want to map a document field
to a *different* class attribute, we can do so using the following syntax::

    _renamed_field = Field('renamed_field', schema.String)

This is sometimes useful for "privatizing" document members that we wish to wrap
in `@property` decorators or other access controls.

We can add our own methods to the WikiPage class, too.  However, the `make()`
method is reserved for object construction and validation.
See the `Bad Data`_ section.

Type Annotations
================

Some type annotations are in Ming, but you need to add a hint to each class to help.
You must be using the "declarative" approach that inherits from `Document`.
The primary goal so far is to improve IDE experience.  They may or may not work with
mypy.  Add some imports and the `m:` line to your models like this:

.. code-block:: python

    import typing

    if typing.TYPE_CHECKING:
        from ming.metadata import Manager

    ...

    class WikiPage(Document):

        class __mongometa__:
            session = session
            name = 'wiki_page'

        m: 'Manager[WikiPage]'

        ...

Using Ming Objects to Represent Mongo Records
=============================================

Now that we've defined a basic schema, let's start playing around with Ming in
the interactive interpreter.  First, make sure you've saved the code below in a
module "tutorial.py"::

    from ming import Session, create_datastore
    from ming import Document, Field, schema

    bind = create_datastore('tutorial')
    session = Session(bind)

    class WikiPage(Document):

        class __mongometa__:
            session = session
            name = 'wiki_page'

        _id = Field(schema.ObjectId)
        title = Field(str)
        text = Field(str)

Now let's fire up the interpreter and start working.  The first thing we'll do is
create a `WikiPage`::

    >>> page = WikiPage(dict(title='MyPage', text=''))
    >>> page
    {'text': '', 'title': 'MyPage'}
    >>> page.title
    'MyPage'
    >>> page['title']
    'MyPage'

As you can see, Ming :class:`documents <ming.base.Document>` can be accessed
either using dictionary-style lookups (`page['title']`) or attribute-style
lookups (`page.title`).  
In fact, all Ming documents are :class:`dict` subclasses, so all the standard
methods on Python :class:`ict` objects  are available.

In order to actually interact with the database, Ming provides a standard
attribute ``.m``, short for **Manager**, on each mapped class.

In order to save the document we just created to the database,
for instance, we would simply type::

    >>> page.m.save()
    >>> page
    {'text': '', '_id': ObjectId('4b1d638ceb033028a0000000'), 'title': 'MyPage'}

When the page was saved to the database, the database assigned a unique `_id`
attribute.  (If we had wished to specify our own `_id`, we could have also done
that.)  Now, let's query the database and make sure that the document actually
got saved::

    >>> WikiPage.m.find().first()
    {'text': u'', '_id': ObjectId('4b1d638ceb033028a0000000'), 'title': u'MyPage'}

And there it is!  Now, let's add some text to the page::

    >>> page.text = 'This is some text on my page'
    >>> page.m.save()
    >>> WikiPage.m.find().first()
    {'text': u'This is some text on my page', '_id': ObjectId('4b1d638ceb033028a0000000'), 'title': u'MyPage'}

Looks like it worked.  One thing we glossed over was the use of the ``.m.find()``
method.  This is the main method we'll use to query the database, and is covered
in the next section.

Querying the Database
=====================

Ming provides an ``.m`` attribute that exposes the same methods available on
:class:`.Session` just bound to the **Collection** or **instance** of your Documents.

The ``.m.find()`` method works just like the ``.find()`` method on collection
objects in pymongo_ and is used for performing queries.

The result of a query is a Python iterator that wraps a pymongo cursor,
converting each result to a :class:`ming.Document <ming.base.Document>` before
yielding it.

Like SQLAlchemy_, we provide several convenience methods on query results
through :class:`Cursor <ming.base.Cursor>`:

one()
  Retrieve a single result from a query.  Raises an exception if the query
  contains either zero or more than one result.
first()
  Retrieve the first result from a query.  If there are no results, return
  ``None``.
all()
  Retrieve all results from a query, storing them in a Python :class:`list`.
count()
  Returns the number of results in a query
limit(limit)
  Restricts the cursor to only return `limit` results
skip(skip)
  Skips ahead `skip` results in the cursor (similar to a SQL OFFSET clause)
sort(\*args, \*\*kwargs)
  Sorts the underlying pymongo cursor using the same semantics as the
  ``pymongo.Cursor.sort()`` method

Ming also provides a convenience method ``.m.get(**kwargs)`` which is equivalent to
``.m.find(kwargs).first()`` for simple queries that are expected to return one
result.  
Some examples: 

    >>> WikiPage.m.find({'title': 'MyPage'}).first()
    {'text': u'', '_id': ObjectId('4b1d638ceb033028a0000000'), 'title': u'MyPage'}
    >>> WikiPage.m.find().count()
    1
    >>> WikiPage.m.get(title='MyPage')
    {'text': u'', '_id': ObjectId('4b1d638ceb033028a0000000'), 'title': u'MyPage'}

Other Sessions
==============

If we have a special case where we want to use a different database session for a model,
other than the one specified in :class:`__mongometa__
<ming.base.Document.__mongometa__>`, we can do::

    foobar = Session.by_name('foobar')
    foobar.save(my_model_instance)

or::

    foobar = Session.by_name('foobar')
    my_model_instance.m(foobar).save()

This could be useful if you have a database session that is connected to a master server,
and another one that is used for the slave (readonly).

Bad Data
========

.. sidebar:: Schema Validation

   Ming documents are validated at certain points in their life cycle.  (Validation
   is where the schema is enforced on the document.)  Generally, schema validation
   occurs when saving the document to the database or when loading it from the
   database.  Additionally, validation is performed when the document is created
   using the `.make()` method.

So what about the schema?  So far, we haven't seen any evidence that Ming is
doing anything with the schema information at all.  Well, the first way that Ming
helps us is by making sure we don't specify values for properties that are not
defined in the object::

    >>> page = tutorial.WikiPage(dict(title='MyPage', text='', fooBar=''))
    >>> page
    {'fooBar': '', 'text': '', 'title': 'MyPage'}
    >>> page.m.save()
    Traceback (most recent call last):
      ...
    formencode.api.Invalid: <class 'tutorial.WikiPage'>:
        Extra keys: set(['fooBar'])

OK, that's nice and all, but wouldn't it be nicer if we could be warned at
creation time?  
Ming provides a convenice method :meth:`make() <ming.base.Document.make>` on the
:class:`ming.Document <ming.base.Document>` with just such behavior::

    >>> page = tutorial.WikiPage.make(dict(title='MyPage', text='', fooBar=''))
    Traceback (most recent call last):
      ...
    formencode.api.Invalid: <class 'tutorial.WikiPage'>:
        Extra keys: set(['fooBar'])

We can also provide default values for properties via the `if_missing` parameter
on a :class:`Field <ming.base.Field>`.  
Change the definition of the `text` property in `tutorial.py` to read::

    text = Field(str, if_missing='')

Now if we restart the interpreter (or reload the tutorial module), we can do the
following::

    >>> page = tutorial.WikiPage.make(dict(title='MyPage'))
    >>> page
    {'text': '', 'title': 'MyPage'}

Ming also supports supplying a callable as an if_missing value so you could put
the creation date in a WikiPage like this::

    from datetime import datetime

    ...

    creation_date = Field(datetime, if_missing=datetime.utcnow)


.. _MongoDB: http://www.mongodb.org/
.. _virtualenv: http://pypi.python.org/pypi/virtualenv
.. _SQLAlchemy: http://www.sqlalchemy.org/
.. _pymongo: http://api.mongodb.org/python/current/api/
.. _Ming: http://sf.net/projects/merciless
