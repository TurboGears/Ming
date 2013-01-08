===============
 Intro to Ming
===============

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

Installing MongoDB and Ming
===========================

To begin working with Ming, you'll need to download and install a copy of
MongoDB.  You can find download instructions at http://www.mongodb.org/downloads

In order to run the server for this tutorial, just create a directory and start mongod
there::

    $ mkdir var
    $ cd var
    $ mkdir mongodata
    $ <wherever you installed mongodb>/mongod --dbpath mongodata

In order to install ming, you simply use setuptools/Distribute's `easy_install`
command.  (We recommend using a virtualenv_ for development.)

::

    $ virtualenv ming_env
    $ source ming_env/bin/activate
    (ming_env)$ easy_install -UZ Ming

Connecting to the Database
==========================

Ming manages your connection to the MongoBD database using an object known as a
:class:`Datastore <ming.datastore.DataStore>`.  The DataStore is actually just a
thin wrapper around a pymongo_ Database object.  
(The actual Database object can always be accessed via the `db` property of the
DataStore instance.  For this tutorial, we will be using a single, global DataStore::

    from ming import create_datastore
    bind = create_datastore('mongodb://localhost:27017/tutorial')

Note that if you're connecting to MongoDB on the default host (localhost) and
port (27017), you can just provide the database name here::

    bind = create_datastore('tutorial')

Ming, like many object-relational mappers (ORMs), revolves around the idea of
model classes.  In order to create these classes, we need a way of connecting
them to the datastore.  Ming uses an object known as a Session to do this.  For
this tutorial, we will be using a single global Session::

    from ming import Session
    session = Session(bind)

We can also :class:`configure() <ming.configure>` ming with a set of urls, using
a config dict of "ming.*" keys.  
The first part must be 'ming', the second part is a session name, and the third
parts are used as parameters to construct a :class:`Datastore
<ming.datastore.DataStore>` object.  

.. sidebar:: Mongo-in-Memory

    Ming also provides a "mongo in memory" implementation, which is non-persistent,
    in Python, and possibly much faster than MongoDB for very small data sets, as
    you might use in testing.
    To use it, just change the connection url to `mim://`

::

    config = {'ming.example.uri': 'mongodb://localhost:27017/tutorial' }
    ming.configure(**config)
    # and later access the named session with:
    Session.by_name('example')

Collection objects
==================

.. sidebar:: Declarative versus Imperative

   There are two styles to define your models in Ming, declarative and
   imperative, and both styles are availabe both at the document level (which
   this tutorial covers) and the ODM layer (covered by the ODM tutorial). Which
   you end up using is mostly a matter of personal style. The declarative style
   actually predated the imperative style, and the main author of Ming uses both
   styles interchangably in application programming based on which seems more
   convenient for the task at hand.

   Due to the history of the declarative model preceding the imperative model,
   you may notice that the documentation is skewed towards the declarative
   model. Keep in mind that most anything you can do declaratively, you can also
   do imperatively in Ming. Also, if you get a chance, feel free to submit
   documentation bugs to the Ming_ project at SourceForge.

Now that that boilerplate is out of the way, we can actually start writing our
models.  We will start with a model representing a WikiPage. We can do that in
"imperative" mode as follows::

    from ming import collection, Field, Document, schema

    WikiPage = collection(
        'wiki_page', session,
        Field('_id', schema.ObjectId),
        Field('title', str),
        Field('text', str))

Here, we define a `WikiPage` Python class with three fields, `_id`, `title`, and
`text`.  We also bind the `WikiPage` to the `session` we defined earlier and the
`wiki_page` name. 

If you prefer a "declarative" style, Ming also provides the following syntax::

    from ming import Field, schema
    from ming.declarative import Document

    class WikiPage(Document):

        class __mongometa__:
            session = session
            name = 'wiki_page'

        _id = Field(schema.ObjectId)
        title = Field(str)
        text = Field(str)

Here, rather than use the `collection()` function, we are defining the class
directly, grouping some of the metadata used by ming into a `__mongometa__` class
in order to reduce namespace conflicts. Note that we don't have to provide the
name of our various `Field` instances as strings here since they already have
names implied by their names as class attributes. If we want to map a document field
to a *different* class attribute, we can do so using the following syntax::

    _renamed_field = Field('renamed_field', str)

This is sometimes useful for "privatizing" document members that we wish to wrap
in `@property` decorators or other access controls.

.. sidebar:: Methods

    We can add our own methods to the WikiPage class, too.  However, the `make()`
    method is reserved for object construction and validation  See the `Bad
    Data`_ section.


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

    >>> import tutorial
    >>> page = tutorial.WikiPage(dict(title='MyPage', text=''))
    >>> page
    {'text': '', 'title': 'MyPage'}
    >>> page.title
    'MyPage'
    >>> page['title']
    'MyPage'

As you can see, Ming :class:`documents <ming.base.Document>` can be accessed
either using dictionary-style lookups (`page['title']`) or attribute-style
lookups (`page.title`).  
In fact, all Ming documents are `dict` subclasses, so all the standard methods on
Python `dict` objects  are available.

In order to actually interact with the database, Ming provides a standard
attribute `.m`, short for :class:`Manager <ming.base.Manager>`, on each mapped
class.  
In order to save the
document we just created to the database, for instance, we would simply type::

    >>> page.m.save()
    >>> page
    {'text': '', '_id': ObjectId('4b1d638ceb033028a0000000'), 'title': 'MyPage'}

When the page was saved to the database, the database assigned a unique `_id`
attribute.  (If we had wished to specify our own `_id`, we could have also done
that.)  Now, let's query the database and make sure that the document actually
got saved::

    >>> tutorial.WikiPage.m.find().first()
    {'text': u'', '_id': ObjectId('4b1d638ceb033028a0000000'), 'title': u'MyPage'}

And there it is!  Now, let's add some text to the page::

    >>> page.text = 'This is some text on my page'
    >>> page.m.save()
    >>> tutorial.WikiPage.m.find().first()
    {'text': u'This is some text on my page', '_id': ObjectId('4b1d638ceb033028a0000000'), 'title': u'MyPage'}

Looks like it worked.  One thing we glossed over was the use of the `.m.find()`
method.  This is the main method we'll use to query the database, and is covered
in the next section.

Querying the Database
=====================

Ming provides an `.m.find()` method on class :class:`managers
<ming.base.Manager>` that works just like the `.find()` method on collection
objects in pymongo_ and is used for performing queries.  
The result of a query is a Python iterator that wraps a pymongo cursor,
converting each result to a :class:`ming.Document <ming.base.Document>` before
yielding it.  
Like SQLAlchemy_, we provide several convenice methods on query results
(:class:`Cursor <ming.base.Cursor>`):

one()
  Retrieve a single result from a query.  Raises an exception if the query
  contains either zero or more than one result.
first()
  Retrieve the first result from a query.  If there are no results, return
  `None`.
all()
  Retrieve all results from a query, storing them in a Python `list`.
count()
  Returns the number of results in a query
limit(limit)
  Restricts the cursor to only return `limit` results
skip(skip)
  Skips ahead `skip` results in the cursor (similar to a SQL OFFSET clause)
sort(\*args, \*\*kwargs)
  Sorts the underlying pymongo cursor using the same semantics as the
  `pymongo.Cursor.sort()` method

Ming also provides a convenience method `.m.get(**kwargs)` which is equivalent to
`.m.find(kwargs).first()` for simple queries that are expected to return one
result.  
Some examples: 

    >>> tutorial.WikiPage.m.find({'title': 'MyPage'}).first()
    {'text': u'', '_id': ObjectId('4b1d638ceb033028a0000000'), 'title': u'MyPage'}
    >>> tutorial.WikiPage.m.find().count()
    1
    >>> tutorial.WikiPage.m.get(title='MyPage')
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

Compound Validators
===================

.. sidebar:: `ming.schema`

   Up till now, we have generally been defining schema items as native Python
   types.  This is a convenient shortcut provided by Ming to reduce your
   finger-typing.  Sometimes, however, you'll need to directly specify the actual
   validator used.  These validators are defined in the :mod:`ming.schema` module.

Ming, like MongoDB, allows for documents to be arbitrarily nested.  
For instance, we might want to keep a `metadata` property on our `WikiPage` that
kept tag and category information.  
To do this, we just need to add a little more complex schema.  
Add the following line to the `WikiPage` definition::

    metadata = Field(dict(
            tags=[str],
            categories=[str]))

Now, what happens when we create a page?

    >>> >>> tutorial.WikiPage.make(dict(title='MyPage'))
    {'text': '', 'title': 'MyPage', 'metadata': {'categories': [], 'tags': []}}
    >>> tutorial.WikiPage.make(dict(title='MyPage', metadata=dict(tags=['foo', 'bar', 'baz'])))
    {'text': '', 'title': 'MyPage', 'metadata': {'categories': [], 'tags': ['foo', 'bar', 'baz']}}

Ming creates the structure for us automatically.  
(If we had wanted to specify a different default value for the `metadata`
property, we could have done so using the `if_missing` parameter, of course.)

Specifying a Migration
======================

One of the most irritating parts of maintaining an application for a while is the
need to do data migrations from one version of the schema to another.
While Ming can't completely remove the pain of migrations, it does seek to make migrations
as simple as possible.

Let's see what's in the database right now::

    >>> tutorial.WikiPage.m.find().all()
    [{'text': u'This is some text on my page', '_id': ObjectId('4b1d638ceb033028a0000000'), 'title': u'MyPage', 'metadata': {'categories': [], 'tags': []}}]

Suppose we decided that we didn't want the `metadata` property; we'd like to
"promote" the `categories` and `tags` properties to be top-level attributes of
the `WikiPage`.  
We might write our new schema as follows::

    class WikiPage(Document):

        class __mongometa__:
            session = session
            name = 'wiki_page'

        _id = Field(schema.ObjectId)
        title = Field(str)
        text = Field(str, if_missing='')
        tags = Field([str])
        categories = Field([str])

But now if we try to .find() things in our database, our data has gone missing::

    >>> tutorial = reload(tutorial)
    >>> tutorial.WikiPage.m.find().all()
    [{'text': u'This is some text on my page', '_id': ObjectId('...'), 
      'title': u'MyPage', 'tags': [], 'categories': [] }]

What we need now is a migration.  Luckily, Ming makes migrations manageable.  
All we need to do is include the previous schema, a migration function in our
:class:`__mongometa__ <ming.base.Document.__mongometa__>` object, and a way to
force the migration. For the 'forcing' part, we'll add a version field to the new
schema::

    class OldWikiPage(Document):
        _id = Field(schema.ObjectId)
        title = Field(str)
        text = Field(str, if_missing='')
        metadata = Field(dict(
                tags=[str],
                categories=[str]))

    class WikiPage(Document):

        class __mongometa__:
            session = session
            name = 'wiki_page'
            version_of = OldWikiPage
            def migrate(data):
                result = dict(
                    data,
                    tags=data['metadata']['tags'],
                    categories=data['metadata']['categories'],
                    version=1)
                del result['metadata']
                return result

        version = Field(1, required=True)
        ...

OK, now let's reload and try that query again::

    >>> tutorial = reload(tutorial)
    >>> tutorial.WikiPage.m.find().all()
    [{'title': u'MyPage', 'text': u'This is some text on my page', 
      'tags': [], 'version': 1, '_id': ObjectId('...'), 'categories': []}]

And that's it.  Migrations are performed lazily as the objects are loaded
from the database.  Note that we can make the `OldWikiPage` a `version_of` and
`EvenOlderWikiPage` and the migration will automatically migrate each object to
the latest version.  If you wish to migrate all the objects in a collection, just
do the following::

    >>> tutorial.WikiPage.m.migrate()

.. _MongoDB: http://www.mongodb.org/
.. _virtualenv: http://pypi.python.org/pypi/virtualenv
.. _SQLAlchemy: http://www.sqlalchemy.org/
.. _pymongo: http://api.mongodb.org/python/current/api/
.. _Ming: http://sf.net/projects/merciless
