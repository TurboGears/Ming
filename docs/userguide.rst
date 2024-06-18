:tocdepth: 3

.. _odm-introduction:

===================
Ming ODM User Guide
===================

Introduction
============

Ming provides an high-level abstraction for modeling objects in MongoDB.
This higher-layer abstraction is referred in this document as the ODM since
it is implemented in the "spirit" of object-relational mappers such as
`SQLAlchemy`_.

The ODM provides several features beyond those provided by basic `PyMongo`_:

**Schema Validation**
    Because MongoDB is schema-less, there are no guarantees given to the
    database client of the format of the data that may be returned from a query;
    you can put any kind of document into a collection that you want.
    The goal of Ming is to allow you to specify the schema for your data
    in Python code and then develop in confidence, knowing the format of data
    you get from a query.

**UnitOfWork**
    Using ODM-enabled sessions allows you to operate on in-memory objects
    exclusively until you are ready to "flush" all changes to the database.
    Although MongoDB doesn't provide transactions, the unit-of-work (UOW) pattern
    provides some of the benefits of transactions by delaying writes until you
    are fairly certain nothing is going to go wrong.

**IdentityMap**
    In base Ming, each query returns unique document objects even if those
    document objects refer to the same document in the database.  The identity
    map in Ming ensures that when two queries each return results that correspond
    to the same document in the database, the queries will return the same Python
    object as well.

**Relations**
    Although MongoDB is non-relational, it is still useful to represent
    relationships between documents in the database.  The ODM layer in Ming
    provides the ability to model relationships between documents as
    straightforward properties on Python objects.


Installing
==========

To begin working with Ming, you'll need to download and install a copy of
MongoDB.  You can find download instructions at http://www.mongodb.org/downloads

In order to install ming, as it's available on PyPI, you can use :program:`pip`
(We recommend using a virtualenv_ for development.):

.. code-block:: console

    $ pip install ming

Alternatively, if you don't have **pip** installed, `download it from PyPi
<http://pypi.python.org/pypi/ming/>`_ and run

.. code-block:: console

    $ python setup.py install

.. note::

    To use the bleeding-edge version of Ming, you can get the source from
    `GitHub <https://github.com/TurboGears/Ming>`_
    and install it as above.

Connecting to MongoDB
=====================

Ming manages your connection to the MongoDB database using an object known as a
:class:`.DataStore`.  The DataStore is actually just a thin wrapper around a pymongo_
connection and Database object.
(The actual Database object can always be accessed via the :attr:`.DataStore.db`
property of the DataStore instance).

.. code-block:: python

    >>> from ming import create_datastore
    >>>
    >>> datastore = create_datastore('mongodb://localhost:27017/tutorial')
    >>> datastore
    <DataStore None>
    >>> # The connection is actually performed lazily
    >>> # the first time db is accessed
    >>> datastore.db
    Database(MongoClient(host=['localhost:27017'], document_class=dict, tz_aware=False, connect=True), 'tutorial')
    >>> datastore
    <DataStore Database(MongoClient(host=['localhost:27017'], document_class=dict, tz_aware=False, connect=True), 'tutorial')>

.. note::

    Ming also provides a "mongo in memory" implementation, which is non-persistent,
    in Python, and possibly much faster than MongoDB for very small data sets, as
    you might use in testing.
    To use it, just change the connection url to ``mim://``

The ODM Session
---------------

Ming, like many object-relational mappers (ORMs), revolves around the idea of
model classes.  In order to create these classes, we need a way of connecting
them to the datastore.  Ming uses an object known as a **Session** to do this.

The session is responsible for this as well as maintaining the unit of work,
identity map, and relations between objects.
The ODM session itself is not designed to be thread-safe, so Ming provides a
thread-local version of the session for safe operation in a multithreaded environment.

Usually you will rely on the session for everything and won't use the datastore directly,
you will just pass it to the Session and rely on the session itself:

.. run-pysnippet:: ming_odm_tutorial connection_session

.. note::

    If you are using the TurboGears2 web framework the framework sets up
    a DataStore for you and passes it to your ``model.init_model`` function
    so there is no need to manually create the datastore.

ReplicaSets
-----------

Connecting to a **ReplicaSet** is possible by passing multiple hosts
separated by *comma* to the ``DataStore`` and the ``replicaSet=`` option
as the url:

.. code-block:: python

    from ming import create_datastore
    from ming.odm import ThreadLocalODMSession

    session = ThreadLocalODMSession(
        bind=create_datastore('mongodb://localhost:27017,localhost:27018/?replicaSet=foo')
    )

When connecting to a **ReplicaSet** some useful options are available that
define how Ming should behave when writing to the database. Additionally to
``replicaSet`` option you might want to check:

    * ``w=X`` Which states how many members of the replica set should have
      replicated the data before and insertion/update is considered done.
      ``X`` can be a number or ``majority`` for more then half. See
      `MongoDB Write Concern Option <http://docs.mongodb.org/manual/reference/connection-string/#uri.w>`_
      for additional details.
    * ``readPreference=[primary|primaryPreferred|secondary|secondaryPreferred|nearest]`` which
      states if queries should only happen on the replicaset master of if they can be performed
      on secondary nodes.

See `MongoDB Documentation <http://docs.mongodb.org/manual/reference/connection-string/#connection-string-options>`_
for a full list of available options inside the mongo url.

Authentication
--------------

Connecting to a MongoDB instance or cluster using authentication
can be done by passing the ``username`` and ``password`` values
in the url itself:

.. code-block:: python

    from ming import create_datastore
    from ming.odm import ThreadLocalODMSession

    session = ThreadLocalODMSession(
        bind=create_datastore('mongodb://myuser:mypassword@localhost:27017/dbname')
    )

Mapped Classes and Documents
============================

In MongoDB documents are just plain dictionaries. In Ming ODM layer documents
are represented by mapped classes, which inherit from :class:`ming.odm.mapped_class.MappedClass`.
MappedClasses do not descend from :class:`dict`, to emphasize the difference between a mapped class
(which may contain relations to other objects) and a MongoDB document (which may not).

To start working with MappedClasses you need a few more imports:

.. include:: src/ming_odm_tutorial.py
   :literal:
   :start-after: #{odm-imports
   :end-before: #}

Mapped Classes also define the **schema** of your data, it means that whenever
you are storing data into the MappedClass it gets validated against the schema.
The attributes available to the document are declared as :class:`.FieldProperty`
instances and their type for validation is specified in a declarative manner:

.. literalinclude:: src/ming_odm_tutorial.py
   :pyobject: WikiPage

The schema instances passed to :class:`.FieldProperty` also provide some options
to ensure that they are not empty or to provide a default value when they are.
See :class:`.FancySchemaItem` class for a list of available options.

.. note::

    For a full list of available schema types refer to :mod:`ming.schema` module.

At the end of the model file, you should call :meth:`.Mapper.compile_all`
to ensure that Ming has full information on all mapped classes:

.. include:: src/ming_odm_tutorial.py
   :literal:
   :start-after: #{compileall
   :end-before: #}

Type Annotations
----------------

Some type annotations are in Ming, but you need to add a hint to each class to help.
The primary goal so far is to improve IDE experience.  They may or may not work with
mypy.  Add some imports and the `query:` line to your models like this:

.. code-block:: python

    import typing

    if typing.TYPE_CHECKING:
        from ming.odm.mapper import Query

    ...

    class WikiPage(MappedClass):
        class __mongometa__:
            session = session
            name = 'wiki_page'

        query: 'Query[WikiPage]'

        ...

Creating Objects
----------------

Once we have the boilerplate out of
the way, we can create instances of the `WikiPage` as any other Python class.  One
thing to notice is that we don't explicitly call the `save()` method on the
`WikiPage`; that will be called for us automatically when we `flush()` the session:

.. run-pysnippet:: ming_odm_tutorial snippet1

The previous line will just create a new `WikiPage` and store it inside the
ODM UnitOfWork and IdentityMap in **new** state:

.. run-pysnippet:: ming_odm_tutorial snippet1_0

As soon as we flush our session the Unit of Work is processed,
all the changes will be reflected on the database itself
and the object switches to **clean** state:

.. run-pysnippet:: ming_odm_tutorial snippet1_1

By default the session will keep track of the objects that has already seen and
that are currently in clean state (which means that they have not been modified
since the last flush to the session). If you want to trash away those objects from
the session itself you can call the `clear` method

.. run-pysnippet:: ming_odm_tutorial snippet1_2

Clearing the session gives you a brand new session, so
keep in mind that after clearing it, it won't have track
anymore of the previous items that were created. While it is possible
to flush the session multiple times, it is common practice in
web applications to clear it only once at the end of the request.

.. note::

    Both flushing the session and clearing it at the end of every request
    in web application can be provided automatically by the :class:`.MingMiddleware`
    WSGI Middleware.

.. note::

    In case you are using the TurboGears2 Web Framework there is no need to use
    the ``MingMiddleware`` as TurboGears will automatically flush and clear
    the session for your at the end of each request.

Querying Objects
----------------

Once we have a `WikiPage` in the database, we can retrieve it using the `.query`
attribute. The query attribute is a proxy to the Session query features which expose
three methods that make possible to query objects :meth:`._ClassQuery.get`,
:meth:`.ODMSession.find` and :meth:`.ODMSession.find_one_and_update`:

.. run-pysnippet:: ming_odm_tutorial snippet2

As you probably noticed while we directly retrieved the object when calling ``.get()``
we needed to append the ``.first()`` call to the result of ``.find()``. That is because
:meth:`.ODMSession.find` actually returns an :class:`.ODMCursor` which allows to retrieve
multiple results, just get the first or count them:

.. run-pysnippet:: ming_odm_tutorial snippet2_1

:meth:`.ODMSession.find` uses the MongoDB query language directly, so if you are already
familiar with `pymongo`_ the same applies. Otherwise you can find the complete query
language specification on `MongoDB Query Operators documentation
<http://docs.mongodb.org/manual/reference/method/db.collection.find/>`_.

The ``find`` operator also allows to retrieve only some fields of the document, thus avoiding
the cost of validating them all. This is especially useful to speed up queries in case of
big subdocuments or list of subdocuments:

.. run-pysnippet:: ming_odm_tutorial snippet2_2
    :skip: 1

.. note::

    Keep in mind that properties validation still applies, so when using the ``fields``
    option you will still get the ``if_missing`` value if they declare one and the query
    will fail if the fields where ``required``.

Notice that querying again a document that was retrieved with a ``fields`` limit won't
add the additional fields unless the ``refresh`` option is used:

.. run-pysnippet:: ming_odm_tutorial snippet2_3

Also applying the ``fields`` limit to a document already in the IdentityMap won't do anything
unless ``refresh`` is used (which is usually not what you want as you already paid the cost
of validating the attributes, so there is not much benefit in throwing them away):

.. run-pysnippet:: ming_odm_tutorial snippet2_4

Editing Objects
---------------

We already know how to create and get back documents, but an ODM won't be of much use
if it didn't enable editing them. As Ming exposes MongoDB documents as objects, to
update a MongoDB object you can simply change one of its properties and the UnitOfWork
will track that the object needs to be updated:

.. run-pysnippet:: ming_odm_tutorial snippet5_1
    :skip: 1
    :emphasize-lines: 17

Another option to edit an object is to actually rely on :meth:`.ODMSession.find_one_and_update`
method which will query the object and update it atomically:

.. run-pysnippet:: ming_odm_tutorial snippet5_3

This is often used to increment counters or acquire global locks in mongodb

.. note::

    ``find_one_and_update`` always refreshes the object in the IdentityMap, so the object
    in your IdentityMap will always get replaced with the newly retrieved value. Make
    sure you properly flushed any previous change to the object and use the ``new`` option
    to avoid retrieving a stale version of the object if you plan to modify it.

Ming also provides a way to update objects outside the ODM through :meth:`.ODMSession.update`.
This might be handy when you want to update the object but don't need the object back
or don't want to pay the cost of retrieving and validating it:

.. run-pysnippet:: ming_odm_tutorial snippet5_4
    :skip: 1
    :emphasize-lines: 1

.. note::

    The :meth:`.ODMSession.refresh` method provides a convenient way to refresh
    state of objects from the database, this is like querying them back with
    ``.find()`` using the ``refresh`` option. So keep in mind that any change
    not yet persisted on the database will be lost.

Deleting Objects
----------------

Deleting objects can be performed by calling the :meth:`._InstQuery.delete` method
which is directly exposed on objects:

.. run-pysnippet:: ming_odm_tutorial snippet5_5
    :emphasize-lines: 2

In case you need to delete multiple objects and don't want to query them back to
call ``.delete()`` on each of them you can use :meth:`.ODMSession.remove` method
to delete objects using a query:

.. run-pysnippet:: ming_odm_tutorial snippet5_6
    :emphasize-lines: 3

Working with SubDocuments
=========================

Ming, like MongoDB, allows for documents to be arbitrarily nested.
For instance, we might want to keep a `metadata` property on our `WikiPage` that
kept tag and category information.

To do this, we just declare a ``WikiPageWithMetadata`` which will
provide a ``metadata`` nested document that contains an array of ``tags``
and an array of ``categories``. This is made possible by the :class:`.schema.Object`
and :class:`.schema.Array` types:

.. literalinclude:: src/ming_odm_tutorial.py
   :pyobject: WikiPageWithMetadata

Now, what happens when we create a page and try to update it?

.. run-pysnippet:: ming_odm_tutorial snippet8
    :emphasize-lines: 8, 11, 12, 19

Ming creates the structure for us automatically.
(If we had wanted to specify a different default value for the `metadata`
property, we could have done so using the `if_missing` parameter, of course.)

.. note::

    In case you want to store complex subdocuments that don't need validation
    or for which you don't want validation for speed reasons you can use the
    :class:`.schema.Anything` type which can store subdocuments and arrays
    without validating them.

.. _relation:

Relating Classes
================

The real power of the ODM comes in being able to handle relations between objects.
On Ming this is made possible by the :class:`.ForeignIdProperty` and :class:`.RelationProperty`
which provide a way to declare references to other documents and retrieve them.

A common use case if for example adding comments on our `WikiPage`, to do so we need
to declare a new ``WikiComment`` class:

.. literalinclude:: src/ming_odm_relations.py
   :pyobject: WikiComment
   :emphasize-lines: 7, 10

Here, we have defined a `ForeignIdProperty` `page_id` to reference the original
`Wikipage`.  This tells Ming to create a field in `WikiComment` that represents a
"foreign key" into the `WikiPage._id` field.  This provide a guide to ming on
how to resolve the relationship between `WikiPage` and `WikiComment` and
how to build queries to fetch the related objects.

In order to actually use the relationship, however, we must use a
`RelationProperty` to reference the related class.

In this case, we will use the property `page` to access the page
about which this comment refers. While to access the comments from
the ``WikiPage`` we will add a ``comments`` property to the page itself:

.. literalinclude:: src/ming_odm_relations.py
   :pyobject: WikiPage
   :emphasize-lines: 10

Now actually use these classes, we need to create some comments:

.. run-pysnippet:: ming_odm_relations snippet1_1
    :skip: 4

And voilà, you have related objects. To get them back you can just use the
``RelationProperty`` available in the ``WikiPage`` as ``comments``:

.. run-pysnippet:: ming_odm_relations snippet1_2
    :skip: 1
    :emphasize-lines: 5

Relations also support updating by replacing the value of the ``RelationProperty``
that drives the relation:

.. run-pysnippet:: ming_odm_relations snippet1_3
    :skip: 1
    :emphasize-lines: 7

While it is not possible to append or remove values from ``RelationProperty``
that are a list (like the ``WikiPage.comments`` in our example it is still
possible to replace its value:

.. run-pysnippet:: ming_odm_relations snippet1_4
    :skip: 1
    :emphasize-lines: 6,7

.. note::

    You should treat ming relations that contain more than one value as
    ``tuples``, they are stored as an immutable list.

List based Relationships (Many-to-Many)
---------------------------------------

Ming supports many-to-many relationships by using MongoDB arrays.
Instead of storing the foreign id as an ObjectId it can be storead as an
array of ObjectId.

This can be achieved by using the ``uselist=True`` option of :class:`.ForeignIdProperty`:

.. literalinclude:: src/ming_odm_relations.py
   :pyobject: Parent
   :emphasize-lines: 8

.. literalinclude:: src/ming_odm_relations.py
   :pyobject: Child

Then you can create and relate objects as you would with any other kind of
relationship:

.. run-pysnippet:: ming_odm_relations snippet2_1
    :skip: 1
    :emphasize-lines: 6,7


Forcing a ForeignIdProperty
---------------------------

By default the :class:`.RelationProperty` will automatically detect the relationship
side and foreign id property. In case the automatic detection fails it is possible
to manually specify it through the ``via="propertyname"`` option of the :class:`.RelationProperty`:

.. code-block:: python

    class WebSite(MappedClass):
        class __mongometa__:
            name='web_site'
            session = self.session

        _id = FieldProperty(schema.ObjectId)

        _index = ForeignIdProperty('WebPage')
        index = RelationProperty('WebPage', via='_index')

        pages = RelationProperty('WebPage', via='_website')

    class WebPage(MappedClass):
        class __mongometa__:
            name='web_page'
            session = self.session

        _id = FieldProperty(schema.ObjectId)

        _website = ForeignIdProperty('WebSite')
        website = RelationProperty('WebSite', via='_website')



Forcing a Relationship side
---------------------------

While it is possible to force a specific :class:`.ForeignIdProperty` there are cases when
it is also necessary to force the relationship side. This might be the case if
both sides of the relationship own a property with the specified name, a common
case is a circular relationship where both sides are the same class.

Specifying the side is possible by passing a ``tuple`` to the ``via`` property with
the second value being ``True`` or ``False`` depending on the fact that the specified
`ForeignIdProperty` should be considered owned by this side of the relationship or not:

.. code-block:: python

        class WebPage(MappedClass):
            class __mongometa__:
                name='web_page'
                session = self.session

            _id = FieldProperty(int)

            children = RelationProperty('WebPage')
            _children = ForeignIdProperty('WebPage', uselist=True)
            parents = RelationProperty('WebPage', via=('_children', False))

Dropping Down Below the ODM
===========================

There might be cases when you want to directly access the `pymongo`_ layer or
get outside of the ODM for speed reasons or to get access to additional features.

This can be achieved by dropping down to the :ref:`ming_baselevel` through the
ODM :class:`.Mapper`. The Foundation Layer is a lower level API that provides
validation and schema enforcement over plain dictionaries. The Mapper has
a ``collection`` attribute that points to the *Foundation Layer collection*.
The Foundation Layer adds additional features over plain dictionaries through
the ``m`` property:

.. run-pysnippet:: ming_odm_tutorial snippet7

You can also operate at the Foundation Layer through the :class:`.Mapper` and
:class:`.ODMSession` by accessing the low level Session implementation and
mapped Collection:

.. run-pysnippet:: ming_odm_tutorial snippet6


.. _MongoDB: http://www.mongodb.org/
.. _virtualenv: http://pypi.python.org/pypi/virtualenv
.. _SQLAlchemy: http://www.sqlalchemy.org/
.. _pymongo: http://api.mongodb.org/python/current/api/
