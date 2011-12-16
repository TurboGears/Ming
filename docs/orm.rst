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

Creating Objects
------------------

Once we have the boilerplate out of
the way, we can create instances of the `WikiPage` as any other Python class.  One
thing to notice is that we don't explicitly call the `save()` method on the
`WikiPage`; that will be called for us automatically when we `flush()` the session:

.. code-block:: python

    wp = WikiPage(title='FirstPage', text='This is my first page')
    session.flush()

The previous two lines will just create a new `WikiPage` and store it inside the
ORM Unit of Work. As soon as we flush our session the Unit of Work is processed
and all the changes will be reflected on the database itself.

By default the session will keep track of the objects that has already seen and
that are currently in clean state. This means that they have not been modified
since the last flush to the session. If you want to trash away those objects from
the session itself you can call the `clear` method

.. code-block:: python

    session.clear()

Clearing the session gives you a brand new session, so
keep in mind that after clearing it, it won't have track
anymore of the previous items that were created. While it is possible
to flush the session multiple times, it is common practice in
web applications to clear it only once at the end of the request.

.. [[[cog interact('ming_orm_tutorial', 1) ]]]
.. [[[end]]]

Querying the ORM
----------------

Once we have a `WikiPage` in the database, we can retrieve it using the `.query`
attribute, modify it, and flush the modified object out to the database:

.. [[[cog interact('ming_orm_tutorial', 2)]]]
.. [[[end]]]

You've already seen how to retrieve single objects from the ORM using the
`query.get()` method on `MappedClass` objects.  You can also perform regular Ming
queries using the `query.find()` method:

.. [[[cog interact('ming_orm_tutorial', 4) ]]]
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
.. [[[end]]]

And voilà, you have related objects.  Note that at present the relations between
objects are read-only, so if you want to make or break a relationship, you must
do it by setting the `ForeignIdProperty`.

ORM Event Interfaces
--------------------

This section describes the various categories of events which can be intercepted within the Ming ORM.

Mapper Events
=============

.. module:: ming.orm.mapper

To use MapperExtension, make your own subclass of it and just send it off to a mapper:

.. code-block:: python

    from ming.orm.mapper import MapperExtension
    class MyExtension(MapperExtension):
        def after_insert(self, obj, st):
            print "instance %s after insert !" % obj

    class MyMappedClass(MappedClass):
        class __mongometa__:
            session = session
            name = 'my_mapped_class'
            extensions = [ MyExtension ]

Multiple extensions will be chained together and processed in order;

.. code-block:: python

    extensions = [ext1, ext2, ext3]

.. autoclass:: MapperExtension
    :members:

Session Events
==============

.. module:: ming.orm.ormsession

The SessionExtension applies plugin points for Session objects
and ORMCursor objects:

.. code-block:: python

    from ming.orm.base import state
    from ming.orm.ormsession import SessionExtension

    class MySessionExtension(SessionExtension):
        def __init__(self, session):
            SessionExtension.__init__(self, session)
            self.objects_added = []
            self.objects_modified = []
            self.objects_deleted = []

        def before_flush(self, obj=None):
            if obj is None:
                self.objects_added = list(self.session.uow.new)
                self.objects_modified = list(self.session.uow.dirty)
                self.objects_deleted = list(self.session.uow.deleted)
            # do something

    ORMSession = ThreadLocalORMSession(session,
                                       extensions=[ProjectSessionExtension])

The same SessionExtension instance can be used with any number of sessions.
It is possible to register extensions on an already created ORMSession using
the `register_extension(extension)` method of the session itself. 
Even calling register_extension it is possible to register the extensions only
before using the session for the first time.

.. autoclass:: SessionExtension
    :members:

Ensuring Indexing
--------------------

The ORM layer permits to ensure indexes over the collections by using the
`__mongometa__` attribute. You can enforce both unique indexing and non-unique
indexing.

For example you can use this to ensure that it won't be possible to duplicate
an object multiple times with the same name

.. code-block:: python

    class Permission(MappedClass):
        class __mongometa__:
            session = session
            name = 'permissions'
            unique_indexes = [('permission_name',),]

        _id = FieldProperty(s.ObjectId)
        permission_name = FieldProperty(s.String)
        description = FieldProperty(s.String)
        groups = FieldProperty(s.Array(str))

If you want more control over your indexes, you can use custom_indexes directly within
`__mongometa__`, this will allow you to explicitly set unique and/or sparse index
flags that same way you could if you were directly calling ensureIndex in the MongoDB
shell. For example, if you had a field like email that you wanted to be unique, but
also allow for it to be Missing

.. code-block:: python

    class User(MappedClass):
        class __mongometa__:
	    session = session
	    name = 'users'
	    custom_indexes = [
		dict(fields=('email',), unique=True, sparse=True)
	    ]

	_id = FieldProperty(s.ObjectId)
	email = FieldProperty(s.String, if_missing=s.Missing)

Now when accessing instances of User, if email is Missing and you attempt to use the
User.email attribute Ming, will throw an AttributeError as it ensures that only
properties that are not Missing are mapped as attributes to the class.

This brings us to the :class:`ming.orm.property.FieldPropertyWithMissingNone`
property type. This allows you to mimic the behavior that you commonly find in a SQL
solution. An indexed and unique field that is also allowed to be NULL
or in this case Missing. A classic example would be a product database where you
want to enter in products but don't have SKU numbers for them yet. Now your product listing
can still call product.sku without throwing an AttributeError.

.. code-block:: python

    class Product(MappedClass):
        class __mongometa__:
	    session = session
	    name = 'products'
	    custom_indexes = [
	        dict(fields=('sku',), unique=True, sparse=True)
	    ]

	_id = FieldProperty(s.ObjectId)
	sku = FieldPropertyWithMissingNone(str, if_missing=s.Missing)

To apply the specified indexes you can then iterate over all the mappers and
call `ensure_indexes` over the mapped collection.

.. code-block:: python

    for mapper in ming.orm.Mapper.all_mappers():
        mainsession.ensure_indexes(mapper.collection)

This needs to be performed each time you change the indexes or the database.
It is common pratice to ensure all the indexes at application startup.

Dropping Down Below the ORM
---------------------------

You can also access the underlying Ming `Document` and (non-ORM) `Session` by
using some helper functions, so all the power of basic Ming (and MongoDB) is
accessible at all times:

.. [[[cog interact('ming_orm_tutorial', 5) ]]]
.. [[[end]]]

