:tocdepth: 2

=============================
Welcome to Ming Documentation
=============================

**Ming** is an Object Document Mapper (like an ORM but for Document based databases),
for MongoDB. Ming extends pymongo providing:

   * Declarative Models
   * Schema Validation and Conversion
   * Schema Evolution
   * Pure InMemory MongoDB Implementation
   * Unit of Work
   * Identity Map
   * One-To-Many, Many-To-One and Many-To-Many Relations

Getting Started
===============

To get started with Ming just install it with::

   $ pip install ming

Connecting to MongoDB
---------------------

Before we start, make sure you have a copy of MongoDB running.
First thing needed to start using Ming is to tell it how to connect to our instance of mongod.
For this we use the :func:`.create_datastore` function, this function creates a connection
to the MongoDB instance, replicaset or cluster specified by the given URL:

.. literalinclude:: src/ming_welcome.py
   :start-after: #{connect-imports
   :end-before: #}

The :class:`.ThreadLocalODMSession` is the object all your models will use to interact with
MongoDB and can be directly used to perform low-level mongodb oprations.
While this provides no particular benefit over using *pymongo* directly
it already permits to create and query documents:

.. run-pysnippet:: ming_welcome snippet1

Using Models
------------

Now that we know how to connect to the Database we can declare models which will be
persisted on the database their session is associated to:

.. literalinclude:: src/ming_welcome.py
   :start-after: #{odm-model
   :end-before: #}

Models can be created by creating them and flushing their changes to the database.
A Model can then be queried back with the ``Model.query.find()`` method:

.. run-pysnippet:: ming_welcome snippet2

To start working with Ming continue with the :ref:`odm-introduction`

Community
=========

To get help with using Ming, use the `Ming Users mailing list
<https://lists.sourceforge.net/lists/listinfo/merciless-discuss>`_ or
the `TurboGears Users mailing list <https://groups.google.com/forum/#!forum/turbogears>`_.

Contributing
============

**Yes please!**  We are always looking for contributions, additions and improvements.

The source is available on `GitHub <https://github.com/TurboGears/Ming>`_
and contributions are always encouraged. Contributions can be as simple as
minor tweaks to this documentation or to ming itself.

To contribute, `fork the project <https://github.com/TurboGears/Ming/fork>`_
and send a pull request.

Changes
=======

See the :doc:`news` for a full list of changes to Ming

Documentation Content
=====================

.. toctree::
   :maxdepth: 2
   :glob:

   userguide
   mongodb_indexes
   migrations
   extensions_and_hooks
   polymorphism
   custom_properties
   baselevel
   reference
   news

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

