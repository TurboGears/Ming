:tocdepth: 3

==============================
Model Evolution and Migrations
==============================

One of the most irritating parts of maintaining an application for a while is the
need to do data migrations from one version of the schema to another.
While Ming can't completely remove the pain of migrations, it does seek to make migrations
as simple as possible.

Performing Migrations
=====================

First of all let's populate our database with some stray data that needs to be migrated:

.. run-pysnippet:: ming_odm_migrations snippet1

Suppose we decided that we want to gather metadata of the pages in a ``metadata``
property, which will contain the ``categories`` and ``tags`` of the page.
We might write our new schema as follows:

.. literalinclude:: src/ming_odm_migrations.py
    :start-after: #{migrate-newmodel
    :end-before: #}

But now if we try to .find() things in our database, our metadata has gone missing:

.. run-pysnippet:: ming_odm_migrations snippet2
    :skip: 1

What we need now is a migration.  Luckily, Ming makes migrations manageable.

First of all we need to declare the previous schema so that Ming knows how
to validate the old values (previous versions schemas are declared using the
:ref:`ming_baselevel` as they are not tracked by the UnitOfWork or IdentityMap):

.. literalinclude:: src/ming_odm_migrations.py
    :start-after: #{migrate-oldschema
    :end-before: #}

Whenever Ming fetches a document from the database it will validate it against
our model schema.

If the validation fails it will check the document against
the previous version of the schema (provided as ``__mongometa__.version_of``)
and if validation passes the ``__mongometa__.migrate`` function is called to upgrade the data.

So, to be able to upgrade our data, all we need to do is include the previous schema,
and a migration function in our ``__mongometa__``:

.. literalinclude:: src/ming_odm_migrations.py
    :start-after: #{migrate-model-with-migration
    :end-before: #}
    :emphasize-lines: 5, 7-11, 17

Then to force the migration we also added a ``_version`` property which passes validation
only when its value is ``1`` (Using :class:`schema.Value`).
As old models do not provide a **_version** field they won't pass validation and
so they will trigger the migrate process:

.. run-pysnippet:: ming_odm_migrations snippet3

And that's it.

Lazy Migrations
===============

Migrations are performed lazily as the objects are loaded from the database, so you only
pay the cost of migration the data you access. Also the migrated data is not saved back
on the database unless the object is modified. This can be easily seen by querying
documents directly through pymongo as on mongodb they still have ``tags`` outside of ``metadata``:

.. run-pysnippet:: ming_odm_migrations snippet4

Eager Migrations
================

If, unlike for lazy migrations, you wish to migrate all the objects in a collection,
and save them back you can use the ``migrate`` function available on the **foundation
layer manager**:

.. run-pysnippet:: ming_odm_migrations snippet5
    :emphasize-lines: 4,5

That will automatically migrate all the documents in the collection one by one.

Chained Migrations
==================

If you evolved your schema multiple times you can chain migrations by adding a ``version_of``
to all the previous versions of the data:

.. literalinclude:: src/ming_odm_migrations.py
    :pyobject: MyModel
    :emphasize-lines: 5,9

Then just apply all the migrations as you normally would:

.. run-pysnippet:: ming_odm_migrations snippet6

The resulting documented changed name from ``"desrever"`` to ``"REVERSED"``
that is because ``_version=1`` forced the name to be uppercase and then
``_version=2`` reversed it.

.. note::

    When migrating make sure you always bring forward the ``_id`` value in the
    old data, or you will end up with duplicated data for each migration step
    as a new id would be generated for newly migrated documents.
