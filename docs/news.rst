Ming News / Release Notes
=====================================

The latest releases support PyMongo v3 and v4

0.15.2 (Dec 19, 2024)
---------------------
* allow mim Connections to work as context managers matching pymongo's MongoClient behavior

0.15.1 (Oct 23, 2024)
---------------------
* support pymongo 4.9+

0.15.0 (Oct 10, 2024)
---------------------
* drop support for Python 3.8
* prevent installation with pymongo 4.9 or higher (earlier Ming versions will have problems with it too)
* add support for mongodb Client Side Field Level Encryption (CSFLE)
* type hint improvements
* If there's an AttributeError within a @LazyProperty, expose it

0.14.0 (Aug 1, 2024)
--------------------

Support PyMongo 4.x (3.x should still work too).  There are several breaking changes, whether you update pymongo or not:

* Replace ``find_and_modify()`` session methods with ``find_one_and_update()``, ``find_one_and_replace()``, 
  and ``find_one_and_delete()`` to closer match pymongo4's API
* Remove ``group()`` session methods as they are unsupported in pymongo4. Use the aggregation pipeline.
* Remove ``map_reduce()`` and ``inline_map_reduce()`` session methods as they are unsupported in pymongo4. 
  Use the aggregation pipeline.
* Several operations now return their mongo-native result objects; UpdateResult, InsertResult, etc.
* MIM: Replace ``mim.Collection.insert()`` with ``insert_one()`` and ``insert_many()`` to match pymongo4
* MIM: Remove deprecated ``manipulate`` and ``safe`` args from pymongo's ``insert_one`` and ``insert_many`` methods
* MIM: Replace ``mim.Collection.update()`` with ``update_one()`` and ``update_many()`` to match pymongo4
* MIM: Replace ``mim.Collection.count()`` and ``mim.Cursor.count()`` with 
  ``mim.Collection.estimated_document_count()`` and ``mim.Collection.count_documents()`` to match pymongo4
* MIM: Replace ``mim.Collection.remove()`` with ``mim.Collection.delete_one()`` 
  and ``mim.Collection.delete_many()`` to match pymongo4
* MIM: Rename ``collection_names()`` and ``database_names()`` to ``list_collection_names()``
  and ``list_database_names``
* MIM: Remove ``mim.Collection.map_reduce()`` and ``mim.Collection.inline_map_reduce()`` to match pymongo4
* MIM: Replace ``ensure_index()`` with ``create_index()`` to match pymongo4

0.13.0 (Mar 16, 2023)
---------------------
* remove Python 3.6 support
* set all DeprecationWarning's stacklevel=2 to show caller
* MIM: verify kwargs in find/find_one

0.12.2 (Nov 15, 2022)
---------------------
* MIM: add support for UUID types
* improve type hints

0.12.1 (Sep 13, 2022)
---------------------
* allow Field(bytes) to work like Field(S.Binary)
* handle rare race condition exception
* MIM: support cursor/find as context manager
* MIM: handle bytes & Binary in queries
* MIM: handle queries given as RawBSONDocument
* improve type hints
* run tests on 3.10 and 3.11
* test fix for python 3.11
* test suite can be run in parallel (tox -p auto)

0.12.0 (Jun 2, 2022)
---------------------
* Remove support for python < 3.6

0.11.2 (Oct 15, 2021)
---------------------
* MIM: support distinct() usage on fields that are lists
* improve a few type hints

0.11.1 (Sep 9, 2021)
---------------------
* Include py.typed and .pyi files in distribution

0.11.0 (Sep 9, 2021)
---------------------
* Drop support for Python 2.7, 3.3, and 3.4
* Support for Python 3.9
* MIM: support sparse unique indexes
* propagate return values from various update/delete/insert operations
* Support __init_subclass__ arguments
* validate() may not have been validating sub-documents
* Add some type annotations

0.10.2 (Jun 19, 2020)
---------------------
* Fix error using save() and no _id
* MIM: Avoid errors from _ensure_orig_key when positional $ is used

0.10.1 (Jun 17, 2020)
---------------------
* fix situation with gridfs indexes and MIM
* fix validate=False and update some MIM params to match pymongo closer

0.10.0 (Jun 8, 2020)
--------------------

* Support for PyMongo 3.10
* Support for Python 3.8
* Removed start_request/end_request from MIM
* Added ``Cursor.close`` to MIM
* Moved testing from ``nose`` to  ``unittest``

0.9.2 (Mar 12, 2020)
--------------------

* Support ODM before_save hook on Python 3

0.9.1 (May 15, 2019)
--------------------

* Allow usage of PyMongo 3.7

0.9.0 (Feb 22, 2019)
--------------------

* Support for Decimal128 type in MongoDB through ``schema.NumberDecimal``
* Deprecation of ``make_safe`` for Ming Documents

0.8.1 (Feb 22, 2019)
--------------------

* Fix for connection string when seedlist is used

0.8.0 (Jan 15, 2019)
--------------------

* ``FormEncode`` is now an optional dependency only required for projects relying on ``ming.configure``.
* Python 3.7 is now officially supported

0.7.1 (Nov 30, 2018)
--------------------

* MIM: Minimal support for ``Collection.aggregate``

0.7.0 (May 10, 2018)
------------------------------------------------
* MIM: Support for PyMongo 3.6
* MIM: Partial support for ``$text`` queries
* MIM: Make created index match more the style pymongo itself stores indexes.
* MIM: Support matching ``$regex`` against arrays.
* MIM: Support fake ``$score`` in projections.
* MIM: Support ``$slice`` in projections.
* MIM: Partial support for bulk writes, currently only ``UpdateOne``.


0.5.7 (Mar 12, 2020)
------------------------------------------------
* Support ODM before_save hook on Python 3

0.5.6 (Apr 2, 2018)
------------------------------------------------
* MIM: match correctly when search values are lists or dicts more than 1 level deep.

0.6.1 (Sep 27, 2017)
--------------------
* MIM: Support searching for $regex that contain text instead of only "startswith"

0.6.0 (Sep 24, 2017)
--------------------
* Support new PyMongo 3.X API
* MIM: Fix duplicated keys are detected on upsertions
* MIM: Support for filters on distinct
* MIM: Provide drop_indexes
* MIM: Simulate collstats
* MIM: Support insert_one and insert_many results
* MIM: Support update_one and update_many results
* MIM: Support indexing whole subdocuments
* MIM: Support for setOnInsert

0.5.5 (Nov 30, 2016)
------------------------------------------------
* MIM: do not raise errors when regex matching against None or non-existant values

0.5.4 (Apr 29, 2016)
------------------------------------------------
* On Python3, bson.Binary actually decodes to bytes
* Support distinct() on cursors (already supported on collections)

0.5.3 (Oct 18, 2015)
------------------------------------------------

* Documentation Rewrite
* Speed improvements on ODM queries that retrieve objects not already tracked by UOW/IMAP.
* Mapper now provides .ensure_all_indexes() method to ensure indexes for all registered mappers.
* MappedClass (ODM Declarative) now supports ``version_of`` and ``migrate`` for migrations.
* MappedClass.query.get now supports _id as its first positional argument
* ODMSession constructor now exposes the ``autoflush`` argument to automatically flush session before ODM queries,
  previously it was always forced to ``False``. Pay attention that as MongoDB has no transactions autoflush will
  actually write the changes to the database.
* ODMSession now exposes ``.db`` and ``.bind`` properties which lead to the underlying pymongo database and DataStore
* Fixed ODMSession.by_name which previously passed the datastore as session argument.
* ODMSession now provides ``.refresh`` method that updates a specific object from the database
* ThreadLocalODMSession now provides ``by_name`` method to configure Thread Safe sessions using ``ming.configure``
* ming.schema.Invalid now has default ``None`` argument for state, it was never used by the way.


0.5.2 (Apr 16, 2015)
------------------------------------------------
* Support for text indexes
* Specify our requirement on pymongo < 3.0 (until supported)

0.5.1 (Apr 6, 2015)
------------------------------------------------
* Cursor compatibility for Python 3

0.5.0 (Jun 5, 2014)
------------------------------------------------
* Compatible with pymongo 2.7
* Compatible with Python 3.3 and 3.4
* Compatible with PyPy
* Fix update_if_not_modified
* MIM: support float comparisons
* ming.configure now allows any extra params to pass through to MongoClient

0.4.7 (Apr 16, 2014)
------------------------------------------------
* Add allow_none option to ForeignIdProperty

0.4.6 (Apr 4, 2014)
------------------------------------------------
* Fixed issue with if_missing for ForeignIdProperty

0.4.5 (Apr 4, 2014)
------------------------------------------------
* avoid extremely long error text
* Fixed random generated ObjectId on empty ForeignIdProperty

0.4.4 (Mar 10, 2014)
------------------------------------------------
* Revert ForeignIdProperty None optimization
* Fix delete event hook signatures
* Fix typo when flushing an individual object flagged for deletion

0.4.3 (Jan 7, 2014)
------------------------------------------------
* Return result of update_partial()
* ManyToMany support relying on a list of ObjectIds
* Make RelationProperty writable
* Support for all pymongo options in custom_indexes declaration
* Permit relationships that point to same model
* Fix wrong behavior for MIM find_and_modify new option and add test case
* ForeignIdProperty None optimization

0.4.2 (Sep 26, 2013)
------------------------------------------------
* bool(cursor) now raises an Exception.  Pre-0.4 it evaluated based on the value
  of `__len__` but since 0.4 removed `__len__` it always returned True (python's default
  behavior) which could be misleading and unexpected.  This forces application code to
  be changed to perform correctly.
* schema migration now raises the new schema error if both old & new are invalid
* aggregation methods added to session.  `distinct`, `aggregate`, etc are now available
  for convenience and pass through directly to pymongo
* MIM: support for indexing multi-valued properties
* MIM: forcing numerical keys as strings
* MIM: add `manipulate` arg to `insert` for closer pymongo compatibility

0.4.1 and 0.3.9 (Aug 30, 2013)
------------------------------------------------

* MIM: Support slicing cursors
* MIM: Fixed exact dot-notation queries
* MIM: Fixed dot-notation queries against null fields
* MIM: Translate time-zone aware timestamps to UTC timestamps.  `pytz` added as dependency
* MIM: Allow the remove argument to `find_and_modify`

0.4 (June 28, 2013)
------------------------------------------------

* removed 'flyway' package from ming.  It is now available from https://github.com/amol-/ming-flyway
  This removes the dependency on PasteScript and will make Python 3 migration easier.
* WebOb dependency is optional.
* removed `cursor.__len__`  You must change `len(query)` to `query.count()` now.  This prevents
  inadvertent extra count queries from running.  https://sourceforge.net/p/merciless/bugs/18/

0.3.2 through 0.3.8
------------------------------------------------

* many improvements to make MIM more like actual mongo
* various fixes and improvements

0.3.2 (rc1) (January 8, 2013)
------------------------------------------------

Some of the larger changes:

* Update to use MongoClient everywhere instead of variants of `pymongo.Connection`
* Remove MasterSlaveConnection and ReplicaSetConnection support

0.3.2 (dev) (July 26, 2012)
------------------------------------------------

Whoops, skipped a version there. Anyway, the bigger changes:

* Speed improvements in validation, particularly `validate_ranges` which allows
  selective validation of arrays
* Allow requiring scalar values to be non-None
* Add support for geospatial indexing
* Updates to engine/datastore creation syntax (use the new `create_engine` or
  `create_datastore`, which are significantly simplified and improved).

0.3 (March 6, 2012)
------------------------------------------------

Lots of snapshot releases, and finally a backwards-breaking change. The biggest change
is the renaming of the ORM to be the ODM.

* Renamed ming.orm to ming.odm
* Lots of bug fixes
* Add gridfs support to Ming
* Add contextual ODM session

0.2.1
----------

It's been a lonnnnng time since our last real release, so here are the high
points (roughly organized from low-level to high-level):

* Support for replica sets
* Support for using gevent with Ming (asynchronous Python library using libevent)
* Add find_and_modify support
* Create Mongo-in-Memory support for testing (mim:// url)
* Some don't shoot-yourself-in-the-foot support (calling .remove() on an
  instance, for example)
* Move away from using formencode.Invalid exception
* Allow skipping Ming validation, unsafe inserts
* Elaborate both the imperative and declarative support in the document- and
  ORM-layers
* Polymorphic inheritance support in the ORM
