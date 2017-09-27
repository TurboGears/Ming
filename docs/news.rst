Ming News / Release Notes
=====================================

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
