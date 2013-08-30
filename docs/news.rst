Ming News / Release Notes
=====================================

0.4 (June 28, 2013)
------------------------------------------------

* removed 'flyway' package from ming.  It is now available from https://github.com/amol-/ming-flyway 
  This removes the dependency on PasteScript and will make Python 3 migration easier.
* WebOb dependency is optional.
* removed `cursor.__len__`  You must change `len(query)` to `query.count()` now.  This prevents
  inadvertent extra count queries from running.  https://sourceforge.net/p/merciless/bugs/18/
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
