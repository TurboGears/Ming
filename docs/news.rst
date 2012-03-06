Ming News / Release Notes
=====================================

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
