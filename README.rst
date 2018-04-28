Ming
====

.. image:: https://travis-ci.org/TurboGears/Ming.png
    :target: https://travis-ci.org/TurboGears/Ming

.. image:: https://coveralls.io/repos/TurboGears/Ming/badge.png
    :target: https://coveralls.io/r/TurboGears/Ming

.. image:: https://img.shields.io/pypi/v/Ming.svg
   :target: https://pypi.python.org/pypi/Ming

.. image:: https://img.shields.io/pypi/pyversions/Ming.svg
    :target: https://pypi.python.org/pypi/Ming

.. image:: https://img.shields.io/pypi/l/Ming.svg
    :target: https://pypi.python.org/pypi/Ming

.. image:: https://img.shields.io/gitter/room/turbogears/Lobby.svg
    :target: https://gitter.im/turbogears/Lobby

.. image:: https://img.shields.io/twitter/follow/turbogearsorg.svg?style=social&label=Follow
    :target: https://twitter.com/turbogearsorg

Ming is a MongoDB ODM ( Object Document Mapper, like an ORM but for Document based databases).

Ming builds on top of ``pymongo`` by exending with:

* Declarative Models
* Schema Validation and Conversion
* Lazy Schema Evolution
* Unit of Work
* Identity Map
* One-To-Many, Many-To-One and Many-To-Many Relations
* Pure InMemory MongoDB Implementation

Getting Started
---------------

To use Ming you need to create a ``Session`` and a few models that
should be managed by it::

    from ming import create_datastore, schema
    from ming.odm import ThreadLocalODMSession, Mapper, MappedClass, FieldProperty

    session = ThreadLocalODMSession(
        bind=create_datastore('mongodb://localhost:27017/dbname')
    )

    class WikiPage(MappedClass):
        class __mongometa__:
            session = session
            name = 'wiki_page'

        _id = FieldProperty(schema.ObjectId)
        title = FieldProperty(schema.String(required=True))
        text = FieldProperty(schema.String(if_missing=''))

    Mapper.compile_all()

Then you can create and query those models::

    >>> WikiPage(title='FirstPage', text='This is a page')
    <WikiPage text='This is a page'
       _id=ObjectId('5ae4ef717ddf1ff6704afff5')
       title='FirstPage'>

    >>> session.flush()  # Flush session to actually create wikipage.

    >>> wp = WikiPage.query.find({'text': 'This is a page'}).first()
    >>> print(wp)
    <WikiPage text='This is a page'
      _id=ObjectId('5ae4ef717ddf1ff6704afff5')
      title='FirstPage'>

Documentation
-------------

If you want to dig further in Ming, you can find the documentation
on http://ming.readthedocs.io/en/latest/