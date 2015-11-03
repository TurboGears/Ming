:tocdepth: 3

.. _odm-events:

====================
ODM Event Interfaces
====================

This section describes the various categories of events which can be intercepted within the Ming ODM.
Events can be trapped by registering :class:`.MapperExtension` and :class:`.SessionExtension` instances
which implement the event handlers for the events you want to trap.

Mapper Events
=============

Mapper events are used to track
To use MapperExtension, make your own subclass of it and just send it off to a mapper:

.. code-block:: python

    from ming.odm.mapper import MapperExtension
    class MyExtension(MapperExtension):
        def after_insert(self, obj, st, sess):
            print "instance %s after insert !" % obj

    class MyMappedClass(MappedClass):
        class __mongometa__:
            session = session
            name = 'my_mapped_class'
            extensions = [ MyExtension ]

Multiple extensions will be chained together and processed in order;

.. code-block:: python

    extensions = [ext1, ext2, ext3]

.. autoclass:: ming.odm.MapperExtension
    :members:
    :noindex:


Session Events
==============

The SessionExtension applies plugin points for Session objects
and ODMCursor objects:

.. code-block:: python

    from ming.odm.base import state
    from ming.odm.odmsession import SessionExtension

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

    ODMSession = ThreadLocalODMSession(session,
                                       extensions=[ProjectSessionExtension])

The same SessionExtension instance can be used with any number of sessions.
It is possible to register extensions on an already created ODMSession using
the `register_extension(extension)` method of the session itself.
Even calling register_extension it is possible to register the extensions only
before using the session for the first time.

.. autoclass:: ming.odm.SessionExtension
    :members:
    :noindex:
