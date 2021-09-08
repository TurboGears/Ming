from collections import defaultdict

from ming.session import Session
from ming.utils import ThreadLocalProxy, ContextualProxy, indent
from ming.base import Object
from ming.exc import MingException
from .base import state, ObjectState, session, _with_hooks, _call_hook
from .mapper import mapper
from .unit_of_work import UnitOfWork
from .identity_map import IdentityMap

class ODMSession(object):
    """Current Ming Session.

    Keeps track of active objects in the IdentityMap and UnitOfWork
    and of the current connection to the database. All the operation
    on MongoDB should happen through the ODMSession to avoid inconsistent
    state between objects updated through the session and outside the session.
    """
    _registry = {}

    def __init__(self, doc_session=None, bind=None, extensions=None,
                 autoflush=False):
        if doc_session is None:
            doc_session = Session(bind)
        if extensions is None: extensions = []
        self.impl = doc_session
        self.uow = UnitOfWork(self)
        self.imap = IdentityMap()
        self.extensions = [ e(self) for e in extensions ]
        self.autoflush = autoflush

    def register_extension(self, extension):
        self.extensions.append(extension(self))

    @property
    def bind(self):
        return self.impl.bind

    @property
    def db(self):
        """Access the low-level pymongo database"""
        return self.impl.db

    @classmethod
    def by_name(cls, name):
        """Retrieve or create a new Session with the given ``name``.

        This is useful to keep around multiple sessions and identify
        them by name. The session registry is global so they are
        available everywhere as far as the ``ming`` module is the same.
        """
        if name in cls._registry:
            result = cls._registry[name]
        else:
            result = cls._registry[name] = cls(bind=Session._datastores.get(name))
        return result

    def mapper(self, cls, collection, **kwargs):
        return mapper(
            cls, collection=collection, session=self, **kwargs)

    def save(self, obj):
        """Add an object to the Session (and its UnitOfWork and IdentityMap).

        Usually objects are automatically added to the session when created.
        This is done by :meth:`._InitDecorator.saving_init` which is the ``__init__``
        method of all the :class`.MappedClass` subclasses instrumented by :class:`.Mapper`.

        So calling this method is usually not required unless the object was
        expunged.
        """
        self.uow.save(obj)
        self.imap.save(obj)
        state(obj).session = self

    def expunge(self, obj):
        """Remove an object from the Session (and its UnitOfWork and IdentityMap)"""
        self.uow.expunge(obj)
        self.imap.expunge(obj)
        state(obj).session = None

    def refresh(self, obj):
        """Refreshes the object in the session by querying it back and updating its state"""
        self.expunge(obj)
        return self.find(obj.__class__, {'_id': obj._id}, refresh=True).first()

    @_with_hooks('flush')
    def flush(self, obj=None):
        """Flush ``obj`` or all the objects in the UnitOfWork.

        When ``obj`` is provided, only ``obj`` is flushed to the
        database, otherwise all the objects in the UnitOfWork
        are persisted on the databases according to their current
        state.
        """
        if self.impl.db is None: return
        if obj is None:
            self.uow.flush()
        else:
            st = state(obj)
            if st.status == st.new:
                self.insert_now(obj, st)
            elif st.status == st.dirty:
                self.update_now(obj, st)
            elif st.status == st.deleted:
                self.delete_now(obj, st)

    @_with_hooks('insert')
    def insert_now(self, obj, st, **kwargs):
        mapper(obj).insert(obj, st, self, **kwargs)

    @_with_hooks('update')
    def update_now(self, obj, st, **kwargs):
        mapper(obj).update(obj, st, self, **kwargs)

    @_with_hooks('delete')
    def delete_now(self, obj, st, **kwargs):
        mapper(obj).delete(obj, st, self, **kwargs)

    def clear(self):
        """Expunge all the objects from the session."""
        # Orphan all objects
        for obj in self.uow:
            state(obj).session = None
        self.uow.clear()
        self.imap.clear()

    def close(self):
        """Clear the session."""
        self.clear()

    def get(self, cls, idvalue):
        """Retrieves ``cls`` by its ``_id`` value passed as ``idvalue``.

        If the object is already available in the IdentityMap
        this acts as a simple cache a returns the current object
        without querying the database. Otherwise a *find* query
        is issued and the object retrieved.

        This the same as calling ``cls.query.get(_id=idvalue)``.
        """
        result = self.imap.get(cls, idvalue)
        if result is None:
            result = self.find(cls, dict(_id=idvalue)).first()
        return result

    def find(self, cls, *args, **kwargs):
        """Retrieves ``cls`` by performing a mongodb query.

        This is the same as calling ``cls.query.find()`` and
        always performs a query on the database. According to
        the ``refresh`` argument the objects are also updated
        in the UnitOfWork or not. Otherwise the UnitOfWork
        keeps the old object state which is the default.

        If the session has ``autoflush`` option, the session
        if flushed before performing the query.

        Arguments are the same as :meth:`pymongo.collection.Collection.find`
        plus the following additional arguments:

            * ``allow_extra`` Whenever to raise an exception in case of extra
              fields not specified in the model definition.
            * ``strip_extra`` Whenever extra fields should be stripped if present.
            * ``validate`` Disable validation or not.

        It returns an :class:`.ODMCursor` with the results.
        """
        if self.autoflush:
            self.flush()

        refresh = kwargs.pop('refresh', False)
        decorate = kwargs.pop('decorate', None)
        m = mapper(cls)

        projection = kwargs.pop('fields', kwargs.pop('projection', None))
        if projection is not None:
            kwargs['projection'] = projection

        ming_cursor = self.impl.find(m.collection, *args, **kwargs)
        odm_cursor = ODMCursor(self, cls, ming_cursor, refresh=refresh, decorate=decorate,
                               fields=kwargs.get('projection'))
        _call_hook(self, 'cursor_created', odm_cursor, 'find', cls, *args, **kwargs)
        return odm_cursor

    def find_and_modify(self, cls, *args, **kwargs):
        """Finds and updates ``cls``.

        Arguments are the same as :meth:`pymongo.collection.Collection.find_and_modify`.

        If the session has ``autoflush`` option, the session
        if flushed before performing the query.

        It returns an :class:`.ODMCursor` with the results.
        """

        decorate = kwargs.pop('decorate', None)
        if self.autoflush:
            self.flush()
        m = mapper(cls)
        obj = self.impl.find_and_modify(m.collection, *args, **kwargs)
        if obj is None: return None
        cursor = ODMCursor(self, cls, iter([ obj ]), refresh=True, decorate=decorate)
        result = cursor.first()
        state(result).status = ObjectState.clean
        return result

    @_with_hooks('remove')
    def remove(self, cls, *args, **kwargs):
        """Delete one or more ``cls`` entries from the collection.

        This performs the delete operation outside of the UnitOfWork,
        so the objects already in the UnitOfWork and IdentityMap are
        unaffected and might be recreated when the session is flushed.

        Arguments are the same as :meth:`pymongo.collection.Collection.remove`.
        """
        m = mapper(cls)
        return m.remove(self, *args, **kwargs)

    def update(self, cls, spec, fields, **kwargs):
        """Updates one or more ``cls`` entries from the collection.

        This performs the update operation outside of the UnitOfWork,
        so the objects already in the UnitOfWork and IdentityMap are
        unaffected and might be restored to previous state when
        the session is flushed.

        Arguments are the same as :meth:`pymongo.collection.Collection.update`.
        """
        m = mapper(cls)
        return m.update_partial(self, spec, fields, **kwargs)

    def update_if_not_modified(self, obj, fields, upsert=False):
        """Updates one entry unless it was modified since first queried.

        Arguments are the same as :meth:`pymongo.collection.Collection.update`.

        Returns whenever the update was performed or not.
        """
        spec = state(obj).original_document
        self.update(obj.__class__, spec, fields, upsert=upsert)
        err = self.impl.db.command(dict(getlasterror=1))
        return bool(err['n'] and err['updatedExisting'])

    def __repr__(self):
        l = ['<session>']
        l.append('  ' + indent(repr(self.uow), 2))
        l.append('  ' + indent(repr(self.imap), 2))
        return '\n'.join(l)

    def ensure_index(self, cls, fields, **kwargs):
        return self.impl.ensure_index(cls, fields, **kwargs)

    def ensure_indexes(self, cls):
        """Ensures all indexes declared in ``cls``"""
        return self.impl.ensure_indexes(cls)

    def drop_indexes(self, cls):
        """Drop all indexes declared in ``cls``"""
        return self.impl.drop_indexes(cls)

    def group(self, cls, *args, **kwargs):
        """Runs a grouping on the model collection.

        Arguments are the same as  :meth:`pymongo.collection.Collection.group`.
        """
        m = mapper(cls)
        return self.impl.group(m.collection, *args, **kwargs)

    def aggregate(self, cls, *args, **kwargs):
        """Runs an aggregation pipeline on the given collection.

        Arguments are the same as  :meth:`pymongo.collection.Collection.aggregate`.
        """
        m = mapper(cls)
        return self.impl.aggregate(m.collection, *args, **kwargs)

    def distinct(self, cls, *args, **kwargs):
        """Get a list of distinct values for a key among all documents in this collection.

        Arguments are the same as  :meth:`pymongo.collection.Collection.distinct`.
        """
        m = mapper(cls)
        return self.impl.distinct(m.collection, *args, **kwargs)

    def map_reduce(self, cls, *args, **kwargs):
        """Runs a MapReduce job and stores results in a collection.

        Arguments are the same as  :meth:`pymongo.collection.Collection.map_reduce`.
        """
        m = mapper(cls)
        return self.impl.map_reduce(m.collection, *args, **kwargs)

    def inline_map_reduce(self, cls, *args, **kwargs):
        """Runs a MapReduce job and keeps results in-memory.

        Arguments are the same as  :meth:`pymongo.collection.Collection.inline_map_reduce`.
        """
        m = mapper(cls)
        return self.impl.inline_map_reduce(m.collection, *args, **kwargs)


class SessionExtension(object):
    """Base class that should be inherited to handle Session events."""

    def __init__(self, session):
        self.session = session
    def before_insert(self, obj, st):
        """Before an object gets inserted in this session"""
        pass
    def after_insert(self, obj, st):
        """After an object gets inserted in this session"""
        pass
    def before_update(self, obj, st):
        """Before an object gets updated in this session"""
        pass
    def after_update(self, obj, st):
        """After an object gets updated in this session"""
        pass
    def before_delete(self, obj, st):
        """Before an object gets deleted in this session"""
        pass
    def after_delete(self, obj, st):
        """After an object gets deleted in this session"""
        pass
    def before_remove(self, cls, *args, **kwargs):
        """Before a remove query is performed session"""
        pass
    def after_remove(self, cls, *args, **kwargs):
        """After a remove query is performed session"""
        pass
    def before_flush(self, obj=None):
        """Before the session is flushed for ``obj``

        If ``obj`` is ``None`` it means all the objects in
        the UnitOfWork which can be retrieved by iterating
        over ``ODMSession.uow``
        """
        pass
    def after_flush(self, obj=None):
        """After the session is flushed for ``obj``

        If ``obj`` is ``None`` it means all the objects in
        the UnitOfWork which can be retrieved by iterating
        over ``ODMSession.uow``
        """
        pass

    def cursor_created(self, cursor, action, *args, **kw):
        """New cursor with the results of a query got created"""
        pass
    def before_cursor_next(self, cursor):
        """Cursor is going to advance to next result"""
        pass
    def after_cursor_next(self, cursor):
        """Cursor has advanced to next result"""
        pass

class ThreadLocalODMSession(ThreadLocalProxy):
    """ThreadLocalODMSession is a thread-safe proxy to :class:`ODMSession`.

    This routes properties and methods to the session in charge of
    the current thread. For a reference of available *methods* and *properties*
    refer to the :class:`ODMSession`.
    """
    _session_registry = ThreadLocalProxy(dict)

    def __init__(self, *args, **kwargs):
        kwargs.setdefault('extensions', [])
        ThreadLocalProxy.__init__(self, ODMSession, *args, **kwargs)

    def _get(self):
        result = super(ThreadLocalODMSession, self)._get()
        self._session_registry.__setitem__(id(self), self)
        return result

    def register_extension(self, extension):
        self._kwargs['extensions'].append(extension)

    def close(self):
        self._get().close()
        super(ThreadLocalODMSession, self).close()

    def mapper(self, cls, collection, **kwargs):
        return mapper(
            cls, collection=collection, session=self, **kwargs)

    @classmethod
    def by_name(cls, name):
        """Retrieve or create a new ThreadLocalODMSession with the given ``name``.

        This is useful to keep around multiple sessions and identify
        them by name. The session registry is global so they are
        available everywhere as far as the ``ming`` module is the same.
        """
        datastore = Session._datastores.get(name)
        if datastore is None:
            return None

        for odmsession in cls._session_registry.values():
            if odmsession.bind is datastore:
                return odmsession
        else:
            return ThreadLocalODMSession(bind=datastore)

    @classmethod
    def flush_all(cls):
        """Flush all the ODMSessions registered in current thread

        Usually is not necessary as only one session is registered per-thread.
        """
        for sess in cls._session_registry.values():
            sess.flush()

    @classmethod
    def close_all(cls):
        """Closes all the ODMSessions registered in current thread.

        Usually is not necessary as only one session is registered per-thread.
        """
        for sess in cls._session_registry.values():
            sess.close()

class ContextualODMSession(ContextualProxy):
    _session_registry = defaultdict(dict)

    def __init__(self, context, *args, **kwargs):
        kwargs.setdefault('extensions', [])
        ContextualProxy.__init__(self, ODMSession, context, *args, **kwargs)
        self._context = context

    def _get(self):
        result = super(ContextualODMSession, self)._get()
        self._session_registry[self._context()][id(self)] = self
        return result

    def mapper(self, cls, collection, **kwargs):
        return mapper(
            cls, collection=collection, session=self, **kwargs)

    def close(self):
        self._get().close()
        super(ContextualODMSession, self).close()
        self._session_registry[self._context()].pop(id(self), None)

    @classmethod
    def flush_all(cls, context):
        for sess in cls._session_registry[context].values():
            sess.flush()

    @classmethod
    def close_all(cls, context):
        for sess in cls._session_registry[context].values():
            sess.close()
        del cls._session_registry[context]


class ODMCursor(object):
    """Represents the results of query.

    The cursors can be iterated over to retrieve the
    results one by one.
    """
    def __bool__(self):
        raise MingException('Cannot evaluate ODMCursor to a boolean')
    __nonzero__ = __bool__  # python 2

    def __init__(self, session, cls, ming_cursor, refresh=False, decorate=None, fields=None):
        self.session = session
        self.cls = cls
        self.mapper = mapper(cls)
        self.ming_cursor = ming_cursor
        self._options = Object(
            refresh=refresh,
            decorate=decorate,
            fields=fields,
            instrument=True)

    def __iter__(self):
        return self

    @property
    def extensions(self):
        return self.session.extensions

    def count(self):
        """Get the number of objects retrieved by the query"""
        return self.ming_cursor.count()

    def distinct(self, *args, **kwargs):
        return self.ming_cursor.distinct(*args, **kwargs)

    def _next_impl(self):
        doc = next(self.ming_cursor)
        obj = self.session.imap.get(self.cls, doc['_id'])
        if obj is None:
            obj = self.mapper.create(doc, self._options, remake=False)
            state(obj).status = ObjectState.clean
            self.session.save(obj)
        elif self._options.refresh:
            # Refresh object
            state(obj).update(doc)
            state(obj).status = ObjectState.clean
        else:
            # Never refresh objects from the DB unless explicitly requested
            pass
        other_session = session(obj)
        if other_session is not None and other_session != self:
            other_session.expunge(obj)
            self.session.save(obj)
        if self._options.decorate is not None:
            return self._options.decorate(obj)
        else:
            return obj

    def next(self):
        _call_hook(self, 'before_cursor_next', self)
        try:
            return self._next_impl()
        finally:
            _call_hook(self, 'after_cursor_next', self)

    __next__ = next

    def options(self, **kwargs):
        odm_cursor = ODMCursor(self.session, self.cls,self.ming_cursor)
        odm_cursor._options = Object(self._options, **kwargs)
        _call_hook(self, 'cursor_created', odm_cursor, 'options', self, **kwargs)
        return odm_cursor

    def limit(self, limit):
        """Limit the number of entries retrieved by the query"""
        odm_cursor = ODMCursor(self.session, self.cls,
                               self.ming_cursor.limit(limit))
        _call_hook(self, 'cursor_created', odm_cursor, 'limit', self, limit)
        return odm_cursor

    def skip(self, skip):
        """Skip the first ``skip`` entries retrieved by the query"""
        odm_cursor = ODMCursor(self.session, self.cls,
                               self.ming_cursor.skip(skip))
        _call_hook(self, 'cursor_created', odm_cursor, 'skip', self, skip)
        return odm_cursor

    def hint(self, index_or_name):
        odm_cursor = ODMCursor(self.session, self.cls,
                               self.ming_cursor.hint(index_or_name))
        _call_hook(self, 'cursor_created', odm_cursor, 'hint', self, index_or_name)
        return odm_cursor

    def sort(self, *args, **kwargs):
        """Sort results of the query.

        See :meth:`pymongo.cursor.Cursor.sort` for details on the available
        arguments.
        """
        odm_cursor = ODMCursor(self.session, self.cls,
                               self.ming_cursor.sort(*args, **kwargs))
        _call_hook(self, 'cursor_created', odm_cursor, 'sort', self, *args, **kwargs)
        return odm_cursor

    def one(self):
        """Gets one result and exaclty one.

        Raises ``ValueError`` exception if less or more than
        one result is returned by the query.
        """
        try:
            result = self.next()
        except StopIteration:
            raise ValueError('Less than one result from .one()')
        try:
            self.next()
        except StopIteration:
            return result
        raise ValueError('More than one result from .one()')

    def first(self):
        """Gets the first result of the query"""
        try:
            return self.next()
        except StopIteration:
            return None

    def all(self):
        """Retrieve all the results of the query"""
        return list(self)

    def rewind(self):
        """Rewind this cursor to its unevaluated state.
        Reset this cursor if it has been partially or completely evaluated.
        Any options that are present on the cursor will remain in effect.
        Future iterating performed on this cursor will cause new queries to be sent to the server,
        even if the resultant data has already been retrieved by this cursor.
        """
        return self.ming_cursor.rewind()
