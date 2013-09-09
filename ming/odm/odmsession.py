from collections import defaultdict

from ming.session import Session
from ming.utils import ThreadLocalProxy, ContextualProxy, indent
from ming.base import Object
from .base import state, ObjectState, session, with_hooks, call_hook
from .mapper import mapper
from .unit_of_work import UnitOfWork
from .identity_map import IdentityMap

class ODMSession(object):

    _registry = {}

    def __init__(self, doc_session=None, bind=None, extensions=None):
        if doc_session is None:
            doc_session = Session(bind)
        if extensions is None: extensions = []
        self.impl = doc_session
        self.uow = UnitOfWork(self)
        self.imap = IdentityMap()
        self.extensions = [ e(self) for e in extensions ]
        self.autoflush = False

    def register_extension(self, extension):
        self.extensions.append(extension(self))

    @classmethod
    def by_name(cls, name):
        if name in cls._registry:
            result = cls._registry[name]
        else:
            result = cls._registry[name] = cls(Session._datastores.get(name))
        return result

    def mapper(self, cls, collection, **kwargs):
        return mapper(
            cls, collection=collection, session=self, **kwargs)
    
    def save(self, obj):
        self.uow.save(obj)
        self.imap.save(obj)
        state(obj).session = self

    def expunge(self, obj):
        self.uow.expunge(obj)
        self.imap.expunge(obj)
        state(obj).session = None

    @with_hooks('flush')
    def flush(self, obj=None):
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
                self.update_now(obj, st)

    @with_hooks('insert')
    def insert_now(self, obj, st, **kwargs):
        mapper(obj).insert(obj, st, self, **kwargs)

    @with_hooks('update')
    def update_now(self, obj, st, **kwargs):
        mapper(obj).update(obj, st, self, **kwargs)

    @with_hooks('delete')
    def delete_now(self, obj, st, **kwargs):
        mapper(obj).delete(obj, st, self, **kwargs)
        
    def clear(self):
        # Orphan all objects
        for obj in self.uow:
            state(obj).session = None
        self.uow.clear()
        self.imap.clear()

    def close(self):
        self.clear()
        if self.impl.bind:
            self.impl.bind.conn.end_request()

    def get(self, cls, idvalue):
        result = self.imap.get(cls, idvalue)
        if result is None:
            result = self.find(cls, dict(_id=idvalue)).first()
        return result

    def find(self, cls, *args, **kwargs):
        refresh = kwargs.pop('refresh', False)
        decorate = kwargs.pop('decorate', None)
        if self.autoflush:
            self.flush()
        m = mapper(cls)
        # args = map(deinstrument, args)
        ming_cursor = self.impl.find(m.collection, *args, **kwargs)
        odm_cursor = ODMCursor(self, cls, ming_cursor, refresh=refresh, decorate=decorate, fields=kwargs.get('fields'))
        call_hook(self, 'cursor_created', odm_cursor, 'find', cls, *args, **kwargs)
        return odm_cursor

    def find_and_modify(self, cls, *args, **kwargs):
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

    @with_hooks('remove')
    def remove(self, cls, *args, **kwargs):
        m = mapper(cls)
        m.remove(self, *args, **kwargs)

    def update(self, cls, spec, fields, **kwargs):
        m = mapper(cls)
        m.update_partial(self, spec, fields, **kwargs)

    def update_if_not_modified(self, obj, fields, upsert=False):
        self.update(obj.__class__, state(obj).original_document, fields, upsert)
        err = self.impl.db.command(dict(getlasterror=1))
        if err['n'] and err['updatedExisting']: return True
        return False

    def __repr__(self):
        l = ['<session>']
        l.append('  ' + indent(repr(self.uow), 2))
        l.append('  ' + indent(repr(self.imap), 2))
        return '\n'.join(l)

    def ensure_index(self, cls, fields, **kwargs):
        return self.impl.ensure_index(cls, fields, **kwargs)

    def ensure_indexes(self, cls):
        return self.impl.ensure_indexes(cls)

    def drop_indexes(self, cls):
        return self.impl.drop_indexes(cls)

    def update_indexes(self, cls, **kwargs):
        return self.impl.update_indexes(cls, **kwargs)

    def group(self, cls, *args, **kwargs):
        m = mapper(cls)
        return self.impl.group(m.collection, *args, **kwargs)

    def aggregate(self, cls, *args, **kwargs):
        m = mapper(cls)
        return self.impl.aggregate(m.collection, *args, **kwargs)

    def distinct(self, cls, *args, **kwargs):
        m = mapper(cls)
        return self.impl.distinct(m.collection, *args, **kwargs)

    def map_reduce(self, cls, *args, **kwargs):
        m = mapper(cls)
        return self.impl.map_reduce(m.collection, *args, **kwargs)

    def inline_map_reduce(self, cls, *args, **kwargs):
        m = mapper(cls)
        return self.impl.inline_map_reduce(m.collection, *args, **kwargs)


class SessionExtension(object):

    def __init__(self, session):
        self.session = session
    def before_insert(self, obj, st): pass
    def after_insert(self, obj, st): pass
    def before_update(self, obj, st): pass
    def after_update(self, obj, st): pass
    def before_delete(self, obj, st): pass
    def after_delete(self, obj, st): pass
    def before_remove(self, cls, *args, **kwargs): pass
    def after_remove(self, cls, *args, **kwargs): pass
    def before_flush(self, obj=None): pass
    def after_flush(self, obj=None): pass

    def cursor_created(self, cursor, action, *args, **kw): pass
    def before_cursor_next(self, cursor): pass
    def after_cursor_next(self, cursor): pass

class ThreadLocalODMSession(ThreadLocalProxy):
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
    def flush_all(cls):
        for sess in cls._session_registry.values():
            sess.flush()

    @classmethod
    def close_all(cls):
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

    def __len__(self):
        return self.count()

    @property
    def extensions(self):
        return self.session.extensions

    def count(self):
        return self.ming_cursor.count()

    def _next_impl(self):
        doc = self.ming_cursor.next()
        obj = self.session.imap.get(self.cls, doc['_id'])
        if obj is None:
            obj = self.mapper.create(doc, self._options)
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
        call_hook(self, 'before_cursor_next', self)
        try:
            return self._next_impl()
        finally:
            call_hook(self, 'after_cursor_next', self)

    def options(self, **kwargs):
        odm_cursor = ODMCursor(self.session, self.cls,self.ming_cursor)
        odm_cursor._options = Object(self._options, **kwargs)
        call_hook(self, 'cursor_created', odm_cursor, 'options', self, **kwargs)
        return odm_cursor

    def limit(self, limit):
        odm_cursor = ODMCursor(self.session, self.cls,
                               self.ming_cursor.limit(limit))
        call_hook(self, 'cursor_created', odm_cursor, 'limit', self, limit)
        return odm_cursor

    def skip(self, skip):
        odm_cursor = ODMCursor(self.session, self.cls,
                               self.ming_cursor.skip(skip))
        call_hook(self, 'cursor_created', odm_cursor, 'skip', self, skip)
        return odm_cursor

    def hint(self, index_or_name):
        odm_cursor = ODMCursor(self.session, self.cls,
                               self.ming_cursor.hint(index_or_name))
        call_hook(self, 'cursor_created', odm_cursor, 'hint', self, index_or_name)
        return odm_cursor

    def sort(self, *args, **kwargs):
        odm_cursor = ODMCursor(self.session, self.cls,
                               self.ming_cursor.sort(*args, **kwargs))
        call_hook(self, 'cursor_created', odm_cursor, 'sort', self, *args, **kwargs)
        return odm_cursor

    def one(self):
        try:
            result = self.next()
        except StopIteration:
            raise ValueError, 'Less than one result from .one()'
        try:
            self.next()
        except StopIteration:
            return result
        raise ValueError, 'More than one result from .one()'

    def first(self):
        try:
            return self.next()
        except StopIteration:
            return None

    def all(self):
        return list(self)
