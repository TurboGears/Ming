from ming.session import Session
from ming.utils import ThreadLocalProxy, indent
from .base import mapper, state, ObjectState, session
from .unit_of_work import UnitOfWork
from .identity_map import IdentityMap

class with_hooks(object):
    'Decorator to use for Session extensions'

    def __init__(self, hook_name):
        self.hook_name = hook_name

    def __call__(self, func):
        before_meth = 'before_' + self.hook_name
        after_meth = 'after_' + self.hook_name
        def before(session, *args, **kwargs):
            for e in session.extensions:
                getattr(e, before_meth)(*args, **kwargs)
        def after(session, *args, **kwargs):
            for e in session.extensions:
                getattr(e, after_meth)(*args, **kwargs)
        def inner(session, *args, **kwargs):
            before(session, *args, **kwargs)
            result = func(session, *args, **kwargs)
            after(session, *args, **kwargs)
            return result
        inner.__name__ = func.__name__
        inner.__doc__ = 'Hook wraper around\n' + repr(func.__doc__)
        return inner

class ORMSession(object):

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

    @classmethod
    def by_name(cls, name):
        if name in cls._registry:
            result = cls._registry[name]
        else:
            result = cls._registry[name] = cls(Session._datastores.get(name))
        return result
    
    def save(self, obj):
        from .mapped_class import MappedClass
        assert isinstance(obj, MappedClass)
        self.uow.save(obj)
        self.imap.save(obj)

    def expunge(self, obj):
        self.uow.expunge(obj)
        self.imap.expunge(obj)

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
    def insert_now(self, obj, st):
        mapper(obj).insert(self, obj, st)
        self.imap.save(obj)

    @with_hooks('update')
    def update_now(self, obj, st):
        mapper(obj).update(self, obj, st)
        self.imap.save(obj)

    @with_hooks('delete')
    def delete_now(self, obj, st):
        mapper(obj).delete(self, obj, st)
        
    def clear(self):
        self.uow.clear()
        self.imap.clear()

    def get(self, cls, idvalue):
        result = self.imap.get(cls, idvalue)
        if result is None:
            result = self.find(cls, dict(_id=idvalue)).first()
        return result

    def find(self, cls, *args, **kwargs):
        if self.autoflush:
            self.flush()
        m = mapper(cls)
        ming_cursor = self.impl.find(m.doc_cls, *args, **kwargs)
        return ORMCursor(self, cls, ming_cursor)

    def find_and_modify(self, cls, *args, **kwargs):
        if self.autoflush:
            self.flush()
        m = mapper(cls)
        obj = self.impl.find_and_modify(m.doc_cls, *args, **kwargs)
        cursor = ORMCursor(self, cls, iter([ obj ]), refresh=True)
        result = cursor.first()
        state(result).status = ObjectState.clean
        return result

    @with_hooks('remove')
    def remove(self, cls, *args, **kwargs):
        m = mapper(cls)
        self.impl.remove(m.doc_cls, *args, **kwargs)

    def update(self, cls, spec, fields, upsert=False):
        m = mapper(cls)
        self.impl.update_partial(m.doc_cls, spec, fields, upsert)

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

class ThreadLocalORMSession(ThreadLocalProxy):
    _session_registry = ThreadLocalProxy(dict)

    def __init__(self, *args, **kwargs):
        ThreadLocalProxy.__init__(self, ORMSession, *args, **kwargs)

    def _get(self):
        result = super(ThreadLocalORMSession, self)._get()
        self._session_registry.__setitem__(id(self), self)
        return result

    def close(self):
        self.clear()
        super(ThreadLocalORMSession, self).close()

    @classmethod
    def flush_all(cls):
        for sess in cls._session_registry.itervalues():
            sess.flush()

    @classmethod
    def close_all(cls):
        for sess in cls._session_registry.itervalues():
            sess.close()

class ORMCursor(object):

    def __init__(self, session, cls, ming_cursor, refresh=False):
        self.session = session
        self.cls = cls
        self.mapper = mapper(cls)
        self.ming_cursor = ming_cursor
        self.refresh = refresh

    def __iter__(self):
        return self

    def __len__(self):
        return self.count()

    def count(self):
        return self.ming_cursor.count()

    def next(self):
        doc = self.ming_cursor.next()
        obj = self.session.imap.get(self.cls, doc['_id'])
        if obj is None:
            obj = self.mapper.create(doc)
            state(obj).status = ObjectState.clean
        elif self.refresh:
            # Refresh object
            state(obj).document.update(doc)
            state(obj).status = ObjectState.clean
        else:
            # Never refresh objects from the DB unless explicitly requested
            pass
        other_session = session(obj)
        if other_session != self:
            other_session.expunge(obj)
            self.session.save(obj)
        return obj

    def limit(self, limit):
        return ORMCursor(self.session, self.cls,
                         self.ming_cursor.limit(limit))

    def skip(self, skip):
        return ORMCursor(self.session, self.cls,
                         self.ming_cursor.skip(skip))

    def hint(self, index_or_name):
        return ORMCursor(self.session, self.cls,
                         self.ming_cursor.hint(index_or_name))

    def sort(self, *args, **kwargs):
        return ORMCursor(self.session, self.cls,
                         self.ming_cursor.sort(*args, **kwargs))

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

    
