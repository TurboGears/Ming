from ming.session import Session
from ming.utils import encode_keys
from .base import mapper, state, ObjectState
from .unit_of_work import UnitOfWork
from .identity_map import IdentityMap

class ORMSession(object):

    _registry = {}

    def __init__(self, bind=None):
        self.impl = Session(bind)
        self.uow = UnitOfWork(self)
        self.imap = IdentityMap()

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

    def flush(self):
        self.uow.flush()

    def insert_now(self, obj, st):
        mapper(obj).insert(self, obj, st)

    def update_now(self, obj, st):
        mapper(obj).update(self, obj, st)

    def delete_now(self, obj, st):
        mapper(obj).update(self, obj, st)
        
    def clear(self):
        self.uow.clear()
        self.imap.clear()

    def get(self, cls, idvalue):
        result = self.imap.get(cls, idvalue)
        if result is None:
            result = self.find(cls, dict(_id=idvalue)).first()
        return result

    def find(self, cls, *args, **kwargs):
        m = mapper(cls)
        ming_cursor = self.impl.find(m.doc_cls, *args, **kwargs)
        return ORMCursor(self, cls, ming_cursor)

    def __repr__(self):
        l = ['<session>']
        for line in repr(self.uow).split('\n'):
            l.append('  ' + line)
        for line in repr(self.imap).split('\n'):
            l.append('  ' + line)
        return '\n'.join(l)

class ORMCursor(object):

    def __init__(self, session, cls, ming_cursor):
        self.session = session
        self.cls = cls
        self.ming_cursor = ming_cursor

    def __getattr__(self, name):
        return getattr(self.ming_cursor, name)

    def __iter__(self):
        return self

    def __len__(self):
        return self.count()

    def next(self):
        doc = self.cursor.next()
        obj = self.session.imap.get(self.cls, doc['_id'])
        if obj is None:
            obj = self.cls(**encode_keys(doc))
        else:
            state(obj).document.update(doc)
        state(obj).status = ObjectState.clean
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
