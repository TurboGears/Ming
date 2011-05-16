from copy import copy

import pymongo

from . import schema as S
from .base import Object
from .utils import fixup_index, LazyProperty

class Field(object):
    '''Represents a mongo field.'''

    def __init__(self, *args, **kwargs):
        if len(args) == 1:
            self.name = None
            self.type = args[0]
        elif len(args) == 2:
            self.name = args[0]
            self.type = args[1]
        else:
            raise TypeError, 'Field() takes 1 or 2 argments, not %s' % len(args)
        self.index = kwargs.pop('index', False)
        self.unique = kwargs.pop('unique', False)
        self.schema = S.SchemaItem.make(self.type, **kwargs)

    def __repr__(self):
        if self.unique:
            flags = 'index unique'
        elif self.index:
            flags = 'index'
        else:
            flags = ''
        return '<Field %s(%s)%s>' % (self.name, self.schema, flags)

class Index(object):

    def __init__(self, *fields, **kwargs):
        self.fields = fields
        self.direction = kwargs.pop('direction', pymongo.ASCENDING)
        self.unique = kwargs.pop('unique', False)
        self.index_spec = fixup_index(fields, self.direction)
        self.name = 'idx_' + '_'.join('%s_%d' % t for t in self.index_spec)
        if kwargs: raise TypeError, 'unknown kwargs: %r' % kwargs

    def __repr__(self):
        specs = [ '%s:%s' % t  for t in self.index_spec ]
        return '<Index (%s) unique=%s>' % (
            ','.join(specs), self.unique)

    def __eq__(self, o):
        return self.index_spec == o.index_spec and self.unique == o.unique

def collection(*args, **kwargs):
    if len(args) < 1:
        raise TypeError, 'collection() takes at least one argument'
    if isinstance(args[0], (basestring, type(None))):
        if len(args) < 2:
            raise TypeError, 'collection(name, session) takes at least two arguments'
        collection_name = args[0]
        session = args[1]
        bases = (_Document,)
        args = args[2:]
    elif isinstance(args[0], type) and issubclass(args[0], _Document):
        bases = (args[0],)
        args = args[1:]
        collection_name = bases[-1].m.collection_name
        session = bases[-1].m.session
    elif hasattr(args[0], '__iter__'):
        bases = tuple(args[0])
        args = args[1:]
        collection_name = bases[-1].m.collection_name
        session = bases[-1].m.session        
    else:
        raise TypeError, (
            'collection(name, session, ...) and collection(base_class) are the'
            ' only valid signatures')
    collection_name =  kwargs.pop(
        'collection_name', collection_name)
    session =  kwargs.pop(
        'session', session)
    fields, indexes = _process_collection_args(bases, *args)
    dct = dict((f.name, _FieldDescriptor(f)) for f in fields)
    cls = type('Document<%s>' % collection_name, bases, dct)
    m = _ClassManager(
        cls, collection_name, session, fields, indexes, **kwargs)
    cls.m = _ManagerDescriptor(m)
    return cls

def _process_collection_args(bases, *args):
    field_index = {}
    indexes = []
    for b in reversed(bases):
        if not hasattr(b, 'm'): continue
        field_index.update(b.m.field_index)
        indexes += b.m.indexes
    for a in args:
        if isinstance(a, Field):
            field_index[a.name] = a
            if a.unique:
                indexes.append(Index(a.name, unique=True))
            elif a.index:
                indexes.append(Index(a.name))
        elif isinstance(a, Index):
            indexes.append(a)
        else:
            raise TypeError, "don't know what to do with %r" % a

    return field_index.values(), indexes

class _ClassManager(object):
    _proxy_methods = (
        'get', 'find', 'find_by', 'remove', 'count', 'update_partial',
        'group', 'ensure_index', 'ensure_indexes', 'index_information',  'drop_indexes' )

    def __init__(
        self, cls, collection_name, session, fields, indexes, 
        polymorphic_on=None, polymorphic_identity=None, polymorphic_registry=None,
        version_of=None, migrate=None,
        before_save=None):
        self.cls = cls
        self.collection_name = collection_name
        self.session = session
        self.field_index = dict((f.name, f) for f in fields)
        self.indexes = indexes

        if polymorphic_on and polymorphic_registry is None:
            self._polymorphic_registry = {}
        else:
            self._polymorphic_registry = polymorphic_registry
        self._polymorphic_on = polymorphic_on
        self.polymorphic_identity = polymorphic_identity
        self._version_of = version_of
        self._migrate = migrate
        self.bases = self._get_bases()
        self.schema = self._get_schema()
        self._before_save = before_save

        def _proxy(name):
            def inner(*args, **kwargs):
                method = getattr(self.session, name)
                return method(self.cls, *args, **kwargs)
            inner.__name__ = name
            return inner

        for method_name in self._proxy_methods:
            setattr(self, method_name, _proxy(method_name))

    def _get_bases(self):
        return tuple(
            b.m for b in self.cls.__bases__
            if hasattr(b, 'm'))

    @property
    def fields(self):
        return self.field_index.values()

    @property
    def polymorphic_registry(self):
        if self._polymorphic_registry is not None: return self._polymorphic_registry
        for b in self.bases:
            if b.polymorphic_registry: return b.polymorphic_registry
        return None

    @property
    def polymorphic_on(self):
        if self._polymorphic_on is not None: return self._polymorphic_on
        for b in self.bases:
            if b.polymorphic_on: return b.polymorphic_on
        return None

    @LazyProperty
    def before_save(self):
        if self._before_save: return self._before_save
        for b in self.bases:
            if b.before_save: return b.before_save
        return None

    def _get_schema(self):
        schema = S.Document()
        for b in self.bases:
            try:
                schema.extend(S.SchemaItem.make(b.schema))
            except AttributeError:
                pass
        schema.fields.update(
            (fld.name, fld.schema)
            for fld in self.fields)
        if not schema.fields:
            return None
        schema.managed_class = self.cls
        if self.polymorphic_registry is not None:
            schema.set_polymorphic(self.polymorphic_on, self.polymorphic_registry, self.polymorphic_identity)
        if self._version_of:
            return S.Migrate(
                self._version_of.m.schema, schema, self._migrate)
        return schema

    def add_index(self, idx):
        self._indexes.append(idx)

    def with_session(self, session):
        '''Return a Manager with an alternate session'''
        result = copy(self)
        result.session = session
        return result

    def migrate(self):
        '''Load each doc in the collection and immediately save it'''
        for doc in self.find(): doc.m.save()

    def make(self, data, allow_extra=False, strip_extra=True):
        if self.schema:
            return self.schema.validate(
                data, allow_extra=allow_extra, strip_extra=strip_extra)
        else:
            return self.cls(data)
        
class _InstanceManager(object):
    _proxy_methods = (
        'save', 'insert', 'upsert', 'delete', 'set', 'increase_field')

    def __init__(self, mgr, inst):
        self.classmanager = mgr
        self.session = mgr.session
        self.inst = inst
        self.schema = mgr.schema
        self.collection_name = mgr.collection_name
        self.before_save = mgr.before_save

        def _proxy(name):
            def inner(*args, **kwargs):
                method = getattr(self.session, name)
                return method(self.inst, *args, **kwargs)
            inner.__name__ = name
            return inner

        for method_name in self._proxy_methods:
            setattr(self, method_name, _proxy(method_name))

class _ManagerDescriptor(object):

    def __init__(self, manager):
        self.manager = manager
        self.initialized = False

    def __get__(self, inst, cls=None):
        if not self.initialized and self.manager.session:
            try:
                self.initialized = True
                self.manager.ensure_indexes()
            except:
                pass
        if inst is None:
            return self.manager
        else:
            return _InstanceManager(self.manager, inst)

class _FieldDescriptor(object):

    def __init__(self, field):
        self.field = field
        self.name = field.name

    def __get__(self, inst, cls=None):
        if inst is None: return self
        try:
            return inst[self.name]
        except KeyError:
            raise AttributeError, self.name

    def __set__(self, inst, value):
        inst[self.name] = value

    def __delete__(self, inst):
        del inst[self.name]
        
class _Document(Object):

    def __init__(self, data=None, skip_from_bson=False):
        if data is None:
            data = {}
        elif not skip_from_bson:
            data = Object.from_bson(data)
        dict.update(self, data)

    @classmethod
    def make(cls, data, allow_extra=False, strip_extra=True):
        'Kind of a virtual constructor'
        return cls.m.make(data, allow_extra=allow_extra, strip_extra=strip_extra)

