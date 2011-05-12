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

class Index(object):

    def __init__(self, *fields, **kwargs):
        self.fields = fields
        self.direction = kwargs.pop('direction', pymongo.ASCENDING)
        self.unique = kwargs.pop('unique', False)
        self.index_spec = fixup_index(fields, self.direction)
        if kwargs: raise TypeError, 'unknown kwargs: %r' % kwargs

    def __repr__(self):
        return '<Index %r unique=%s>' % (
            self.index_spec, self.unique)

    def __eq__(self, o):
        return self.index_spec == o.index_spec and self.unique == o.unique

def collection(*args, **kwargs):
    if len(args) < 1:
        raise TypeError, 'collection() takes at least one argument'
    if isinstance(args[0], basestring):
        if len(args) < 2:
            raise TypeError, 'collection(name, session) takes at least two arguments'
        name = args[0]
        if isinstance(args[1], type) and issubclass(args[1], _Document):
            bases = (args[1],)
            session = args[1].m.session
        else:
            session = args[1]
            bases = (_Document,)
        args = args[2:]
    elif isinstance(args[0], type) and issubclass(args[0], _Document):
        name = args[0].m.name
        session = args[0].m.session
        bases = (args[0],)
        args = args[1:]
    else:
        raise TypeError, (
            'collection(name, metadata, ...) and collection(base_class) are the'
            ' only valid signatures')
    return _prepare_collection(
        type, 'Document<%s>' % name, name,
        session, bases,
        *args, **kwargs)

def _prepare_collection(
    metaclass, classname, collection_name, session, bases,
    *args, **kwargs):
    dct = {}
    fields = []
    indexes = []
    for a in args:
        if isinstance(a, Field):
            dct[a.name] = _FieldDescriptor(a)
            fields.append(a)
            if a.unique:
                indexes.append(Index(a.name, unique=True))
            elif a.index:
                indexes.append(Index(a.name))
        elif isinstance(a, Index):
            indexes.append(a)
        else:
            raise TypeError, "don't know what to do with %r" % a
            
    cls = metaclass(classname, bases, dct)
    m = ClassManager(
        cls, collection_name, session, fields, indexes, **kwargs)
    cls.m = _ManagerDescriptor(m)
    return cls

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

class ClassManager(object):
    _proxy_methods = (
        'get', 'find', 'find_by', 'remove', 'count', 'update_partial',
        'group', 'ensure_index', 'index_information',  'drop_indexes' )

    def __init__(
        self, cls, name, session, fields, indexes, 
        polymorphic_on=None, polymorphic_identity=None,
        version_of=None, migrate=None,
        before_save=None):
        self.cls = cls
        self.name = name
        self.session = session
        self.fields = fields
        self._indexes = indexes

        if polymorphic_on:
            self._polymorphic_on = polymorphic_on
            self._polymorphic_registry = {}
        else:
            self._polymorphic_on = self._polymorphic_registry = None
        self.polymorphic_identity = polymorphic_identity
        self.version_of = version_of
        self.base = self._get_base()
        self.schema = self._get_schema(
            polymorphic_on, polymorphic_identity,
            version_of, migrate)
        self._before_save = before_save

        def _proxy(name):
            def inner(*args, **kwargs):
                method = getattr(self.session, name)
                return method(self.cls, *args, **kwargs)
            inner.__name__ = name
            return inner

        for method_name in self._proxy_methods:
            setattr(self, method_name, _proxy(method_name))

    def _get_base(self):
        possible_bases = []
        for base in self.cls.__bases__:
            try:
                possible_bases.append(base.m)
            except AttributeError:
                pass
        if len(possible_bases) == 1:
            return possible_bases[0]
        elif len(possible_bases) > 1:
            raise TypeError, (
                'Multiple inheritance of document models is unsupported')
        else: return None

    def _get_schema(
        self, polymorphic_on, polymorphic_identity,
        version_of, migrate):
        schema = S.Document()
        for base in self.cls.__bases__:
            try:
                schema.extend(S.SchemaItem.make(base.m.schema))
            except AttributeError:
                pass
        schema.fields.update(
            (fld.name, fld.schema)
            for fld in self.fields)
        if not schema.fields:
            return None
        schema.managed_class = self.cls
        if polymorphic_on:
            self.polymorphic_registry = {}
            self.polymorphic_on = polymorphic_on
            schema.set_polymorphic(
                self.polymorphic_on, self.polymorphic_registry, polymorphic_identity)
        elif self.base and self.base.polymorphic_registry:
            self.polymorphic_registry = self.base.polymorphic_registry
            self.polymorphic_on = self.base.polymorphic_on
            schema.set_polymorphic(
                self.polymorphic_on, self.polymorphic_registry, polymorphic_identity)
        else:
            self.polymorphic_on = self.polymorphic_registry  = None
        if version_of:
            return S.Migrate(
                version_of.m.schema, schema, migrate)
        return schema

    @LazyProperty
    def before_save(self):
        if self._before_save: return self._before_save
        return self.base and self.base.before_save
    
    @property
    def indexes(self):
        for base in self.cls.__bases__:
            try:
                for idx in base.m.indexes:
                    yield idx
            except AttributeError:
                pass
        for idx in self._indexes: yield idx

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

    def ensure_indexes(self):
        """Ensures all the indexes defined for this manager are created"""
        for idx in self.indexes:
            self.session.ensure_index(
                self.cls, idx.index_spec, unique=idx.unique)

    def make(self, data, allow_extra=False, strip_extra=True):
        if self.schema:
            return self.schema.validate(
                data, allow_extra=allow_extra, strip_extra=strip_extra)
        else:
            return self.cls(data)
        
class InstanceManager(object):
    _proxy_methods = (
        'save', 'insert', 'upsert', 'delete', 'set', 'increase_field')

    def __init__(self, mgr, inst):
        self.session = mgr.session
        self.inst = inst
        self.schema = mgr.schema
        self.name = mgr.name
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
                self.manager.ensure_indexes()
                self.initialized = True
            except:
                pass
        if inst is None:
            return self.manager
        else:
            return InstanceManager(self.manager, inst)

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
        
