import logging

from copy import copy
from threading import Lock

import pymongo
import six
from pymongo.errors import ConnectionFailure

from . import schema as S
from .base import Object
from .utils import fixup_index, LazyProperty
from .exc import MongoGone

log = logging.getLogger(__name__)

class Field(object):
    """Represents a Field in a MongoDB Document

    It accepts one or two positional arguments and any number of
    keyword arguments.

    If only one positional argument is provided that is considered to
    be the schema of the Field which is one of the types available in
    :mod:`ming.schema` module.

    If two positional arguments are provided the first is considered being
    the name of the field inside the MongoDB Document and the second is
    still the schema type.

    The ``index``, ``unique`` and ``sparse`` keyword arguments are also
    consumed by ``Field`` to automatically create an index for the field.

    Additional arguments are just forwarded to Schema constructor if
    the provided schema argument is not already an instance. So see
    the classes declared in :mod:`ming.schema` for details on the
    other available keyword arguments.
    """
    def __init__(self, *args, **kwargs):
        if len(args) == 1:
            if isinstance(args[0], str):
                raise ValueError('When called with only one argument Field() '
                                 'parameter should be the field type')

            self.name = None
            self.type = args[0]
        elif len(args) == 2:
            self.name = args[0]
            self.type = args[1]
        else:
            raise TypeError('Field() takes 1 or 2 argments, not %s' % len(args))
        self.index = kwargs.pop('index', False)
        self.unique = kwargs.pop('unique', False)
        self.sparse = kwargs.pop('sparse', False)
        self.schema = S.SchemaItem.make(self.type, **kwargs)

    def __repr__(self):
        if self.unique:
            flags = 'index unique'
            if self.sparse:
                flags += ' sparse'
        elif self.sparse:
            flags = 'index sparse'
        elif self.index:
            flags = 'index'
        else:
            flags = ''
        return '<Field %s(%s)%s>' % (self.name, self.schema, flags)

class Index(object):
    """Represents a MongoDB Index of a field.

    """
    def __init__(self, *fields, **kwargs):
        direction = kwargs.pop('direction', pymongo.ASCENDING)
        unique = kwargs.pop('unique', False)
        sparse = kwargs.pop('sparse', False)

        # Background indexing is currently enforced by Ming
        kwargs.pop('background', None)

        self.fields = kwargs.pop('fields', fields)
        self.index_spec = fixup_index(self.fields, direction)
        self.name = kwargs.get('name', 'idx_' + '_'.join('%s_%s' % t for t in self.index_spec))

        self.index_options = kwargs.copy()
        self.index_options['unique'] = unique
        self.index_options['sparse'] = sparse

    def __repr__(self):
        specs = [ '%s:%s' % t  for t in self.index_spec ]
        return '<Index (%s) options=%s>' % (','.join(specs), self.index_options)

    def __getattr__(self, option_name):
        try:
            return self.index_options[option_name]
        except KeyError:
            raise AttributeError('Index %s has no option %s' % (self.index_spec, option_name))

    def __eq__(self, o):
        return self.index_spec == o.index_spec and self.index_options == o.index_options

def collection(*args, **kwargs):
    fields, indexes, collection_name, bases, session = _process_collection_args(
        args, kwargs)
    dct = dict((f.name, _FieldDescriptor(f)) for f in fields)
    if 'polymorphic_identity' in kwargs:
        clsname = 'Document<%s:%s>' % (
            collection_name, kwargs['polymorphic_identity'])
    else:
        clsname = 'Document<%s>' % collection_name
    cls = type(clsname, bases, dct)
    m = _ClassManager(
        cls, collection_name, session, fields, indexes, **kwargs)
    cls.m = _ManagerDescriptor(m)
    return cls

def _process_collection_args(args, kwargs):
    if len(args) < 1:
        raise TypeError('collection() takes at least one argument')
    if isinstance(args[0], six.string_types + (type(None),)):
        if len(args) < 2:
            raise TypeError('collection(name, session) takes at least two arguments')
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
        raise TypeError(
            'collection(name, session, ...) and collection(base_class) are the'
            ' only valid signatures')
    collection_name =  kwargs.pop(
        'collection_name', collection_name)
    session =  kwargs.pop(
        'session', session)
    field_index = {}
    indexes = []
    for b in reversed(bases):
        if not hasattr(b, 'm'): continue
        field_index.update(b.m.field_index)
        indexes += b.m.indexes
    for a in args:
        if isinstance(a, Field):
            if a.name is None:
                raise ValueError("Field %s is missing a valid name" % (a,))

            field_index[a.name] = a
            if a.unique:
                if a.sparse:
                    indexes.append(Index(a.name, unique=True, sparse=True))
                else:
                    indexes.append(Index(a.name, unique=True, sparse=False))
            elif a.sparse:
                indexes.append(Index(a.name, unique=False, sparse=True))
            elif a.index:
                indexes.append(Index(a.name))
        elif isinstance(a, Index):
            indexes.append(a)
        else:
            raise TypeError("don't know what to do with %r" % (a,))

    return field_index.values(), indexes, collection_name, bases, session

class _CurriedProxyClass(type):

    def __new__(meta, name, bases, dct):
        methods = dct['_proxy_methods']
        proxy_of = dct['_proxy_on']
        proxy_args = dct['_proxy_args']

        def _proxy(name):
            def inner(self, *args, **kwargs):
                target = getattr(self, proxy_of)
                method = getattr(target, name)
                curried_args = [ getattr(self, argname) for argname in proxy_args ]
                all_args = tuple(curried_args) + args
                return method(*all_args, **kwargs)
            inner.__name__ = name
            return inner
        for meth in methods:
            dct[meth] = _proxy(meth)
        cls = type.__new__(meta, name, bases, dct)
        return cls

@six.add_metaclass(_CurriedProxyClass)
class _InstanceManager(object):
    _proxy_methods = (
        'save', 'insert', 'upsert', 'delete', 'set', 'increase_field')
    _proxy_on='session'
    _proxy_args=('inst',)

    def __init__(self, mgr, inst):
        self.classmanager = mgr
        self.session = mgr.session
        self.inst = inst
        self.schema = mgr.schema
        self.collection_name = mgr.collection_name
        self.before_save = mgr.before_save
        return

        def _proxy(name):
            def inner(*args, **kwargs):
                method = getattr(self.session, name)
                return method(self.inst, *args, **kwargs)
            inner.__name__ = name
            return inner

        for method_name in self._proxy_methods:
            setattr(self, method_name, _proxy(method_name))

@six.add_metaclass(_CurriedProxyClass)
class _ClassManager(object):
    _proxy_on='session'
    _proxy_args=('cls',)
    _proxy_methods = (
        'get', 'find', 'find_by', 'remove', 'count', 'update_partial',
        'group', 'ensure_index', 'ensure_indexes', 'index_information',  'drop_indexes',
        'find_and_modify', 'aggregate', 'distinct', 'map_reduce', 'inline_map_reduce',
        )
    InstanceManagerClass=_InstanceManager

    def __init__(
        self, cls, collection_name, session, fields, indexes,
        polymorphic_on=None, polymorphic_identity=None,
        polymorphic_registry=None,
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
        return

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
    def collection(self):
        return self.session.db[self.collection_name]

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


class _ManagerDescriptor(object):

    def __init__(self, manager):
        self.manager = manager
        self.indexes_ensured = False
        self._lock = Lock()

    @property
    def engine(self):
        try:
            return self.manager.session.bind.bind
        except AttributeError as e:
            return None # engine not yet configured

    def _ensure_indexes(self):
        session = self.manager.session
        if session is None: return
        if session.bind is None: return
        collection = self.manager.collection
        try:
            with self._lock:
                for idx in self.manager.indexes:
                    collection.ensure_index(idx.index_spec, background=True,
                                            **idx.index_options)
        except (MongoGone, ConnectionFailure) as e:
            if e.args[0] == 'not master':
                log.info('Could not run ensure_indexes because the connection is not to a master.  This is expected when connecting to a slave')
            else:
                # raise all other connection issues
                raise
        self.indexes_ensured = True

    def _auto_ensure_indexes(self):
        if not self.indexes_ensured and self.engine and self.engine._auto_ensure_indexes:
            self._ensure_indexes()

    def __get__(self, inst, cls=None):
        self._auto_ensure_indexes()
        if inst is None:
            return self.manager
        else:
            return self.manager.InstanceManagerClass(self.manager, inst)


class _FieldDescriptor(object):

    def __init__(self, field):
        self.field = field
        self.name = field.name

    def __get__(self, inst, cls=None):
        if inst is None: return self
        try:
            return inst[self.name]
        except KeyError:
            raise AttributeError(self.name)

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
    def make(cls, data, **kwargs):
        'Kind of a virtual constructor'
        return cls.m.make(data, **kwargs)



