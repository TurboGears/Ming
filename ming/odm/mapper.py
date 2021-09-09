import six
import warnings
from copy import copy

from ming.base import Object, NoDefault
from ming.utils import wordwrap

from .base import ObjectState, state, _with_hooks
from .property import FieldProperty


def mapper(cls, collection=None, session=None, **kwargs):
    """Gets or creates the mapper for the given ``cls`` :class:`.MappedClass`"""
    if collection is None and session is None:
        if isinstance(cls, type):
            return Mapper.by_class(cls)
        elif isinstance(cls, six.string_types):
            return Mapper.by_classname(cls)
        else:
            return Mapper._mapper_by_class[cls.__class__]
    return Mapper(cls, collection, session, **kwargs)


class Mapper(object):
    """Keeps track of :class:`.MappedClass` subclasses.

     The Mapper is in charge of linking together the Ming Schema Validation layer,
     the Session and a MappedClass. It also compiles the Schema Validation for
     Mapped Classes if they don't already have one.

     Mapper also instruments mapped classes by adding the additional ``.query`` property
     and behaviours.

     You usually won't be using the Mapper directly apart from :meth:`.compile_all`
     and :meth:`.ensure_all_indexes` methods.
     """
    _mapper_by_collection = {}
    _mapper_by_class = {}
    _mapper_by_classname = {}
    _all_mappers = []
    _compiled = False

    def __init__(self, mapped_class, collection, session, **kwargs):
        self.mapped_class = mapped_class
        self.collection = collection
        self.session = session
        self.properties = []
        self.property_index = {}
        classname = '%s.%s' % (mapped_class.__module__, mapped_class.__name__)
        self._mapper_by_collection[collection] = self
        self._mapper_by_class[mapped_class] = self
        self._mapper_by_classname[classname] = self
        self._all_mappers.append(self)
        properties = kwargs.pop('properties', {})
        include_properties = kwargs.pop('include_properties', None)
        exclude_properties = kwargs.pop('exclude_properties', [])
        extensions = kwargs.pop('extensions', [])
        self.extensions = [e(self) for e in extensions]
        self.options = Object(kwargs.pop('options', dict(refresh=False, instrument=True)))
        if kwargs:
            raise TypeError('Unknown kwd args: %r' % kwargs)
        self._instrument_class(properties, include_properties, exclude_properties)

    def __repr__(self):
        return '<Mapper %s:%s>' % (
            self.mapped_class.__name__, self.collection.m.collection_name)

    @_with_hooks('insert')
    def insert(self, obj, state, session, **kwargs):
        doc = self.collection(state.document, skip_from_bson=True)
        ret = session.impl.insert(doc, validate=False)
        state.status = state.clean
        return ret

    @_with_hooks('update')
    def update(self, obj, state, session, **kwargs):
        fields = state.options.get('fields', None)
        if fields is None:
            fields = ()

        doc = self.collection(state.document, skip_from_bson=True)
        ret = session.impl.save(doc, *fields, validate=False)
        state.status = state.clean
        return ret

    @_with_hooks('delete')
    def delete(self, obj, state, session, **kwargs):
        doc = self.collection(state.document, skip_from_bson=True)
        return session.impl.delete(doc)

    @_with_hooks('remove')
    def remove(self, session, *args, **kwargs):
        return session.impl.remove(self.collection, *args, **kwargs)

    def create(self, doc, options, remake=True):
        if remake is True or type(doc) is not self.collection:
            # When querying, the ODMCursor already receives data from ming.Cursor
            # which already constructed and validated the documents as collections.
            # So it will leverage the remake=False option to avoid re-validating
            doc = self.collection.make(doc)
        mapper = self.by_collection(type(doc))
        return mapper._from_doc(doc, Object(self.options, **options), validate=False)

    def base_mappers(self):
        for base in self.mapped_class.__bases__:
            if base in self._mapper_by_class:
                yield self._mapper_by_class[base]

    def all_properties(self):
        seen = set()
        for p in self.properties:
            if p.name in seen: continue
            seen.add(p.name)
            yield p
        for base in self.base_mappers():
            for p in base.all_properties():
                if p.name in seen: continue
                seen.add(p.name)
                yield p

    @classmethod
    def by_collection(cls, collection_class):
        return cls._mapper_by_collection[collection_class]

    @classmethod
    def by_class(cls, mapped_class):
        return cls._mapper_by_class[mapped_class]

    @classmethod
    def by_classname(cls, name):
        try:
            return cls._mapper_by_classname[name]
        except KeyError:
            for n, mapped_class in six.iteritems(cls._mapper_by_classname):
                if n.endswith('.' + name): return mapped_class
            raise

    @classmethod
    def all_mappers(cls):
        return cls._all_mappers

    @classmethod
    def compile_all(cls):
        """Compiles Schema Validation details for all :class:`.MappedClass` subclasses"""
        for m in cls.all_mappers():
            m.compile()

    @classmethod
    def clear_all(cls):
        for m in cls.all_mappers():
            m._compiled = False
        cls._all_mappers = []
        cls._mapper_by_classname.clear()
        cls._mapper_by_class.clear()
        cls._mapper_by_collection.clear()
        
    @classmethod
    def ensure_all_indexes(cls):
        """Ensures indexes for each registered :class:`.MappedClass` subclass are created"""
        for m in cls.all_mappers():
            if m.session:
                m.session.ensure_indexes(m.collection)

    def compile(self):
        if self._compiled: return
        self._compiled = True
        for p in self.properties:
            p.compile(self)

    def update_partial(self, session, *args, **kwargs):
        return session.impl.update_partial(self.collection, *args, **kwargs)

    def _from_doc(self, doc, options, validate=True):
        obj = self.mapped_class.__new__(self.mapped_class)
        obj.__ming__ = _ORMDecoration(self, obj, options)
        st = state(obj)

        # Make sure that st.document is never the same as st.original_document
        # otherwise mutating one mutates the other.
        # There is no need to deepcopy as nested mutable objects are already
        # copied by InstrumentedList and InstrumentedObj to instrument them.
        st.original_document = doc

        if validate is False:
            # .create calls this after it already created the document with the
            # right type and so it got already validated. We re-validate it
            # only if explicitly requested.
            st.document = copy(doc)
        elif self.collection.m.schema:
            st.document = self.collection.m.schema.validate(doc)
        else:
            warnings.warn(
                "You're trying to build an ODM object from a collection with "
                "no schema. While this will work, please note that it's not "
                "too useful, since no schema means that there are no fields "
                "mapped from the database document onto the object.",
                UserWarning)
            st.document = copy(doc)
        st.status = st.new
        return obj

    def _instrument_class(self, properties, include_properties, exclude_properties):
        self.mapped_class.query = _QueryDescriptor(self)
        properties = dict(properties)
        # Copy properties from inherited mappers
        for b in self.base_mappers():
            for prop in b.properties:
                properties.setdefault(prop.name, copy(prop))
        # Copy default properties from collection class
        for fld in self.collection.m.fields:
            properties.setdefault(fld.name, FieldProperty(fld))
        # Handle include/exclude_properties
        if include_properties:
            properties = dict((k,properties[k]) for k in include_properties)
        for k in exclude_properties:
            properties.pop(k, None)
        for k,v in six.iteritems(properties):
            v.name = k
            v.mapper = self
            setattr(self.mapped_class, k, v)
            self.properties.append(v)
            self.property_index[k] = v
        _InitDecorator.decorate(self.mapped_class, self)
        inst = self._instrumentation()
        for k in ('__repr__', '__getitem__', '__setitem__', '__contains__',
                  'delete'):
            if getattr(self.mapped_class, k, ()) == getattr(object, k, ()):
                setattr(self.mapped_class, k, getattr(inst, k))

    def _instrumentation(self):
        class _Instrumentation(object):
            def __repr__(self_):
                properties = [
                    '%s=%s' % (prop.name, prop.repr(self_))
                    for prop in mapper(self_).properties
                    if prop.include_in_repr ]
                return wordwrap(
                    '<%s %s>' %
                    (self_.__class__.__name__, ' '.join(properties)),
                    60,
                    indent_subsequent=2)
            def delete(self_):
                return self_.query.delete()
            def __getitem__(self_, name):
                try:
                    return getattr(self_, name)
                except AttributeError:
                    raise KeyError(name)
            def __setitem__(self_, name, value):
                setattr(self_, name, value)
            def __contains__(self_, name):
                return hasattr(self_, name)
        return _Instrumentation


class MapperExtension(object):
    """Base class that should be inherited to handle Mapper events."""

    def __init__(self, mapper):
        self.mapper = mapper

    def before_insert(self, instance, state, sess):
        """Receive an object instance and its current state before that
        instance is inserted into its collection."""
        pass
    def after_insert(self, instance, state, sess):
        """Receive an object instance and its current state after that
        instance is inserted into its collection."""
        pass
    def before_update(self, instance, state, sess):
        """Receive an object instance and its current state before that
        instance is updated."""
        pass
    def after_update(self, instance, state, sess):
        """Receive an object instance and its current state after that
        instance is updated."""
        pass
    def before_delete(self, instance, state, sess):
        """Receive an object instance and its current state before that
        instance is deleted."""
        pass
    def after_delete(self, instance, state, sess):
        """Receive an object instance and its current state after that
        instance is deleted."""
    def before_remove(self, sess, *args, **kwargs):
        """Before a remove query is performed for this class"""
        pass
    def after_remove(self, sess, *args, **kwargs):
        """After a remove query is performed for this class"""
        pass

class _ORMDecoration(object):

    def __init__(self, mapper, instance, options):
        self.mapper = mapper
        self.instance = instance
        self.state = ObjectState(options, None)
        self.state.document = Object()
        self.state.original_document = Object()

class _QueryDescriptor(object):

    def __init__(self, mapper):
        self.classquery = _ClassQuery(mapper)

    def __get__(self, instance, cls=None):
        if instance is None: return self.classquery
        else: return _InstQuery(self.classquery, instance)

class _ClassQuery(object):
    """Provides ``.query`` attribute for :class:`MappedClass`."""
    _proxy_methods = (
        'find', 'find_and_modify', 'remove', 'update', 'group', 'distinct',
        'aggregate', 'map_reduce', 'inline_map_reduce')

    def __init__(self, mapper):
        self.mapper = mapper
        self.session = self.mapper.session
        self.mapped_class = self.mapper.mapped_class

        def _proxy(name):
            def inner(*args, **kwargs):
                method = getattr(self.session, name)
                return method(self.mapped_class, *args, **kwargs)
            inner.__name__ = name
            return inner

        for method_name in self._proxy_methods:
            setattr(self, method_name, _proxy(method_name))

    def get(self, _id=NoDefault, **kwargs):
        """Proxies :meth:`.ODMSession.get` and :meth:`.ODMSession.find`

        In case a single argument named ``_id`` is provided the query
        is performed using :meth:`.ODMSession.get` otherwise is forwarded
        to :meth:`.ODMSession.find`
        """

        if _id is not NoDefault and not kwargs:
            return self.session.get(self.mapped_class, _id)

        if _id is not NoDefault:
            kwargs['_id'] = _id
        return self.find(kwargs).first()

    def find_by(self, **kwargs):
        return self.find(kwargs)

class _InstQuery(object):
    """Provides the ``.delete()`` method on :class:`MappedClass` instances.

    It also provides the ``.query`` property on instances, which acts the same
    as the class query property.
    """
    _proxy_methods = (
        'update_if_not_modified',
    )

    def __init__(self, classquery, instance):
        self.classquery = classquery
        self.mapper = classquery.mapper
        self.session = classquery.session
        self.mapped_class = classquery.mapped_class
        self.instance = instance

        def _proxy(name):
            def inner(*args, **kwargs):
                method = getattr(self.session, name)
                return method(self.instance, *args, **kwargs)
            inner.__name__ = name
            return inner

        for method_name in self._proxy_methods:
            setattr(self, method_name, _proxy(method_name))

        # Some methods are just convenient (and safe)
        self.find = self.classquery.find
        self.get = self.classquery.get

    def delete(self):
        """Mark the object for deletion on next flush"""
        st = state(self.instance)
        st.status = st.deleted

    def update(self, fields, **kwargs):
        return self.classquery.update(
            {'_id': self.instance._id },
            fields)

class _InitDecorator(object):

    def __init__(self, mapper, func):
        self.mapper = mapper
        self.func = func

    @property
    def schema(self):
        return self.mapper.collection.m.schema

    def saving_init(self, self_):
        def __init__(*args, **kwargs):
            self_.__ming__ = _ORMDecoration(self.mapper, self_, Object(self.mapper.options))
            self.func(self_, *args, **kwargs)
            if self.mapper.session:
                self.save(self_)
        return __init__

    def save(self, obj):
        if self.schema:
            obj.__ming__.state.validate(self.schema)
        self.mapper.session.save(obj)

    def nonsaving_init(self, self_):
        def __init__(*args, **kwargs):
            self.func(self_, *args, **kwargs)
        return __init__

    def __get__(self, self_, cls=None):
        if self_ is None: return self
        if self.mapper.mapped_class == cls:
            return self.saving_init(self_)
        else:
            return self.nonsaving_init(self_)

    @classmethod
    def decorate(cls, mapped_class, mapper):
        old_init = mapped_class.__init__
        if isinstance(old_init, cls):
            mapped_class.__init__ = cls(mapper, old_init.func)
        elif old_init == object.__init__:
            mapped_class.__init__ = cls(mapper, _basic_init)
        else:
            mapped_class.__init__ = cls(mapper, old_init)

def _basic_init(self_, **kwargs):
    for k,v in six.iteritems(kwargs):
        setattr(self_, k, v)
