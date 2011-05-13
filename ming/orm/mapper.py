from ming.base import Object
from ming.utils import wordwrap
from ming.metadata import Field

from .base import ObjectState, state
from .icollection import InstrumentedObj
from .property import FieldProperty

def mapper(cls, collection=None, session=None, **kwargs):
    if collection is None and session is None:
        return cls.query.mapper
    return Mapper(cls, collection, session, **kwargs)

class Mapper(object):
    _mapper_by_collection = {}

    def __init__(self, cls, collection, session, **kwargs):
        self.cls = cls
        self.collection = collection
        self.session = session
        self.properties = []
        self._mapper_by_collection[collection] = self
        properties = kwargs.pop('properties', {})
        include_properties = kwargs.pop('include_properties', None)
        exclude_properties = kwargs.pop('exclude_properties', [])
        if kwargs:
            raise TypeError, 'Unknown kwd args: %r' % kwargs
        self._instrument_class(properties, include_properties, exclude_properties)

    def __repr__(self):
        return '<Mapper %s:%s>' % (
            self.cls.__name__, self.collection.m.collection_name)

    def insert(self, obj, state, **kwargs):
        doc = self.collection(state.document, skip_from_bson=True)
        doc.m.insert(**kwargs)
        self.session.save(obj)
        state.status = state.clean

    def update(self, obj, state, **kwargs):
        doc = self.collection(state.document, skip_from_bson=True)
        doc.m.save(**kwargs)
        self.session.save(obj)
        state.status = state.clean

    def delete(self, obj, state, **kwargs):
        doc = self.collection(state.document, skip_from_bson=True)
        doc.m.delete(**kwargs)
        self.session.expunge(obj)

    def remove(self, *args, **kwargs):
        self.collection.m.remove(*args, **kwargs)

    def create(self, doc):
        doc = self.collection.make(doc)
        mapper = self.mapper_for(type(doc))
        return mapper._from_doc(doc)

    @classmethod
    def mapper_for(cls, collection_class):
        return cls._mapper_by_collection[collection_class]

    def _from_doc(self, doc):
        obj = self.cls.__new__(self.cls)
        obj.__ming__ = _ORMDecoration(self, obj)
        st = state(obj)
        st.document = doc
        st.status = st.new
        self.session.save(obj)
        return obj
    
    def update_partial(self, *args, **kwargs):
        self.collection.m.update_partial(*args, **kwargs)

    def _instrument_class(self, properties, include_properties, exclude_properties):
        self.cls.query = _QueryDescriptor(self)
        base_properties = dict((fld.name, fld) for fld in self.collection.m.fields)
        properties = dict(base_properties, **properties)
        if include_properties:
            properties = dict((k,properties[k]) for k in include_properties)
        for k in exclude_properties:
            properties.pop(k, None)
        for k,v in properties.iteritems():
            v.name = k
            if isinstance(v, Field):
                v = FieldProperty(v)
            v.mapper = self
            setattr(self.cls, k, v)
            self.properties.append(v)
        orig_init = self.cls.__init__
        if orig_init is object.__init__:
            def orig_init(self_, **kwargs):
                for k,v in kwargs.iteritems():
                    setattr(self_, k, v)
        def __init__(self_, *args, **kwargs):
            self_.__ming__ = _ORMDecoration(self, self_)
            self.session.save(self_)
            orig_init(self_, *args, **kwargs)
        self.cls.__init__ = __init__
        inst = self._instrumentation()
        for k in ('__repr__', '__getitem__', '__setitem__', '__contains__',
                  'delete'):
            if getattr(self.cls, k, ()) == getattr(object, k, ()):
                setattr(self.cls, k, getattr(inst, k).im_func)

    def _instrumentation(self):
        class _Instrumentation(object):
            def __repr__(self_):
                properties = [
                    '%s=%s' % (prop.name, prop.repr(self_))
                    for prop in self.properties
                    if prop.include_in_repr ]
                return wordwrap(
                    '<%s %s>' % 
                    (self.cls.__name__, ' '.join(properties)),
                    60,
                    indent_subsequent=2)
            def delete(self_):
                self_.query.delete()
            def __getitem__(self_, name):
                try:
                    return getattr(self_, name)
                except AttributeError:
                    raise KeyError, name
            def __setitem__(self_, name, value):
                setattr(self_, name, value)
            def __contains__(self_, name):
                return hasattr(self_, name)
        return _Instrumentation


class _ORMDecoration(object):

    def __init__(self, mapper, instance):
        self.mapper = mapper
        self.instance = instance
        self.state = ObjectState()
        tracker = _DocumentTracker(self.state)
        self.state.document = InstrumentedObj(tracker)
        self.state.raw = Object()

class _QueryDescriptor(object):

    def __init__(self, mapper):
        self.classquery = _ClassQuery(mapper)

    def __get__(self, instance, cls=None):
        if instance is None: return self.classquery
        else: return _InstQuery(self.classquery, instance)

class _ClassQuery(object):
    _proxy_methods = (
        'find', 'find_and_modify', 'remove', 'update' )

    def __init__(self, mapper):
        self.mapper = mapper
        self.session = self.mapper.session
        self.cls = self.mapper.cls

        def _proxy(name):
            def inner(*args, **kwargs):
                method = getattr(self.session, name)
                return method(self.cls, *args, **kwargs)
            inner.__name__ = name
            return inner

        for method_name in self._proxy_methods:
            setattr(self, method_name, _proxy(method_name))

    def get(self, **kwargs):
        return self.find(kwargs).first()

    def find_by(self, **kwargs):
        return self.find(kwargs)
    

class _InstQuery(object):
    _proxy_methods = (
        'update_if_not_modified',
        )

    def __init__(self, classquery, instance):
        self.classquery = classquery
        self.mapper = classquery.mapper
        self.session = classquery.session
        self.cls = classquery.cls
        self.instance = instance

        def _proxy(name):
            def inner(*args, **kwargs):
                method = getattr(self.session, name)
                return method(self.instance, *args, **kwargs)
            inner.__name__ = name
            return inner

        for method_name in self._proxy_methods:
            setattr(self, method_name, _proxy(method_name))
        self.find = self.classquery.find

    def delete(self):
        st = state(self.instance)
        st.status = st.deleted

class _DocumentTracker(object):
    __slots__ = ('state',)

    def __init__(self, state):
        self.state = state
        self.state.tracker = self

    def soil(self, value):
        self.state.soil()
    added_item = soil
    removed_item = soil
    cleared = soil

