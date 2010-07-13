from ming.base import Field
from ming.utils import LazyProperty
from .base import session, state, mapper, lookup_class
from .icollection import InstrumentedList

class ORMError(Exception): pass
class AmbiguousJoin(ORMError): pass
class NoJoin(ORMError): pass

class ORMProperty(object):
    include_in_repr = True

    def __init__(self):
        self.name = None
        self.cls = None

    def __get__(self, instance, cls=None):
        raise NotImplementedError, '__get__'

    def __set__(self, instance, value):
        raise TypeError, '%r is a read-only property on %r' % (
            self.name, self.cls)

    def insert(self, mapper, session, instance, state):
        pass

    def update(self, mapper, session, instance, state):
        pass

    def delete(self, mapper, session, instance, state):
        pass

    def compile(self):
        pass

    def __repr__(self):
        return '<%s %s>' % (
            self.__class__.__name__, self.name)

class FieldProperty(ORMProperty):

    def __init__(self, field_type, *args, **kwargs):
        ORMProperty.__init__(self)
        self.field_type = field_type
        self.args = args
        self.kwargs = kwargs
        self.field = Field(field_type, *args, **kwargs)
        if self.name == '_id':
            self.__get__ = self._get_id

    def repr(self, doc):
        try:
            return repr(self.__get__(doc))
        except AttributeError:
            return '<Missing>'

    def __get__(self, instance, cls=None):
        if instance is None: return self
        st = state(instance)
        return getattr(st.document, self.name)

    def _get_id(self, instance, cls=None):
        if instance is None: return self
        st = state(instance)
        try:
            return getattr(st.document, self.name)
        except AttributeError:
            session(instance).flush(instance)
            return getattr(st.document, self.name)

    def __set__(self, instance, value):
        st = state(instance)
        st.soil()
        st.document[self.name] = value

class ForeignIdProperty(ORMProperty):

    def __init__(self, related, *args, **kwargs):
        ORMProperty.__init__(self)
        self.args = args
        self.kwargs = kwargs
        self.field_type = None
        self.field = None
        if isinstance(related, type):
            self.related = related
        else:
            self._related_classname = related

    @LazyProperty
    def related(self):
        return lookup_class(self._related_classname)

    def compile(self):
        self.field_type = self.related._id.field_type
        self.field = Field(self.field_type, *self.args, **self.kwargs)
        
    def repr(self, doc):
        try:
            return repr(self.__get__(doc))
        except AttributeError:
            return '<Missing>'

    def __get__(self, instance, cls=None):
        if instance is None: return self
        st = state(instance)
        return getattr(st.document, self.name)

    def __set__(self, instance, value):
        st = state(instance)
        st.soil()
        st.document[self.name] = value

class RelationProperty(ORMProperty): 
    include_in_repr = False

    def __init__(self, related, via=None, fetch=True):
        ORMProperty.__init__(self)
        self.via = via
        self.via_property = None
        self.fetch = fetch
        self.join = None
        if isinstance(related, type):
            self.related = related
        else:
            self._related_classname = related

    @LazyProperty
    def related(self):
        return lookup_class(self._related_classname)

    def compile(self):
        self.join = self._infer_join()
        
    def _infer_join(self):
        cls = self.cls
        rel = self.related
        own_mapper = mapper(self.cls)
        own_props = [ p for p in own_mapper.properties
                      if isinstance(p, ForeignIdProperty)
                      and p.related == rel ]
        if self.via:
            own_props = [ p for p in own_props if p.name == self.via ]
        rel_mapper = mapper(self.related)
        rel_props = [ p for p in rel_mapper.properties
                      if isinstance(p, ForeignIdProperty)
                      and p.related == cls ]
        if self.via:
            rel_props = [ p for p in rel_props if p.name == self.via ]
        if len(own_props) == 1:
            return ManyToOneJoin(cls, rel, own_props[0])
        if len(rel_props) == 1:
            return OneToManyJoin(cls, rel, rel_props[0])
        if own_props or rel_props:
            raise AmbiguousJoin, (
                'Ambiguous join, satisfying keys are %r' %
                [ p.name for p in own_props + rel_props ])
        else:
            raise NoJoin, 'No join keys found between %s and %s' % (
                cls, rel)

    def repr(self, doc):
        try:
            return repr(self.__get__(doc))
        except AttributeError:
            return '<Missing>'

    def __get__(self, instance, cls=None):
        if instance is None: return self
        if self.fetch:
            st = state(instance)
            result = st.extra_state.get(self, ())
            if result is ():
                result = st.extra_state[self] = self.join.load(instance)
            return result
        else:
            return self.join.iterator(instance)

class ManyToOneJoin(object):

    def __init__(self, own_cls, rel_cls, prop):
        self.own_cls, self.rel_cls, self.prop = own_cls, rel_cls, prop

    def load(self, instance):
        key_value = self.prop.__get__(instance, self.own_cls)
        return self.rel_cls.query.get(_id=key_value)

    def iterator(self, instance):
        return [ self.load(instance) ]

class OneToManyJoin(object):

    def __init__(self, own_cls, rel_cls, prop):
        self.own_cls, self.rel_cls, self.prop = own_cls, rel_cls, prop

    def load(self, instance):
        return InstrumentedList(
            OneToManyTracker(state(instance)),
            self.iterator(instance))
        
    def iterator(self, instance):
        key_value = instance._id
        return self.rel_cls.query.find({self.prop.name:key_value})
        return [ self.load(instance) ]

class OneToManyTracker(object):
    __slots__ = ('state',)

    def __init__(self, state):
        self.state = state

    def soil(self, value):
        raise TypeError, 'read-only'
    added_item = soil
    removed_item = soil
    cleared = soil

