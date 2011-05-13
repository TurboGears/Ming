from ming.metadata import Field
from ming.utils import LazyProperty
from ming import schema as S
from .base import session, state
from .icollection import InstrumentedList, instrument

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

    def compile(self, mapper):
        pass

    def __repr__(self):
        return '<%s %s>' % (
            self.__class__.__name__, self.name)

class FieldProperty(ORMProperty):

    def __init__(self, field_type, *args, **kwargs):
        ORMProperty.__init__(self)
        if isinstance(field_type, Field):
            self.field = field_type
            if args or kwargs:
                raise TypeError, 'Unexpected args: %r, %r' % (args, kwargs)
        else:
            self.field = Field(field_type, *args, **kwargs)
        self.name = self.field.name
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
        value = st.document.get(self.name, S.Missing)
        if value is S.Missing:
            value = st.raw.get(self.name, S.Missing)
            value = self.field.schema.validate(value)
            if value is S.Missing: raise AttributeError, self.name
            value = instrument(value, st.tracker)
            last_status = st.status
            st.document[self.name] = value
            st.status = last_status
        return value

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
        st.document[self.name] = value

class ForeignIdProperty(ORMProperty):

    def __init__(self, related, *args, **kwargs):
        ORMProperty.__init__(self)
        self.args = args
        self.kwargs = kwargs
        if isinstance(related, type):
            self.related = related
        else:
            self._related_classname = related

    @LazyProperty
    def related(self):
        from .mapper import mapper
        return mapper(self._related_classname).mapped_class

    @LazyProperty
    def field(self):
        return Field(self.name, self.related._id.field.type)

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
        if isinstance(related, type):
            self.related = related
        else:
            self._related_classname = related

    @LazyProperty
    def related(self):
        from .mapper import mapper
        return mapper(self._related_classname).mapped_class

    @LazyProperty
    def join(self):
        from .mapper import mapper
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
        self.cls = cls
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

