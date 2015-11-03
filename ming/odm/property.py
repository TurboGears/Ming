from ming.metadata import Field
from ming.utils import LazyProperty
from ming import schema as S
from .base import session, state
from .icollection import instrument, deinstrument
import six

class ORMError(Exception): pass
class AmbiguousJoin(ORMError): pass
class NoJoin(ORMError): pass

class ORMProperty(object):
    include_in_repr = True

    def __init__(self):
        self.name = None

    def __get__(self, instance, cls=None):
        raise NotImplementedError('__get__')

    def __set__(self, instance, value):
        raise TypeError('%r is a read-only property on %r' % (
            self.name, self.mapper))

    def compile(self, mapper):
        pass

    def __repr__(self):
        return '<%s %s>' % (
            self.__class__.__name__, self.name)

class FieldProperty(ORMProperty):
    """Declares property for a value stored in a MongoDB Document.

    The provided arguments are just forwarded to :class:`.Field` class
    which is actually in charge of managing the value and its validation.

    For details on available options in ``FieldProperty`` just rely on
    :class:`.Field` documentation.
    """
    def __init__(self, field_type, *args, **kwargs):
        ORMProperty.__init__(self)
        if isinstance(field_type, Field):
            self.field = field_type
            if args or kwargs:
                raise TypeError('Unexpected args: %r, %r' % (args, kwargs))
        else:
            self.field = Field(field_type, *args, **kwargs)
        if not isinstance(self.field.name, six.string_types + (type(None),)):
            raise TypeError('Field name must be string or None, not %r' % (
                self.field.name))
        self.name = self.field.name
        if self.name == '_id':
            self.__get__ = self._get_id

    @property
    def include_in_repr(self):
        if isinstance(self.field.schema, S.Deprecated): return False
        return True

    def repr(self, doc):
        try:
            return repr(self.__get__(doc))
        except AttributeError:
            return '<Missing>'

    def __get__(self, instance, cls=None):
        if instance is None: return self
        st = state(instance)
        if not st.options.instrument:
            return st.document[self.name]
        try:
            return st.instrumented(self.name)
        except KeyError:
            value = self.field.schema.validate(S.Missing)
            if value is S.Missing:
                raise AttributeError(self.name)
            else:
                st.document[self.name] = value
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
        value = deinstrument(value)
        value = self.field.schema.validate(value)
        if st.document.get(self.name, ()) != value:
            st.set(self.name, value)
            st.soil()

    def __delete__(self, instance, cls=None):
        st = state(instance)
        st.delete(self.name)


class FieldPropertyWithMissingNone(FieldProperty):
    """A class like FieldProperty with one exception.
    If you use if_missing=S.Missing with FieldPropertyWithMissingNone
    when a value in Mongo is not present, instead of Ming throwing
    an AttributeError, Ming will return a value of None for that attribute.
    """
    def __get__(self, instance, cls=None):
        if instance is None: return self
        st = state(instance)
        if not st.options.instrument:
            return st.document[self.name]
        try:
            return st.instrumented(self.name)
        except KeyError:
            value = self.field.schema.validate(S.Missing)
            if value is S.Missing:
                return None
            else:
                st.document[self.name] = value
            return value


class ForeignIdProperty(FieldProperty):
    """Declares a field to store one or more ObjectIds of a related objects.

    The ``related`` argument must be the related entity class or the name
    of the related entity class (to avoid circular dependencies). The
    field itself will actually store and retrieve :class:`bson.ObjectId` instances.

    ``uselist`` argument can be used to tell Ming whenever the object can relate
    to more than one remote object (many-to-many) and so the ids are stored in
    a MongoDB Array.

    Usually a ForeignIdProperty with value ``None`` means that the object is not
    related to any other entity, in case you have entities that might have ``None``
    as their ids you can use ``allow_none`` option to tell Ming that ``None`` is
    a valid foreign id.
    """

    def __init__(self, related, uselist=False, allow_none=False, *args, **kwargs):
        ORMProperty.__init__(self)
        self.args = args
        self.kwargs = kwargs
        self.uselist = uselist
        self.allow_none = allow_none
        if self.allow_none and self.uselist:
            raise AttributeError("allow_none with uselist is not supported")

        if isinstance(related, type):
            self._compiled = True
            self.related = related
        else:
            self._compiled = False
            self._related_classname = related

    @LazyProperty
    def related(self):
        if not self._compiled: raise AttributeError('related')
        from .mapper import mapper
        return mapper(self._related_classname).mapped_class

    @LazyProperty
    def field(self):
        if not self._compiled: raise AttributeError('field')
        if self.uselist:
            self.kwargs.setdefault('if_missing', [])
            return Field(self.name, [self.related._id.field.type], **self.kwargs)
        else:
            self.kwargs.setdefault('if_missing', None)
            return Field(self.name, self.related._id.field.type, **self.kwargs)

    def compile(self, mapper):
        if self._compiled: return
        self._compiled = True
        fld = self.field
        mgr = mapper.collection.m
        mgr.field_index[fld.name] = fld
        mgr.schema = mgr._get_schema()


class RelationProperty(ORMProperty):
    """Provides a way to access OneToMany, ManyToOne and ManyToMany relations.

    The RelationProperty relies on :class:`.ForeignIdProperty` to actually
    understand how the relation is composed and how to retrieve the related data.

    Assigning a new value to the relation will properly update the related objects.
    """
    include_in_repr = False

    def __init__(self, related, via=None, fetch=True):
        ORMProperty.__init__(self)

        via_property_owner = None
        if isinstance(via, tuple):
            # Makes possible to force a side of the relationship
            # when specifying the via property
            via, via_property_owner = via

        self.via_property_owner = via_property_owner
        self.via = via
        self.fetch = fetch
        if isinstance(related, type):
            self.related = related
        else:
            self._related_classname = related

    @LazyProperty
    def related(self):
        from .mapper import mapper
        return mapper(self._related_classname).mapped_class

    def _detect_foreign_keys(self, mapper, related, otherside):

        props = [ p for p in mapper.all_properties()
                  if isinstance(p, ForeignIdProperty)
                  and issubclass(related, p.related) ]

        if self.via:
            props = [ p for p in props if p.name == self.via ]
            if self.via_property_owner is otherside:
                props = []

        return props

    @LazyProperty
    def join(self):
        from .mapper import mapper
        cls = self.mapper.mapped_class
        rel = self.related

        own_props = self._detect_foreign_keys(self.mapper, rel, False)
        rel_props = self._detect_foreign_keys(mapper(self.related), cls, True)

        if len(own_props) == 1:
            if own_props[0].uselist:
                return ManyToManyListJoin(cls, rel, own_props[0], True)
            else:
                return ManyToOneJoin(cls, rel, own_props[0])
        if len(rel_props) == 1:
            if rel_props[0].uselist:
                return ManyToManyListJoin(cls, rel, rel_props[0], False)
            else:
                return OneToManyJoin(cls, rel, rel_props[0])
        if own_props or rel_props:
            raise AmbiguousJoin(
                'Ambiguous join, satisfying keys are %r' %
                [ p.name for p in own_props + rel_props ])
        else:
            raise NoJoin('No join keys found between %s and %s' % (
                cls, rel))

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

    def __set__(self, instance, value):
        self.join.set(instance, value)

class ManyToOneJoin(object):

    def __init__(self, own_cls, rel_cls, prop):
        self.own_cls, self.rel_cls, self.prop = own_cls, rel_cls, prop

    def load(self, instance):
        key_value = self.prop.__get__(instance, self.own_cls)
        if key_value is None and self.prop.allow_none is False:
            # Avoid one unnecessary lookup on DB by
            # considering ForeignIdPropery None value as
            # not related to any other entity.
            return None
        return self.rel_cls.query.get(_id=key_value)

    def iterator(self, instance):
        return [ self.load(instance) ]

    def set(self, instance, value):
        if value is not None:
            value = value._id
        self.prop.__set__(instance, value)

class OneToManyJoin(object):

    def __init__(self, own_cls, rel_cls, prop):
        self.own_cls, self.rel_cls, self.prop = own_cls, rel_cls, prop

    def load(self, instance):
        return instrument(
            list(self.iterator(instance)),
            OneToManyTracker(state(instance)))

    def iterator(self, instance):
        key_value = instance._id
        return self.rel_cls.query.find({self.prop.name:key_value})

    def set(self, instance, value):
        value = [v if isinstance(v, self.rel_cls._id.field.type) else v._id for v in value]

        # Retrieve all the referenced objects and update them.
        instance_id = instance._id
        foreign_id_owners = self.rel_cls.query.find({self.prop.name: instance_id})
        for foreign_id_owner in foreign_id_owners:
            # Remove all existing refereces in relationship
            setattr(foreign_id_owner, self.prop.name, None)

        for foreign_id_owner_id in value:
            # Update relationship to requested value
            list_owner = self.rel_cls.query.get(_id=foreign_id_owner_id)
            setattr(list_owner, self.prop.name, instance._id)

class OneToManyTracker(object):
    __slots__ = ('state',)

    def __init__(self, state):
        self.state = state

    def soil(self, value):
        raise TypeError('read-only')
    added_item = soil
    added_items = soil
    removed_item = soil
    removed_items = soil
    cleared = soil

class ManyToManyListJoin(object):

    def __init__(self, own_cls, rel_cls, prop, detains_list):
        self.own_cls, self.rel_cls, self.prop = own_cls, rel_cls, prop
        self.detains_list = detains_list

    def load(self, instance):
        return instrument(
            list(self.iterator(instance)),
            ManyToManyListTracker(state(instance)))

    def iterator(self, instance):
        if self.detains_list:
            # instance is the class owning the list
            related_ids = self.prop.__get__(instance, self.prop.name)
            field_name = self.rel_cls._id.field.name
            return self.rel_cls.query.find({field_name: {'$in': related_ids}})
        else:
            # instance is the class without the list
            instance_id = instance._id
            return self.rel_cls.query.find({self.prop.name: instance_id})

    def set(self, instance, value):
        value = [v if isinstance(v, self.rel_cls._id.field.type) else v._id for v in value]

        if self.detains_list:
            # instance is the class owning the list
            setattr(instance, self.prop.name, value)
        else:
            # instance doesn't own the list, need to retrieve all the
            # referenced objects and update them.
            instance_id = instance._id
            foreign_id_list_owners = self.rel_cls.query.find({self.prop.name: instance_id})
            for list_owner in foreign_id_list_owners:
                # Remove all existing refereces in relationship
                current_value = getattr(list_owner, self.prop.name)
                updated_value = list(current_value)
                updated_value.remove(instance_id)
                setattr(list_owner, self.prop.name, updated_value)

            for list_owner_id in value:
                # Update relationship to requested value
                list_owner = self.rel_cls.query.get(_id=list_owner_id)
                current_value = getattr(list_owner, self.prop.name)
                updated_value = current_value + [instance_id]
                setattr(list_owner, self.prop.name, updated_value)


class ManyToManyListTracker(OneToManyTracker):
    pass
