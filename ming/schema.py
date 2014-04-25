import types
import logging

from copy import deepcopy
from datetime import datetime

import bson
import pymongo
import pytz
import six

from .utils import LazyProperty
from .base import Object as BaseObject, Missing, NoDefault

log = logging.getLogger(__name__)

NoneType = type(None)


# lifted from formencode.validators
# but separate, so TurboGears special handling of formencode.validators.Invalid won't kick in incorrectly
class Invalid(Exception):

    """
    This is raised in response to invalid input.  It has several
    public attributes:

    msg:
        The message, *without* values substituted.  For instance, if
        you want HTML quoting of values, you can apply that.
    substituteArgs:
        The arguments (a dictionary) to go with `msg`.
    str(self):
        The message describing the error, with values substituted.
    value:
        The offending (invalid) value.
    state:
        The state that went with this validator.  This is an
        application-specific object.
    error_list:
        If this was a compound validator that takes a repeating value,
        and sub-validator(s) had errors, then this is a list of those
        exceptions.  The list will be the same length as the number of
        values -- valid values will have None instead of an exception.
    error_dict:
        Like `error_list`, but for dictionary compound validators.
    """

    def __init__(self, msg,
                 value, state, error_list=None, error_dict=None):
        Exception.__init__(self, msg)
        self.msg = msg
        self.value = value
        self.state = state
        self.error_list = error_list
        self.error_dict = error_dict
        assert (not self.error_list or not self.error_dict), (
                "Errors shouldn't have both error dicts and lists "
                "(error %s has %s and %s)"
                % (self, self.error_list, self.error_dict))

    def __str__(self):
        val = self.msg
        #if self.value:
        #    val += " (value: %s)" % repr(self.value)
        return val

    def __unicode__(self):
        if isinstance(self.msg, unicode):
            return self.msg
        elif isinstance(self.msg, str):
            return self.msg.decode('utf8')
        else:
            return unicode(self.msg)


class SchemaItem(object):
    '''Part of a MongoDB schema.  The validate() method is called when a record
    is loaded from the DB or saved to it.  It should return a "validated" object,
    raising an Invalid exception if the object is invalid.  If it returns
    Missing, the field will be stripped from its parent object.'''

    def validate(self, d, **kw):
        'convert/validate an object or raise an Invalid exception'
        raise NotImplementedError('validate')

    @classmethod
    def make(cls, field, *args, **kwargs):
        '''Build a SchemaItem from a "shorthand" schema.  The `field` param:

        * int - int or long
        * str - string or unicode
        * float - float, int, or long
        * bool - boolean value
        * datetime - datetime.datetime object
        * None - Anything
        * [] - Array of Anything objects
        * [type] - array of objects of type "type"
        * { fld: type... } - dict-like object with field "fld" of type "type"
        * { type: type... } - dict-like object with fields of type "type"
        * anything else (e.g. literal values), must match exactly

        ``*args`` and ``**kwargs`` are passed on to the specific class of ``SchemaItem`` created.
        '''
        if isinstance(field, list):
            if len(field) == 0:
                field = Array(Anything(), *args, **kwargs)
            elif len(field) == 1:
                field = Array(field[0], *args, **kwargs)
            else:
                raise ValueError('Array must have 0-1 elements')
        elif isinstance(field, dict):
            field = Object(field, *args, **kwargs)
        elif field is None:
            field = Anything(*args, **kwargs)
        elif field in SHORTHAND:
            field = SHORTHAND[field]
        if isinstance(field, type):
            field = field(*args, **kwargs)
        if not isinstance(field, SchemaItem):
            field = Value(field, *args, **kwargs)
        return field

class Migrate(SchemaItem):
    '''Use when migrating from one field type to another
    '''
    def __init__(self, old, new, migration_function):
        self.old, self.new, self.migration_function = (
            SchemaItem.make(old),
            SchemaItem.make(new),
            migration_function)

    def validate(self, value, **kw):
        try:
            return self.new.validate(value, **kw)
        except Invalid as new_error:
            try:
                value = self.old.validate(value, **kw)
            except Invalid:
                raise new_error
            else:
                value = self.migration_function(value)
                return self.new.validate(value, **kw)

    @classmethod
    def obj_to_list(cls, key_name, value_name=None):
        '''Migration function to go from object ``{ key: value }`` to
        list ``[ { key_name: key, value_name: value} ]``.  If value_name is ``None``,
        then value must be an object and the result will be a list
        ``[ { key_name: key, **value } ]``.
        '''
        def migrate_scalars(value):
            return [
                BaseObject({ key_name: k, value_name: v})
                for k,v in six.iteritems(value) ]
        def migrate_objects(value):
            return [
                BaseObject(dict(v, **{key_name:k}))
                for k,v in six.iteritems(value) ]
        if value_name is None:
            return migrate_objects
        else:
            return migrate_scalars

class Deprecated(SchemaItem):
    '''Used for deprecated fields -- they will be stripped from the object.
    '''
    def validate(self, value, **kw):
        if value is not Missing:
            # log.debug('Stripping deprecated field value %r', value)
            pass
        return Missing

class FancySchemaItem(SchemaItem):
    '''Simple SchemaItem wrapper providing required and if_missing fields.

    If the value is present, then the result of the _validate method is returned.
    '''
    required=False
    if_missing=Missing

    def __init__(self, required=NoDefault, if_missing=NoDefault):
        '''
        :param bool required: if ``True`` and this field is missing, an ``Invalid`` exception will be raised
        :param if_missing: provides a default value for this field if the field is missing
        :type if_missing: value or callable
        '''
        if required is not NoDefault:
            self.required = required
        if if_missing is not NoDefault:
            self.if_missing = if_missing
        if self.required:
            self.validate = self._validate_required
        elif isinstance(self.if_missing, (NoneType,) + six.string_types + six.integer_types):
            self.validate = self._validate_fast_missing
        else:
            self.validate = self._validate_optional

    def __repr__(self):
        return '<%s required=%s if_missing=...>' % (
            self.__class__.__name__, self.required)

    @LazyProperty
    def _callable_if_missing(self):
        return isinstance(
            self.if_missing, (
                types.FunctionType,
                types.MethodType,
                types.BuiltinFunctionType))

    def _validate_required(self, value, **kw):
        if value is Missing:
            raise Invalid('Missing field', value, None)
        return self._validate(value, **kw)

    def _validate_fast_missing(self, value, **kw):
        if (value is Missing
            or value == self.if_missing):
            return self.if_missing
        return self._validate(value, **kw)

    def _validate_optional(self, value, **kw):
        if value is Missing:
            if self.if_missing == []:
                return []
            elif self._callable_if_missing:
                return self.if_missing()
            elif self.if_missing is Missing:
                return self.if_missing
            else:
                return deepcopy(self.if_missing) # handle mutable defaults
        if value == self.if_missing:
            return value
        return self._validate(value, **kw)

    def _validate(self, value, **kw): return value

class Anything(FancySchemaItem):
    'Anything goes - always passes validation unchanged except dict=>Object'

    def validate(self, value, **kw):
        if isinstance(value, dict) and not isinstance(value, BaseObject):
            return BaseObject(value)
        return value

class Object(FancySchemaItem):
    '''Used for dict-like validation.  Also ensures that the incoming object does
    not have any extra keys AND performs polymorphic validation (which means that
    ParentClass._validate(...) sometimes will return an instance of ChildClass).
    '''

    def __init__(self, fields=None, required=False, if_missing=NoDefault):
        if fields is None: fields = {}
        FancySchemaItem.__init__(self, required, if_missing)
        self.fields = dict((name, SchemaItem.make(field))
                           for name, field in six.iteritems(fields))
        if len(self.fields) == 1:
            name, field = list(self.fields.items())[0]
            if not isinstance(name, str):
                self._validate = lambda d, **kw: (
                    self._validate_homogenous(name, field, d, **kw))

    @LazyProperty
    def field_items(self):
        return sorted(self.fields.items())

    def __repr__(self):
        l = [ super(Object, self).__repr__() ]
        for k,f in six.iteritems(self.fields):
            l.append('  %s: %s' % (k, repr(f).replace('\n', '\n    ')))
        return '\n'.join(l)

    def if_missing(self):
        return BaseObject(
            (k, v.validate(Missing))
            for k,v in six.iteritems(self.fields)
            if isinstance(k, six.string_types))

    def _validate_homogenous(self, name, field, d, **kw):
        if not isinstance(d, dict): raise Invalid('notdict: %s' % (d,), d, None)
        l_Missing = Missing
        name_validator = SchemaItem.make(name)
        to_set = []
        errors = []
        for k,v in six.iteritems(d):
            try:
                k = name_validator.validate(k, **kw)
                v = field.validate(v, **kw)
                if v is not l_Missing:
                    to_set.append((k,v))
            except Invalid as inv:
                errors.append((name, inv))
        if errors:
            error_dict = dict(errors)
            msg = '\n'.join('%s:%s' % t for t in six.iteritems(error_dict))
            raise Invalid(msg, d, None, error_dict=error_dict)
        return BaseObject(to_set)

    def _validate_core(self, d, to_set, errors, **kw):
        l_Missing = Missing
        # try common case (no Invalid)
        try:
            validated = [
                (name, field.validate(d.get(name, l_Missing), **kw))
                for name, field in self.field_items ]
            to_set.extend([
                (name, value)
                for name, value in validated
                if value is not l_Missing])
            return
        except Invalid:
            pass
        # Go back and re-scan for the invalid items
        for name,field in self.field_items:
            try:
                value = field.validate(d.get(name, l_Missing), **kw)
                if value is not l_Missing:
                    to_set.append((name, value))
            except Invalid as inv:
                errors.append((name, inv))

    def _validate(self, d, allow_extra=False, strip_extra=False):
        if not isinstance(d, dict): raise Invalid('notdict: %s' % (d,), d, None)
        if allow_extra and not strip_extra:
            to_set = list(d.items())
        else:
            to_set = []
        errors = []
        self._validate_core(d, to_set, errors, allow_extra=allow_extra, strip_extra=strip_extra)
        if errors:
            error_dict = dict(errors)
            msg = '\n'.join('%s:%s' % t for t in errors)
            raise Invalid(msg, d, None, error_dict=error_dict)
        result = BaseObject(to_set)
        if not allow_extra:
            try:
                extra_keys = set(six.iterkeys(d)) - set(six.iterkeys(self.fields))
            except AttributeError as ae:
                raise Invalid(str(ae), d, None)
            if extra_keys:
                raise Invalid('Extra keys: %r' % extra_keys, d, None)
        return result

    def extend(self, other):
        if other is None: return
        self.fields.update(other.fields)

class Document(Object):
    '''Used for dict-like validation, adding polymorphic validation (which means that
    ParentClass._validate(...) sometimes will return an instance of ChildClass).
    '''

    def __init__(self, fields=None,
                 required=False, if_missing=NoDefault):
        super(Document, self).__init__(fields, required, if_missing)
        self.polymorphic_on = self.polymorphic_registry = None
        self.managed_class=None

    def get_polymorphic_cls(self, data):
        l_Missing = Missing
        if self.polymorphic_registry:
            disc = data.get(self.polymorphic_on, Missing)
            if disc is Missing:
                mm = self.managed_class.m
                disc = getattr(mm, 'polymorphic_identity', Missing)
            if disc is not l_Missing:
                cls = self.polymorphic_registry[disc]
            return cls
        return self.managed_class

    def validate(self, value, **kw):
        try:
            return super(Document, self).validate(value, **kw)
        except Invalid as inv:
            if self.managed_class:
                inv.msg = '%s:\n    %s' % (
                    self.managed_class,
                    inv.msg.replace('\n', '\n    '))
            raise

    def _validate(self, d, allow_extra=False, strip_extra=False):
        cls = self.get_polymorphic_cls(d)
        if cls is None or cls == self.managed_class:
            result = cls.__new__(cls)
            result.update(super(Document, self)._validate(
                    d, allow_extra=allow_extra, strip_extra=strip_extra))
            return result
        return cls.m.make(
            d, allow_extra=allow_extra, strip_extra=strip_extra)

    def set_polymorphic(self, field, registry, identity):
        self.polymorphic_on = field
        self.polymorphic_registry = registry
        if self.polymorphic_on:
            registry[identity] = self.managed_class

class Array(FancySchemaItem):
    '''Array/list validator.  All elements of the array must pass validation by a
    single field_type (which itself may be Anything, however).
    '''

    def __init__(self, field_type, **kw):
        required = kw.pop('required', False)
        if_missing = kw.pop('if_missing', [])
        validate_ranges = kw.pop('validate_ranges', None)
        FancySchemaItem.__init__(self, required, if_missing)
        self._field_type = field_type
        if validate_ranges:
            self._validate = lambda d, **kw: (
                self._range_validate(validate_ranges, d, **kw))
        else:
            self._validate = self._full_validate

    def __repr__(self):
        l = [ super(Array, self).__repr__() ]
        l.append('  ' + repr(self.field_type).replace('\n', '\n    '))
        return '\n'.join(l)

    @LazyProperty
    def field_type(self):
        return SchemaItem.make(self._field_type)

    def _range_validate(self, ranges, d, **kw):
        result = d[:]
        for range in ranges:
            result[range] = self._full_validate(d[range], **kw)
        return result

    def _full_validate(self, d, **kw):
        if d is None: d = []
        if not isinstance(d, (list, tuple)):
            raise Invalid('Not a list or tuple', d, None)
        # try common case (no Invalid)
        validate = self.field_type.validate
        try:
            return [
                validate(value, **kw)
                for value in d ]
        except Invalid:
            pass
        # Find the invalid values
        error_list = [ None ] * len(d)
        for i, value in enumerate(d):
            try:
                validate(value, **kw)
            except Invalid as inv:
                error_list[i] = inv
        msg = '\n'.join(('[%s]:%s' % (i,v))
                        for i,v in enumerate(error_list)
                        if v is not None)
        raise Invalid(msg, d, None, error_list=error_list)


class Scalar(FancySchemaItem):
    '''Validate that a value is NOT an array or dict'''
    if_missing=None
    def _validate(self, value, **kw):
        if isinstance(value, (tuple, list, dict)):
            raise Invalid('%r is not a scalar' % value, value, None)
        return value

class ParticularScalar(Scalar):
    '''Validate that a value is NOT an array or dict and is a particular type
    '''
    type=()
    def __init__(self, **kw):
        self._allow_none = kw.pop('allow_none', True)
        if not self._allow_none:
            kw.setdefault('if_missing', Missing)
        super(ParticularScalar, self).__init__(**kw)

    def _validate(self, value, **kw):
        if self._allow_none and value is None: return value
        if not isinstance(value, self.type):
            raise Invalid('%s is not a %r' % (value, self.type),
                          value, None)
        return value

class OneOf(ParticularScalar):
    def __init__(self, *options, **kwargs):
        self.options = options
        ParticularScalar.__init__(self, **kwargs)

    def _validate(self, value, **kw):
        if value not in self.options:
            raise Invalid('%s is not in %r' % (value, self.options),
                          value, None)
        return value

class Value(FancySchemaItem):
    '''Validate that a value is equal'''
    if_missing=None
    def __init__(self, value, **kw):
        self.value = value
        FancySchemaItem.__init__(self, **kw)

    def _validate(self, value, **kw):
        if value != self.value:
            raise Invalid('%r != %r' % (value, self.value),
                          value, None)
        return value

class String(ParticularScalar):
    type=six.string_types
class Int(ParticularScalar):
    type=six.integer_types
    def _validate(self, value, **kw):
        if isinstance(value, float) and round(value) == value:
            value = int(value)
        return super(Int, self)._validate(value, **kw)
class Float(ParticularScalar):
    type=(float,) + six.integer_types
class DateTimeTZ(ParticularScalar):
    type=datetime
class DateTime(DateTimeTZ):
    def _validate(self, value, **kw):
        value = DateTimeTZ._validate(self, value, **kw)
        if value is None: return value
        if not isinstance(value, self.type):
            raise Invalid('%s is not a %r' % (value, self.type),
                          value, None)
        # Truncate microseconds and keep milliseconds only (mimics BSON datetime)
        value = value.replace(microsecond=(value.microsecond // 1000) * 1000)
        # Convert a local timestamp to UTC
        if value.tzinfo:
            value = value.astimezone(pytz.utc).replace(tzinfo=None)
        return value
class Bool(ParticularScalar):
    type=bool
class Binary(ParticularScalar):
    type=bson.Binary
class ObjectId(Scalar):
    def if_missing(self):
        '''Provides a pymongo.bson.ObjectId as default'''
        return bson.ObjectId()
    def _validate(self, value, **kw):
        try:
            if value is None: return value
            value = Scalar._validate(self, value, **kw)
            if isinstance(value, bson.ObjectId):
                return value
            elif isinstance(value, six.string_types):
                return bson.ObjectId(str(value))
            else:
                raise Invalid('%s is not a bson.ObjectId' % value, value, None)
        except Invalid:
            raise
        except Exception as ex:
            raise Invalid(str(ex), value, None)

# Shorthand for various SchemaItems
SHORTHAND={
    int:Int,
    str:String,
    float:Float,
    bool:Bool,
    datetime:DateTime}


