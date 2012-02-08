import types
import logging

from copy import deepcopy
from datetime import datetime

import bson
import pymongo

from .utils import LazyProperty

log = logging.getLogger(__name__)


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

class Missing(tuple):
    '''Missing is a sentinel used to indicate a missing key or missing keyword
    argument (used since None sometimes has meaning)'''
    def __repr__(self):
        return '<Missing>'
class NoDefault(tuple):
    '''NoDefault is a sentinel used to indicate a keyword argument was not
    specified.  Used since None and Missing mean something else
    '''
    def __repr__(self):
        return '<NoDefault>'
Missing = Missing()
NoDefault = NoDefault()

class SchemaItem(object):
    '''Part of a MongoDB schema.  The validate() method is called when a record
    is loaded from the DB or saved to it.  It should return a "validated" object,
    raising an Invalid exception if the object is invalid.  If it returns
    Missing, the field will be stripped from its parent object.'''

    def validate(self, d):
        'convert/validate an object or raise an Invalid exception'
        raise NotImplementedError, 'validate'

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
                raise ValueError, 'Array must have 0-1 elements'
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
        except Invalid:
            value = self.old.validate(value, **kw)
            value = self.migration_function(value)
            return self.new.validate(value, **kw)

    @classmethod
    def obj_to_list(cls, key_name, value_name=None):
        '''Migration function to go from object ``{ key: value }`` to
        list ``[ { key_name: key, value_name: value} ]``.  If value_name is ``None``,
        then value must be an object and the result will be a list
        ``[ { key_name: key, **value } ]``.
        '''
        from . import base
        def migrate_scalars(value):
            return [
                base.Object({ key_name: k, value_name: v})
                for k,v in value.iteritems() ]
        def migrate_objects(value):
            return [
                base.Object(dict(v, **{key_name:k}))
                for k,v in value.iteritems() ]
        if value_name is None:
            return migrate_objects
        else:
            return migrate_scalars

class Deprecated(SchemaItem):
    '''Used for deprecated fields -- they will be stripped from the object.
    '''
    def validate(self, value):
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
    
    def validate(self, value, **kw):
        if value is None and self.required:
            value = Missing
        if value is Missing:
            if self.required:
                raise Invalid('Missing field', value, None)
            elif self.if_missing is Missing:
                return self.if_missing
            elif self._callable_if_missing:
                return self.if_missing()
            else:
                return deepcopy(self.if_missing) # handle mutable defaults
        try:
            if value == self.if_missing:
                return value
        except Invalid:
            pass
        except:
            pass
        return self._validate(value, **kw)

    def _validate(self, value, **kw): return value

class Anything(FancySchemaItem):
    'Anything goes - always passes validation unchanged except dict=>Object'

    def validate(self, value, **kw):
        from . import base
        if isinstance(value, dict) and not isinstance(value, base.Object):
            return base.Object(value)
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
                           for name, field in fields.iteritems())

    def __repr__(self):
        l = [ super(Object, self).__repr__() ]
        for k,f in self.fields.iteritems():
            l.append('  %s: %s' % (k, repr(f).replace('\n', '\n    ')))
        return '\n'.join(l) 

    @LazyProperty
    def if_missing(self):
        from . import base
        return base.Object(
            (k, v.validate(Missing))
            for k,v in self.fields.iteritems()
            if isinstance(k, basestring))

    def _validate(self, d, allow_extra=False, strip_extra=False):
        from . import base
        result = base.Object()
        if not isinstance(d, dict): raise Invalid('notdict: %s' % (d,), d, None)
        error_dict = {}
        check_extra = True
        to_set = []
        for name,field in self.fields.iteritems():
            if isinstance(name, basestring):
                try:
                    value = field.validate(d.get(name, Missing))
                    to_set.append((name, value))
                except Invalid, inv:
                    error_dict[name] = inv
            else:
                # Validate all items in d against this field. No
                #    need in this case to deal with 'extra' keys
                check_extra = False
                allow_extra=True
                name_validator = SchemaItem.make(name)
                for name, value in d.iteritems():
                    try:
                        to_set.append((
                                name_validator.validate(name),
                                field.validate(value)))
                    except Invalid, inv:
                        error_dict[name] = inv
        if error_dict:
            msg = '\n'.join('%s:%s' % t for t in error_dict.iteritems())
            raise Invalid(msg, d, None, error_dict=error_dict)
        for name, value in to_set:
            if value is Missing: continue
            result[name] = value
        if check_extra:
            try:
                extra_keys = set(d.iterkeys()) - set(self.fields.iterkeys())
            except AttributeError, ae:
                raise Invalid(str(ae), d, None)
            if extra_keys and not allow_extra:
                raise Invalid('Extra keys: %r' % extra_keys, d, None)
            if extra_keys and not strip_extra:
                for ek in extra_keys:
                    result[ek] = d[ek]
        return result

    def extend(self, other):
        if other is None: return
        self.fields.update(other.fields)

class Document(Object):
    '''Used for dict-like validation, adding polymorphic validation (which means that
    ParentClass._validate(...) sometimes will return an instance of ChildClass).
    '''

    def __init__(self, fields=None, required=False, if_missing=NoDefault):
        super(Document, self).__init__(fields, required, if_missing)
        self.polymorphic_on = self.polymorphic_registry = None
        self.managed_class=None

    def validate(self, value, **kw):
        try:
            return super(Document, self).validate(value, **kw)
        except Invalid, inv:
            if self.managed_class:
                inv.msg = '%s:\n    %s' % (
                    self.managed_class,
                    inv.msg.replace('\n', '\n    '))
            raise

    def _validate(self, d, allow_extra=False, strip_extra=False):
        cls = self.managed_class
        if self.polymorphic_registry:
            disc = d.get(self.polymorphic_on, Missing)
            if disc is Missing:
                mm = self.managed_class.m
                disc = getattr(mm, 'polymorphic_identity', Missing)
            if disc is not Missing:
                cls = self.polymorphic_registry[disc]
                d[self.polymorphic_on] = disc
        if cls is None or cls == self.managed_class:
            result = cls.__new__(cls)
            result.update(super(Document, self)._validate(d, allow_extra, strip_extra))
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
        FancySchemaItem.__init__(self, required, if_missing)
        self._field_type = field_type

    def __repr__(self):
        l = [ super(Array, self).__repr__() ]
        l.append('  ' + repr(self.field_type).replace('\n', '\n    '))
        return '\n'.join(l) 

    @LazyProperty
    def field_type(self):
        return SchemaItem.make(self._field_type)

    def _validate(self, d):
        result = []
        error_list = []
        has_errors = False
        if d is None:
            d = []
        try:
            if not isinstance(d, (list, tuple)):
                raise Invalid('Not a list or tuple', d, None)
            for value in d:
                try:
                    value = self.field_type.validate(value)
                    result.append(value)
                    error_list.append(None)
                except Invalid, inv:
                    error_list.append(inv)
                    has_errors = True
            if has_errors:
                msg = '\n'.join(('[%s]:%s' % (i,v))
                                for i,v in enumerate(error_list)
                                if v)
                raise Invalid(msg, d, None, error_list=error_list)
            return result
        except Invalid:
            raise
        except TypeError, ex:
            raise Invalid(str(ex), d, None)
        

class Scalar(FancySchemaItem):
    '''Validate that a value is NOT an array or dict'''
    if_missing=None
    def _validate(self, value):
        if isinstance(value, (tuple, list, dict)):
            raise Invalid('%r is not a scalar' % value, value, None)
        return value

class ParticularScalar(Scalar):
    '''Validate that a value is NOT an array or dict and is a particular type
    '''
    type=()
    def _validate(self, value):
        value = Scalar._validate(self, value)
        if value is None: return value
        if not isinstance(value, self.type):
            raise Invalid('%s is not a %r' % (value, self.type),
                          value, None)
        return value

class OneOf(ParticularScalar):
    def __init__(self, *options, **kwargs):
        self.options = options
        ParticularScalar.__init__(self, **kwargs)

    def _validate(self, value):
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
        
    def _validate(self, value):
        if value != self.value:
            raise Invalid('%r != %r' % (value, self.value),
                          value, None)
        return value

class String(ParticularScalar):
    type=basestring
class Int(ParticularScalar):
    type=(int,long)
    def _validate(self, value):
        if isinstance(value, float) and round(value) == value:
            value = int(value)
        return super(Int, self)._validate(value)
class Float(ParticularScalar):
    type=(float,int,long)
class DateTimeTZ(ParticularScalar):
    type=datetime
class DateTime(DateTimeTZ):
    def _validate(self, value):
        value = DateTimeTZ._validate(self, value)
        if value is None: return value
        if not isinstance(value, self.type):
            raise Invalid('%s is not a %r' % (value, self.type),
                          value, None)
        value = value.replace(microsecond=0,tzinfo=None)
        return value
class Bool(ParticularScalar):
    type=bool
class Binary(ParticularScalar):
    type=bson.Binary
class ObjectId(Scalar):
    def if_missing(self):
        '''Provides a pymongo.bson.ObjectId as default'''
        return bson.ObjectId()
    def _validate(self, value):
        try:
            value = Scalar._validate(self, value)
            if value is None: return value
            if isinstance(value, bson.ObjectId):
                return value
            elif isinstance(value, basestring):
                return bson.ObjectId(str(value))
            else:
                raise Invalid('%s is not a bson.ObjectId' % value, value, None)
        except Invalid:
            raise
        except Exception, ex:
            raise Invalid(str(ex), value, None)

# Shorthand for various SchemaItems
SHORTHAND={
    int:Int,
    str:String,
    float:Float,
    bool:Bool,
    datetime:DateTime}
    

