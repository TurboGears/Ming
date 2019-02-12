"""Ming Base module.  Good stuff here.
"""
import decimal
import warnings
from collections import defaultdict
from datetime import datetime

import bson
import six
from bson import Decimal128

from ming.exc import MingException

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

#: This is the value that Missing fields in MongoDB documents receive.
Missing = Missing()
NoDefault = NoDefault()

class Object(dict):
    'Dict providing object-like attr access'
    __slots__ = ()

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        if name in self.__class__.__dict__:
            super(Object, self).__setattr__(name, value)
        else:
            self.__setitem__(name, value)

    @classmethod
    def from_bson(cls, bson):
        if isinstance(bson, dict):
            return cls((k, cls.from_bson(v))
                       for k,v in six.iteritems(bson))
        elif isinstance(bson, list):
            return [ cls.from_bson(v) for v in bson ]
        else:
            return bson

    def make_safe(self):
        warnings.warn("make_safe is now deprecated. "
                      "If your code relies on make_safe for validation, "
                      "you should consider adding your own layer of validation", DeprecationWarning)
        safe_self = _safe_bson(self)
        self.update(safe_self)


class Cursor(object):
    '''Python class proxying a MongoDB cursor, constructing and validating
    objects that it tracks
    '''

    def __bool__(self):
        raise MingException('Cannot evaluate Cursor to a boolean')
    __nonzero__ = __bool__  # python 2

    def __init__(self, cls, cursor, allow_extra=True, strip_extra=True):
        self.cls = cls
        self.cursor = cursor
        self._allow_extra = allow_extra
        self._strip_extra = strip_extra

    def __iter__(self):
        return self

    def next(self):
        doc = next(self.cursor)
        if doc is None: return None
        return self.cls.make(
            doc,
            allow_extra=self._allow_extra,
            strip_extra=self._strip_extra)

    __next__ = next

    def count(self):
        return self.cursor.count()

    def distinct(self, *args, **kwargs):
        return self.cursor.distinct(*args, **kwargs)

    def limit(self, limit):
        self.cursor = self.cursor.limit(limit)
        return self

    def skip(self, skip):
        self.cursor = self.cursor.skip(skip)
        return self

    def hint(self, index_or_name):
        self.cursor = self.cursor.hint(index_or_name)
        return self

    def sort(self, *args, **kwargs):
        self.cursor = self.cursor.sort(*args, **kwargs)
        return self

    def one(self):
        try:
            result = self.next()
        except StopIteration:
            raise ValueError('Less than one result from .one()')
        try:
            self.next()
        except StopIteration:
            return result
        raise ValueError('More than one result from .one()')

    def first(self):
        try:
            return self.next()
        except StopIteration:
            return None

    def all(self):
        return list(self)

    def rewind(self):
        self.cursor = self.cursor.rewind()
        return self

NoneType = type(None)
def _safe_bson(obj, _no_warning=False):
    '''Verify that the obj is safe for bsonification (in particular, no tuples or
    Decimal objects
    '''
    if not _no_warning:
        warnings.warn("_safe_bson is now deprecated. "
                      "If your code relies on _safe_bson for validation, "
                      "you should consider adding your own layer of validation", DeprecationWarning)
    if isinstance(obj, list):
        return [ _safe_bson(o, True) for o in obj ]
    elif isinstance(obj, dict):
        return Object((k, _safe_bson(v, True)) for k,v in six.iteritems(obj))
    elif isinstance(obj, six.string_types + six.integer_types + (
            float, datetime, NoneType,
            bson.ObjectId)):
        return obj
    elif isinstance(obj, decimal.Decimal):
        return Decimal128(obj)
    else:
        assert False, '%s is not safe for bsonification: %r' % (
            type(obj), obj)
