from ming.base import Document
from ming.utils import EmptyClass

from .base import Decoration, mapper, session
from .mapper import Mapper

class MappedClassMeta(type):

    def __init__(cls, name, bases, dct):
        cls.__ming__ = EmptyClass()
        cls.__ming__.mapper = Mapper(cls, dct)

class QueryDescriptor(object):

    def __get__(self, instance, cls):
        if instance is not None:
            cls = instance.__class__
        return Query(cls)

class MappedClass(object):

    __metaclass__ = MappedClassMeta
    __mongometa__ = Document.__mongometa__
    query = QueryDescriptor()
    _registry = {}

    def __init__(self, **kwargs):
        self.__ming__ = deco = Decoration(self, kwargs)
        session(self).save(self)
        self._registry[self.__name__] = self

    def __repr__(self):
        properties = [ '%s=%s' % (prop.name, prop.repr(self))
                       for prop in mapper(self).properties
                       if prop.include_in_repr ]
        return '<%s %s>' % (
            self.__class__.__name__, ' '.join(properties))

    @classmethod
    def compile_all(cls):
        for mc in cls._registry.itervalues():
            mapper(mc).compile()

class Query(object):

    def __init__(self, cls):
        self.cls = cls
        self.session = cls.__mongometa__.session

    def get(self, **kwargs):
        if kwargs.keys() == '_id':
            return self.session.get(self.cls, **kwargs)
        return self.session.find(self.cls, **kwargs).first()

    def find(self, *args, **kwargs):
        return self.session.find(self.cls, *args, **kwargs)
