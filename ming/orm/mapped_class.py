from ming.base import Document
from ming.utils import EmptyClass, wordwrap

from .base import Decoration, mapper, session, state
from .mapper import Mapper

class MappedClassMeta(type):

    def __init__(cls, name, bases, dct):
        cls.__ming__ = EmptyClass()
        cls.__ming__.mapper = Mapper(cls)
        cls._registry[cls.__name__] = cls

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

    def __repr__(self):
        properties = [ '%s=%s' % (prop.name, prop.repr(self))
                       for prop in mapper(self).properties
                       if prop.include_in_repr ]
        return wordwrap(
            '<%s %s>' % 
            (self.__class__.__name__, ' '.join(properties)),
            60,
            indent_subsequent=2)
             
                        

    @classmethod
    def compile_all(cls):
        for mc in cls._registry.itervalues():
            mapper(mc).compile()

    def delete(self):
        st = state(self)
        st.status = st.deleted

class Query(object):

    def __init__(self, cls):
        self.cls = cls
        self.session = cls.__mongometa__.session

    def get(self, **kwargs):
        if kwargs.keys() == ['_id']:
            return self.session.get(self.cls, kwargs['_id'])
        return self.session.find(self.cls, kwargs).first()

    def find(self, *args, **kwargs):
        return self.session.find(self.cls, *args, **kwargs)

    def remove(self, *args, **kwargs):
        return self.session.remove(self.cls, *args, **kwargs)
    
