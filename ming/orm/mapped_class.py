from ming.base import Document, DocumentMeta
from ming.utils import EmptyClass, wordwrap, all_class_properties, encode_keys

from .base import Decoration, mapper, session, state
from .property import ORMProperty

class Mapper(object):

    def __init__(self, mapped_class):
        self._mapped_class = mapped_class
        self._dct = dict(all_class_properties(mapped_class))
        self._compiled = False
        # Setup properties
        self.property_index = dict(
            (k,v) for k,v in self._dct.iteritems()
            if isinstance(v, ORMProperty))
        for k,v in self.property_index.iteritems():
            v.name = k
            v.cls = mapped_class

    def __repr__(self):
        return '<Mapper for %s>' % (
            self._mapped_class.__name__)

    @property
    def properties(self):
        return self.property_index.itervalues()

    def compile(self):
        if self._compiled: return self
        for p in self.properties:
            p.compile()
        self.doc_cls = make_document_class(self._mapped_class, self._dct)
        self._compiled = True
        mm = self._mapped_class.__mongometa__ = self.doc_cls.__mongometa__
        if mm.polymorphic_registry is not None:
            if not hasattr(mm, 'orm_polymorphic_registry'):
                mm.orm_polymorphic_registry = {}
            for name, value in mm.polymorphic_registry.iteritems():
                if value is self.doc_cls:
                    mm.orm_polymorphic_registry[name] = self._mapped_class
        return self

    def insert(self, session, obj, state):
        # Allow properties to do any insertion magic they want to
        for prop in self.property_index.itervalues():
            prop.insert(self, session, obj, state)
        # Actually insert the document
        doc = self.doc_cls(state.document)
        session.impl.insert(doc)
        if '_id' in doc:
            state.document['_id'] = doc._id
        session.save(obj)
        state.status = state.clean

    def update(self, session, obj, state):
        # Allow properties to do any insertion magic they want to
        for prop in self.property_index.itervalues():
            prop.update(self, session, obj, state)
        # Actually insert the document
        doc = self.doc_cls(state.document)
        session.impl.save(doc)
        if '_id' in doc:
            state.document['_id'] = doc._id
        state.status = state.clean

    def delete(self, session, obj, state):
        # Allow properties to do any insertion magic they want to
        for prop in self.property_index.itervalues():
            prop.delete(self, session, obj, state)
        # Actually insert the document
        doc = self.doc_cls(state.document)
        session.impl.delete(doc)
        session.expunge(obj)

    def remove(self, *args, **kwargs):
        session(self._mapped_class).remove(self._mapped_class, *args, **kwargs)

    def create(self, doc):
        mm = self._mapped_class.__mongometa__
        opr = getattr(mm, 'orm_polymorphic_registry', None)
        if opr:
            discriminator = doc[mm.polymorphic_on]
            cls = opr[discriminator]
        else:
            cls = self._mapped_class
        return cls(**encode_keys(doc))

class MappedClassMeta(type):

    def __init__(cls, name, bases, dct):
        cls.__ming__ = EmptyClass()
        cls.__ming__.mapper = Mapper(cls)
        cls._registry[cls.__name__] = cls

class QueryDescriptor(object):

    def __get__(self, instance, cls):
        if instance is not None:
            cls = instance.__class__
        return Query(cls, instance)

class MappedClass(object):

    __metaclass__ = MappedClassMeta
    __mongometa__ = Document.__mongometa__
    query = QueryDescriptor()
    _registry = {}

    def __init__(self, **kwargs):
        self.__ming__ = Decoration(self, kwargs)
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
             
    def __getitem__(self, name):
        try:
            return getattr(self, name)
        except AttributeError:
            raise KeyError, name

    def __setitem__(self, name, value):
        return setattr(self, name, value)

    def __contains__(self, name):
        return hasattr(self, name)

    @classmethod
    def compile_all(cls):
        for mc in cls._registry.itervalues():
            mapper(mc).compile()

    def delete(self):
        '''Schedule the instance for deletion'''
        st = state(self)
        st.status = st.deleted

class Query(object):

    def __init__(self, cls, instance):
        self.cls = cls
        self.instance = instance
        self.session = cls.__mongometa__.session

    def get(self, **kwargs):
        if kwargs.keys() == ['_id']:
            return self.session.get(self.cls, kwargs['_id'])
        return self.session.find(self.cls, kwargs).first()

    def find(self, *args, **kwargs):
        return self.session.find(self.cls, *args, **kwargs)

    def find_and_modify(self, *args, **kwargs):
        return self.session.find_and_modify(self.cls, *args, **kwargs)

    def update_if_not_modified(self, *args, **kwargs):
        return self.session.update_if_not_modified(self.instance, *args, **kwargs)

    def find_by(self, **kwargs):
        return self.session.find(self.cls, kwargs)

    def remove(self, *args, **kwargs):
        return self.session.remove(self.cls, *args, **kwargs)

    def update(self, *args, **kwargs):
        return self.session.update(self.cls, *args, **kwargs)
    
def make_document_class(mapped_class, dct):
    name = '_ming_document_' + mapped_class.__name__
    bases = mapped_class.__bases__
    doc_bases = tuple( mapper(base).compile().doc_cls
                       for base in bases
                       if hasattr(base, '__ming__') )
    if not doc_bases:
        doc_bases = (Document,)
    doc_dct = dict(
        (k, v.field)
        for k,v in dct.iteritems()
        if hasattr(v, 'field'))
    doc_dct['__mongometa__'] = dct['__mongometa__']
    return DocumentMeta(name, doc_bases, doc_dct)

