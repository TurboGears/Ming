from ming.metadata import collection, Field, Index
from .mapper import mapper
from .property import ORMProperty

class _MappedClassMeta(type):

    def __init__(cls, name, bases, dct):
        cls._registry['%s.%s' % (cls.__module__, cls.__name__)] = mapper(cls)
        cls._compiled = False
        
    def __new__(meta, name, bases, dct):
        # Get the mapped base class (if any)
        mapped_bases = [
            b for b in bases if hasattr(b, 'query') ]
        # Build up the mongometa class
        if len(mapped_bases) == 1:
            doc_base = mapper(mapped_bases[0]).collection
            if hasattr(mapped_bases[0], '__mongometa__'):
                mm_bases = (mapped_bases[0].__mongometa__,)
            else:
                mm_bases = (object,)
        else:
            doc_base = None
            mm_bases = (object,)
        mm_dict = {}
        if '__mongometa__' in dct:
            mm_dict.update(dct['__mongometa__'].__dict__)
        dct['__mongometa__'] = mm = type(
            '__mongometa__<%s>' % name,
            mm_bases,
            mm_dict)
        if hasattr(mm, 'collection_class'):
            collection_class = mm.collection
        else:
            collection_class = meta._build_collection_class(doc_base, dct, mm, mm_dict)
        clsdict = {}
        properties = {}
        include_properties = getattr(mm, 'include_properties', [])
        exclude_properties = getattr(mm, 'exclude_properties', [])
        for k,v in dct.iteritems():
            if isinstance(v, ORMProperty):
                v.name = k
                properties[k] = v
            else:
                clsdict[k] = v
        cls = type.__new__(meta, name, bases, clsdict)
        mapper(cls, collection_class, mm.session,
               properties=properties,
               include_properties=include_properties,
               exclude_properties=exclude_properties)
        return cls
        
    @classmethod
    def _build_collection_class(meta, doc_base, dct, mm, mm_dict):
        fields = []
        indexes = []
        # Set the names of the fields
        for k,v in dct.iteritems():
            if hasattr(v, 'field'):
                if v.field.name is None:
                    v.field.name = k
                if v.name is None:
                    v.name = k
                fields.append(v.field)
        # Get the index information
        for idx in getattr(mm, 'indexes', []):
            indexes.append(Index(idx))
        for idx in getattr(mm, 'unique_indexes', []):
            indexes.append(Index(idx, unique=True))
        collection_kwargs = dict(
            polymorphic_on=mm_dict.get('polymorphic_on', None),
            polymorphic_identity=getattr(mm, 'polymorphic_identity', None))
        if doc_base is None:
            collection_cls = collection(
                mm.name, mm.session,
                *(fields + indexes),
                **collection_kwargs)
        else:
            if mm.name is not None:
                collection_kwargs['override_name'] = mm.name
            if mm.session is not None:
                collection_kwargs['override_session'] = mm.session.impl
            collection_cls = collection(
                doc_base, *(fields + indexes), **collection_kwargs)
        return collection_cls

class MappedClass(object):
    __metaclass__ = _MappedClassMeta
    _registry = {}
    class __mongometa__:
        name=None
        session=None

    @classmethod
    def compile_all(cls):
        return
        for mapper in cls._registry.itervalues():
            mapper.mapped_class.compile(mapper)

    @classmethod
    def compile(cls, mapper):
        if cls._compiled: return
        for p in mapper.properties:
            p.compile(mapper)
        cls._compiled = True


