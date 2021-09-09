from ming.metadata import collection, Index
from .mapper import mapper
from .property import ORMProperty

import six

class _MappedClassMeta(type):
 
    def __init__(cls, name, bases, dct, **kwargs):
        cls._registry['%s.%s' % (cls.__module__, cls.__name__)] = mapper(cls)
        cls._compiled = False

    def __new__(meta, name, bases, dct, **kwargs):
        # Get the mapped base class(es)
        mapped_bases = [b for b in bases if hasattr(b, 'query')]
        doc_bases = [mapper(b).collection for b in mapped_bases]
        # Build up the mongometa class
        mm_bases = tuple(
            (b.__mongometa__ for b in mapped_bases if hasattr(b, '__mongometa__'))
        )
        if not mm_bases:
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
            collection_class = meta._build_collection_class(doc_bases, dct, mm, mm_dict)
        clsdict = {}
        properties = {}
        include_properties = getattr(mm, 'include_properties', [])
        exclude_properties = getattr(mm, 'exclude_properties', [])
        extensions = getattr(mm, 'extensions', [])
        for k,v in six.iteritems(dct):
            if isinstance(v, ORMProperty):
                v.name = k
                properties[k] = v
            else:
                clsdict[k] = v
        cls = type.__new__(meta, name, bases, clsdict, **kwargs)
        mapper(cls, collection_class, mm.session,
               properties=properties,
               include_properties=include_properties,
               exclude_properties=exclude_properties,
               extensions=extensions)
        return cls

    @classmethod
    def _build_collection_class(meta, doc_bases, dct, mm, mm_dict):
        fields = []
        indexes = []
        # Set the names of the fields
        for k,v in six.iteritems(dct):
            try:
                field = getattr(v, 'field', None)
            except:
                continue
            if field is not None:
                if field.name is None:
                    field.name = k
                fields.append(v.field)
        # Get the index information
        for idx in getattr(mm, 'indexes', []):
            indexes.append(Index(idx))
        for idx in getattr(mm, 'unique_indexes', []):
            indexes.append(Index(idx, unique=True))
        for idx in getattr(mm, 'custom_indexes', []):
            indexes.append(Index(**idx))
        collection_kwargs = dict(
            polymorphic_on=mm_dict.get('polymorphic_on', None),
            polymorphic_identity=getattr(mm, 'polymorphic_identity', None),
            version_of=getattr(mm, 'version_of', None),
            migrate=getattr(mm, 'migrate', None)
        )
        if hasattr(mm, 'before_save'):
            collection_kwargs['before_save'] = getattr(mm.before_save, '__func__', mm.before_save)
        if not doc_bases:
            collection_cls = collection(
                mm.name, mm.session and mm.session.impl,
                *(fields + indexes),
                **collection_kwargs)
        else:
            if mm.name is not None:
                collection_kwargs['collection_name'] = mm.name
            if mm.session is not None:
                collection_kwargs['session'] = mm.session.impl
            collection_cls = collection(
                doc_bases, *(fields + indexes), **collection_kwargs)
        return collection_cls

@six.add_metaclass(_MappedClassMeta)
class MappedClass(object):
    """Declares a Ming Mapped Document.

    Mapped Documents provide a declarative interface to
    schema, relations and properties declaration for your
    Models stored as MongoDB Documents.

    MappedClasses required that a ``__mongometa__`` subclass
    is available inside which provides the details regarding
    the ``name`` of the collection storing the documents and
    the ``session`` used to store the documents::

        class WikiPage(MappedClass):
            class __mongometa__:
                session = session
                name = 'wiki_page'

            _id = FieldProperty(schema.ObjectId)
            title = FieldProperty(schema.String(required=True))
            text = FieldProperty(schema.String(if_missing=''))

    """
    _registry = {}

    class __mongometa__:
        name=None
        session=None
