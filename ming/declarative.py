from .metadata import Field, Index
from .metadata import _Document, _FieldDescriptor, _ManagerDescriptor, _ClassManager

import six


class _DocumentMeta(type):
    def __new__(meta, classname, bases, dct, **kwargs):
        mm = _build_mongometa(bases, dct)
        collection_name = mm.name
        session = mm.session
        fields = []
        indexes = []
        # Inherit appropriate fields & indexes
        for b in bases:
            if not hasattr(b, 'm'): continue
            fields += b.m.fields
            indexes += b.m.indexes
        # Set the names of the fields
        clsdct = {}
        for k,v in six.iteritems(dct):
            if isinstance(v, Field):
                if v.name is None: v.name = k
                fields.append(v)
                v = _FieldDescriptor(v)
            clsdct[k] = v
        # Get the index information
        for idx in getattr(mm, 'indexes', []):
            indexes.append(Index(idx))
        for idx in getattr(mm, 'unique_indexes', []):
            indexes.append(Index(idx, unique=True))
        for idx in getattr(mm, 'custom_indexes', []):
            indexes.append(Index(**idx))
        # parse optional args
        polymorphic_on=getattr(mm, 'polymorphic_on', None)
        polymorphic_identity=getattr(mm, 'polymorphic_identity', None)
        polymorphic_registry=getattr(mm, 'polymorphic_registry', None)
        version_of=getattr(mm, 'version_of', None)
        migrate = getattr(mm, 'migrate', None)
        before_save = getattr(mm, 'before_save', None)
        if migrate:
            migrate = getattr(migrate, '__func__', migrate)
        if before_save:
            before_save = getattr(before_save, '__func__', before_save)
        cls = type.__new__(meta, classname, bases, clsdct, **kwargs)
        m = _ClassManager(
            cls, collection_name, session, fields, indexes,
            polymorphic_on=polymorphic_on,
            polymorphic_identity=polymorphic_identity,
            polymorphic_registry=polymorphic_registry,
            version_of=version_of,
            migrate=migrate,
            before_save=before_save)
        cls.m = _ManagerDescriptor(m)
        cls.__mongometa__ = mm
        return cls

def _build_mongometa(bases, dct):
    mm_bases = []
    for base in bases:
        mm = getattr(base, '__mongometa__', None)
        if mm is None: continue
        mm_bases.append(mm)
    mm_dict = {}
    if '__mongometa__' in dct:
        mm_dict.update(dct['__mongometa__'].__dict__)
    return type('__mongometa__', tuple(mm_bases), mm_dict)


@six.add_metaclass(_DocumentMeta)
class Document(_Document):

    class __mongometa__:
        name=None
        session=None
        indexes = []


