import mimetypes
from datetime import datetime

import gridfs

from ming.utils import LazyProperty
from ming import schema as S
from ming.metadata import Field, _FieldDescriptor
from ming.metadata import _ClassManager, _InstanceManager
from ming.metadata import _ManagerDescriptor
from ming.metadata import _process_collection_args

def filesystem(*args, **kwargs):
    fields, indexes, collection_name, bases, session = _process_collection_args(
        args, kwargs)
    field_index = dict((f.name, f) for f in fields)
    field_index.setdefault(
        'filename', Field('filename', str, index=True))
    field_index.setdefault(
        'content_type', Field('contentType', str, index=True))
    field_index.setdefault('_id', Field('_id', S.ObjectId()))
    field_index.setdefault('chunkSize', Field('chunkSize', int))
    field_index.setdefault('length', Field('length', int))
    field_index.setdefault('md5', Field('md5', str))
    field_index.setdefault('uploadDate', Field('uploadDate', datetime))
    dct = dict((k, _FieldDescriptor(f)) for k,f in field_index.items())

    cls = type('Filesystem<%s>' % collection_name, bases, dct)
    fields = field_index.values()
    m = _FSClassManager(
        cls, collection_name, session, fields, indexes, **kwargs)
    cls.m = _ManagerDescriptor(m)
    return cls

class _FSInstanceManager(_InstanceManager):
    _proxy_methods = (
        'save', 'insert', 'upsert', 'set', 'increase_field')
    _proxy_on = _InstanceManager._proxy_on
    _proxy_args = _InstanceManager._proxy_args

    def delete(self):
        self.classmanager.fs.delete(self.inst._id)

class _FSClassManager(_ClassManager):
    InstanceManagerClass=_FSInstanceManager
    _proxy_methods = _ClassManager._proxy_methods
    _proxy_on = _ClassManager._proxy_on
    _proxy_args = _ClassManager._proxy_args

    def __init__(
        self, cls, root_collection_name, session, fields, indexes,
        polymorphic_on=None, polymorphic_identity=None,
        polymorphic_registry=None,
        version_of=None, migrate=None,
        before_save=None):
        self.root_collection_name = root_collection_name
        super(_FSClassManager, self).__init__(
            cls, root_collection_name + '.files', session, fields, indexes,
            polymorphic_on, polymorphic_identity,
            polymorphic_registry, version_of, migrate, before_save)

    @LazyProperty
    def fs(self):
        return gridfs.GridFS(self.session.db, self.root_collection_name)

    def _guess_type(self, filename):
        t = mimetypes.guess_type(filename)
        if t and t[0]:
            return t[0]
        else:
            return 'application/octet-stream'

    def new_file(self, filename, **kwargs):
        kwargs.setdefault('contentType', self._guess_type(filename))
        kwargs.setdefault('encoding', 'ascii')
        obj = self.fs.new_file(filename=filename, **kwargs)
        return _ClosingProxy(obj)

    def exists(self, *args, **kwargs):
        return self.fs.exists(*args, **kwargs)

    def put(self, filename, data, **kwargs):
        kwargs.setdefault('contentType', self._guess_type(filename))
        kwargs.setdefault('encoding', 'ascii')
        return self.fs.put(data, filename=filename, **kwargs)

    def get_file(self, file_id):
        return self.fs.get(file_id)

    def get_last_version(self, filename=None, **kwargs):
        return self.fs.get_last_version(filename, **kwargs)

    def get_version(self, filename=None, version=-1, **kwargs):
        return self.fs.get_version(filename, version, **kwargs)

class _ClosingProxy(object):

    def __init__(self, thing):
        self.thing = thing

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.thing.close()

    def __getattr__(self, name):
        return getattr(self.thing, name)

