import logging
from functools import update_wrapper

import bson.errors

import pymongo
import pymongo.errors
import pymongo.collection
import pymongo.database

from .base import Cursor, Object
from .datastore import DataStore
from .utils import fixup_index, fix_write_concern
from . import exc

log = logging.getLogger(__name__)

def annotate_doc_failure(func):
    '''Decorator to wrap a session operation so that any pymongo errors raised
    will note the document that caused the failure
    '''
    def wrapper(self, doc, *args, **kwargs):
        try:
            return func(self, doc, *args, **kwargs)
        except (pymongo.errors.OperationFailure, bson.errors.BSONError) as e:
            doc_preview = str(doc)
            if len(doc_preview) > 5000:
                doc_preview = doc_preview[:5000] + '...'
            e.args = e.args + (('doc:  ' + doc_preview),)
            raise
    return update_wrapper(wrapper, func)


class Session:
    _registry = {}
    _datastores = {}

    def __init__(self, bind: DataStore = None):
        '''
        bind may be a lazy parameter, established later with ming.configure
        '''
        self.bind = bind

    @classmethod
    def by_name(cls, name):
        if name in cls._registry:
            result = cls._registry[name]
        else:
            result = cls._registry[name] = cls(cls._datastores.get(name))
        return result

    def _impl(self, cls) -> pymongo.collection.Collection:
        try:
            return self.db[cls.m.collection_name]
        except TypeError:
            raise exc.MongoGone('MongoDB is not connected')

    @property
    def db(self) -> pymongo.database.Database:
        if not self.bind:
            raise exc.MongoGone('No MongoDB connection for "%s"' % getattr(self, '_name', 'unknown connection'))
        return self.bind.db

    def get(self, cls, **kwargs):
        bson = self._impl(cls).find_one(kwargs)
        if bson is None: return None
        return cls.make(bson, allow_extra=True, strip_extra=True)

    def find(self, cls, *args, **kwargs):
        if not args and kwargs:
            raise ValueError('A query dict is typically the first param to find() but it is not present. '
                             'Moreover, **kwargs were found.  Kwargs are only used for options and not query criteria. '
                             'If you really want to search with no criteria and use kwarg options, pass an explicit {} as your criteria dict.')

        allow_extra = kwargs.pop('allow_extra', True)
        strip_extra = kwargs.pop('strip_extra', True)
        validate = kwargs.pop('validate', True)

        projection = kwargs.pop('projection', None)
        if projection is not None:
            kwargs['projection'] = projection

        collection = self._impl(cls)
        cursor = collection.find(*args, **kwargs)

        find_spec = kwargs.get('filter', None) or args[0] if args else {}

        if not validate:
            return (cls(o, skip_from_bson=True) for o in cursor)

        return Cursor(cls, cursor,
                      allow_extra=allow_extra,
                      strip_extra=strip_extra,
                      find_spec=find_spec)

    def remove(self, cls, filter={}, *args, **kwargs):
        fix_write_concern(kwargs)
        for kwarg in kwargs:
            if kwarg not in ('spec_or_id', 'w'):
                raise ValueError("Unexpected kwarg %s.  Did you mean to pass a dict?  If only sent kwargs, pymongo's remove()"
                                 " would've emptied the whole collection.  Which we're pretty sure you don't want." % kwarg)
        return self._impl(cls).delete_many(filter, *args, **kwargs)

    def find_by(self, cls, **kwargs):
        return self.find(cls, kwargs)

    def count(self, cls):
        return self._impl(cls).estimated_document_count()

    def create_index(self, cls, fields, **kwargs):
        index_fields = fixup_index(fields)
        return self._impl(cls).create_index(index_fields, **kwargs)

    def ensure_index(self, cls, fields, **kwargs):
        return self.create_index(cls, fields, **kwargs)

    def ensure_indexes(self, cls):
        for idx in cls.m.indexes:
            self.create_index(cls, idx.index_spec, background=True, **idx.index_options)

    def aggregate(self, cls, *args, **kwargs):
        return self._impl(cls).aggregate(*args, **kwargs)

    def distinct(self, cls, *args, **kwargs):
        return self._impl(cls).distinct(*args, **kwargs)

    def update_partial(self, cls, spec, fields, upsert=False, **kw):
        multi = kw.pop('multi', False)
        if multi is True:
            return self._impl(cls).update_many(spec, fields, upsert, **kw)
        return self._impl(cls).update_one(spec, fields, upsert, **kw)

    def find_one_and_update(self, cls, *args, **kwargs):
        return self._impl(cls).find_one_and_update(*args, **kwargs)

    def find_one_and_replace(self, cls, *args, **kwargs):
        return self._impl(cls).find_one_and_replace(*args, **kwargs)

    def find_one_and_delete(self, cls, *args, **kwargs):
        return self._impl(cls).find_one_and_delete(*args, **kwargs)

    def _prep_save(self, doc, validate):
        hook = doc.m.before_save
        if hook: hook(doc)
        if validate:
            if doc.m.schema is None:
                data = dict(doc)
            else:
                data = doc.m.schema.validate(doc)
            doc.update(data)
        else:
            data = dict(doc)
        return data

    @annotate_doc_failure
    def save(self, doc, *args, **kwargs) -> bson.ObjectId:
        """
        Can either

            args
                   N            Y   
           |---------------------------|
   _id  N  |   insert     |   raise    |
           |---------------------------|
        Y  |   replace    |   update   |
           |---------------------------|
        """
        data = self._prep_save(doc, kwargs.pop('validate', True))

        # if _id is None:
        #     doc.pop('_id', None)

        new_id = None
        if args:
            if '_id' in doc:
                arg_data = {arg: data[arg] for arg in args}
                result = self._impl(doc).update_one(
                    dict(_id=doc._id), {'$set': arg_data},
                    **fix_write_concern(kwargs)
                )
            else:
                raise ValueError('Cannot save a subset without an _id')
        else:
            if '_id' in doc:
                result = self._impl(doc).replace_one(
                    dict(_id=doc._id), data,
                    upsert=True, **fix_write_concern(kwargs)
                )
                new_id = result.upserted_id
            else:
                result = self._impl(doc).insert_one(
                    data, **fix_write_concern(kwargs)
                )
                new_id = result.inserted_id
            if result and ('_id' not in doc) and (new_id is not None):
                doc._id = new_id

        return result

    @annotate_doc_failure
    def insert(self, doc, **kwargs):
        data = self._prep_save(doc, kwargs.pop('validate', True))
        bson = self._impl(doc).insert_one(data, **fix_write_concern(kwargs))
        if bson and '_id' not in doc:
            doc._id = bson
        return bson

    @annotate_doc_failure
    def upsert(self, doc, spec_fields, **kwargs):
        self._prep_save(doc, kwargs.pop('validate', True))
        if type(spec_fields) != list:
            spec_fields = [spec_fields]
        return self._impl(doc).update_one({k:doc[k] for k in spec_fields},
                               {'$set': doc},
                               upsert=True)

    @annotate_doc_failure
    def delete(self, doc):
        return self._impl(doc).delete_one({'_id':doc._id})

    def _set(self, doc, key_parts, value):
        if len(key_parts) == 0:
            return
        elif len(key_parts) == 1:
            doc[key_parts[0]] = value
        else:
            self._set(doc[key_parts[0]], key_parts[1:], value)

    @annotate_doc_failure
    def set(self, doc, fields_values):
        """
        sets a key/value pairs, and persists those changes to the datastore
        immediately
        """
        fields_values = Object.from_bson(fields_values)
        for k,v in fields_values.items():
            self._set(doc, k.split('.'), v)
        impl = self._impl(doc)
        return impl.update_one({'_id':doc._id}, {'$set':fields_values})

    @annotate_doc_failure
    def increase_field(self, doc, **kwargs):
        """
        usage: increase_field(key=value)
        Sets a field to value, only if value is greater than the current value
        Does not change it locally
        """
        key = list(kwargs.keys())[0]
        value = kwargs[key]
        if value is None:
            raise ValueError(f"{key}={value}")

        if key not in doc:
            self._impl(doc).update_one(
                {'_id': doc._id, key: None},
                {'$set': {key: value}}
            )
        self._impl(doc).update_one(
            {'_id': doc._id, key: {'$lt': value}},
            # failed attempt at doing it all in one operation
            #{'$where': "this._id == '%s' && (!(%s in this) || this.%s < '%s')"
            #    % (doc._id, key, key, value)},
            {'$set': {key: value}},
        )

    def index_information(self, cls):
        return self._impl(cls).index_information()

    def drop_indexes(self, cls):
        try:
            return self._impl(cls).drop_indexes()
        except:
            pass
