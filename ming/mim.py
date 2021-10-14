'''mim.py - Mongo In Memory - stripped-down version of mongo that is
non-persistent and hopefully much, much faster
'''
import re
import sys
import time
import itertools
from itertools import chain
import collections
import logging
import warnings
from datetime import datetime
from hashlib import md5
from functools import cmp_to_key

import pickle

try:
    import spidermonkey
    from spidermonkey import Runtime
except ImportError:
    Runtime = None

from ming import compat
from ming.utils import LazyProperty

import bson
import six
from pymongo import database, collection, ASCENDING, MongoClient, UpdateOne
from pymongo.errors import InvalidOperation, OperationFailure, DuplicateKeyError
from pymongo.results import DeleteResult, UpdateResult, InsertManyResult, InsertOneResult

log = logging.getLogger(__name__)


class Connection(object):
    _singleton = None

    @classmethod
    def get(cls):
        if cls._singleton is None:
            cls._singleton = cls()
        return cls._singleton

    def __init__(self):
        self._databases = {}

        # Clone defaults from a MongoClient instance.
        mongoclient = MongoClient()
        self.read_preference = mongoclient.read_preference
        self.write_concern = mongoclient.write_concern
        self.codec_options = mongoclient.codec_options
        self.read_concern = getattr(mongoclient, 'read_concern', None)

    def drop_all(self):
        self._databases = {}

    def clear_all(self):
        '''Remove all data, but keep the indexes'''
        for db in self._databases.values():
            db.clear()

    def _make_database(self):
        return Database(self)

    def __getattr__(self, name):
        return self[name]

    def __getitem__(self, name):
        return self._get(name)

    def _get(self, name):
        try:
            return self._databases[name]
        except KeyError:
            db = self._databases[name] = Database(self, name)
            return db

    def database_names(self):
        return self._databases.keys()

    def drop_database(self, name):
        try:
            del self._databases[name]
        except KeyError:
            # Mongodb does not complain when dropping a non existing DB.
            pass

    def __repr__(self):
        return 'mim.Connection()'

    def _ensure_connected(self):
        # For pymongo 2.7 compatibility
        return True

    def _is_writable(self):
        return True


class Database(database.Database):
    def __init__(self, client, name, **__):
        super(Database, self).__init__(client, name)
        self._name = name
        self._client = client
        self._collections = {}
        if Runtime is not None:
            self._jsruntime = Runtime()
        else:
            self._jsruntime = None

    def __repr__(self):
        return "mim.Database(%r, %r)" % (self.__client, self.__name)

    def with_options(self, codec_options=None, read_preference=None, write_concern=None, read_concern=None):
        # options have no meaning for MIM
        return self

    @property
    def name(self):
        return self._name

    @property
    def connection(self):
        return self._client

    @property
    def client(self):
        return self._client

    def _make_collection(self):
        return Collection(self)

    def command(self, command,
                value=1, check=True, allowable_errors=None, **kwargs):
        if isinstance(command, six.string_types):
            command = {command:value}
            command.update(**kwargs)
        if 'filemd5' in command:
            checksum = md5()
            for chunk in self.chef.file.chunks.find().sort('n'):
                checksum.update(chunk['data'])
            return dict(md5=checksum.hexdigest())
        elif 'findandmodify' in command:
            coll = self._collections[command['findandmodify']]
            before = coll.find_one(command['query'], sort=command.get('sort'))
            upsert = False
            if before is None:
                upsert = True
                if command.get('upsert'):
                    before = dict(command['query'])
                    coll.insert(before)
                else:
                    raise OperationFailure('No matching object found')
            coll.update(command['query'], command['update'])
            if command.get('new', False) or upsert:
                return dict(value=coll.find_one(dict(_id=before['_id'])))
            return dict(value=before)
        elif 'mapreduce' in command:
            collection = command.pop('mapreduce')
            return self._handle_mapreduce(collection, **command)
        elif 'distinct' in command:
            collection = self._collections[command['distinct']]
            key = command['key']
            filter = command.get('filter')
            all_vals = chain.from_iterable(_lookup(d, key) for d in collection.find(filter=filter))
            return sorted(set(all_vals))
        elif 'getlasterror' in command:
            return dict(connectionId=None, err=None, n=0, ok=1.0)
        elif 'collstats' in command:
            collection = self._collections[command['collstats']]

            # We simulate everything based on the first object size,
            # doesn't probably make sense to go through all the objects to compute this.
            # Also instead of evaluating their in-memory size we use pickle
            # as python stores references.
            first_object_size = len(pickle.dumps(next(iter(collection._data.values()), {})))
            return {
                "ns": '%s.%s' % (collection.database.name, collection.name),
                "count": len(collection._data),
                "size": first_object_size * len(collection._data),
                "avgObjSize": first_object_size,
                "storageSize": first_object_size * len(collection._data)
            }
        else:
            raise NotImplementedError(repr(command))

    def _handle_mapreduce(self, collection,
                          query=None, map=None, reduce=None, out=None, finalize=None):
        if self._jsruntime is None:
            raise ImportError('Cannot import spidermonkey, required for MIM mapreduce')
        j = self._jsruntime.new_context()
        tmp_j = self._jsruntime.new_context()
        temp_coll = collections.defaultdict(list)
        def emit(k, v):
            k = topy(k)
            if isinstance(k, dict):
                k = bson.BSON.encode(k)
            temp_coll[k].append(v)
        def emit_reduced(k, v):
            print(k,v)
        # Add some special MongoDB functions
        j.execute('var NumberInt = Number;')
        j.add_global('emit', emit)
        j.add_global('emit_reduced', emit_reduced)
        j.execute('var map=%s;' % map)
        j.execute('var reduce=%s;' % reduce)
        if finalize:
            j.execute('var finalize=%s;' % finalize)
        if query is None: query = {}
        # Run the map phase
        def topy(obj):
            if isinstance(obj, spidermonkey.Array):
                return [topy(x) for x in obj]
            if isinstance(obj, spidermonkey.Object):
                tmp_j.add_global('x', obj)
                js_source = tmp_j.execute('x.toSource()')
                if js_source.startswith('(new Date'):
                    # Date object by itself
                    obj = datetime.fromtimestamp(tmp_j.execute('x.valueOf()')/1000.)
                elif js_source.startswith('({'):
                    # Handle recursive conversion in case we got back a
                    # mapping with multiple values.
                    # spidermonkey changes all js number strings to int/float
                    # changing back to string here for key protion, since bson requires it
                    obj = dict((str(a), topy(obj[a])) for a in obj)
                else:
                    assert False, 'Cannot convert %s to Python' % (js_source)
            elif isinstance(obj, collections.Mapping):
                return dict((k, topy(v)) for k,v in six.iteritems(obj))
            elif isinstance(obj, six.string_types):
                return obj
            elif isinstance(obj, collections.Sequence):
                return [topy(x) for x in obj]
            return obj
        def tojs(obj):
            if isinstance(obj, six.string_types):
                return obj
            elif isinstance(obj, datetime):
                ts = 1000. * time.mktime(obj.timetuple())
                ts += (obj.microsecond / 1000.)
                return j.execute('new Date(%f)' % (ts))
            elif isinstance(obj, collections.Mapping):
                return dict((k,tojs(v)) for k,v in six.iteritems(obj))
            elif isinstance(obj, collections.Sequence):
                result = j.execute('new Array()')
                for v in obj:
                    result.push(tojs(v))
                return result
            else: return obj
        for obj in self._collections[collection].find(query):
            obj = tojs(obj)
            j.execute('map').apply(obj)
        # Run the reduce phase
        reduced = topy(dict(
            (k, j.execute('reduce')(k, tojs(values)))
            for k, values in six.iteritems(temp_coll)))
        # Run the finalize phase
        if finalize:
            reduced = topy(dict(
                (k, j.execute('finalize')(k, tojs(value)))
                for k, value in six.iteritems(reduced)))
        # Handle the output phase
        result = dict()
        assert len(out) == 1
        if out.keys() == ['reduce']:
            result['result'] = out.values()[0]
            out_coll = self[out.values()[0]]
            for k, v in six.iteritems(reduced):
                doc = out_coll.find_one(dict(_id=k))
                if doc is None:
                    out_coll.insert(dict(_id=k, value=v))
                else:
                    doc['value'] = topy(j.execute('reduce')(k, tojs([v, doc['value']])))
                    out_coll.save(doc)
        elif out.keys() == ['merge']:
            result['result'] = out.values()[0]
            out_coll = self[out.values()[0]]
            for k, v in six.iteritems(reduced):
                out_coll.save(dict(_id=k, value=v))
        elif out.keys() == ['replace']:
            result['result'] = out.values()[0]
            self._collections.pop(out.values()[0], None)
            out_coll = self[out.values()[0]]
            for k, v in six.iteritems(reduced):
                out_coll.save(dict(_id=k, value=v))
        elif out.keys() == ['inline']:
            result['results'] = [
                dict(_id=k, value=v)
                for k,v in six.iteritems(reduced) ]
        else:
            raise TypeError('Unsupported out type: %s' % out.keys())
        return result


    def __getattr__(self, name):
        return self[name]

    def __getitem__(self, name):
        return self._get(name)

    def _get(self, name):
        try:
            return self._collections[name]
        except KeyError:
            db = self._collections[name] = Collection(self, name)
            return db

    def __repr__(self):
        return 'mim.Database(%s)' % self.name

    def collection_names(self):
        return self._collections.keys()

    def drop_collection(self, name):
        del self._collections[name]

    def clear(self):
        for coll in self._collections.values():
            coll.clear()


class Collection(collection.Collection):
    def __init__(self, database, name):
        super(Collection, self).__init__(database, name)
        self._name = name
        self._database = database
        self._data = {}
        self._unique_indexes = {}  # name -> doc index {key_values -> _id}
        self._indexes = {}  # name -> dict of details (including 'key' entry)

    def __repr__(self):
        return "mim.Collection(%r, %r)" % (self._database, self.__name)

    def clear(self):
        self._data = {}
        for ui in self._unique_indexes.values():
            ui.clear()

    @property
    def name(self):
        return self._name

    @property
    def database(self):
        return self._database

    def with_options(self, codec_options=None, read_preference=None, write_concern=None, read_concern=None):
        # options have no meaning for MIM
        return self

    def drop(self):
        self._database.drop_collection(self._name)

    def __getattr__(self, name):
        return self._database['%s.%s' % (self.name, name)]

    def _find(self, spec, sort=None, **kwargs):
        bson_safe(spec)
        def _gen():
            for doc in six.itervalues(self._data):
                mspec = match(spec, doc)
                if mspec is not None: yield doc, mspec
        return _gen()

    def find(self, filter=None, projection=None, skip=0, limit=0, **kwargs):
        if filter is None:
            filter = {}
        cur = Cursor(collection=self, projection=projection, limit=limit, skip=skip,
                     _iterator_gen=lambda: self._find(filter, **kwargs))
        sort = kwargs.get('sort')
        if sort:
            cur = cur.sort(sort)
        return cur

    def find_one(self, filter_or_id=None, *args, **kwargs):
        if filter_or_id is not None and not isinstance(filter_or_id, dict):
            filter_or_id = {"_id": filter_or_id}
        for result in self.find(filter_or_id, *args, **kwargs):
            return result
        return None

    def __find_and_modify(self, query=None, update=None, fields=None,
                          upsert=False, remove=False, **kwargs):
        if query is None: query = {}
        before = self.find_one(query, sort=kwargs.get('sort'))
        upserted = False
        if before is None:
            upserted = True
            if upsert:
                result = self.__update(query, update, upsert=True)
                query = {'_id': result['upserted']}
            else:
                return None

        before = self.find_one(query, sort=kwargs.get('sort'))

        if remove:
            self.__remove({'_id': before['_id']})
        elif not upserted:
            self.__update({'_id': before['_id']}, update)

        return_new = kwargs.get('new', False)
        if return_new:
            return self.find_one(dict(_id=before['_id']), fields)
        elif upserted:
            return None
        else:
            return Projection(fields).apply(before)

    def find_and_modify(self, query=None, update=None, fields=None,
                        upsert=False, remove=False, **kwargs):
        warnings.warn('find_and_modify is now deprecated, please use find_one_and_delete, '
                      'find_one_and_replace, find_one_and_update)', DeprecationWarning)
        return self.__find_and_modify(query, update, fields, upsert, remove, **kwargs)

    def find_one_and_delete(self, filter, projection=None, sort=None, **kwargs):
        return self.__find_and_modify(filter, fields=projection, remove=True, sort=sort, **kwargs)

    def find_one_and_replace(self, filter, replacement, projection=None, sort=None,
                             return_document=False, **kwargs):
        # ReturnDocument.BEFORE -> False
        # ReturnDocument.AFTER -> True
        return self.__find_and_modify(filter, update=replacement, fields=projection,
                                      sort=sort, new=return_document, **kwargs)

    def find_one_and_update(self, filter, update, projection=None, sort=None,
                            return_document=False, **kwargs):
        # ReturnDocument.BEFORE -> False
        # ReturnDocument.AFTER -> True
        return self.__find_and_modify(filter, update=update, fields=projection,
                                      sort=sort, new=return_document, **kwargs)

    def count(self, filter=None, **kwargs):
        return self.find(filter, **kwargs).count()

    def __insert(self, doc_or_docs, manipulate=True, **kwargs):
        result = []
        if not isinstance(doc_or_docs, list):
            doc_or_docs = [ doc_or_docs ]
        for doc in doc_or_docs:
            if not manipulate:
                doc = bcopy(doc)
            bson_safe(doc)
            _id = doc.get('_id', ())
            if _id == ():
                _id = doc['_id'] = bson.ObjectId()
            result.append(_id)
            if _id in self._data:
                if kwargs.get('w', 1):
                    raise DuplicateKeyError('duplicate ID on insert')
                continue
            self._index(doc)
            self._data[_id] = bcopy(doc)
        return result

    def insert(self, doc_or_docs, manipulate=True, **kwargs):
        warnings.warn('insert is now deprecated, please use insert_one or insert_many', DeprecationWarning)
        return self.__insert(doc_or_docs, manipulate, **kwargs)

    def insert_one(self, document, session=None):
        result = self.__insert(document)
        if result:
            result = result[0]
        return InsertOneResult(result or None, True)

    def insert_many(self, documents, ordered=True, session=None):
        result = self.__insert(documents)
        return InsertManyResult(result, True)

    def save(self, doc, **kwargs):
        warnings.warn('save is now deprecated, please use insert_one or replace_one', DeprecationWarning)
        _id = doc.get('_id', ())
        if _id == ():
            return self.__insert(doc)
        else:
            self.__update({'_id':_id}, doc, upsert=True)
            return _id

    def replace_one(self, filter, replacement, upsert=False):
        return self._update(filter, replacement, upsert)

    def __update(self, spec, updates, upsert=False, multi=False):
        bson_safe(spec)
        bson_safe(updates)
        result = dict(
            connectionId=None,
            updatedExisting=False,
            err=None,
            ok=1.0,
            n=0,
            nModified=0
        )
        for doc, mspec in self._find(spec):
            self._deindex(doc)
            mspec.update(updates)
            self._index(doc)
            result['n'] += 1
            result['nModified'] += 1
            if not multi: break
        if result['n']:
            result['updatedExisting'] = True
            return result
        if upsert:
            doc = dict(spec)
            MatchDoc(doc).update(updates, upserted=upsert)
            _id = doc.get('_id', ())
            if _id == ():
                _id = doc['_id'] = bson.ObjectId()
            if _id in self._data:
                raise DuplicateKeyError('duplicate ID on upsert')
            self._index(doc)
            self._data[_id] = bcopy(doc)
            result['upserted'] = _id
            return result
        else:
            return result

    def update(self, spec, updates, upsert=False, multi=False):
        warnings.warn('update is now deprecated, please use update_many or update_one', DeprecationWarning)
        return self.__update(spec, updates, upsert, multi)

    def update_many(self, filter, update, upsert=False):
        result = self.__update(filter, update, upsert, multi=True)
        return UpdateResult(result, True)

    def update_one(self, filter, update, upsert=False):
        result = self.__update(filter, update, upsert, multi=False)
        return UpdateResult(result, True)

    def __remove(self, spec=None, **kwargs):
        result = dict(n=0)
        multi = kwargs.get('multi', True)
        if spec is None: spec = {}
        new_data = {}
        for id, doc in six.iteritems(self._data):
            if match(spec, doc) and (multi or result['n'] == 0):
                result['n'] += 1
                self._deindex(doc)
            else:
                new_data[id] = doc
        self._data = new_data
        return result

    def remove(self, spec=None, **kwargs):
        warnings.warn('remove is now deprecated, please use delete_many or delete_one', DeprecationWarning)
        self.__remove(spec, **kwargs)

    def delete_one(self, filter, session=None):
        res = self.__remove(filter, multi=False)
        return DeleteResult(res, True)

    def delete_many(self, filter, session=None):
        res = self.__remove(filter, multi=True)
        return DeleteResult(res, True)

    def list_indexes(self, session=None):
        return Cursor(self, lambda: self._indexes.values())

    def ensure_index(self, key_or_list, unique=False, cache_for=300,
                     name=None, **kwargs):
        if isinstance(key_or_list, list):
            keys = tuple(tuple(k) for k in key_or_list)
        else:
            keys = ([key_or_list, ASCENDING],)
        if name:
            index_name = name
        else:
            index_name = '_'.join([k[0] for k in keys])
        self._indexes[index_name] = { "key": list(keys) }
        self._indexes[index_name].update(kwargs)
        if not unique: return
        self._indexes[index_name]['unique'] = True
        self._unique_indexes[index_name] = docindex = {}

        # update the document index with any existing records
        for id, doc in six.iteritems(self._data):
            key_values = self._extract_index_key(doc, keys)
            docindex[key_values] = id

        return index_name

    # ensure_index is now deprecated.
    def create_index(self, keys, **kwargs):
        return self.ensure_index(keys, **kwargs)

    def index_information(self):
        return dict(
            (index_name, fields)
            for index_name, fields in six.iteritems(self._indexes))

    def drop_index(self, iname):
        self._indexes.pop(iname, None)
        self._unique_indexes.pop(iname, None)

    def drop_indexes(self):
        for iname in list(self._indexes.keys()):
            self.drop_index(iname)

    def _get_wc_override(self):
        '''For gridfs compatibility'''
        return {}

    def __repr__(self):
        return 'mim.Collection(%r, %s)' % (self._database, self.name)

    def _extract_index_key(self, doc, keys):
        key_values = list()
        for key in keys:
            sub, key = _traverse_doc(doc, key[0])
            key_values.append(sub.get(key, None))
        return bson.BSON.encode({'k': key_values})

    _null_index_key = bson.BSON.encode({'k': [None]})

    def _index(self, doc):
        if '_id' not in doc: return
        for iname, docindex in six.iteritems(self._unique_indexes):
            idx_info = self._indexes[iname]
            key_values = self._extract_index_key(doc, idx_info['key'])
            if idx_info.get('sparse') and key_values == self._null_index_key:
                continue
            old_id = docindex.get(key_values, ())
            if old_id == doc['_id']: continue
            if old_id in self._data:
                raise DuplicateKeyError('%r: %s' % (self, idx_info))
            docindex[key_values] = doc['_id']

    def _deindex(self, doc):
        for iname, docindex in six.iteritems(self._unique_indexes):
            keys = self._indexes[iname]['key']
            key_values = self._extract_index_key(doc, keys)
            docindex.pop(key_values, None)

    def map_reduce(self, map, reduce, out, full_response=False, **kwargs):
        if isinstance(out, six.string_types):
            out = { 'replace':out }
        cmd_args = {'mapreduce': self.name,
                    'map': map,
                    'reduce': reduce,
                    'out': out,
                    }
        cmd_args.update(kwargs)
        return self.database.command(cmd_args)

    def distinct(self, key, filter=None, **kwargs):
        return self.database.command({'distinct': self.name,
                                      'key': key,
                                      'filter': filter})

    def bulk_write(self, requests, ordered=True,
                   bypass_document_validation=False):
        for step in requests:
            if isinstance(step, UpdateOne):
                self.update_one(step._filter, step._doc, upsert=step._upsert)
            else:
                raise NotImplementedError(
                    "MIM currently doesn't support %s operations" % type(step)
                )

    def aggregate(self, pipeline, **kwargs):
        steps = {}
        for step in pipeline:
            if set(step.keys()) & set(steps.keys()):
                raise ValueError(
                    'MIM currently supports a single step per type. Duplicate %s' % step
                )
            if set(step.keys()) - {'$match', '$project', '$sort', '$limit'}:
                raise ValueError(
                    'MIM currently only supports $match,$project,$sort,$limit steps.'
                )
            steps.update(step)

        sort = steps.get('$sort', None)
        if isinstance(sort, (bson.SON, dict)):
            sort = list(sort.items())
        return self.find(filter=steps.get('$match', {}),
                         sort=sort,
                         projection=steps.get('$project', None),
                         limit=steps.get('$limit', None))


class Cursor(object):
    def __init__(self, collection, _iterator_gen,
                 sort=None, skip=None, limit=None, projection=None):
        if isinstance(projection, (tuple, list)):
            projection = dict((f, 1) for f in projection)

        self._collection = collection
        self._iterator_gen = _iterator_gen
        self._sort = sort
        self._skip = skip or None    # cope with 0 being passed.
        self._limit = limit or None  # cope with 0 being passed.
        self._projection = Projection(projection)
        self._safe_to_chain = True

    @LazyProperty
    def iterator(self):
        self._safe_to_chain = False
        # normally a (doc, match) tuple but could be a single doc (e.g. when gridfs indexes involved)
        result = (doc_match[0] if isinstance(doc_match, tuple) else doc_match
                  for doc_match in self._iterator_gen())
        if self._sort is not None:
            result = sorted(result, key=cmp_to_key(
                    cursor_comparator(self._sort)))
        if self._skip is not None:
            result = itertools.islice(result, self._skip, sys.maxsize)
        if self._limit is not None:
            result = itertools.islice(result, abs(self._limit))
        return iter(result)

    def clone(self, **overrides):
        result = Cursor(
            collection=self._collection,
            _iterator_gen=self._iterator_gen,
            sort=self._sort,
            skip=self._skip,
            limit=self._limit,
            projection=self._projection._projection,
        )
        for k,v in overrides.items():
            setattr(result, k, v)
        return result

    def rewind(self):
        if not self._safe_to_chain:
            del self.iterator
            self._safe_to_chain = True

    def count(self):
        return sum(1 for x in self._iterator_gen())

    def __getitem__(self, key):
        # Le *sigh* -- this is the only place apparently where pymongo *does*
        # clone
        clone = self.clone()
        if isinstance(key, slice):
            _clone = clone
            start, end = key.start, key.stop # step not supported
            if start is None:
                start = 0
            _clone = _clone.skip(start)
            if end is not None:
                _clone = _clone.limit(end-start)
            return _clone
        elif isinstance(key, int):
            return clone.skip(key).next()
        raise TypeError('indicies must be integers, not %s' % type(key))

    def __iter__(self):
        return self

    def next(self):
        value = six.next(self.iterator)
        value = bcopy(value)
        value = self._projection.apply(value)

        # mim doesn't currently do anything with codec_options, so this doesn't do anything currently
        # but leaving it here as a placeholder for the future - otherwise we should delete wrap_as_class()
        return wrap_as_class(value, self._collection.codec_options.document_class)

    __next__ = next

    def sort(self, key_or_list, direction=ASCENDING):
        if not self._safe_to_chain:
            raise InvalidOperation('cannot set options after executing query')
        if not isinstance(key_or_list, list):
            key_or_list = [ (key_or_list, direction) ]
        keys = []
        for t in key_or_list:
            if isinstance(t, tuple):
                keys.append(t)
            else:
                keys.append(t, ASCENDING)
        self._sort = keys
        return self # I'd rather clone, but that's not what pymongo does here

    def all(self):
        return list(self.iterator)

    def skip(self, skip):
        if not self._safe_to_chain:
            raise InvalidOperation('cannot set options after executing query')
        self._skip = skip
        return self # I'd rather clone, but that's not what pymongo does here

    def limit(self, limit):
        if not self._safe_to_chain:
            raise InvalidOperation('cannot set options after executing query')
        self._limit = limit or None
        return self # I'd rather clone, but that's not what pymongo does here

    def distinct(self, key):
        all_vals = chain.from_iterable(_lookup(d, key) for d in self.all())
        return sorted(set(all_vals))

    def hint(self, index):
        # checks indexes, but doesn't actually use hinting
        if type(index) == list:
            test_idx = [(i, direction) for i, direction in index if i != '$natural']
            values = [[k for k in i["key"]] for i in self._collection._indexes.values()]
            if test_idx and test_idx not in values:
                raise OperationFailure('database error: bad hint. Valid values: %s' % values)
        elif isinstance(index, six.string_types):
            if index not in self._collection._indexes.keys():
                raise OperationFailure('database error: bad hint. Valid values: %s'
                        % self._collection._indexes.keys())
        elif index == None:
            pass
        else:
            raise TypeError('hint index should be string, list of tuples, or None, but was %s' % type(index))
        return self

    def add_option(self, *args, **kwargs):
        # Adding options to MIM does nothing.
        pass

    def close(self):
        self._iterator_gen = lambda: iter(())


def cursor_comparator(keys):
    def comparator(a, b):
        for k,d in keys:
            x = list(_lookup(a, k, None))
            y = list(_lookup(b, k, None))
            part = BsonArith.cmp(x, y)
            if part: return part * d
        return 0
    return comparator


class BsonArith(object):
    _types = None
    _index = None

    @classmethod
    def cmp(cls, x, y):
        x_bson = cls.to_bson(x)
        y_bson = cls.to_bson(y)

        if len(x_bson) != len(y_bson):
            return compat.base_cmp(len(x_bson),
                                   len(y_bson))

        for index, a_val in enumerate(x_bson):
            b_val = y_bson[index]
            if hasattr(a_val, 'get') and hasattr(b_val, 'get'):
                return compat.dict_cmp(a_val, b_val)
            if a_val != b_val:
                return compat.base_cmp(a_val, b_val)
        return 0

    @classmethod
    def to_bson(cls, val):
        if val is (): return val
        tp = cls.bson_type(val)
        return (tp, cls._types[tp][0](val))

    @classmethod
    def bson_type(cls, value):
        if cls._index is None:
            cls._build_index()
        tp = cls._index.get(type(value), None)
        if tp is not None: return tp
        for tp, (conv, types) in enumerate(cls._types):
            if isinstance(value, tuple(types)):
                cls._index[type(value)] = tp
                return tp
        raise KeyError(type(value))

    @classmethod
    def _build_index(cls):
        cls._build_types()
        cls._index = {}
        for tp, (conv, types) in enumerate(cls._types):
            for t in types:
                cls._index[t] = tp

    @classmethod
    def _build_types(cls):
        # this is a list of conversion functions, and the types they apply to
        cls._types = [
            (lambda x:x, [ type(None) ]),
            (lambda x:x, [ int ] + list(six.integer_types)),
            (lambda x:x, list(set([str, six.text_type]))),
            (lambda x:{k: cls.to_bson(v) for k, v in six.iteritems(x)}, [ dict, MatchDoc ]),
            (lambda x:list(cls.to_bson(i) for i in x), [ list, MatchList ]),
            (lambda x:x, [ tuple ]),
            (lambda x:x, [ bson.Binary ]),
            (lambda x:x, [ bson.ObjectId ]),
            (lambda x:x, [ bool ]),
            (lambda x:x, [ datetime ]),
            (lambda x:x, [ bson.Regex ] ),
            (lambda x:x, [ float ]),
        ]


def match(spec, doc):
    """Checks if the given ``doc`` matches the provided ``spec``.

    This is used by find and other places in need of checking
    documents against a provided filter.

    Returns the ``MatchDoc`` instance for the matching document
    or ``None`` if the document doesn't match.
    """
    spec = bcopy(spec)
    if '$or' in spec:
        if any(match(branch, doc) for branch in spec.pop('$or')):
            return match(spec, doc)
        return None
    mspec = MatchDoc(doc)
    try:
        for k,v in six.iteritems(spec):
            subdoc, subdoc_key = mspec.traverse(*k.split('.'))
            for op, value in _parse_query(v):
                if not subdoc.match(subdoc_key, op, value):
                    return None
    except KeyError:
        raise
    return mspec


class Match(object):
    """Foundation for ``MatchDoc`` and ``MatchList``.

    Provides all the functions that you might need to apply
    against a document or a list of documents to check if
    it matches a query or to apply update operations to it
    in case of an update query.
    """
    def match(self, key, op, value):
        log.debug('match(%r, %r, %r, %r)',
                  self, key, op, value)
        val = self.get(key, ())
        if isinstance(val, MatchList):
            if val.match('$', op, value): return True
        if op == '$eq':
            if isinstance(value, (bson.RE_TYPE, bson.Regex)):
                return self._match_regex(value, val)
            return BsonArith.cmp(val, value) == 0
        if op == '$regex':
            if not isinstance(value, (bson.RE_TYPE, bson.Regex)):
                value = re.compile(value)
            return self._match_regex(value, val)
        if op == '$options':
            # $options is currently only correlated to $regex and is not a standalone operator
            # always True to prevent code that use for example case insensitive regex from failing
            # tests without any reason
            log.warn('$options not implemented')
            return True
        if op == '$ne': return BsonArith.cmp(val, value) != 0
        if op == '$gt': return BsonArith.cmp(val, value) > 0
        if op == '$gte': return BsonArith.cmp(val, value) >= 0
        if op == '$lt': return BsonArith.cmp(val, value) < 0
        if op == '$lte': return BsonArith.cmp(val, value) <= 0
        if op == '$in':
            for ele in value:
                if self.match(key, '$eq', ele):
                    return True
            return False
        if op == '$nin':
            for ele in value:
                if self.match(key, '$eq', ele):
                    return False
            return True
        if op == '$exists':
            if value: return val != ()
            else: return val == ()
        if op == '$all':
            for ele in value:
                if not self.match(key, '$eq', ele):
                    return False
            return True
        if op == '$elemMatch':
            if not isinstance(val, MatchList): return False
            for ele in val:
                m = match(value, ele)
                if m: return True
            return False
        if op == '$search':
            collection = get_collection_from_objectid(self['_id'])
            for _keys in (index['key'] for index in collection._indexes.values()):
                for field in (key[0] for key in _keys if key[1] == 'text'):
                    if value.lower() in self[field].lower():
                        return True
            return False
        raise NotImplementedError(op)

    def _match_regex(self, regex, val):
        if isinstance(regex, bson.Regex):
            regex = regex.try_compile()

        if isinstance(val, MatchList):
            for item in val:
                if bool(item not in (None, ()) and regex.search(item)):
                    return True
            return False
        return bool(val not in (None, ()) and regex.search(val))

    def getvalue(self, path):
        parts = path.split('.')
        subdoc, key = self.traverse(*parts)
        return subdoc[key]

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def update(self, updates, upserted=False):
        newdoc = {}
        for k, v in six.iteritems(updates):
            if k.startswith('$'): break
            newdoc[k] = bcopy(v)
        if newdoc:
            self._orig.clear()
            self._orig.update(bcopy(newdoc))
            return

        for op, update_parts in six.iteritems(updates):
            func = getattr(self, '_op_' + op[1:], None)
            if func is None:
                raise NotImplementedError(op)
            if getattr(func, 'upsert_only', False) and not upserted:
                continue
            for k,arg in update_parts.items():
                subdoc, key = self.traverse(k)
                func(subdoc, key, arg)
                if getattr(func, 'ensure_key', False):
                    # The function would create all intermediate subdocuments
                    # if they didn't exist already.
                    self._ensure_orig_key(k)

        validate(self._orig)

    def _ensure_orig_key(self, k):
        """Ensures that the path k leads to an existing subdocument in the matched document.

        While traversing a MatchDoc creates all the intermediate missing subdocuments
        in the MatchDoc._doc, the original document in the collection will still be lacking them.

        This ensures that all steps that lead to the path and were missing in the
        collection document are created and correspond to the objects that were created
        into the MatchDoc while traversing it, so that any change to the MatchDoc is reflected
        into the collection document too.
        """
        path = k.split('.')
        doc = self
        for step in path[:-1]:
            if isinstance(doc, MatchList):
                if step == '$':
                    return
                step = int(step)
            if step not in doc._orig:
                doc._orig[step] = doc._doc[step]._orig
            doc = doc._doc[step]

    def _op_inc(self, subdoc, key, arg):
        subdoc.setdefault(key, 0)
        subdoc[key] += arg
    _op_inc.ensure_key = True

    def _op_set(self, subdoc, key, arg):
        if isinstance(subdoc, list):
            key = int(key)
        subdoc[key] = bcopy(arg)
    _op_set.ensure_key = True

    def _op_setOnInsert(self, subdoc, key, arg):
        subdoc[key] = bcopy(arg)
    _op_setOnInsert.upsert_only = True
    _op_setOnInsert.ensure_key = True

    def _op_unset(self, subdoc, key, arg):
        try:
            del subdoc[key]
        except KeyError:
            # $unset should do nothing if the key doesn't exist.
            pass

    def _op_push(self, subdoc, key, arg):
        l = subdoc.setdefault(key, [])
        if isinstance(arg, dict) and "$each" in arg:
            args = arg.get("$each")
        else:
            args = [arg]
        for member in args:
            l.append(bcopy(member))
    _op_push.ensure_key = True

    def _op_pop(self, subdoc, key, arg):
        l = subdoc.setdefault(key, [])
        if arg == 1:
            del l[-1]
        else:
            del l[0]

    def _op_pushAll(self, subdoc, key, arg):
        l = subdoc.setdefault(key, [])
        l.extend(bcopy(arg))
    _op_pushAll.ensure_key = True

    def _op_addToSet(self, subdoc, key, arg):
        l = subdoc.setdefault(key, [])

        if isinstance(arg, dict) and "$each" in arg:
            args = arg.get("$each")
        else:
            args = [arg]
        for member in args:
            if member not in l:
                l.append(bcopy(member))
    _op_addToSet.ensure_key = True

    def _op_pull(self, subdoc, key, arg):
        l = subdoc.setdefault(key, [])
        if isinstance(arg, dict):
            subdoc[key] = [
                vv for vv in l
                if not match(arg, vv) ]
        else:
            subdoc[key] = [
                vv for vv in l
                if not compare('$eq', arg, vv) ]

    def _op_pullAll(self, subdoc, key, arg):
        l = subdoc.setdefault(key, [])
        subdoc[key] = [
            vv for vv in l
            if not compare('$in', vv, arg) ]


class MatchDoc(Match):
    """A document matching a specific query.

    A document that is a candidate for execution of a query,
    gets promoted to a ``MatchDoc``. This exposes enables the
    features needed to:

        - check if the document matches against the query
        - update the document if the query was an update one.
    """

    def __init__(self, doc):
        self._orig = doc
        self._doc = {}
        for k,v in six.iteritems(doc):
            if isinstance(v, list):
                self._doc[k] = MatchList(v)
            elif isinstance(v, dict):
                self._doc[k] = MatchDoc(v)
            else:
                self._doc[k] = v
    def traverse(self, first, *rest):
        """Resolves a parent.child.leaf path within the document.

        Returns a tuple ``(subdoc, leaf_key)`` where:

        - ``subdoc`` is a new ``MatchDoc`` for the deepest subdocument
          containing the leaf key expressed by the provided path.
        - ``leaf_key`` is the key of the field in ``subdoc``containing
          the value that the path provided leads to.

        For example in case of a document like::

            {
                'root': {
                    'subdoc': {
                        'value': 5
                    }
                }
            }

        The path ``root.subdoc.value`` will return a ``MatchDoc``
        for the subdocument ``root.subdoc`` as the ``subdoc`` and
        ``value`` as the ``leaf_key``.
        """
        if not rest:
            if '.' in first:
                return self.traverse(*(first.split('.')))
            return self, first
        if first not in self._doc:
            self._doc[first] = MatchDoc({})
        if self._doc[first] is None:
            return MatchDoc({}), None
        return self[first].traverse(*rest)
    def iteritems(self):
        return six.iteritems(self._doc)
    def items(self):
        return self.iteritems()
    def __eq__(self, o):
        return isinstance(o, MatchDoc) and self._doc == o._doc
    def __hash__(self):
        return hash(self._doc)
    def __repr__(self):
        return 'M%r' % (self._doc,)
    def __getitem__(self, key):
        return self._doc[key]
    def __delitem__(self, key):
        del self._doc[key]
        del self._orig[key]
    def __setitem__(self, key, value):
        self._doc[key] = value
        self._orig[key] = value
    def setdefault(self, key, default):
        self._doc.setdefault(key, default)
        return self._orig.setdefault(key, default)
    def keys(self):
        return self._doc.keys()


class MatchList(Match):
    """A List that is part of a document matching a query.

    A ``MatchDoc`` might contain lists within itself,
    all those lists will be wrapped in a ``MatchList``
    so their content can be traversed, tested against the filter
    and updated according to the query being executed.
    """
    def __init__(self, doc, pos=None):
        self._orig = doc
        self._doc = []
        for ele in doc:
            if isinstance(ele, list):
                self._doc.append(MatchList(ele))
            elif isinstance(ele, dict):
                self._doc.append(MatchDoc(ele))
            else:
                self._doc.append(ele)
        self._pos = pos
    def __iter__(self):
        return iter(self._doc)
    def traverse(self, first, *rest):
        if not rest:
            return self, first
        return self[first].traverse(*rest)
    def match(self, key, op, value):
        if key == '$':
            for i, item in enumerate(self._doc):
                if self.match(i, op, value):
                    if self._pos is None:
                        self._pos = i
                    return True
            return None
        try:
            m = super(MatchList, self).match(key, op, value)
            if m: return m
        except:
            pass
        for ele in self:
            if (isinstance(ele, Match)
                and ele.match(key, op, value)):
                return True

    def __eq__(self, o):
        return isinstance(o, MatchList) and self._doc == o._doc
    def __hash__(self):
        return hash(self._doc)
    def __repr__(self):
        return 'M<%r>%r' % (self._pos, self._doc)
    def __getitem__(self, key):
        try:
            if key == '$':
                if self._pos is None:
                    return self._doc[0]
                else:
                    return self._doc[self._pos]
            else:
                return self._doc[int(key)]
        except IndexError:
            raise KeyError(key)
    def __setitem__(self, key, value):
        if key == '$':
            key = self._pos
        if isinstance(self._doc, list):
            key = int(key)
        self._doc[key] = value
        self._orig[key] = value
    def __delitem__(self, key):
        if key == '$':
            key = self._pos
        del self._doc[int(key)]
        del self._orig[int(key)]
    def setdefault(self, key, default):
        if key == '$':
            key = self._pos
        if key <= len(self._orig):
            return self._orig[key]
        while key >= len(self._orig):
            self._doc.append(None)
            self._orig.append(None)
        self._doc[key] = default
        self._orig[key] = default


def _parse_query(v):
    if isinstance(v, dict) and all(k.startswith('$') for k in v.keys()):
        return v.items()
    else:
        return [('$eq', v)]

def _part_match(op, value, key_parts, doc, allow_list_compare=True):
    if not key_parts:
        return compare(op, doc, value)
    elif isinstance(doc, list) and allow_list_compare:
        for v in doc:
            if _part_match(op, value, key_parts, v, allow_list_compare=False):
                return True
        else:
            return False
    else:
        return _part_match(op, value, key_parts[1:], doc.get(key_parts[0], ()))

def _lookup(doc, k, default=()):
    try:
        k_parts = k.split('.')
        for i, part in enumerate(k_parts):
            if isinstance(doc, list):
                for item in doc:
                    remaining_parts = '.'.join(k_parts[i:])
                    yield from _lookup(item, remaining_parts, default)
                return
            else:
                doc = doc[part]
    except KeyError:
        if default != ():
            yield default
        raise
    yield doc

def compare(op, a, b):
    if op == '$gt': return BsonArith.cmp(a, b) > 0
    if op == '$gte': return BsonArith.cmp(a, b) >= 0
    if op == '$lt': return BsonArith.cmp(a, b) < 0
    if op == '$lte': return BsonArith.cmp(a, b) <= 0
    if op == '$eq':
        if hasattr(b, 'match'):
            return b.match(a)
        elif isinstance(a, list):
            if a == b: return True
            return b in a
        else:
            return a == b
    if op == '$ne': return a != b
    if op == '$in':
        if isinstance(a, list):
            for ele in a:
                if ele in b:
                    return True
            return False
        else:
            return a in b
    if op == '$nin': return a not in b
    if op == '$exists':
        return a != () if b else a == ()
    if op == '$all':
        return set(a).issuperset(b)
    if op == '$elemMatch':
        return match(b, a)
    raise NotImplementedError(op)

def validate(doc):
    for k,v in six.iteritems(doc):
        assert '$' not in k
        assert '.' not in k
        if hasattr(v, 'items'):
            validate(v)

def bson_safe(obj):
    bson.BSON.encode(obj)

def bcopy(obj):
    if isinstance(obj, dict):
        return bson.BSON.encode(obj).decode()
    elif isinstance(obj, list):
        return list(map(bcopy, obj))
    else:
        return obj

def wrap_as_class(value, as_class):
    if isinstance(value, dict):
        return as_class(dict(
                (k, wrap_as_class(v, as_class))
                for k,v in value.items()))
    elif isinstance(value, list):
        return [ wrap_as_class(v, as_class) for v in value ]
    else:
        return value


def _traverse_doc(doc, key):
    path = key.split('.')
    cur = doc
    for part in path[:-1]:
        cur = cur.setdefault(part, {})
    return cur, path[-1]


class Projection(object):
    """Applies a projection to a field matching a query.

    The projection is applied by the Cursor while consuming
    the iterator of documents matching the query.
    """
    def __init__(self, projection):
        self._projection = projection

    def apply(self, doc):
        if not self._projection:
            return doc

        if [_ for _ in self._projection.values() if _ in (1, True)]:
            # If at least one projected field was projected with 1/True
            # it means that the fields should be limited to the specified fields.
            # The ``_id`` is always there unless explicitly excluded.
            result = {'_id': doc['_id']}
        else:
            # Otherwise it means that we are excluding some fields (those at 0)
            # or applying projection operations and so all fields that are
            # not explicitly excluded should be projected.
            result = doc

        for name, value in self._projection.items():
            if value in (0, False):
                self._pop_key(result, name)
                continue

            if isinstance(value, dict):
                for projection_op, op_args in value.items():
                    if projection_op == '$slice':
                        v = op_args
                        if isinstance(v, list):
                            # skip and limit
                            l, key = _traverse_doc(doc, name)
                            l[key] = l[key][v[0]:v[0] + v[1]]
                        else:
                            l, key = _traverse_doc(doc, name)
                            if v < 0:
                                l[key] = l[key][v:]
                            else:
                                l[key] = l[key][:v]
                    elif projection_op == '$meta':
                        if op_args == 'textScore':
                            subdoc, subkey = _traverse_doc(doc, name)
                            subdoc[subkey] = 1.0  # Currently we always fake a 1.0 score.
                        else:
                            raise ValueError('Unsupported $meta projection %s' % op_args)
                    else:
                        raise ValueError('Unsupported projection operator %s' % projection_op)

            sub_doc, key = _traverse_doc(doc, name)
            sub_result, key = _traverse_doc(result, name)
            try:
                sub_result[key] = sub_doc[key]
            except KeyError:
                log.debug("Field %s doesn't in %s, skip..." % (key, sub_doc.keys()))
                pass
        return result

    def _pop_key(self, doc, key):
        """Removes a key or subkey from the document.

        The key can be in the form subdoc.subkey to
        remove a key from a subdocument.
        """
        path = key.split('.')
        cur = doc
        for step in path[:-1]:
            cur = cur[step]
        cur.pop(path[-1], None)


def get_collection_from_objectid(_id):
    for db in Connection.get()._databases.values():
        for collection in db._collections.values():
            if _id in collection._data:
                return collection
    return None

