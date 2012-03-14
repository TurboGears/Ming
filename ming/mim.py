'''mim.py - Mongo In Memory - stripped-down version of mongo that is
non-persistent and hopefully much, much faster
'''
import sys
import itertools
import collections
from datetime import datetime
from hashlib import md5

try:
    from spidermonkey import Runtime
except ImportError:
    Runtime = None

from ming.utils import LazyProperty

import bson
from pymongo.errors import InvalidOperation, OperationFailure, DuplicateKeyError
from pymongo import database, collection, ASCENDING

class Connection(object):
    _singleton = None

    @classmethod
    def get(cls):
        if cls._singleton is None:
            cls._singleton = cls()
        return cls._singleton

    def __init__(self):
        self._databases = {}

    def drop_all(self):
        self._databases = {}

    def clear_all(self):
        '''Remove all data, but keep the indexes'''
        for db in self._databases.values():
            db.clear()

    def end_request(self):
        pass

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
        del self._databases[name]

    def __repr__(self):
        return 'mim.Connection()'

class Database(database.Database):

    def __init__(self, connection, name):
        self._name = name
        self._connection = connection
        self._collections = {}
        if Runtime is not None:
            self._jsruntime = Runtime()
        else:
            self._jsruntime = None

    @property
    def name(self):
        return self._name

    @property
    def connection(self):
        return self._connection

    def _make_collection(self):
        return Collection(self)

    def command(self, command,
                value=1, check=True, allowable_errors=None, **kwargs):
        if isinstance(command, basestring):
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
                    raise OperationFailure, 'No matching object found'
            coll.update(command['query'], command['update'])
            if command.get('new', False) or upsert:
                return dict(value=coll.find_one(dict(_id=before['_id'])))
            return dict(value=before)
        elif 'mapreduce' in command:
            collection = command.pop('mapreduce')
            return self._handle_mapreduce(collection, **command)
        else:
            raise NotImplementedError, repr(command)

    def _handle_mapreduce(self, collection,
                          query=None, map=None, reduce=None, out=None):
        if self._jsruntime is None:
            raise ImportError, 'Cannot import spidermonkey, required for MIM mapreduce'
        j = self._jsruntime.new_context()
        temp_coll = collections.defaultdict(list)
        def emit(k, v):
            if isinstance(k, dict):
                k = bson.BSON.encode(k)
            temp_coll[k].append(v)
        def emit_reduced(k, v):
            print k,v 
        j.add_global('emit', emit)
        j.add_global('emit_reduced', emit_reduced)
        j.execute('var map=%s;' % map)
        j.execute('var reduce=%s;' % reduce)
        if query is None: query = {}
        # Run the map phase
        def tojs(obj):
            if isinstance(obj, basestring):
                return obj
            elif isinstance(obj, datetime):
                return j.execute('new Date("%s")' % obj.ctime())
            elif isinstance(obj, collections.Mapping):
                return dict((k,tojs(v)) for k,v in obj.iteritems())
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
        reduced = dict(
            (k, j.execute('reduce')(k, tojs(values)))
            for k, values in temp_coll.iteritems())
        # Handle the output phase
        result = dict()
        assert len(out) == 1
        if out.keys() == ['reduce']:
            result['result'] = out.values()[0]
            out_coll = self[out.values()[0]]
            for k, v in reduced.iteritems():
                doc = out_coll.find_one(dict(_id=k))
                if doc is None:
                    out_coll.insert(dict(_id=k, value=v))
                else:
                    doc['value'] = j.execute('reduce')(k, tojs([v, doc['value']]))
                    out_coll.save(doc)
        elif out.keys() == ['merge']:
            result['result'] = out.values()[0]
            out_coll = self[out.values()[0]]
            for k, v in reduced.iteritems():
                out_coll.save(dict(_id=k, value=v))
        elif out.keys() == ['replace']:
            result['result'] = out.values()[0]
            self._collections.pop(out.values()[0], None)
            out_coll = self[out.values()[0]]
            for k, v in reduced.iteritems():
                out_coll.save(dict(_id=k, value=v))
        elif out.keys() == ['inline']:
            result['results'] = [
                dict(_id=k, value=v)
                for k,v in reduced.iteritems() ]
        else:
            raise TypeError, 'Unsupported out type: %s' % out.keys()
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
        self._name = self.__name = name
        self._database = database
        self._data = {}
        self._unique_indexes = {}
        self._indexes = {}

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

    def drop(self):
        self._database.drop_collection(self._name)

    def __getattr__(self, name):
        return self._database['%s.%s' % (self.name, name)]

    def _find(self, spec, sort=None):
        bson_safe(spec)
        def _gen():
            for doc in self._data.itervalues():
                if match(spec, doc): yield doc
        return _gen()

    def find(self, spec=None, fields=None, as_class=dict, **kwargs):
        if spec is None:
            spec = {}
        sort = kwargs.pop('sort', None)
        cur = Cursor(lambda:self._find(spec, **kwargs), fields=fields, as_class=as_class)
        if sort:
            cur = cur.sort(sort)
        return cur

    def find_one(self, spec_or_id=None, *args, **kwargs):
        if spec_or_id is not None and not isinstance(spec_or_id, dict):
            spec_or_id = {"_id": spec_or_id}
        for result in self.find(spec_or_id, *args, **kwargs):
            return result
        return None

    def find_and_modify(self, query=None, update=None, upsert=False, **kwargs):
        if query is None: query = {}
        before = self.find_one(query, sort=kwargs.get('sort'))
        upserted = False
        if before is None:
            upserted = True
            if upsert:
                before = dict(query)
                self.insert(before)
            else:
                raise OperationFailure, 'No matching object found'
        self.update(query, update)
        if kwargs.get('new', False) or upserted:
            return self.find_one(dict(_id=before['_id']))
        return before

    def insert(self, doc_or_docs, safe=False):
        if not isinstance(doc_or_docs, list):
            doc_or_docs = [ doc_or_docs ]
        for doc in doc_or_docs:
            bson_safe(doc)
            _id = doc.get('_id', ())
            if _id == ():
                _id = doc['_id'] = bson.ObjectId()
            if _id in self._data:
                if safe: raise OperationFailure('duplicate ID on insert')
                continue
            self._index(doc)
            self._data[_id] = bcopy(doc)
        return _id

    def save(self, doc, safe=False):
        _id = doc.get('_id', ())
        if _id == ():
            return self.insert(doc, safe=safe)
        else:
            self.update({'_id':_id}, doc, upsert=True, safe=safe)
            return _id

    def update(self, spec, document, upsert=False, safe=False, multi=False):
        bson_safe(spec)
        bson_safe(document)
        updated = False
        for doc in self._find(spec):
            self._deindex(doc) 
            update(doc, document)
            self._index(doc) 
            updated = True
            if not multi: break
        if updated: return
        if upsert:
            doc = dict(spec)
            doc.update(document)
            _id = doc.get('_id', ())
            if _id == ():
                _id = doc['_id'] = bson.ObjectId()
            self._index(doc) 
            self._data[_id] = bcopy(doc)
            return _id

    def remove(self, spec=None, **kwargs):
        if spec is None: spec = {}
        new_data = {}
        for id, doc in self._data.iteritems():
            if match(spec, doc):
                self._deindex(doc)
            else:
                new_data[id] = doc
        self._data = new_data

    def ensure_index(self, key_or_list, unique=False, ttl=300,
                     name=None, background=None, sparse=False):
        if isinstance(key_or_list, list):
            keys = tuple(k[0] for k in key_or_list)
        else:
            keys = (key_or_list,)
        index_name = '_'.join(keys)
        self._indexes[index_name] =[ (k, 0) for k in keys ]
        if not unique: return
        self._unique_indexes[keys] = index = {}
        for id, doc in self._data.iteritems():
            key_values = tuple(doc.get(key, None) for key in keys)
            index[key_values] =id
        return index_name

    def index_information(self):
        return dict(
            (index_name, dict(key=fields))
            for index_name, fields in self._indexes.iteritems())

    def drop_index(self, iname):
        index = self._indexes.pop(iname, None)
        if index is None: return
        keys = tuple(i[0] for i in index)
        self._unique_indexes.pop(keys, None)

    def __repr__(self):
        return 'mim.Collection(%r, %s)' % (self._database, self.name)

    def _index(self, doc):
        if '_id' not in doc: return
        for keys, index in self._unique_indexes.iteritems():
            key_values = tuple(doc.get(key, None) for key in keys)
            old_id = index.get(key_values, ())
            if old_id == doc['_id']: continue
            if old_id in self._data:
                raise DuplicateKeyError, '%r: %s' % (self, keys)
            index[key_values] = doc['_id']

    def _deindex(self, doc):
        for keys, index in self._unique_indexes.iteritems():
            key_values = tuple(doc.get(key, None) for key in keys)
            index.pop(key_values, None)

class Cursor(object):

    def __init__(self, iterator_gen, sort=None, skip=None, limit=None, fields=None, as_class=dict):
        self._iterator_gen = iterator_gen
        self._sort = sort
        self._skip = skip
        self._limit = limit
        self._fields = fields
        self._as_class = as_class
        self._safe_to_chain = True

    @LazyProperty
    def iterator(self):
        self._safe_to_chain = False
        result = self._iterator_gen()
        if self._sort is not None:
            result = sorted(result, cmp=cursor_comparator(self._sort))
        if self._skip is not None:
            result = itertools.islice(result, self._skip, sys.maxint)
        if self._limit is not None:
            result = itertools.islice(result, abs(self._limit))
        return iter(result)

    def count(self):
        return sum(1 for x in self._iterator_gen())

    def __iter__(self):
        return self

    def next(self):
        value = self.iterator.next()
        value = bcopy(value)
        if self._fields:
            value = dict((k, value[k]) for k in self._fields)
        return wrap_as_class(value, self._as_class)

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
        return self

    def all(self):
        return list(self._iterator_gen())

    def skip(self, skip):
        if not self._safe_to_chain:
            raise InvalidOperation('cannot set options after executing query')
        self._skip = skip
        return self

    def limit(self, limit):
        if not self._safe_to_chain:
            raise InvalidOperation('cannot set options after executing query')
        self._limit = limit
        return self

def cursor_comparator(keys):
    def comparator(a, b):
        for k,d in keys:
            part = cmp(_lookup(a, k, None), _lookup(b, k, None))
            if part: return part * d
        return 0
    return comparator

def match(spec, doc):
    '''TODO:
    currently this should match, but it doesn't:
    match({'tags.tag':'test'}, {'tags':[{'tag':'test'}]})
    '''
    try:
        for k,v in spec.iteritems():
            if k == '$or':
                if not isinstance(spec[k], list):
                    raise InvalidOperation('$or clauses must be provided in a list')
                for query in v:
                    if match(query, doc): break
                else:
                    return False
            else:
                op, value = _parse_query(v)
                if not _part_match(op, value, k.split('.'), doc):
                    return False
    except (AttributeError, KeyError), ex:
        return False
    return True

def _parse_query(v):
    if isinstance(v, dict) and len(v) == 1 and v.keys()[0].startswith('$'):
        return v.keys()[0], v.values()[0]
    else:
        return '$eq', v

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
        for part in k.split('.'):
            doc = doc[part]
    except KeyError:
        if default != (): return default
        raise
    return doc

def compare(op, a, b):
    if op == '$gt': return a > b
    if op == '$gte': return a >= b
    if op == '$lt': return a < b
    if op == '$lte': return a <= b
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
    raise NotImplementedError, op
        
def update(doc, updates):
    newdoc = {}
    for k, v in updates.iteritems():
        if k.startswith('$'): continue
        newdoc[k] = bcopy(v)
    if newdoc:
        doc.clear()
        doc.update(newdoc)
    for k, v in updates.iteritems():
        if k == '$inc':
            for kk, vv in v.iteritems():
                doc[kk] = doc.get(kk, 0) + vv
        elif k == '$push':
            for kk, vv in v.iteritems():
                doc[kk].append(vv)
        elif k == '$addToSet':
            for kk, vv in v.iteritems():
                if vv not in doc[kk]:
                    doc[kk].append(vv)
        elif k == '$pull':
            for kk, vv in v.iteritems():
                doc[kk] = [
                    vvv for vvv in doc[kk] if vvv != vv ]
        elif k == '$set':
            doc.update(v)
        elif k.startswith('$'):
            raise NotImplementedError, k
    validate(doc)
                
def validate(doc):
    for k,v in doc.iteritems():
        assert '$' not in k
        assert '.' not in k
        if hasattr(v, 'iteritems'):
            validate(v)
            
def bson_safe(obj):
    bson.BSON.encode(obj)

def bcopy(obj):
    if isinstance(obj, dict):
        return bson.BSON.encode(obj).decode()
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
