'''mim.py - Mongo In Memory - stripped-down version of mongo that is
non-persistent and hopefully much, much faster
'''
import sys
import itertools
from copy import deepcopy

from ming.utils import LazyProperty

from pymongo.errors import OperationFailure, DuplicateKeyError
from pymongo.bson import ObjectId
from pymongo import database, ASCENDING

class Connection(object):
    _singleton = None

    @classmethod
    def get(cls):
        if cls._singleton is None:
            cls._singleton = cls()
        return cls._singleton

    def __init__(self):
        self._databases = {}

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

    @property
    def name(self):
        return self._name

    def _make_collection(self):
        return Collection(self)

    def command(self, command):
        if 'filemd5' in command:
            return dict(md5='42') # completely bogus value; will it work?
        elif 'findandmodify' in command:
            coll = self._collections[command['findandmodify']]
            before = coll.find_one(command['query'])
            coll.update(command['query'], command['update'])
            if command.get('new', False):
                return dict(value=coll.find_one(command['query']))
            return dict(value=before)
        else:
            raise NotImplementedError, repr(command.items()[0])

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

class Collection(object):

    def __init__(self, database, name):
        self._name = name
        self._database = database
        self._data = {}
        self._unique_indexes = {}
        self._indexes = {}

    @property
    def name(self):
        return self._name

    @property
    def database(self):
        return self._database

    def __getattr__(self, name):
        return self._database['%s.%s' % (self.name, name)]

    def _find(self, spec):
        for doc in self._data.itervalues():
            if match(spec, doc): yield doc

    def find(self, spec=None):
        if spec is None:
            spec = {}
        return Cursor(lambda:self._find(spec))

    def find_one(self, spec):
        for x in self.find(spec):
            return x

    def insert(self, doc_or_docs, safe=False):
        if not isinstance(doc_or_docs, list):
            doc_or_docs = [ doc_or_docs ]
        for doc in doc_or_docs:
            _id = doc.get('_id', ())
            if _id == ():
                _id = doc['_id'] = ObjectId()
            if _id in self._data:
                if safe: raise OperationFailure('duplicate ID on insert')
                continue
            self._index(doc)
            self._data[_id] = deepcopy(doc)
        return _id

    def save(self, doc, safe=False):
        _id = doc.get('_id', ())
        if _id == ():
            return self.insert(doc, safe=safe)
        else:
            self.update({'_id':_id}, doc, upsert=True, safe=safe)
            return _id

    def update(self, spec, document, upsert=False, safe=False):
        updated = False
        for doc in self._find(spec):
            self._deindex(doc) 
            update(doc, document)
            self._index(doc) 
            updated = True
        if updated: return
        if upsert:
            doc = dict(spec)
            doc.update(document)
            _id = doc.get('_id', ())
            if _id == ():
                _id = doc['_id'] = ObjectId()
            self._index(doc) 
            self._data[_id] = deepcopy(doc)
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

    def ensure_index(self, key_or_list, unique=False, ttl=300, name=None, background=None):
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
        return dict(self._indexes)

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

    def __init__(self, iterator_gen, sort=None, skip=None, limit=None):
        self._iterator_gen = iterator_gen
        self._sort = sort
        self._skip = skip
        self._limit = limit

    @LazyProperty
    def iterator(self):
        result = self._iterator_gen()
        if self._sort is not None:
            result = sorted(result, cmp=cursor_comparator(self._sort))
        if self._skip is not None:
            result = itertools.islice(result, self._skip, sys.maxint)
        if self._limit is not None:
            result = itertools.islice(result, self._limit)
        return iter(result)

    def count(self):
        return sum(1 for x in self._iterator_gen())

    def __iter__(self):
        return self

    def next(self):
        value = self.iterator.next()
        return deepcopy(value)

    def sort(self, key_or_list, direction=ASCENDING):
        if not isinstance(key_or_list, list):
            key_or_list = [ (key_or_list, direction) ]
        keys = []
        for t in key_or_list:
            if isinstance(t, tuple):
                keys.append(t)
            else:
                keys.append(t, ASCENDING)
        return Cursor(
            self._iterator_gen,
            sort=keys,
            skip=self._skip,
            limit=self._limit)

    def all(self):
        return list(self._iterator_gen())

    def skip(self, skip):
        return Cursor(
            self._iterator_gen,
            sort=self._sort,
            skip=skip,
            limit=self._limit)

    def limit(self, limit):
        return Cursor(
            self._iterator_gen,
            sort=self._sort,
            skip=self._skip,
            limit=limit)

def cursor_comparator(keys):
    def comparator(a, b):
        for k,d in keys:
            part = cmp(a.get(k), b.get(k))
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
            op, value = _parse_query(v)
            if not _part_match(op, value, k.split('.'), doc): return False
        return True
    except (AttributeError, KeyError), ex:
        return False

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
        return _part_match(op, value, key_parts[1:], doc[key_parts[0]])

def _lookup(doc, k):
    for part in k.split('.'):
        doc = doc[part]
    return doc

def compare(op, a, b):
    if op == '$gt': return a > b
    if op == '$gte': return a >= b
    if op == '$lt': return a < b
    if op == '$lte': return a <= b
    if op == '$eq':
        if hasattr(b, 'match'):
            return b.match(a)
        else:
            return a == b
    if op == '$ne': return a != b
    if op == '$in': return a in b
    if op == '$nin': return a not in b
    raise NotImplementedError, op
        
def update(doc, updates):
    newdoc = {}
    for k, v in updates.iteritems():
        if k.startswith('$'): continue
        newdoc[k] = deepcopy(v)
    if newdoc:
        doc.clear()
        doc.update(newdoc)
    for k, v in updates.iteritems():
        if k == '$inc':
            for kk, vv in v.iteritems():
                doc[kk] += vv
        elif k == '$push':
            for kk, vv in v.iteritems():
                doc[kk].append(vv)
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
            
        
