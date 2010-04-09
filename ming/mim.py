'''mim.py - Mongo In Memory - stripped-down version of mongo that is
non-persistent and hopefully much, much faster
'''
import sys
import itertools
from copy import deepcopy

from pymongo.errors import OperationFailure
from pymongo.bson import ObjectId
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
        return Cursor(self._find(spec))

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
            update(doc, document)
            updated = True
        if updated: return
        if upsert:
            doc = dict(spec)
            doc.update(document)
            _id = doc.get('_id', ())
            if _id == ():
                _id = doc['_id'] = ObjectId()
            self._data[_id] = deepcopy(doc)
            return _id

    def remove(self, spec, **kwargs):
        self._data = dict(
            (k, doc) for k,doc in self._data.iteritems()
            if not match(spec, doc))

    def ensure_index(self, *args, **kwargs):
        pass

    def __repr__(self):
        return 'mim.Collection(%r, %s)' % (self._database, self.name)

class Cursor(object):

    def __init__(self, it):
        self._iterator = it
        self._count = None
        self._count_lb = 0

    def count(self):
        if self._count is None:
            self._iterator, local_iter = itertools.tee(self._iterator)
            for x in local_iter:
                self._count_lb += 1
            self._count = self._count_lb
        return self._count

    def __iter__(self):
        return self

    def next(self):
        nextval = self._iterator.next()
        if self._count is None:
            self._count_lb += 1
        return deepcopy(nextval)

    def sort(self, key_or_list, direction=ASCENDING):
        if not isinstance(key_or_list, list):
            key_or_list = [ (key_or_list, direction) ]
        keys = []
        for t in key_or_list:
            if isinstance(t, tuple):
                keys.append(t)
            else:
                keys.append(t, pymongo.ASCENDING)
        return Cursor(
            iter(sorted(self._iterator, cmp=cursor_comparator(keys))))

    def all(self):
        return list(self)

    def skip(self, skip):
        self._iterator = itertools.islice(self._iterator, skip, sys.maxint)
        return self

    def limit(self, limit):
        self._iterator = itertools.islice(self._iterator, limit)
        return self


def cursor_comparator(keys):
    def comparator(a, b):
        for k,d in keys:
            part = cmp(a.get(k), b.get(k))
            if part: return part * d
        return 0
    return comparator

def match(spec, doc):
    try:
        for k,v in spec.iteritems():
            if k.startswith('$'):
                raise NotImplementedError, k
            ns = doc
            for part in k.split('.'):
                ns = ns[part]
            if ns != v:
                return False
        return True
    except (AttributeError, KeyError), ex:
        return False
        
def update(doc, updates):
    for k, v in updates.iteritems():
        if k.startswith('$'): continue
        doc[k] = deepcopy(v)
    for k, v in updates.iteritems():
        if k == '$inc':
            for kk, vv in v.iteritems():
                doc[kk] += vv
        elif k.startswith('$'):
            import pdb; pdb.set_trace()
            raise NotImplementedError, k
                
