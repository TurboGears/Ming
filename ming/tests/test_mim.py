import re
import uuid
from datetime import datetime
from unittest import TestCase

import bson
from bson.raw_bson import RawBSONDocument

from ming import create_datastore, mim
from pymongo import UpdateOne, CursorType
from pymongo.errors import OperationFailure, DuplicateKeyError
from unittest.mock import patch


class TestDatastore(TestCase):

    def setUp(self):
        self.bind = create_datastore('mim:///testdb')
        self.bind.conn.drop_all()
        self.bind.db.coll.insert_one({'_id':'foo', 'a':2, 'c':[1,2,3], 'z': {'egg': 'spam', 'spam': 'egg'}})
        for r in range(4):
            self.bind.db.rcoll.insert_one({'_id':'r%s' % r, 'd':r})

    def test_limit(self):
        f = self.bind.db.rcoll.find
        self.assertEqual(2, len(f({}).limit(2).all()))
        self.assertEqual(4, len(f({}).limit(0).all()))

    def test_regex(self):
        f = self.bind.db.rcoll.count_documents
        assert 4 == f(dict(_id=re.compile(r'r\d+')))
        assert 2 == f(dict(_id=re.compile(r'r[0-1]')))

    def test_regex_options(self):
        f = self.bind.db.rcoll.count_documents
        assert 2 == f(dict(_id={'$regex': 'r[0-1]', '$options': 'i'}))

    def test_eq(self):
        f = self.bind.db.rcoll.count_documents
        assert 1 == f(dict(d={'$eq': 0}))

    def test_ne(self):
        f = self.bind.db.rcoll.count_documents
        assert 3 == f(dict(d={'$ne': 0}))

    def test_gt(self):
        f = self.bind.db.rcoll.count_documents
        assert 1 == f(dict(d={'$gt': 2}))
        assert 0 == f(dict(d={'$gt': 3}))

    def test_gte(self):
        f = self.bind.db.rcoll.count_documents
        assert 2 == f(dict(d={'$gte': 2}))
        assert 1 == f(dict(d={'$gte': 3}))

    def test_lt(self):
        f = self.bind.db.rcoll.count_documents
        assert 0 == f(dict(d={'$lt': 0}))
        assert 1 == f(dict(d={'$lt': 1}))
        assert 2 == f(dict(d={'$lt': 2}))

    def test_lte(self):
        f = self.bind.db.rcoll.count_documents
        assert 1 == f(dict(d={'$lte': 0}))
        assert 2 == f(dict(d={'$lte': 1}))
        assert 3 == f(dict(d={'$lte': 2}))

    def test_range_equal(self):
        f = self.bind.db.rcoll.count_documents
        assert 1 == f(dict(d={'$gte': 2, '$lte': 2}))
        assert 2 == f(dict(d={'$gte': 1, '$lte': 2}))
        assert 0 == f(dict(d={'$gte': 4, '$lte': -1}))

    def test_range_inequal(self):
        f = self.bind.db.rcoll.count_documents
        assert 0 == f(dict(d={'$gt': 2, '$lt': 2}))
        assert 1 == f(dict(d={'$gt': 2, '$lt': 4}))
        assert 0 == f(dict(d={'$gt': 1, '$lt': 2}))
        assert 1 == f(dict(d={'$gt': 1, '$lt': 3}))
        assert 0 == f(dict(d={'$gt': 4, '$lt': -1}))

    def test_exists(self):
        f = self.bind.db.coll.count_documents
        assert 1 == f(dict(a={'$exists':True}))
        assert 0 == f(dict(a={'$exists':False}))
        assert 0 == f(dict(b={'$exists':True}))
        assert 1 == f(dict(b={'$exists':False}))

    def test_all(self):
        f = self.bind.db.coll.count_documents
        assert 1 == f(dict(c={'$all':[1,2]}))
        assert 1 == f(dict(c={'$all':[1,2,3]}))
        assert 0 == f(dict(c={'$all':[2,3,4]}))
        assert 1 == f(dict(c={'$all':[]}))

    def test_or(self):
        f = self.bind.db.coll.count_documents
        assert 1 == f(dict({'$or': [{'c':{'$all':[1,2,3]}}]}))
        assert 0 == f(dict({'$or': [{'c':{'$all':[4,2,3]}}]}))
        assert 1 == f(dict({'$or': [{'a': 2}, {'c':{'$all':[1,2,3]}}]}))
        self.assertEqual(0, f(dict({'_id': 'bar', '$or': [{'a': 2}, {'c':{'$all':[1,2,3]}}]})))
        self.assertEqual(1, f(dict({'_id': 'foo', '$or': [{'a': 2}, {'c':{'$all':[1,2,3]}}]})))

    def test_find_with_projection_list(self):
        o = self.bind.db.coll.find_one({'a': 2}, projection=['a'])
        assert o['a'] == 2
        assert o['_id'] == 'foo'
        assert 'c' not in o

    def test_find_with_projection_of_1(self):
        o = self.bind.db.coll.find_one({'a': 2}, projection={'c': 1})
        assert o['_id'] == 'foo'  # the _id must be always present
        assert 'a' not in o
        assert o['c'] == [1, 2, 3]

    def test_find_with_projection_of_0(self):
        o = self.bind.db.coll.find_one({'a': 2}, projection={'c': 0})
        assert o['_id'] == 'foo'
        assert o['a'] == 2
        assert 'c' not in o

    def test_find_with_projection_of_0_dotted(self):
        o = self.bind.db.coll.find_one({'a': 2}, projection={'z.egg': 0})
        assert o['_id'] == 'foo'
        assert o['a'] == 2
        assert o['z'] == {'spam': 'egg'}

    def test_find_with_progection_positive_slice(self):
        o = self.bind.db.coll.find_one({'a': 2}, projection={'c': {'$slice': 2}})
        assert o['_id'] == 'foo'
        assert o['a'] == 2
        assert o['c'] == [1, 2]

    def test_find_with_projection_negative_slice(self):
        o = self.bind.db.coll.find_one({'a': 2}, projection={'c': {'$slice': -2}})
        assert o['_id'] == 'foo'
        assert o['a'] == 2
        assert o['c'] == [2, 3]

    def test_find_with_projection_skip_and_limit_slice(self):
        o = self.bind.db.coll.find_one({'a': 2}, projection={'c': {'$slice': [1, 1]}})
        assert o['_id'] == 'foo'
        assert o['a'] == 2
        assert o['c'] == [2]

    def test_find_with_projection_of_text_score(self):
        o = self.bind.db.coll.find_one({'a': 2}, projection={'score': {'$meta': 'textScore'}})
        assert o['_id'] == 'foo'
        assert o['a'] == 2
        assert o['c'] == [1, 2, 3]
        assert o['score'] == 1.0  # MIM currently always reports 1 as the score.

    def test_find_with_invalid_kwargs(self):
        self.assertRaises(TypeError, self.bind.db.coll.find, foo=123)
        self.assertRaises(TypeError, self.bind.db.coll.find, {'a': 2}, foo=123)
        self.assertRaises(TypeError, self.bind.db.coll.find_one, foo=123)
        self.bind.db.coll.find(allow_disk_use=True)  # kwargs that pymongo knows are ok
        self.bind.db.coll.find(cursor_type=CursorType.EXHAUST)

    def test_rewind(self):
        collection = self.bind.db.coll
        collection.insert_one({'a':'b'})

        cursor = collection.find()
        doc = cursor[0]
        cursor.next()
        cursor.rewind()
        assert cursor.next() == doc

    def test_close(self):
        collection = self.bind.db.coll
        collection.insert_one({'a': 'b'})
        cursor = collection.find()
        cursor.close()
        self.assertRaises(StopIteration, cursor.next)

    def test_cursor_context_manager(self):
        collection = self.bind.db.coll
        collection.insert_one({'a': 'b'})
        with collection.find() as cursor:
            pass
        self.assertRaises(StopIteration, cursor.next)

    def test_search(self):
        conn = mim.Connection().get()
        coll = conn.searchdatabase.coll
        coll.create_index([('field', 'text')])
        coll.insert_one({'field': 'text to be searched'})
        coll.insert_one({'field': 'text to be'})
        assert coll.count_documents({'$text': {'$search': 'searched'}}) == 1


class TestDottedOperators(TestCase):

    def setUp(self):
        self.bind = create_datastore('mim:///testdb')
        self.bind.conn.drop_all()
        self.bind.db.coll.insert_one(
            {'_id':'foo', 'a':2,
             'b': { 'c': 1, 'd': 2, 'e': [1,2,3],
                    'f': [ { 'g': 1 }, { 'g': 2 } ] },
             'x': {} })
        self.coll = self.bind.db.coll

    def test_inc_dotted_dollar(self):
        self.coll.update_many({'b.e': 2}, { '$inc': { 'b.e.$': 1 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.e': 1 })
        self.assertEqual(obj, { 'b': { 'e': [ 1,3,3 ] } })

    def test_inc_dotted_dollar_middle1(self):
        # match on g=1 and $inc by 10
        self.coll.update_many({'b.f.g': 1}, { '$inc': { 'b.f.$.g': 10 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.f': 1 })
        self.assertEqual(obj, { 'b': { 'f': [ { 'g': 11 }, { 'g': 2 } ] }})

    def test_find_dotted(self):
        self.assertEqual(self.coll.count_documents({'b.c': 1}), 1)
        self.assertEqual(self.coll.count_documents({'b.c': 2}), 0)
        self.assertEqual(0, len(self.coll.find({'x.y.z': 1}).all()))

    def test_inc_dotted(self):
        self.coll.update_many({}, { '$inc': { 'b.c': 4 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.c': 1 })
        self.assertEqual(obj, { 'b': { 'c': 5 } })

    def test_set_dotted(self):
        self.coll.update_many({}, { '$set': { 'b.c': 4 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.c': 1 })
        self.assertEqual(obj, { 'b': { 'c': 4 } })

    def test_set_dotted_with_integer(self):
        self.bind.db.coll.insert_one(
            {'_id':'foo2', 'a':2,
             'b': [1,2,3],
             'x': {} })
        self.coll.update_many({'_id': 'foo2'}, {'$set': {'b.0': 4}})
        obj = self.coll.find_one({'_id': 'foo2'})
        self.assertEqual(obj, {'a': 2, 'x': {}, '_id': 'foo2', 'b': [4, 2, 3]})

    def test_unset_dotted(self):
        self.coll.update_many({}, { '$unset': { 'b.f.1.g': 1 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.f': 1 })
        self.assertEqual(obj, { 'b': { 'f': [{'g': 1}, {}] } })

        # Check that it even works for keys that are not there.
        self.coll.update_many({}, { '$unset': { 'b.this_does_not_exists': 1 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.f': 1 })
        self.assertEqual(obj, { 'b': { 'f': [{'g': 1}, {}] } })

        # Check that unsetting subkeys of a nonexisting subdocument has no side effect
        self.coll.update_many({}, {'$unset': {'this_does_not_exists.x.y.z': 1}})
        obj = self.coll.find_one({}, { '_id': 0, 'b.f': 1 })
        self.assertEqual(obj, { 'b': { 'f': [{'g': 1}, {}] } })

    def test_push_dotted(self):
        self.coll.update_many({}, { '$push': { 'b.e': 4 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.e': 1 })
        self.assertEqual(obj, { 'b': { 'e': [1,2,3,4] } })

    def test_addToSet_dotted(self):
        self.coll.update_many({}, { '$addToSet': { 'b.e': 4 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.e': 1 })
        self.assertEqual(obj, { 'b': { 'e': [1,2,3,4] } })
        self.coll.update_many({}, { '$addToSet': { 'b.e': 4 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.e': 1 })
        self.assertEqual(obj, { 'b': { 'e': [1,2,3,4] } })

    def test_addToSet_empty(self):
        self.coll.update_many({}, { '$unset': { 'b': True, 'x': True, 'a': True } })
        self.coll.update_many({}, { '$addToSet': { 'y.z': 4 } })
        obj = self.coll.find_one({ '_id': 'foo'})
        self.assertEqual(obj, {'_id': 'foo', 'y': {'z': [4]}})

    def test_project_dotted(self):
        obj = self.coll.find_one({}, { 'b.e': 1 })
        self.assertEqual(obj, { '_id': 'foo', 'b': { 'e': [ 1,2,3] } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.e': 1 })
        self.assertEqual(obj, { 'b': { 'e': [ 1,2,3] } })

    def test_lt_dotted(self):
        obj = self.coll.find_one({'b.c': { '$lt': 1 } })
        self.assertEqual(obj, None)
        obj = self.coll.find_one({'b.c': { '$lt': 2 } })
        self.assertNotEqual(obj, None)

    def test_pull_dotted(self):
        self.coll.update_many(
            {},
            { '$pull': { 'b.f': { 'g': { '$gt': 1 } } } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.f': 1 } )
        self.assertEqual(obj, { 'b': { 'f': [ {'g': 1 } ] } } )

    def test_pull_all_dotted(self):
        self.coll.update_many(
            {},
            { '$pullAll': { 'b.f': [{'g': 1 }] } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.f': 1 } )
        self.assertEqual(obj, { 'b': { 'f': [ {'g': 2 } ] } } )

    def test_pop_dotted(self):
        self.coll.update_many(
            {},
            { '$pop': { 'b.f': 1 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.f': 1 } )
        self.assertEqual(obj, { 'b': { 'f': [ {'g': 1 } ] } } )


class TestCommands(TestCase):

    sum_js = '''function(key,values) {
        var total = 0;
        for(var i = 0; i < values.length; i++) {
            total += values[i]; }
        return total; }'''

    first_js = 'function(key,values) { return values[0]; }'
    concat_js = 'function(key,vs) { return [].concat.apply([], vs);}'

    def setUp(self):
        self.bind = create_datastore('mim:///testdb')
        self.bind.conn.drop_all()
        self.doc = {'_id':'foo', 'a':2, 'c':[1,2,3]}
        self.bind.db.coll.insert_one(self.doc)

    def test_filemd5(self):
        self.assertEqual(
            dict(md5='d41d8cd98f00b204e9800998ecf8427e'),
            self.bind.db.command('filemd5'))

    def test_findandmodify_old(self):
        result = self.bind.db.command(
            'findandmodify', 'coll',
            query=dict(_id='foo'),
            update={'$inc': dict(a=1)},
            new=False)
        self.assertEqual(result['value'], self.doc)
        newdoc = self.bind.db.coll.find().next()
        self.assertEqual(newdoc['a'], 3, newdoc)

    def test_findandmodify_new(self):
        result = self.bind.db.command(
            'findandmodify', 'coll',
            query=dict(_id='foo'),
            update={'$inc': dict(a=1)},
            new=True)
        self.assertEqual(result['value']['a'], 3)
        newdoc = self.bind.db.coll.find().next()
        self.assertEqual(newdoc['a'], 3, newdoc)

class TestCollection(TestCase):

    def setUp(self):
        self.bind = create_datastore('mim:///testdb')
        self.bind.conn.drop_all()

    def test_getitem_clones(self):
        test = self.bind.db.test
        test.insert_one({'a':'b'})
        cursor = test.find()
        doc = cursor[0]
        self.assertEqual(cursor.next(), doc)

    def test_upsert_simple(self):
        test = self.bind.db.test
        test.update_many(
            dict(_id=0, a=5),
            {'$set': dict(b=6) },
            upsert=True)
        doc = test.find_one()
        self.assertEqual(doc, dict(_id=0, a=5, b=6))

    def test_upsert_duplicated(self):
        test = self.bind.db.test
        test.create_index([('a', 1)], unique=True)

        # Try with any index
        test.update_many({'x': 'NOT_FOUND1'}, {'$set': {'a': 0}}, upsert=True)
        try:
            test.update_many({'x': 'NOT_FOUND2'}, {'$set': {'a': 0}}, upsert=True)
        except DuplicateKeyError:
            pass
        else:
            assert False, 'Had to detect duplicate key'

        # Now try with _id
        test.update_many({'x': 'NOT_FOUND3'}, {'$set': {'_id': 0}}, upsert=True)
        try:
            test.update_many({'x': 'NOT_FOUND4'}, {'$set': {'_id': 0}}, upsert=True)
        except DuplicateKeyError:
            pass
        else:
            assert False, 'Had to detect duplicate key'

    def test_upsert_setOnInsert(self):
        test = self.bind.db.test
        test.update_many(
            dict(_id=0, a=5),
            {'$set': dict(b=6),
             '$setOnInsert': dict(c=7)},
            upsert=True)
        doc = test.find_one()
        self.assertEqual(doc, dict(_id=0, a=5, b=6, c=7))

        test.update_many(dict(_id=0, a=5), {'$set': dict(b=0, c=0)})
        test.update_many(
            dict(_id=0, a=5),
            {'$set': dict(b=2),
             '$setOnInsert': dict(c=7)},
            upsert=True)
        doc = test.find_one()
        self.assertEqual(doc, dict(_id=0, a=5, b=2, c=0))

    def test_upsert_inc(self):
        test = self.bind.db.test
        test.update_many(
            dict(_id=0, a=5),
            {'$inc': dict(a=2, b=3) },
            upsert=True)
        doc = test.find_one()
        self.assertEqual(doc, dict(_id=0, a=7, b=3))

    def test_upsert_push(self):
        test = self.bind.db.test
        test.update_many(
            dict(_id=0, a=5),
            {'$push': dict(c=1) },
            upsert=True)
        doc = test.find_one()
        self.assertEqual(doc, dict(_id=0, a=5, c=[1]))

    def test_update_addToSet_with_each(self):
        self.bind.db.coll.insert_one({'_id': 0, 'a': [1, 2, 3]})
        self.bind.db.coll.update_many({},
                                 {'$addToSet': {'a': {'$each': [0, 2, 4]}}})
        doc = self.bind.db.coll.find_one()
        self.assertEqual(len(doc['a']), 5)

    def test_find_with_skip(self):
        for i in range(5):
            self.bind.db.coll.insert_one({'_id':str(i), 'a':i})
        result = self.bind.db.coll.find({}, skip=2)
        result = list(result)
        self.assertEqual(len(result), 3)

    def test_find_with_limit(self):
        for i in range(5):
            self.bind.db.coll.insert_one({'_id':str(i), 'a':i})
        result = self.bind.db.coll.find({}, limit=2)
        result = list(result)
        self.assertEqual(len(result), 2)

    def test_find_with_slice_skip(self):
        for i in range(5):
            self.bind.db.coll.insert_one({'_id':str(i), 'a':i})
        result = self.bind.db.coll.find().sort('a')[3:]
        result = list(result)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['a'], 3)

    def test_find_with_slice_limit(self):
        for i in range(5):
            self.bind.db.coll.insert_one({'_id':str(i), 'a':i})
        result = self.bind.db.coll.find().sort('a')[:2]
        result = list(result)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['a'], 0)

    def test_find_with_slice_skip_limit(self):
        for i in range(5):
            self.bind.db.coll.insert_one({'_id':str(i), 'a':i})
        result = self.bind.db.coll.find().sort('a')[2:4]
        result = list(result)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['a'], 2)

    def test_find_with_slice_invalid(self):
        try:
            self.bind.db.coll.find()['random']
        except TypeError:
            return
        self.fail('No TypeError exception raised')

    def test_find_with_paging(self):
        for i in range(5):
            self.bind.db.coll.insert_one({'_id':str(i), 'a':i})
        result_all = self.bind.db.coll.find()
        result_all = list(result_all)
        result_page1 = self.bind.db.coll.find({}, skip=0, limit=3)
        result_page2 = self.bind.db.coll.find({}, skip=3, limit=3)
        result_paging = list(result_page1) + list(result_page2)
        self.assertEqual(result_all, result_paging)

    def test_distinct(self):
        for i in range(5):
            self.bind.db.coll.insert_one({'_id':str(i), 'a':'A'})
        result = self.bind.db.coll.distinct('a')
        self.assertEqual(result, ['A'])

    def test_distinct_subkey(self):
        for i in range(5):
            self.bind.db.coll.insert_one({'_id': str(i), 'a': {'b': 'A'}})
        result = self.bind.db.coll.distinct('a.b')
        self.assertEqual(result, ['A'])

    def test_distinct_sublist(self):
        for i in range(5):
            self.bind.db.coll.insert_one({'_id': str(i),
                                      'a': [{'b': 'A', 'z': 'z', 'f': {'f': 'F'}},
                                            {'b': 'C', 'z': 'z', 'f': {'f': 'G'}}]})
        result = self.bind.db.coll.distinct('a.b')
        self.assertEqual(result, ['A', 'C'])
        result = self.bind.db.coll.distinct('a.z')
        self.assertEqual(result, ['z'])
        result = self.bind.db.coll.distinct('a.f.f')
        self.assertEqual(result, ['F', 'G'])

    def test_distinct_filtered(self):
        for i in range(5):
            self.bind.db.coll.insert_one({'_id': i, 'a': 'A'})
        result = self.bind.db.coll.distinct('_id', filter={'_id': {'$lte': 2}})
        self.assertEqual(set(result), {0, 1, 2})

    def test_find_one_and_update_returns_none_on_no_entries(self):
        self.assertEqual(None, self.bind.db.foo.find_one_and_update({'i': 1}, {'$set': {'i': 2}}))

    def test_find_one_and_update_returns_none_on_upsert_and_no_new(self):
        self.assertEqual(None, self.bind.db.foo.find_one_and_update({'i': 1},
                                                                    {'$set': {'i': 2}},
                                                                    upsert=True, return_document=False))

    def test_find_one_and_replace_returns_none_on_upsert_and_no_new(self):
        self.assertEqual(None, self.bind.db.foo.find_one_and_replace({'i': 1},
                                                                     {'i': 2},
                                                                     upsert=True, return_document=False))

    def test_find_one_and_update_returns_old_value_on_no_return_document(self):
        self.bind.db.foo.insert_one({'_id': 1, 'i': 1})
        self.assertEqual({'_id': 1, 'i': 1}, self.bind.db.foo.find_one_and_update({'i': 1},
                                                                                  {'$set': {'i': 2}},
                                                                                  return_document=False))

    def test_one_and_update_returns_new_value_on_new(self):
        self.bind.db.foo.insert_one({'_id': 1, 'i': 1})
        self.assertEqual({'_id': 1, 'i': 2}, self.bind.db.foo.find_one_and_update({'i': 1},
                                                                                  {'$set': {'i': 2}},
                                                                                  return_document=True))

    def test_find_one_and_replace_returns_new_value_on_new(self):
        self.bind.db.foo.insert_one({'_id': 1, 'i': 1})
        self.assertEqual({'_id': 1, 'i': 2}, self.bind.db.foo.find_one_and_replace({'i': 1},
                                                                                   {'i': 2},
                                                                                   return_document=True))

    def test_find_one_and_replace_ignores_id(self):
        self.bind.db.foo.insert_one({'_id': 1, 'i': 1})
        self.assertEqual({'_id': 1, 'i': 2}, self.bind.db.foo.find_one_and_replace({'i': 1},
                                                                                   {'i': 2},
                                                                                   return_document=True))

    def test_find_one_and_replace_fails_with_set(self):
        self.bind.db.foo.insert_one({'_id': 1, 'i': 1})
        with self.assertRaises(ValueError):
            self.bind.db.foo.find_one_and_replace({'i': 1},
                                                  {'$set': {'i': 2}},
                                                  return_document=True)

    def test_find_one_and_update_returns_new_value_on_new_filter_id(self):
        self.bind.db.foo.insert_one({'i': 1})
        self.assertEqual({'i': 2}, self.bind.db.foo.find_one_and_update({'i': 1},
                                                                        {'$set': {'i': 2}},
                                                                        projection={'_id': False, 'i': True},
                                                                        return_document=True))

    def test_find_one_and_update_returns_new_value_on_new_upsert(self):
        self.assertEqual({'_id': 1, 'i': 2}, self.bind.db.foo.find_one_and_update({'i': 1},
                                                                                  {'$set': {'_id': 1, 'i': 2}},
                                                                                  return_document=True,
                                                                                  upsert=True))
        
    def test_find_one_and_update_fails_with_id(self):
        self.bind.db.foo.insert_one({'_id': 1, 'i': 1})
        with self.assertRaises(ValueError):
            self.bind.db.foo.find_one_and_update({'i': 1},
                                                 {'_id': 2, 'i': 2},
                                                 return_document=True)

    def test_find_one_and_replace_returns_new_value_on_new_upsert(self):
        doc = self.bind.db.foo.find_one_and_replace({'i': 1},
                                                    {'i': 2},
                                                    return_document=True,
                                                    upsert=True)
        self.assertIsInstance(doc.pop("_id"), bson.ObjectId)
        self.assertEqual({'i': 2}, doc)

    def test_find_one_and_delete_returns_projection(self):
        self.bind.db.col.insert_one({'_id': 1, 'i': 1})
        self.assertEqual({'i': 1}, self.bind.db.col.find_one_and_delete({'_id': 1},
                                                                          projection={'_id': False, 'i': True}))
        self.assertEqual(0, self.bind.db.col.estimated_document_count())

    def test_hint_simple(self):
        self.bind.db.coll.create_index([('myindex', 1)])

        cursor = self.bind.db.coll.find().hint([('$natural', 1)])
        self.assertEqual(type(cursor), type(self.bind.db.coll.find()))
        cursor = self.bind.db.coll.find().hint([('myindex', 1)])
        self.assertEqual(type(cursor), type(self.bind.db.coll.find()))
        cursor = self.bind.db.coll.find().hint('myindex')
        self.assertEqual(type(cursor), type(self.bind.db.coll.find()))
        cursor = self.bind.db.coll.find().hint(None)
        self.assertEqual(type(cursor), type(self.bind.db.coll.find()))

    def test_hint_invalid(self):
        self.assertRaises(OperationFailure, self.bind.db.coll.find().hint, [('foobar', 1)])
        self.assertRaises(OperationFailure, self.bind.db.coll.find().hint, 'foobar')
        self.assertRaises(TypeError, self.bind.db.coll.find().hint, 123)

    def test_index_information(self):
        self.bind.db.coll.create_index([('myfield', 1)],
                                       background=True,
                                       expireAfterSeconds=42,
                                       unique=True)
        info = self.bind.db.coll.index_information()
        self.assertEqual(info['myfield']['key'], [('myfield', 1)])
        self.assertEqual(info['myfield']['background'], 1)
        self.assertEqual(info['myfield']['expireAfterSeconds'], 42)
        self.assertEqual(info['myfield']['unique'], True)

    def test_unique_index_subdocument(self):
        coll = self.bind.db.coll

        coll.create_index([('x.y', 1)], unique=True)
        coll.insert_one({'x': {'y': 1}})
        coll.insert_one({'x': {'y': 2}})
        self.assertRaises(DuplicateKeyError, coll.insert_one, {'x': {'y': 2}})

    def test_unique_index_whole_sdoc(self):
        coll = self.bind.db.coll

        coll.create_index([('x', 1)], unique=True)
        coll.insert_one({'x': {'y': 1}})
        coll.insert_one({'x': {'y': 2}})
        self.assertRaises(DuplicateKeyError, coll.insert_one, {'x': {'y': 2}})

    def test_unique_sparse_index_subdocument(self):
        coll = self.bind.db.coll

        coll.create_index([('x.y', 1)], unique=True, sparse=True)
        coll.insert_one({'x': {'y': 1}})

        # no duplicate key error on these:
        coll.insert_one({'x': {'y': None}})
        coll.insert_one({'x': {'y': None}})
        coll.insert_one({'x': {'other': 'field'}})
        coll.insert_one({'x': {'other': 'field'}})
        # still errors on an existing duplication
        self.assertRaises(DuplicateKeyError, coll.insert_one, {'x': {'y': 1}})

    def test_unique_sparse_index_whole_sdoc(self):
        coll = self.bind.db.coll

        coll.create_index([('x', 1)], unique=True, sparse=True)
        coll.insert_one({'x': {'y': 1}})
        # no duplicate key error on these:
        coll.insert_one({'x': None})
        coll.insert_one({'x': None})
        coll.insert_one({'other': 'field'})
        coll.insert_one({'other': 'field'})
        # still errors on an existing duplication
        self.assertRaises(DuplicateKeyError, coll.insert_one, {'x': {'y': 1}})

    def test_delete_many(self):
        coll = self.bind.db.coll

        coll.insert_one({'dme-m': 1})
        coll.insert_one({'dme-m': 1})
        coll.insert_one({'dme-m': 2})

        self.assertEqual(coll.delete_many({'dme-m': 1}).deleted_count, 2)

    def test_delete_one(self):
        coll = self.bind.db.coll

        coll.insert_one({'dme-o': 1})
        coll.insert_one({'dme-o': 1})
        coll.insert_one({'dme-o': 2})

        self.assertEqual(coll.delete_one({'dme-o': 1}).deleted_count, 1)

    def test_find_one_and_delete(self):
        coll = self.bind.db.coll

        coll.insert_one({'_id': 1, 'dme-o': 1})
        coll.insert_one({'_id': 2, 'dme-o': 1})
        coll.insert_one({'_id': 3, 'dme-o': 2})

        self.assertEqual({'_id': 1, 'dme-o': 1}, coll.find_one_and_delete({'dme-o': 1}))
        self.assertEqual(len(list(coll.find({'dme-o': {'$exists': True}}))), 2)

    def test_find_bytes(self):
        coll = self.bind.db.coll
        # bytes
        coll.insert_one({'x': b'foo'})
        self.assertIsNotNone(coll.find_one({'x': b'foo'}))
        self.assertIsNotNone(coll.find_one({'x': bson.Binary(b'foo')}))

        # Binary, same as bytes
        coll.insert_one({'x': bson.Binary(b'bar')})
        self.assertIsNotNone(coll.find_one({'x': b'bar'}))
        self.assertIsNotNone(coll.find_one({'x': bson.Binary(b'bar')}))

        # Binary with different subtype, NOT like bytes
        coll.insert_one({'x': bson.Binary(b'woah', bson.binary.USER_DEFINED_SUBTYPE)})
        self.assertIsNone(coll.find_one({'x': b'woah'}))
        self.assertIsNone(coll.find_one({'x': bson.Binary(b'woah')}))
        self.assertIsNotNone(coll.find_one({'x': bson.Binary(b'woah', bson.binary.USER_DEFINED_SUBTYPE)}))

    def test_find_RawBSONDocument(self):
        coll = self.bind.db.coll
        coll.insert_one({'x': 5})
        # real simple filter
        result = coll.find_one(RawBSONDocument(bson.encode({
            'x': 5
        })))
        self.assertIsNotNone(result)
        # nested filter
        result = coll.find_one(RawBSONDocument(bson.encode({
            '$or': [{'x': 5}, {'y': 7}]
        })))
        self.assertIsNotNone(result)

    def test_find_UUID(self):
        coll = self.bind.db.coll
        uu = uuid.UUID('{12345678-1234-5678-1234-567812345678}')
        coll.insert_one({'x': uu})
        # real simple filter
        result = coll.find_one({'x': uu})
        self.assertIsNotNone(result)


class TestBsonCompare(TestCase):

    def test_boolean_bson_type(self):
        assert mim.BsonArith.cmp(True, True) == 0
        assert mim.BsonArith.cmp(True, False) == 1
        assert mim.BsonArith.cmp(False, True) == -1
        assert mim.BsonArith.cmp(False, False) == 0
        assert mim.BsonArith.cmp(False, bson.ObjectId()) == 1
        assert mim.BsonArith.cmp(True, datetime.fromordinal(1)) == -1

    def test_float_bson_type(self):
        assert mim.BsonArith.cmp(1, 1) == 0
        assert mim.BsonArith.cmp(1.1, 1.1) == 0
        assert mim.BsonArith.cmp(1.1, -1.3) == 1
        assert mim.BsonArith.cmp(1.1, 1.1111) == -1


class TestMatch(TestCase):

    def test_simple_match(self):
        mspec = mim.match({'foo': 4}, { 'foo': 4 })
        self.assertEqual(mspec, mim.MatchDoc({'foo': 4}))

    def test_dotted_match(self):
        mspec = mim.match({'foo.bar': 4}, { 'foo': { 'bar': 4 } })
        self.assertEqual(mspec, mim.MatchDoc({'foo': mim.MatchDoc({'bar': 4}) } ))

    def test_list_match(self):
        mspec = mim.match({'foo.bar': 4}, { 'foo': { 'bar': [1,2,3,4,5] } })
        self.assertEqual(mspec, mim.MatchDoc({
                    'foo': mim.MatchDoc({'bar': mim.MatchList([1,2,3,4,5], pos=3) } ) }))
        self.assertEqual(mspec.getvalue('foo.bar.$'), 4)

    def test_elem_match(self):
        mspec = mim.match({'foo': { '$elemMatch': { 'bar': 1, 'baz': 2 } } },
                          {'foo': [ { 'bar': 1, 'baz': 2 } ] })
        self.assertIsNotNone(mspec)
        mspec = mim.match({'foo': { '$elemMatch': { 'bar': 1, 'baz': 2 } } },
                          {'foo': [ { 'bar': 1, 'baz': 1 }, { 'bar': 2, 'baz': 2 } ] })
        self.assertIsNone(mspec)

    def test_gt(self):
        spec = { 'd': { '$gt': 2 } }
        self.assertIsNone(mim.match(spec, { 'd': 1 } ))
        self.assertIsNone(mim.match(spec, { 'd': 2 } ))
        self.assertIsNotNone(mim.match(spec, { 'd': 3 } ))

    def test_gte(self):
        spec = { 'd': { '$gte': 2 } }
        self.assertIsNone(mim.match(spec, { 'd': 1} ))
        self.assertIsNotNone(mim.match(spec, { 'd': 2 } ))
        self.assertIsNotNone(mim.match(spec, { 'd': 3} ))

    def test_lt(self):
        spec = { 'd': { '$lt': 2 } }
        self.assertIsNotNone(mim.match(spec, { 'd': 1 } ))
        self.assertIsNone(mim.match(spec, { 'd': 2 } ))
        self.assertIsNone(mim.match(spec, { 'd': 3 } ))

    def test_lte(self):
        spec = { 'd': { '$lte': 2 } }
        self.assertIsNotNone(mim.match(spec, { 'd': 1 } ))
        self.assertIsNotNone(mim.match(spec, { 'd': 2 } ))
        self.assertIsNone(mim.match(spec, { 'd': 3 } ))

    def test_range(self):
        doc = { 'd': 2 }
        self.assertIsNotNone(mim.match({'d': { '$gt': 1, '$lt': 3 } }, doc))
        self.assertIsNone(mim.match({'d': { '$gt': 1, '$lt': 2 } }, doc))
        self.assertIsNotNone(mim.match({'d': { '$gt': 1, '$lte': 2 } }, doc))

    def test_exists(self):
        doc = { 'd': 2 }
        self.assertIsNotNone(mim.match({'d': { '$exists': 1 } }, doc))
        self.assertIsNone(mim.match({'d': { '$exists': 0 } }, doc))
        self.assertIsNone(mim.match({'e': { '$exists': 1 } }, doc))
        self.assertIsNotNone(mim.match({'e': { '$exists': 0 } }, doc))

    def test_all(self):
        doc = { 'c': [ 1, 2 ] }
        self.assertIsNotNone(mim.match({'c': {'$all': [] } }, doc))
        self.assertIsNotNone(mim.match({'c': {'$all': [1] } }, doc))
        self.assertIsNotNone(mim.match({'c': {'$all': [1, 2] } }, doc))
        self.assertIsNone(mim.match({'c': {'$all': [1, 2, 3] } }, doc))

    def test_or(self):
        doc = { 'd': 2 }
        self.assertIsNotNone(mim.match(
                {'$or': [ { 'd': 1 }, { 'd': 2 } ] },
                doc))
        self.assertIsNone(mim.match(
                {'$or': [ { 'd': 1 }, { 'd': 3 } ] },
                doc))

    def test_traverse_list(self):
        doc = { 'a': [ { 'b': 1 }, { 'b': 2 } ] }
        self.assertIsNotNone(mim.match( {'a.b': 1 }, doc))

    def test_regex_match(self):
        doc = { 'a': 'bar', 'b': 'bat', 'c': None}
        regex = re.compile(r'ba[rz]')
        self.assertIsNotNone(mim.match( {'a': regex}, doc))
        self.assertIsNone(mim.match( {'b': regex}, doc))
        self.assertIsNone(mim.match( {'c': regex}, doc))
        self.assertIsNone(mim.match( {'d': regex}, doc))

    def test_regex_match_inside(self):
        doc = { 'a': 'bar', 'b': 'bat', 'c': None}
        regex = re.compile(r'ar')
        self.assertIsNotNone(mim.match( {'a': regex}, doc))
        self.assertIsNone(mim.match( {'b': regex}, doc))
        self.assertIsNone(mim.match( {'c': regex}, doc))
        self.assertIsNone(mim.match( {'d': regex}, doc))

    def test_regex_match_begin(self):
        doc = { 'a': 'bar', 'b': 'bet', 'c': None}
        regex = re.compile(r'^ba')
        self.assertIsNotNone(mim.match( {'a': regex}, doc))
        self.assertIsNone(mim.match( {'b': regex}, doc))
        self.assertIsNone(mim.match( {'c': regex}, doc))
        self.assertIsNone(mim.match( {'d': regex}, doc))

    def test_regex_match_array(self):
        doc = { 'a': ['hello world'], 'b': ['good night'], 'c': ['this is hello world'],
                'd': ['one', 'two', 'hello three']}
        regex = re.compile(r'^hello')
        self.assertIsNotNone(mim.match( {'a': regex}, doc))
        self.assertIsNone(mim.match( {'b': regex}, doc))
        self.assertIsNone(mim.match( {'c': regex}, doc))
        self.assertIsNotNone(mim.match( {'d': regex}, doc))

    def test_subdoc_partial(self):
        doc = {'a': {'b': 1, 'c': 1}}
        self.assertIsNotNone(mim.match({'a.b': 1}, doc))
        self.assertIsNone(mim.match({'a.b': 2}, doc))

    def test_subdoc_exact(self):
        doc = {'a': {'b': 1}}
        self.assertIsNotNone(mim.match({'a': {'b': 1}}, doc))
        self.assertIsNone(mim.match({'a': {'b': 2}}, doc))
        self.assertIsNone(mim.match({'a': {'b': 1, 'c': 1}}, doc))

    def test_subdoc_deep(self):
        doc = {'a': {'b': {'c': 1}}}
        self.assertIsNotNone(mim.match({'a': {'b': {'c': 1}}}, doc))

    def test_subdoc_deep_list(self):
        doc = {'a': [0, [1, 1, 1], 2, 3]}
        self.assertIsNotNone(mim.match({'a': [0, [1, 1, 1], 2, 3]}, doc))

    def test_traverse_none(self):
        doc = {'a': None}
        self.assertIsNone(mim.match({'a.b.c': 1}, doc))

    def test_uuid_match(self):
        uu = uuid.UUID('{12345678-1234-5678-1234-567812345678}')
        uu_same = uuid.UUID('{12345678-1234-5678-1234-567812345678}')
        uu_diff = uuid.UUID('{12345678-1234-5678-1234-567812345670}')
        doc = {'a': uu}
        self.assertIsNotNone(mim.match({'a': uu_same}, doc))
        self.assertIsNone(mim.match({'a': uu_diff}, doc))


class TestBulkOperations(TestCase):
    def setUp(self):
        self.bind = create_datastore('mim:///testdb')
        self.bind.conn.drop_all()
        
    def test_update_one(self):
        coll = self.bind.db.coll
        coll.insert_one({'dme-o': 1})
        coll.insert_one({'dme-o': 1})

        coll.bulk_write([
            UpdateOne({'dme-o': 1}, {'$set': {'dme-o': 2}})
        ])

        data = sorted(a['dme-o'] for a in coll.find({'dme-o': {'$exists': True}}))
        self.assertEqual(data, [1, 2])


class TestAggregate(TestCase):

    def setUp(self):
        self.bind = create_datastore('mim:///testdb')
        self.bind.conn.drop_all()
        self.bind.db.coll.insert_one({'_id':'foo', 'a':2, 'c':[1,2,3],
                                  'z': {'egg': 'spam', 'spam': 'egg'}})
        for r in range(4):
            self.bind.db.rcoll.insert_one({'_id':'r%s' % r, 'd':r})

    def test_aggregate_match(self):
        res = self.bind.db.rcoll.aggregate([{'$match': {'d': {'$lt': 2}}}])
        self.assertEqual(len(list(res)), 2)

    def test_aggregate_match_sort(self):
        res = self.bind.db.rcoll.aggregate([{'$match': {'d': {'$lt': 2}}},
                                            {'$sort': {'d': -1}}])
        res = list(res)
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]['d'], 1)

        res = self.bind.db.rcoll.aggregate([{'$match': {'d': {'$lt': 2}}},
                                            {'$sort': {'d': 1}}])
        res = list(res)
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0]['d'], 0)

    def test_aggregate_match_sort_limit(self):
        res = self.bind.db.rcoll.aggregate([{'$match': {'d': {'$lt': 2}}},
                                            {'$sort': {'d': -1}},
                                            {'$limit': 1}])
        res = list(res)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['d'], 1)
