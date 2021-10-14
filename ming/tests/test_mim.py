import re
from datetime import datetime
from unittest import TestCase

import bson
from ming import create_datastore, mim
from pymongo import UpdateOne
from pymongo.errors import OperationFailure, DuplicateKeyError
from mock import patch


class TestDatastore(TestCase):

    def setUp(self):
        self.bind = create_datastore('mim:///testdb')
        self.bind.conn.drop_all()
        self.bind.db.coll.insert({'_id':'foo', 'a':2, 'c':[1,2,3], 'z': {'egg': 'spam', 'spam': 'egg'}})
        for r in range(4):
            self.bind.db.rcoll.insert({'_id':'r%s' % r, 'd':r})

    def test_limit(self):
        f = self.bind.db.rcoll.find
        self.assertEqual(2, len(f({}).limit(2).all()))
        self.assertEqual(4, len(f({}).limit(0).all()))

    def test_regex(self):
        f = self.bind.db.rcoll.find
        assert 4 == f(dict(_id=re.compile(r'r\d+'))).count()
        assert 2 == f(dict(_id=re.compile(r'r[0-1]'))).count()

    def test_regex_options(self):
        f = self.bind.db.rcoll.find
        assert 2 == f(dict(_id={'$regex': 'r[0-1]', '$options': 'i'})).count()

    def test_eq(self):
        f = self.bind.db.rcoll.find
        assert 1 == f(dict(d={'$eq': 0})).count()

    def test_ne(self):
        f = self.bind.db.rcoll.find
        assert 3 == f(dict(d={'$ne': 0})).count()

    def test_gt(self):
        f = self.bind.db.rcoll.find
        assert 1 == f(dict(d={'$gt': 2})).count()
        assert 0 == f(dict(d={'$gt': 3})).count()

    def test_gte(self):
        f = self.bind.db.rcoll.find
        assert 2 == f(dict(d={'$gte': 2})).count()
        assert 1 == f(dict(d={'$gte': 3})).count()

    def test_lt(self):
        f = self.bind.db.rcoll.find
        assert 0 == f(dict(d={'$lt': 0})).count()
        assert 1 == f(dict(d={'$lt': 1})).count()
        assert 2 == f(dict(d={'$lt': 2})).count()

    def test_lte(self):
        f = self.bind.db.rcoll.find
        assert 1 == f(dict(d={'$lte': 0})).count()
        assert 2 == f(dict(d={'$lte': 1})).count()
        assert 3 == f(dict(d={'$lte': 2})).count()

    def test_range_equal(self):
        f = self.bind.db.rcoll.find
        assert 1 == f(dict(d={'$gte': 2, '$lte': 2})).count()
        assert 2 == f(dict(d={'$gte': 1, '$lte': 2})).count()
        assert 0 == f(dict(d={'$gte': 4, '$lte': -1})).count()

    def test_range_inequal(self):
        f = self.bind.db.rcoll.find
        assert 0 == f(dict(d={'$gt': 2, '$lt': 2})).count()
        assert 1 == f(dict(d={'$gt': 2, '$lt': 4})).count()
        assert 0 == f(dict(d={'$gt': 1, '$lt': 2})).count()
        assert 1 == f(dict(d={'$gt': 1, '$lt': 3})).count()
        assert 0 == f(dict(d={'$gt': 4, '$lt': -1})).count()

    def test_exists(self):
        f = self.bind.db.coll.find
        assert 1 == f(dict(a={'$exists':True})).count()
        assert 0 == f(dict(a={'$exists':False})).count()
        assert 0 == f(dict(b={'$exists':True})).count()
        assert 1 == f(dict(b={'$exists':False})).count()

    def test_all(self):
        f = self.bind.db.coll.find
        assert 1 == f(dict(c={'$all':[1,2]})).count()
        assert 1 == f(dict(c={'$all':[1,2,3]})).count()
        assert 0 == f(dict(c={'$all':[2,3,4]})).count()
        assert 1 == f(dict(c={'$all':[]})).count()

    def test_or(self):
        f = self.bind.db.coll.find
        assert 1 == f(dict({'$or': [{'c':{'$all':[1,2,3]}}]})).count()
        assert 0 == f(dict({'$or': [{'c':{'$all':[4,2,3]}}]})).count()
        assert 1 == f(dict({'$or': [{'a': 2}, {'c':{'$all':[1,2,3]}}]})).count()
        self.assertEqual(0, f(dict({'_id': 'bar', '$or': [{'a': 2}, {'c':{'$all':[1,2,3]}}]})).count())
        self.assertEqual(1, f(dict({'_id': 'foo', '$or': [{'a': 2}, {'c':{'$all':[1,2,3]}}]})).count())

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

    def test_rewind(self):
        collection = self.bind.db.coll
        collection.insert({'a':'b'}, safe=True)

        cursor = collection.find()
        doc = cursor[0]
        cursor.next()
        cursor.rewind()
        assert cursor.next() == doc

    def test_close(self):
        collection = self.bind.db.coll
        collection.insert({'a': 'b'})
        cursor = collection.find()
        cursor.close()
        self.assertRaises(StopIteration, cursor.next)

    def test_search(self):
        conn = mim.Connection().get()
        coll = conn.searchdatabase.coll
        coll.create_index([('field', 'text')])
        coll.insert({'field': 'text to be searched'})
        coll.insert({'field': 'text to be'})
        assert coll.find({'$text': {'$search': 'searched'}},
                         {'score': {'$meta': 'textScore'}}).count() == 1


class TestDottedOperators(TestCase):

    def setUp(self):
        self.bind = create_datastore('mim:///testdb')
        self.bind.conn.drop_all()
        self.bind.db.coll.insert(
            {'_id':'foo', 'a':2,
             'b': { 'c': 1, 'd': 2, 'e': [1,2,3],
                    'f': [ { 'g': 1 }, { 'g': 2 } ] },
             'x': {} })
        self.coll = self.bind.db.coll

    def test_inc_dotted_dollar(self):
        self.coll.update({'b.e': 2}, { '$inc': { 'b.e.$': 1 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.e': 1 })
        self.assertEqual(obj, { 'b': { 'e': [ 1,3,3 ] } })

    def test_inc_dotted_dollar_middle1(self):
        # match on g=1 and $inc by 10
        self.coll.update({'b.f.g': 1}, { '$inc': { 'b.f.$.g': 10 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.f': 1 })
        self.assertEqual(obj, { 'b': { 'f': [ { 'g': 11 }, { 'g': 2 } ] }})

    def test_find_dotted(self):
        self.assertEqual(self.coll.find({'b.c': 1}).count(), 1)
        self.assertEqual(self.coll.find({'b.c': 2}).count(), 0)
        self.assertEqual(0, len(self.coll.find({'x.y.z': 1}).all()))

    def test_inc_dotted(self):
        self.coll.update({}, { '$inc': { 'b.c': 4 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.c': 1 })
        self.assertEqual(obj, { 'b': { 'c': 5 } })

    def test_set_dotted(self):
        self.coll.update({}, { '$set': { 'b.c': 4 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.c': 1 })
        self.assertEqual(obj, { 'b': { 'c': 4 } })

    def test_set_dotted_with_integer(self):
        self.bind.db.coll.insert(
            {'_id':'foo2', 'a':2,
             'b': [1,2,3],
             'x': {} })
        self.coll.update({'_id': 'foo2'}, {'$set': {'b.0': 4}})
        obj = self.coll.find_one({'_id': 'foo2'})
        self.assertEqual(obj, {u'a': 2, u'x': {}, u'_id': u'foo2', u'b': [4, 2, 3]})

    def test_unset_dotted(self):
        self.coll.update({}, { '$unset': { 'b.f.1.g': 1 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.f': 1 })
        self.assertEqual(obj, { 'b': { 'f': [{u'g': 1}, {}] } })

        # Check that it even works for keys that are not there.
        self.coll.update({}, { '$unset': { 'b.this_does_not_exists': 1 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.f': 1 })
        self.assertEqual(obj, { 'b': { 'f': [{u'g': 1}, {}] } })

        # Check that unsetting subkeys of a nonexisting subdocument has no side effect
        self.coll.update({}, {'$unset': {'this_does_not_exists.x.y.z': 1}})
        obj = self.coll.find_one({}, { '_id': 0, 'b.f': 1 })
        self.assertEqual(obj, { 'b': { 'f': [{u'g': 1}, {}] } })

    def test_push_dotted(self):
        self.coll.update({}, { '$push': { 'b.e': 4 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.e': 1 })
        self.assertEqual(obj, { 'b': { 'e': [1,2,3,4] } })

    def test_addToSet_dotted(self):
        self.coll.update({}, { '$addToSet': { 'b.e': 4 } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.e': 1 })
        self.assertEqual(obj, { 'b': { 'e': [1,2,3,4] } })
        self.coll.update({}, { '$addToSet': { 'b.e': 4 } })
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
        self.coll.update(
            {},
            { '$pull': { 'b.f': { 'g': { '$gt': 1 } } } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.f': 1 } )
        self.assertEqual(obj, { 'b': { 'f': [ {'g': 1 } ] } } )

    def test_pull_all_dotted(self):
        self.coll.update(
            {},
            { '$pullAll': { 'b.f': [{'g': 1 }] } })
        obj = self.coll.find_one({}, { '_id': 0, 'b.f': 1 } )
        self.assertEqual(obj, { 'b': { 'f': [ {'g': 2 } ] } } )

    def test_pop_dotted(self):
        self.coll.update(
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
        self.bind.db.coll.insert(self.doc)

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


class TestMRCommands(TestCommands):

    def setUp(self):
        super(TestMRCommands, self).setUp()
        if not self.bind.db._jsruntime:
            self.skipTest("Javascript Runtime Unavailable")

    def test_mr_inline(self):
        result = self.bind.db.command(
            'mapreduce', 'coll',
            map='function(){ emit(1, this.a); }',
            reduce=self.sum_js,
            out=dict(inline=1))
        self.assertEqual(result['results'], [ dict(_id=1, value=2) ])

    def test_mr_inline_date_key(self):
        dt = datetime.utcnow()
        dt = dt.replace(microsecond=123000)
        self.bind.db.date_coll.insert({'a': dt })
        result = self.bind.db.command(
            'mapreduce', 'date_coll',
            map='function(){ emit(1, this.a); }',
            reduce=self.first_js,
            out=dict(inline=1))
        self.assertEqual(result['results'][0]['value'], dt)

    def test_mr_inline_date_value(self):
        result = self.bind.db.command(
            'mapreduce', 'coll',
            map='function(){ emit(1, new Date()); }',
            reduce=self.first_js,
            out=dict(inline=1))
        self.assertEqual(result['results'][0]['_id'], 1)
        self.assert_(isinstance(result['results'][0]['value'], datetime))

    # MAP_TIMESTAMP and REDUCE_MIN_MAX are based on the recipe
    # http://cookbook.mongodb.org/patterns/finding_max_and_min_values_for_a_key
    MAP_TIMESTAMP = bson.code.Code("""
    function () {
        emit('timestamp', { min : this.timestamp,
                            max : this.timestamp } )
    }
    """)

    REDUCE_MIN_MAX = bson.code.Code("""
    function (key, values) {
        var res = values[0];
        for ( var i=1; i<values.length; i++ ) {
            if ( values[i].min < res.min )
               res.min = values[i].min;
            if ( values[i].max > res.max )
               res.max = values[i].max;
        }
        return res;
    }
    """)

    def test_mr_inline_multi_date_response(self):
        # Calculate the min and max timestamp with one mapreduce call,
        # and return a mapping containing both values.
        self.bind.db.coll.remove()
        docs = [{'timestamp': datetime(2013, 1, 1, 14, 0)},
                {'timestamp': datetime(2013, 1, 9, 14, 0)},
                {'timestamp': datetime(2013, 1, 19, 14, 0)},
                ]
        for d in docs:
            self.bind.db.date_coll.insert(d)
        result = self.bind.db.date_coll.map_reduce(
            map=self.MAP_TIMESTAMP,
            reduce=self.REDUCE_MIN_MAX,
            out={'inline': 1})
        expected = [{'value': {'min': docs[0]['timestamp'],
                               'max': docs[-1]['timestamp']},
                     '_id': 'timestamp'}]
        print('RESULTS:', result['results'])
        print('EXPECTED:', expected)
        self.assertEqual(result['results'], expected)

    def test_mr_inline_collection(self):
        result = self.bind.db.coll.map_reduce(
            map='function(){ emit(1, this.a); }',
            reduce=self.sum_js,
            out=dict(inline=1))
        self.assertEqual(result['results'], [ dict(_id=1, value=2) ])

    def test_mr_finalize(self):
        result = self.bind.db.coll.map_reduce(
            map='function(){ emit(1, this.a); }',
            reduce=self.sum_js,
            out=dict(inline=1),
            finalize='function(k, v){ return v + 42; }')
        self.assertEqual(result['results'], [ dict(_id=1, value=44) ])

    def test_mr_merge(self):
        result = self.bind.db.command(
            'mapreduce', 'coll',
            map='function(){ emit(1, this.a+1); }',
            reduce=self.sum_js,
            out=dict(merge='coll'))
        self.assertEqual(result['result'], 'coll')
        self.assertEqual(
            sorted(list(self.bind.db.coll.find())),
            sorted([ self.doc, dict(_id=1, value=3) ]))

    def test_mr_merge_collection(self):
        result = self.bind.db.coll.map_reduce(
            map='function(){ emit(1, this.a+1); }',
            reduce=self.sum_js,
            out=dict(merge='coll'))
        self.assertEqual(result['result'], 'coll')
        self.assertEqual(
            sorted(list(self.bind.db.coll.find())),
            sorted([ self.doc, dict(_id=1, value=3) ]))

    def test_mr_replace(self):
        result = self.bind.db.command(
            'mapreduce', 'coll',
            map='function(){ emit(1, this.a+1); }',
            reduce=self.sum_js,
            out=dict(replace='coll'))
        self.assertEqual(result['result'], 'coll')
        self.assertEqual(
            list(self.bind.db.coll.find()),
            [ dict(_id=1, value=3) ])

    def test_mr_replace_collection(self):
        result = self.bind.db.coll.map_reduce(
            map='function(){ emit(1, this.a+1); }',
            reduce=self.sum_js,
            out=dict(replace='coll'))
        self.assertEqual(result['result'], 'coll')
        self.assertEqual(
            list(self.bind.db.coll.find()),
            [ dict(_id=1, value=3) ])

    def test_mr_reduce(self):
        self.bind.db.reduce.insert(dict(
                _id=1, value=42))
        result = self.bind.db.command(
            'mapreduce', 'coll',
            map='function(){ emit(1, this.a+1); }',
            reduce=self.sum_js,
            out=dict(reduce='reduce'))
        self.assertEqual(result['result'], 'reduce')
        self.assertEqual(
            list(self.bind.db.reduce.find()),
            [ dict(_id=1, value=45) ])

    def test_mr_reduce_list(self):
        self.bind.db.reduce.insert(dict(
                _id=1, value=[42]))
        result = self.bind.db.command(
            'mapreduce', 'coll',
            map='function(){ emit(1, [1]); }',
            reduce=self.concat_js,
            out=dict(reduce='reduce'))
        self.assertEqual(result['result'], 'reduce')
        self.assertEqual(
            list(self.bind.db.reduce.find()),
            [ dict(_id=1, value=[1, 42]) ])

    def test_mr_reduce_collection(self):
        self.bind.db.reduce.insert(dict(
                _id=1, value=42))
        result = self.bind.db.coll.map_reduce(
            map='function(){ emit(1, this.a+1); }',
            reduce=self.sum_js,
            out=dict(reduce='reduce'))
        self.assertEqual(result['result'], 'reduce')
        self.assertEqual(
            list(self.bind.db.reduce.find()),
            [ dict(_id=1, value=45) ])

    def test_mr_replace_number_key_obj(self):
        # testing numerical keys nested in objects being reduced
        self.bind.db.coll.remove()
        docs = [ {'val': {'id': 1, 'c': 5}} ]
        for d in docs:
            self.bind.db.date_coll.insert(d)
        result = self.bind.db.date_coll.map_reduce(
            map='function(){ var d = {}; d[new String(this.val.id)] = this.val.c; emit("val", d); }',
            reduce=self.first_js,
            out=dict(replace='coll'))
        self.assertEqual(result['result'], 'coll')
        expected = [{u'_id': u'val', u'value': {u'1': 5}}]
        self.assertEqual(
            list(self.bind.db.coll.find()),
            expected)

    def test_mr_inline_number_key_obj(self):
        # testing numerical keys nested in objects being reduced
        self.bind.db.coll.remove()
        docs = [ {'val': {'id': 1, 'c': 5}} ]
        for d in docs:
            self.bind.db.date_coll.insert(d)
        result = self.bind.db.date_coll.map_reduce(
            map='function(){ var d = {}; d[new String(this.val.id)] = this.val.c; emit("val", d); }',
            reduce=self.first_js,
            out=dict(inline=1))
        expected = [{'_id': u'val', 'value': {'1': 5}}]
        self.assertEqual(result['results'], expected)


class TestCollection(TestCase):

    def setUp(self):
        self.bind = create_datastore('mim:///testdb')
        self.bind.conn.drop_all()

    def test_getitem_clones(self):
        test = self.bind.db.test
        test.insert({'a':'b'})
        cursor = test.find()
        doc = cursor[0]
        self.assertEqual(cursor.next(), doc)

    def test_upsert_simple(self):
        test = self.bind.db.test
        test.update(
            dict(_id=0, a=5),
            {'$set': dict(b=6) },
            upsert=True)
        doc = test.find_one()
        self.assertEqual(doc, dict(_id=0, a=5, b=6))

    def test_upsert_duplicated(self):
        test = self.bind.db.test
        test.ensure_index([('a', 1)], unique=True)

        # Try with any index
        test.update({'x': 'NOT_FOUND1'}, {'$set': {'a': 0}}, upsert=True)
        try:
            test.update({'x': 'NOT_FOUND2'}, {'$set': {'a': 0}}, upsert=True)
        except DuplicateKeyError:
            pass
        else:
            assert False, 'Had to detect duplicate key'

        # Now try with _id
        test.update({'x': 'NOT_FOUND3'}, {'$set': {'_id': 0}}, upsert=True)
        try:
            test.update({'x': 'NOT_FOUND4'}, {'$set': {'_id': 0}}, upsert=True)
        except DuplicateKeyError:
            pass
        else:
            assert False, 'Had to detect duplicate key'

    def test_upsert_setOnInsert(self):
        test = self.bind.db.test
        test.update(
            dict(_id=0, a=5),
            {'$set': dict(b=6),
             '$setOnInsert': dict(c=7)},
            upsert=True)
        doc = test.find_one()
        self.assertEqual(doc, dict(_id=0, a=5, b=6, c=7))

        test.update(dict(_id=0, a=5), {'$set': dict(b=0, c=0)})
        test.update(
            dict(_id=0, a=5),
            {'$set': dict(b=2),
             '$setOnInsert': dict(c=7)},
            upsert=True)
        doc = test.find_one()
        self.assertEqual(doc, dict(_id=0, a=5, b=2, c=0))

    def test_upsert_inc(self):
        test = self.bind.db.test
        test.update(
            dict(_id=0, a=5),
            {'$inc': dict(a=2, b=3) },
            upsert=True)
        doc = test.find_one()
        self.assertEqual(doc, dict(_id=0, a=7, b=3))

    def test_upsert_push(self):
        test = self.bind.db.test
        test.update(
            dict(_id=0, a=5),
            {'$push': dict(c=1) },
            upsert=True)
        doc = test.find_one()
        self.assertEqual(doc, dict(_id=0, a=5, c=[1]))

    def test_update_addToSet_with_each(self):
        self.bind.db.coll.insert({'_id': 0, 'a': [1, 2, 3]})
        self.bind.db.coll.update({},
                                 {'$addToSet': {'a': {'$each': [0, 2, 4]}}})
        doc = self.bind.db.coll.find_one()
        self.assertEqual(len(doc['a']), 5)

    def test_find_with_skip(self):
        for i in range(5):
            self.bind.db.coll.insert({'_id':str(i), 'a':i})
        result = self.bind.db.coll.find({}, skip=2)
        result = list(result)
        self.assertEqual(len(result), 3)

    def test_find_with_limit(self):
        for i in range(5):
            self.bind.db.coll.insert({'_id':str(i), 'a':i})
        result = self.bind.db.coll.find({}, limit=2)
        result = list(result)
        self.assertEqual(len(result), 2)

    def test_find_with_slice_skip(self):
        for i in range(5):
            self.bind.db.coll.insert({'_id':str(i), 'a':i})
        result = self.bind.db.coll.find().sort('a')[3:]
        result = list(result)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['a'], 3)

    def test_find_with_slice_limit(self):
        for i in range(5):
            self.bind.db.coll.insert({'_id':str(i), 'a':i})
        result = self.bind.db.coll.find().sort('a')[:2]
        result = list(result)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['a'], 0)

    def test_find_with_slice_skip_limit(self):
        for i in range(5):
            self.bind.db.coll.insert({'_id':str(i), 'a':i})
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
            self.bind.db.coll.insert({'_id':str(i), 'a':i})
        result_all = self.bind.db.coll.find()
        result_all = list(result_all)
        result_page1 = self.bind.db.coll.find({}, skip=0, limit=3)
        result_page2 = self.bind.db.coll.find({}, skip=3, limit=3)
        result_paging = list(result_page1) + list(result_page2)
        self.assertEqual(result_all, result_paging)

    def test_distinct(self):
        for i in range(5):
            self.bind.db.coll.insert({'_id':str(i), 'a':'A'})
        result = self.bind.db.coll.distinct('a')
        self.assertEqual(result, ['A'])

    def test_distinct_subkey(self):
        for i in range(5):
            self.bind.db.coll.insert({'_id': str(i), 'a': {'b': 'A'}})
        result = self.bind.db.coll.distinct('a.b')
        self.assertEqual(result, ['A'])

    def test_distinct_sublist(self):
        for i in range(5):
            self.bind.db.coll.insert({'_id': str(i),
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
            self.bind.db.coll.insert({'_id': i, 'a': 'A'})
        result = self.bind.db.coll.distinct('_id', filter={'_id': {'$lte': 2}})
        self.assertEqual(set(result), {0, 1, 2})

    def test_find_and_modify_returns_none_on_no_entries(self):
        self.assertEqual(None, self.bind.db.foo.find_and_modify({'i': 1}, {'$set': {'i': 2}}))

    def test_find_and_modify_returns_none_on_upsert_and_no_new(self):
        self.assertEqual(None, self.bind.db.foo.find_and_modify({'i': 1},
                                                                {'$set': {'i': 2}},
                                                                upsert=True, new=False))

    def test_find_and_modify_returns_old_value_on_no_new(self):
        self.bind.db.foo.insert({'_id': 1, 'i': 1})
        self.assertEqual({'_id': 1, 'i': 1}, self.bind.db.foo.find_and_modify({'i': 1},
                                                                              {'$set': {'i': 2}},
                                                                              new=False))

    def test_find_and_modify_returns_new_value_on_new(self):
        self.bind.db.foo.insert({'_id': 1, 'i': 1})
        self.assertEqual({'_id': 1, 'i': 2}, self.bind.db.foo.find_and_modify({'i': 1},
                                                                              {'$set': {'i': 2}},
                                                                              new=True))

    def test_find_and_modify_returns_new_value_on_new_filter_id(self):
        self.bind.db.foo.insert({'i': 1})
        self.assertEqual({'i': 2}, self.bind.db.foo.find_and_modify({'i': 1},
                                                                    {'$set': {'i': 2}},
                                                                    fields={'_id': False, 'i': True},
                                                                    new=True))

    def test_find_and_modify_returns_new_value_on_new_upsert(self):
        self.assertEqual({'_id': 1, 'i': 2}, self.bind.db.foo.find_and_modify({'i': 1},
                                                                              {'$set': {'_id': 1,
                                                                                        'i': 2}},
                                                                              new=True,
                                                                              upsert=True))

    def test_find_and_modify_with_remove(self):
        self.bind.db.col.insert({'_id': 1})
        self.assertEqual({'_id': 1}, self.bind.db.col.find_and_modify({'_id': 1}, remove=True))
        self.assertEqual(0, self.bind.db.col.count())

    def test_hint_simple(self):
        self.bind.db.coll.ensure_index([('myindex', 1)])

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
        self.bind.db.coll.ensure_index([('myfield', 1)],
                                       background=True,
                                       expireAfterSeconds=42,
                                       unique=True)
        info = self.bind.db.coll.index_information()
        self.assertEqual(info['myfield']['key'], [('myfield', 1)])
        self.assertEqual(info['myfield']['background'], 1)
        self.assertEqual(info['myfield']['expireAfterSeconds'], 42)
        self.assertEqual(info['myfield']['unique'], True)

    def test_insert_manipulate_false(self):
        doc = {'x': 1}
        self.bind.db.coll.insert(doc, manipulate=False)
        self.assertEqual(doc, {'x': 1})

    def test_insert_manipulate_true(self):
        doc = {'x': 1}
        sample_id = bson.ObjectId()
        # Cannot patch the class itself, otherwise isinstance() checks will fail on PyPy
        with patch('bson.ObjectId.__init__', autospec=True, return_value=None, side_effect=lambda *args: args[0]._ObjectId__validate(sample_id)):
            self.bind.db.coll.insert(doc, manipulate=True)
        self.assertEqual(doc, {'x': 1, '_id': sample_id})

    def test_save_id(self):
        doc = {'_id': bson.ObjectId(), 'x': 1}
        self.bind.db.coll.save(doc)

    def test_save_no_id(self):
        doc = {'x': 1}
        self.bind.db.coll.save(doc)
        assert isinstance(doc['_id'], bson.ObjectId)

    def test_unique_index_subdocument(self):
        coll = self.bind.db.coll

        coll.ensure_index([('x.y', 1)], unique=True)
        coll.insert({'x': {'y': 1}})
        coll.insert({'x': {'y': 2}})
        self.assertRaises(DuplicateKeyError, coll.insert, {'x': {'y': 2}})

    def test_unique_index_whole_sdoc(self):
        coll = self.bind.db.coll

        coll.ensure_index([('x', 1)], unique=True)
        coll.insert({'x': {'y': 1}})
        coll.insert({'x': {'y': 2}})
        self.assertRaises(DuplicateKeyError, coll.insert, {'x': {'y': 2}})

    def test_unique_sparse_index_subdocument(self):
        coll = self.bind.db.coll

        coll.ensure_index([('x.y', 1)], unique=True, sparse=True)
        coll.insert({'x': {'y': 1}})

        # no duplicate key error on these:
        coll.insert({'x': {'y': None}})
        coll.insert({'x': {'y': None}})
        coll.insert({'x': {'other': 'field'}})
        coll.insert({'x': {'other': 'field'}})
        # still errors on an existing duplication
        self.assertRaises(DuplicateKeyError, coll.insert, {'x': {'y': 1}})

    def test_unique_sparse_index_whole_sdoc(self):
        coll = self.bind.db.coll

        coll.ensure_index([('x', 1)], unique=True, sparse=True)
        coll.insert({'x': {'y': 1}})
        # no duplicate key error on these:
        coll.insert({'x': None})
        coll.insert({'x': None})
        coll.insert({'other': 'field'})
        coll.insert({'other': 'field'})
        # still errors on an existing duplication
        self.assertRaises(DuplicateKeyError, coll.insert, {'x': {'y': 1}})

    def test_delete_many(self):
        coll = self.bind.db.coll

        coll.insert({'dme-m': 1})
        coll.insert({'dme-m': 1})
        coll.insert({'dme-m': 2})

        self.assertEqual(coll.delete_many({'dme-m': 1}).deleted_count, 2)

    def test_delete_one(self):
        coll = self.bind.db.coll

        coll.insert({'dme-o': 1})
        coll.insert({'dme-o': 1})
        coll.insert({'dme-o': 2})

        self.assertEqual(coll.delete_one({'dme-o': 1}).deleted_count, 1)

    def test_find_one_and_delete(self):
        coll = self.bind.db.coll

        coll.insert({'dme-o': 1})
        coll.insert({'dme-o': 1})
        coll.insert({'dme-o': 2})

        coll.find_one_and_delete({'dme-o': 1})
        self.assertEqual(len(list(coll.find({'dme-o': {'$exists': True}}))), 2)


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


class TestBulkOperations(TestCase):
    def setUp(self):
        self.bind = create_datastore('mim:///testdb')
        self.bind.conn.drop_all()
        
    def test_update_one(self):
        coll = self.bind.db.coll
        coll.insert({'dme-o': 1})
        coll.insert({'dme-o': 1})

        coll.bulk_write([
            UpdateOne({'dme-o': 1}, {'$set': {'dme-o': 2}})
        ])

        data = sorted([a['dme-o'] for a in coll.find({'dme-o': {'$exists': True}})])
        self.assertEqual(data, [1, 2])


class TestAggregate(TestCase):

    def setUp(self):
        self.bind = create_datastore('mim:///testdb')
        self.bind.conn.drop_all()
        self.bind.db.coll.insert({'_id':'foo', 'a':2, 'c':[1,2,3],
                                  'z': {'egg': 'spam', 'spam': 'egg'}})
        for r in range(4):
            self.bind.db.rcoll.insert({'_id':'r%s' % r, 'd':r})

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
