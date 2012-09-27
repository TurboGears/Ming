from datetime import datetime
from unittest import TestCase

from ming import create_datastore

class TestDatastore(TestCase):

    def setUp(self):
        self.bind = create_datastore('mim:///testdb')
        self.bind.conn.drop_all()
        self.bind.db.coll.insert({'_id':'foo', 'a':2, 'c':[1,2,3]})
        for r in range(4):
            self.bind.db.rcoll.insert({'_id':'r%s' % r, 'd':r})

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

    def test_mr_inline_collection(self):
        result = self.bind.db.coll.map_reduce(
            map='function(){ emit(1, this.a); }',
            reduce=self.sum_js,
            out=dict(inline=1))
        self.assertEqual(result['results'], [ dict(_id=1, value=2) ])

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

class TestCollection(TestCase):
    
    def setUp(self):
        self.bind = create_datastore('mim:///testdb')
        self.bind.conn.drop_all()

    def test_upsert_simple(self):
        test = self.bind.db.test
        test.update(
            dict(_id=0, a=5),
            {'$set': dict(b=6) },
            upsert=True)
        doc = test.find_one()
        self.assertEqual(doc, dict(_id=0, a=5, b=6))
        
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

    def test_distinct(self):
        for i in range(5):
            self.bind.db.coll.insert({'_id':str(i), 'a':'A'})
        result = self.bind.db.coll.distinct('a')
        self.assertEqual(result, ['A'])

    def test_find_and_modify_returns_none_on_no_entries(self):
        self.assertEqual(None, self.bind.db.foo.find_and_modify({'i': 1}, {'$set': {'i': 2}}))
