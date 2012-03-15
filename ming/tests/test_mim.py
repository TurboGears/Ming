from unittest import TestCase

from ming import datastore as DS

class TestDatastore(TestCase):

    def setUp(self):
        self.bind = DS.DataStore(master='mim:///', database='testdb')
        self.bind.conn.drop_all()
        self.bind.db.coll.insert({'_id':'foo', 'a':2, 'c':[1,2,3]})

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

    def setUp(self):
        self.bind = DS.DataStore(master='mim:///', database='testdb')
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
