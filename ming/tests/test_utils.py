from unittest import TestCase, main, SkipTest

import pymongo

from ming import utils

class TestUtils(TestCase):

    def test_lazy_property(self):
        counter = [ 0 ]
        class MyClass(object):
            @utils.LazyProperty
            def prop(self):
                counter[0] += 1
                return 5
        obj = MyClass()
        self.assertEqual(counter, [0])
        self.assertEqual(obj.prop, 5)
        self.assertEqual(counter, [1])
        self.assertEqual(obj.prop, 5)
        self.assertEqual(counter, [1])

    def test_wordwrap(self):
        s='''The quick brown fox jumped over the lazy dog'''
        lines = utils.wordwrap(s, width=20, indent_first=5,
                               indent_subsequent=2).split('\n')
        self.assertEqual(lines[0].strip(), 'The quick brown')
        self.assertEqual(lines[1].strip(), 'fox jumped over')
        self.assertEqual(lines[2].strip(), 'the lazy dog')

    def test_indent(self):
        s='''The quick brown fox jumped over the lazy dog'''
        lines = utils.indent(utils.wordwrap(s, width=20), 4).split('\n')
        self.assertEqual(lines[0], 'The quick brown fox')
        self.assertEqual(lines[1], '    jumped over the lazy')
        self.assertEqual(lines[2], '    dog')

    def test_fixup_index(self):
        self.assertEqual(
            utils.fixup_index('foo'),
            [('foo', pymongo.ASCENDING)])
        self.assertEqual(
            utils.fixup_index(['foo']),
            [('foo', pymongo.ASCENDING)])
        self.assertEqual(
            utils.fixup_index([('foo', pymongo.ASCENDING)]),
            [('foo', pymongo.ASCENDING)])
        self.assertEqual(
            utils.fixup_index([('foo', pymongo.DESCENDING)]),
            [('foo', pymongo.DESCENDING)])
        self.assertEqual(
            utils.fixup_index(('foo', 'bar')),
            [('foo', pymongo.ASCENDING), ('bar', pymongo.ASCENDING)])
        self.assertEqual(
            utils.fixup_index([('foo',pymongo.DESCENDING), 'bar']),
            [('foo', pymongo.DESCENDING), ('bar', pymongo.ASCENDING)])
        completed = [ ('a', 1), ('b', -1) ]
        self.assertEqual(completed, utils.fixup_index([completed]))

    def test_fixup_text_index(self):
        if not hasattr(pymongo, 'TEXT'):
            raise SkipTest('text index not supported in this pymongo version')
        self.assertEqual(
            [('foo', pymongo.TEXT)],
            utils.fixup_index([('foo', pymongo.TEXT)]))
        self.assertEqual(
            [('foo', pymongo.TEXT), ('bar', pymongo.ASCENDING)],
            utils.fixup_index([('foo', pymongo.TEXT), 'bar']))

if __name__ == '__main__':
    main()

