from unittest import TestCase, main

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

    def test_uri_parse(self):
        uri = 'mongo://user:password@host:100/path?a=5'
        result = utils.parse_uri(uri, b='5')
        self.assertEqual(result, dict(
                scheme='mongo',
                host='host',
                username='user',
                password='password',
                port=100,
                path='/path',
                query=dict(a='5', b='5')))

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
        

if __name__ == '__main__':
    main()

