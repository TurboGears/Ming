from datetime import datetime
from decimal import Decimal
from unittest import TestCase, main
import copy
import six

from bson import ObjectId

from ming.base import Object

class TestObject(TestCase):

    def test_object_copyable(self):
        "Object is pretty basic concept, so must be freely copyable."
        obj = Object(foo=1, bar='str')
        obj1 = copy.copy(obj)
        obj2 = copy.deepcopy(obj)
        self.assertEqual(obj, obj1)
        self.assertEqual(obj, obj2)

    def test_get_set(self):
        d = dict(a=1, b=2)
        obj = Object(d, c=3)
        self.assertEqual(1, obj.a)
        self.assertEqual(1, obj['a'])
        self.assertEqual(3, obj.c)
        self.assertEqual(3, obj['c'])
        obj.d = 5
        self.assertEqual(5, obj['d'])
        self.assertEqual(5, obj.d)
        self.assertEqual(obj, dict(a=1, b=2, c=3, d=5))
        self.assertRaises(AttributeError, getattr, obj, 'e')

    def test_from_bson(self):
        bson = dict(
            a=[1,2,3],
            b=dict(c=5))
        obj = Object.from_bson(bson)
        self.assertEqual(obj, dict(a=[1,2,3], b=dict(c=5)))

    def test_safe(self):
        now = datetime.now()
        oid = ObjectId()

        c = [ 'foo', 1, 1.0, now,
              Decimal('0.3'), None, oid ]

        safe_obj = Object(
            a=[1,2,3],
            b=dict(a=12),
            c=c)
        safe_obj.make_safe()
        expected = Object(
                a=[1,2,3], b=dict(a=12),
                c=c)
        self.assertTrue(expected, safe_obj)

        unsafe_obj = Object(
            my_tuple=(1,2,3))
        self.assertRaises(AssertionError, unsafe_obj.make_safe)


if __name__ == '__main__':
    main()
