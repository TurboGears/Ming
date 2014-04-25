from unittest import TestCase

from mock import Mock

from ming.odm.icollection import instrument, deinstrument, InstrumentedObj

class TestICollection(TestCase):

    def setUp(self):
        self.obj = dict(
            a=[ 1,2,3 ])
        self.tracker = Mock()
        self.iobj = instrument(self.obj, self.tracker)
        self.obj1 = deinstrument(self.iobj)
        self.list = [ 1,2,3 ]
        self.ilist = instrument(list(self.list), self.tracker)
        self.list1 = deinstrument(self.ilist)
        class Child(InstrumentedObj):
            attr = 5
        self.Child = Child

    def test_instrument(self):
        self.assertNotEqual(type(self.iobj), dict)
        self.assertNotEqual(type(self.iobj['a']), list)
        self.assertNotEqual(type(self.ilist), list)
        self.assertEqual(type(self.obj1), dict)
        self.assertEqual(type(self.obj1['a']), list)
        self.assertEqual(type(self.list1), list)
        self.assertEqual(self.list1, deinstrument(self.ilist))

    def test_derived(self):
        ch = self.Child({}, self.tracker)
        ch.attr = 10
        self.assertEqual(ch.attr, 10)
        self.assertRaises(KeyError, ch.__getitem__, 'attr')

    def test_iobj(self):
        self.iobj['b'] = 5
        self.tracker.added_item.assert_called_with(5)
        self.iobj['b'] = 10
        self.tracker.added_item.assert_called_with(10)
        self.tracker.removed_item.assert_called_with(5)
        del self.iobj['b']
        self.tracker.removed_item.assert_called_with(10)
        self.assertEqual(self.iobj.a, [1,2,3])
        self.assertRaises(AttributeError, getattr, self.iobj, 'b')
        self.iobj.b = '5'
        self.iobj.pop('b')
        self.tracker.removed_item.assert_called_with('5')
        self.iobj.popitem()
        self.tracker.removed_item.assert_called_with([1,2,3])
        self.iobj.clear()
        self.tracker.cleared.assert_called_with()
        self.iobj.update(dict(a=5, b=6),
                         c=7, d=8)
        self.assertEqual(self.iobj, dict(a=5, b=6, c=7, d=8))
        self.iobj.replace(dict(a=5, b=6))
        self.assertEqual(self.iobj, dict(a=5, b=6))
        assert self.iobj.get('a', 4) == 5, self.iobj
        assert self.iobj.get('x') == None, self.iobj
        assert self.iobj.get('x', 7) == 7, self.iobj

    def test_ilist(self):
        self.ilist[0] = 5
        self.assertEqual(self.ilist[0], 5)
        self.tracker.removed_item.assert_called_with(1)
        self.tracker.added_item.assert_called_with(5)
        self.ilist[:2] = [1,2,3]
        self.tracker.removed_items.assert_called_with([5,2])
        self.tracker.added_items.assert_called_with([1,2,3])
        self.assertEqual(self.ilist, [1,2,3,3])
        del self.ilist[0]
        self.tracker.removed_item.assert_called_with(1)
        self.assertEqual(self.ilist, [2,3,3])
        del self.ilist[:1]
        self.tracker.removed_items.assert_called_with([2])
        self.assertEqual(self.ilist, [3,3])
        self.ilist += self.list
        self.tracker.added_items.assert_called_with([1,2,3])
        self.assertEqual(self.ilist, [3,3,1,2,3])
        self.ilist *= 2
        self.tracker.added_items.assert_called_with([3,3,1,2,3])
        self.assertEqual(self.ilist, [3,3,1,2,3] * 2)
        self.ilist *= 0
        self.tracker.removed_items.assert_called_with([3,3,1,2,3] * 2)
        self.assertEqual(self.ilist, [])
        self.ilist.insert(0, 1)
        self.tracker.added_item.assert_called_with(1)
        self.ilist.insert(0, 2)
        self.tracker.added_item.assert_called_with(2)
        self.assertEqual(self.ilist, [2, 1])
        self.assertEqual(self.ilist.pop(), 1)
        self.tracker.removed_item.assert_called_with(1)
        self.ilist.replace([1,2,3,4])
        self.ilist.remove(2)
        self.assertEqual(self.ilist, [1,3,4])
        self.tracker.removed_item.assert_called_with(2)
        self.assertRaises(ValueError, self.ilist.remove, 22)
        self.assertEqual(self.ilist.pop(0), 1)
        self.tracker.removed_item.assert_called_with(1)


