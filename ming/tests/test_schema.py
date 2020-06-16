from decimal import Decimal, ROUND_DOWN, ROUND_HALF_DOWN
from unittest import TestCase, main
from datetime import datetime, date

from bson import Decimal128

import ming.datastore
import pytz
from ming import Document, Field
from ming import schema as S

class TestQuerySafety(TestCase):

    def setUp(self):
        self.bind = ming.create_datastore('mim:///datastore')
        self.bind.conn.drop_all()
        self.bind.db.coll.insert({'_id':'foo', 'a':2, 'b':3})
        self.session = ming.Session(self.bind)
        class Doc(Document):
            class __mongometa__:
                name='coll'
                session = self.session
            _id=Field(str)
            a=Field(int)
        self.Doc = Doc

    def test_extra_fields_stripped(self):
        r = self.Doc.m.find().all()
        assert  r == [ dict(a=2, _id='foo') ], r
        assert 'Doc' in str(type(r[0]))
        r = self.Doc.m.find({}, allow_extra=True).all()
        assert  r == [ dict(a=2, _id='foo') ], r
        r = self.Doc.m.get(_id='foo')
        assert  r == dict(a=2, _id='foo'), r

    def test_extra_fields_not_stripped(self):
        r = self.Doc.m.find({}, strip_extra=False).all()
        assert r == [ dict(a=2, b=3, _id='foo') ], r

    def test_extra_fields_not_allowed(self):
        q = self.Doc.m.find({}, allow_extra=False)
        self.assertRaises(S.Invalid, q.all)

    def test_no_validate(self):
        r = list(self.Doc.m.find({}, validate=False))
        assert 'Doc' in str(type(r[0]))
        assert r == [dict(_id='foo', a=2, b=3)], r


class TestSchemaItem(TestCase):

    def test_make_array(self):
        si_any = S.SchemaItem.make([])
        si_int = S.SchemaItem.make([int])
        self.assertEqual(S.Array, si_any.__class__)
        self.assertEqual(S.Anything, si_any.field_type.__class__)
        self.assertEqual(S.Int, si_int.field_type.__class__)
        self.assertRaises(ValueError, S.SchemaItem.make, [int, str])

    def test_dont_allow_none(self):
        si = S.Int(allow_none=False)
        self.assertRaises(S.Invalid, si.validate, None)

    def test_validate_limited_range(self):
        si = S.Array(
            int,
            validate_ranges=[slice(0, 2) ])
        si.validate([1,2,'foo', 'bar'])
        self.assertRaises(S.Invalid, si.validate, [1,'foo', 'bar'])

    def test_dict_is_not_array(self):
        si = S.SchemaItem.make([])
        self.assertRaises(S.Invalid, si.validate, {})

    def test_truncate_microseconds(self):
        si = S.SchemaItem.make(datetime)
        self.assertEqual(
            datetime(2012,2,8,12,42,14,123000),
            si.validate(datetime(2012,2,8,12,42,14,123456)))

    def test_timezone_conversion(self):
        si = S.SchemaItem.make(datetime)
        self.assertEqual(
            datetime(2012,2,8,20,42,14,123000),
            si.validate(pytz.timezone('US/Pacific').localize(datetime(2012,2,8,12,42,14,123456))))

    def test_date_to_datetime(self):
        si = S.SchemaItem.make(datetime)
        self.assertEqual(datetime(2018, 2, 7, 0, 0), si.validate(date(2018, 2, 7)))

    def test_migrate(self):
        si = S.Migrate(int, str, str)
        self.assertEqual(si.validate(1), '1')
        self.assertEqual(si.validate('1'), '1')
        si = S.Migrate(
            {str:{'a':int}},
            [ dict(key=str, a=int) ],
            S.Migrate.obj_to_list('key'))
        self.assertEqual(si.validate(dict(foo=dict(a=1))),
                         [ dict(key='foo', a=1) ])
        si = S.Migrate(
            {str:int},
            [ dict(key=str, value=int) ],
            S.Migrate.obj_to_list('key', 'value'))
        self.assertEqual(si.validate(dict(foo=1)),
                         [ dict(key='foo', value=1) ])

    def test_migrate_both_invalid(self):
        def fixer(x):
            x['a'] = [x['a']]
            return x
        si = S.Migrate(
            {'a': str},
            {'a': [str], 'b': int},
            fixer)
        self.assertRaisesRegexp(S.Invalid, 'int',
                                lambda: si.validate(dict(a=['a'], b='b')))

    def test_none(self):
        si = S.SchemaItem.make(None)
        si.validate(1)
        si.validate(None)
        si.validate({'a':'b'})

    def test_deprecated(self):
        si = S.SchemaItem.make(dict(
                a=S.Deprecated(),
                b=int))
        self.assertEqual(si.validate(dict(a=5, b=6)),
                         dict(b=6))

    def test_fancy(self):
        si = S.SchemaItem.make(dict(
                a=S.Int(required=True),
                b=S.Int(if_missing=5)))
        self.assertRaises(S.Invalid, si.validate, dict(b=10))
        self.assertEqual(si.validate(dict(a=10)),
                         dict(a=10, b=5))

    def test_validation(self):
        si = S.SchemaItem.make({str:int})
        self.assertEqual(si.validate(dict(a=5)), dict(a=5))
        self.assertRaises(S.Invalid, si.validate, dict(a='as'))
        self.assertRaises(S.Invalid, si.validate, {5:5})

    def test_validate_base(self):
        si = S.SchemaItem()
        self.assertRaises(NotImplementedError, si.validate, None)

    def test_nested_objects(self):
        nested_object = S.Object(dict(a=int, b=int), if_missing=None)
        si = S.SchemaItem.make(dict(
                a=S.Object(dict(a=int, b=int), if_missing=None)))
        result = si.validate(dict())
        self.assertEqual(result, dict(a=None))

    def test_exact_value(self):
        si = S.SchemaItem.make(dict(version=4))
        self.assertEqual(si.validate(dict(version=4)), dict(version=4))
        self.assertRaises(S.Invalid, si.validate, dict(version=3))

    def test_missing(self):
        self.assertEqual(repr(S.Missing), '<Missing>')

    def test_nodefault(self):
        self.assertEqual(repr(S.NoDefault), '<NoDefault>')


class TestNumberDecimal(TestCase):
    def test_dont_allow_none(self):
        si = S.NumberDecimal(allow_none=False)
        self.assertRaises(S.Invalid, si.validate, None)

    def test_allow_none(self):
        si = S.NumberDecimal()
        assert si.validate(None) is None

    def test_specify_precision(self):
        si = S.NumberDecimal(precision=2)
        assert si.validate(0.123456789) == Decimal128("0.12")
        assert si.validate(0.125432109) == Decimal128("0.13")

    def test_specify_rounding(self):
        si = S.NumberDecimal(precision=2, rounding=ROUND_DOWN)
        assert si.validate(0.129999999) == Decimal128("0.12")
        assert si.validate(0.123456789) == Decimal128("0.12")

    def test_input_int_precision_explicit(self):
        si = S.NumberDecimal(precision=2)
        assert si.validate(0) == Decimal128("0.00")
        assert si.validate(1) == Decimal128("1.00")

    def test_input_float_precision_explicit(self):
        si = S.NumberDecimal(precision=2)
        assert si.validate(12.42) == Decimal128("12.42")

    def test_input_decimal_precision_explicit(self):
        si = S.NumberDecimal(precision=2)
        assert si.validate(Decimal("12.42")) == Decimal128("12.42")

    def test_input_decimal128_precision_explicit(self):
        si = S.NumberDecimal(precision=2)
        assert si.validate(Decimal128("12.42")) == Decimal128("12.42")

    def test_input_int(self):
        si = S.NumberDecimal()
        assert si.validate(0) == Decimal128("0")
        assert si.validate(1) == Decimal128("1")

    def test_input_float(self):
        si = S.NumberDecimal()
        validated = si.validate(12.42)
        assert isinstance(validated, Decimal128)
        quantized = validated.to_decimal().quantize(Decimal("0.01"),
                                                    rounding=ROUND_HALF_DOWN)
        assert quantized == Decimal("12.42")

    def test_input_decimal(self):
        si = S.NumberDecimal()
        assert si.validate(Decimal("12.42")) == Decimal128("12.42")

    def test_input_decimal128(self):
        si = S.NumberDecimal()
        assert si.validate(Decimal128("12.42")) == Decimal128("12.42")

    def test_input_str(self):
        si = S.NumberDecimal(precision=2)
        self.assertRaises(S.Invalid, si.validate, "12.42")


if __name__ == '__main__':
    main()
