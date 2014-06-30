from unittest import TestCase

# Various test suite methods missing on Python2.6
if not hasattr(TestCase, 'assertIsNotNone'):
    def _assertIsNotNone(self, expr, *args):
        self.assertTrue(expr is not None, *args)
    TestCase.assertIsNotNone = _assertIsNotNone

if not hasattr(TestCase, 'assertIsNone'):
    def _assertIsNone(self, expr, *args):
        self.assertTrue(expr is None, *args)
    TestCase.assertIsNone = _assertIsNone

if not hasattr(TestCase, 'assertRaisesRegexp'):
    import re
    def _assertRaisesRegexp(self, expected_exception, expected_regexp,
                                 callable_obj, *args, **kwargs):
            try:
                callable_obj(*args, **kwargs)
            except expected_exception as exc_value:
                if isinstance(expected_regexp, (str, unicode)):
                    expected_regexp = re.compile(expected_regexp)
                if not expected_regexp.search(str(exc_value)):
                    raise self.failureException(
                        '"%s" does not match "%s"' %
                        (expected_regexp.pattern, str(exc_value)))
            else:
                if hasattr(expected_exception, '__name__'):
                    excName = expected_exception.__name__
                else:
                    excName = str(expected_exception)
                raise self.failureException("%s not raised" % excName)
    TestCase.assertRaisesRegexp = _assertRaisesRegexp
