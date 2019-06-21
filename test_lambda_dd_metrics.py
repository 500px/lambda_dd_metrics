import unittest
import sys

from lambda_dd_metrics import DataDogMetrics
try:
    import mock
except ImportError:
    if sys.version_info < (3,4):
        # No built-in mock library
        raise
    from unittest import mock

mock_time = mock.MagicMock()


@mock.patch('time.time', mock_time)
class TestDataDogMetrics(unittest.TestCase):

    def test_incr(self):
        mock_time.return_value = float(1234)
        dd = DataDogMetrics('test')
        actual = dd.incr('test_metric')

        expected = 'MONITORING|1234|1|count|test.test_metric'

        self.assertEqual(expected, actual)

    def test_incr_with_tag(self):
        mock_time.return_value = float(1234)
        dd = DataDogMetrics('test')

        expected = 'MONITORING|1234|1|count|test.test_metric|#tag'
        for tags in (['tag'], ('tag',), {'tag'}, frozenset({'tag'})):
            actual = dd.incr('test_metric', tags=tags)
            self.assertEqual(expected, actual)

        return

    def test_incr_with_tags(self):
        mock_time.return_value = float(1234)
        dd = DataDogMetrics('test')

        expected = 'MONITORING|1234|1|count|test.test_metric|#tag1,tag2'
        for tags in (['tag1', 'tag2'], ('tag1', 'tag2')):
            actual = dd.incr('test_metric', tags=tags)
            self.assertEqual(expected, actual)

        expected_alt_order = 'MONITORING|1234|1|count|test.test_metric|#tag2,tag1'
        for tags in ({'tag1', 'tag2'}, frozenset({'tag1', 'tag2'})):
            actual = dd.incr('test_metric', tags=tags)
            self.assertLess( { actual }, { expected, expected_alt_order })
        return

    def test_incr_with_stats_group(self):
        mock_time.return_value = float(1234)
        dd = DataDogMetrics('test', 'test_group')
        actual = dd.incr('test_metric')

        expected = 'MONITORING|1234|1|count|test.test_metric|#group:test_group'

        self.assertEqual(expected, actual)

    def test_incr_with_count(self):
        mock_time.return_value = float(1234)
        dd = DataDogMetrics('test')
        actual = dd.incr('test_metric', 5)

        expected = 'MONITORING|1234|5|count|test.test_metric'

        self.assertEqual(expected, actual)

    def test_gauge(self):
        mock_time.return_value = float(1234)
        dd = DataDogMetrics('test')
        actual = dd.gauge('test_metric', 1)

        expected = 'MONITORING|1234|1|gauge|test.test_metric'

        self.assertEqual(expected, actual)

    def test_histogram(self):
        mock_time.return_value = float(1234)
        dd = DataDogMetrics('test')
        actual = dd.histogram('test_metric', 1)

        expected = 'MONITORING|1234|1|histogram|test.test_metric'

        self.assertEqual(expected, actual)

    def test_timer(self):
        mock_time.side_effect = [float(100), float(200), float(1234)]
        dd = DataDogMetrics('test')
        mock_histogram = dd.histogram = mock.MagicMock(wraps=dd.histogram)

        dd.timer('test_metric')(lambda x: x)('echo')
        actual_call = mock_histogram.call_args

        expected_call = mock.call('test_metric', float(100), None)

        self.assertEqual(expected_call, actual_call)

    def test_set(self):
        dd = DataDogMetrics('test')
        with self.assertRaises(NotImplementedError):
            dd.set('test_metric', { 1,2,3 }, [ 'tag' ])

        return
