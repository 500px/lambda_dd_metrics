import unittest
import decimal
import sys

HAS_MODERN_UNITTEST = (sys.version_info >= (3,4))

from lambda_dd_metrics import DataDogMetrics, AggregatedDataDogMetrics, logger
try:
    import mock
except ImportError:
    if not HAS_MODERN_UNITTEST:
        # No built-in mock library
        raise
    from unittest import mock

mock_time = mock.MagicMock()


@mock.patch('lambda_dd_metrics.DataDogMetrics._get_timestamp', mock_time)
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

    def test_aggregate_counters(self):
        mock_time.return_value = float(1234)
        dd = AggregatedDataDogMetrics('test')
        dd.incr('test_metric', 5)
        dd.incr('test_metric', 2.25, [ 'tag1'])
        dd.incr('test_metric', 3),
        dd.incr('test_metric', tags =[ 'tag2', 'tag1'])
        dd.incr('test_metric')
        dd.incr('metric2', 7, [ 'tag1'])
        dd.incr('test_metric', 11, [ 'tag1', 'tag2'])
        dd.incr('test_metric', decimal.Decimal("-1.25"), [ 'tag1'])
        lines = set(dd.flush())
        expected = {
            'MONITORING|1234|9|count|test.test_metric',
            'MONITORING|1234|1.00|count|test.test_metric|#tag1',
            'MONITORING|1234|12|count|test.test_metric|#tag1,tag2',
            'MONITORING|1234|7|count|test.metric2|#tag1',
        }
        self.assertEqual(lines, expected)
        return

    def test_aggregate_guages(self):
        mock_time.return_value = float(1234)
        dd = AggregatedDataDogMetrics('test')
        dd.gauge('test_metric', 5)
        dd.gauge('test_metric', 2, [ 'tag1'])
        dd.gauge('test_metric', 3)
        dd.gauge('metric2', 7, [ 'tag1'])
        dd.gauge('test_metric', 6, [ 'tag1', 'tag2'])
        lines = set(dd.flush())
        expected = {
            'MONITORING|1234|3|gauge|test.test_metric',
            'MONITORING|1234|2|gauge|test.test_metric|#tag1',
            'MONITORING|1234|6|gauge|test.test_metric|#tag1,tag2',
            'MONITORING|1234|7|gauge|test.metric2|#tag1',
        }
        self.assertEqual(lines, expected)
        return

    @unittest.skipUnless(HAS_MODERN_UNITTEST, "No support for testing logging")
    def test_logging(self):
        mock_time.return_value = float(1234)
        dd = AggregatedDataDogMetrics('log_test')
        with self.assertLogs(logger, "INFO") as cm:
            dd.incr('test_metric', 5)
            dd.incr('test_metric', 2, [ 'tag1'])
            dd.incr('test_metric', -1)
            dd.gauge('metric2', 7, [ 'tag1'])
            assert dd.flush_all() == 3
            self.assertEqual(set(cm.output), {
                "INFO:lambda_dd_metrics:Sent 2 log_test count metric(s).",
                "INFO:lambda_dd_metrics:Sent 1 log_test gauge metric(s)."
            })


        with self.assertLogs(logger, "INFO") as cm:
            dd.incr('test_metric', 5)
            del dd
            self.assertEqual(cm.output, [
                "INFO:lambda_dd_metrics:Sent 1 log_test count metric(s).",
                "INFO:lambda_dd_metrics:Sent 1 log_test total remaining metric(s)."
            ])

        return
