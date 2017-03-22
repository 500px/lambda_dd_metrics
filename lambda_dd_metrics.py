#!/usr/bin/env python
'''
Simple interface for reporting metrics to DataDog.
'''

from __future__ import print_function
import logging
import os
import time

class DataDogMetrics(object):
    '''
    Datadog supports printing to stdout to report metrics.
    This only gives us counts and gauges:
        https://www.datadoghq.com/blog/monitoring-lambda-functions-datadog

    Another method would be via the API but that one only supports gauges
        and requires auth, which I'd rather not do until they've added
        support for histograms and counts.

    Follows the interface from go-utils:
        https://github.com/500px/go-utils/blob/master/metrics/statsd_client.go
    '''
    def __init__(self, service_prefix, stats_group):
        self.service_prefix = service_prefix
        self.stats_group = stats_group

        logging.basicConfig()
        self.logger = logging.getLogger()
        log_level = logging.getLevelName(os.environ.get('LOG_LEVEL', 'WARNING'))
        self.logger.setLevel(log_level)

    def incr(self, metric_name, count=1, tags=None):
        '''
        Incr - Increment a counter metric, providing a count of occurances per second
        '''
        full_metric_name = self._build_metric_name(metric_name)
        all_tags = self._build_tags(tags)
        self._print_metric('count', full_metric_name, count, all_tags)

    def gauge(self, metric_name, value, tags=None):
        '''
        Gauge - Gauges are a constant data type. They are not subject to averaging,
        and they don't change unless you change them. That is, once you set a gauge value,
        it will be a flat line on the graph until you change it again
        '''
        full_metric_name = self._build_metric_name(metric_name)
        all_tags = self._build_tags(tags)
        self._print_metric('gauge', full_metric_name, value, all_tags)

    def histogram(self, metric_name, value, tags=None):
        '''
        Histogram - Send a histogram, tracking multiple samples of a metric
        '''
        # NOT SUPPORTED YET

    def timing(self, metric_name, delta, tags=None):
        '''
        Timing - Track a duration event
        '''
        self.histogram(metric_name, delta, tags)

    def set(self, metric_name, value, tags=None):
        '''
        Set - Send a metric that tracks the number of unique items in a set
        '''
        # NOT SUPPORTED YET

    def _build_tags(self, tags=None):
        default_tags = ['group:%s' % self.stats_group]
        return (tags or []) + default_tags

    def _build_metric_name(self, metric_name):
        return '{0}.{1}'.format(self.service_prefix, metric_name)

    def _print_metric(self, metric_type, metric_name, value, tags):
        unix_epoch_timestamp = int(time.time())
        print('MONITORING|{0}|{1}|{2}|{3}|#{4}'.format(
            unix_epoch_timestamp, value, metric_type, metric_name, ','.join(tags)
        ))
