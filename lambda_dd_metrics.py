#!/usr/bin/env python
'''
Simple interface for reporting metrics to DataDog.
'''

from __future__ import print_function
from functools import wraps
import time


class DataDogMetrics(object):
    '''
    Datadog supports printing to stdout to report metrics. This only gives us
    counts and gauges:
        https://www.datadoghq.com/blog/monitoring-lambda-functions-datadog

    Another method would be via the API but that one only supports gauges and
    requires auth, which I'd rather not do until they've added support for
    histograms and counts.
    '''
    def __init__(self, service_prefix, stats_group=None):
        self.service_prefix = service_prefix
        self.default_tags = ['group:%s' % stats_group] if stats_group is not None else []

    def incr(self, metric_name, count=1, tags=None):
        '''
        Incr - Increment a counter metric, providing a count of occurrences per
        second.
        '''
        full_metric_name = self._build_metric_name(metric_name)
        all_tags = self._build_tags(tags)
        return self._print_metric('count', full_metric_name, count, all_tags)

    def gauge(self, metric_name, value, tags=None):
        '''
        Gauge - Gauges are a constant data type. They are not subject to
        averaging, and they don't change unless you change them. That is, once
        you set a gauge value, it will be a flat line on the graph until you
        change it again.
        '''
        full_metric_name = self._build_metric_name(metric_name)
        all_tags = self._build_tags(tags)
        return self._print_metric('gauge', full_metric_name, value, all_tags)

    def histogram(self, metric_name, value, tags=None):
        '''
        Histogram - Send a histogram, tracking multiple samples of a metric
        '''
        full_metric_name = self._build_metric_name(metric_name)
        all_tags = self._build_tags(tags)
        return self._print_metric('histogram', full_metric_name, value, all_tags)

    def timer(self, metric_name, tags=None):
        '''
        Timer - A convenient decorator that automatically records the runtime
        of your function and reports it as a histogram.
        '''
        def decorator(function):
            @wraps(function)
            def wrapper(*args, **kwargs):
                start_time = time.time()
                ret_val = function(*args, **kwargs)
                duration = time.time() - start_time
                self.histogram(metric_name, duration, tags)
                return ret_val

            return wrapper
        return decorator

    def timing(self, metric_name, delta, tags=None):
        '''
        Timing - Track a duration event
        '''
        return self.histogram(metric_name, delta, tags)

    def set(self, metric_name, value, tags=None):
        '''
        Set - Send a metric that tracks the number of unique items in a set
        '''
        # NOT SUPPORTED YET

    def _build_tags(self, tags=None):
        return (tags or []) + self.default_tags

    def _build_metric_name(self, metric_name):
        return '{0}.{1}'.format(self.service_prefix, metric_name)

    def _print_metric(self, metric_type, metric_name, value, tags):
        unix_epoch_timestamp = int(time.time())
        metric = 'MONITORING|{0}|{1}|{2}|{3}'.format(
            unix_epoch_timestamp,
            value,
            metric_type,
            metric_name)
        if tags:
            metric += '|#{}'.format(','.join(tags))
        print(metric)
        return metric
