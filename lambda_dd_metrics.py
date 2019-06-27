#!/usr/bin/env python
'''
Simple interface for reporting metrics to DataDog.
'''

from __future__ import print_function
import itertools
import collections
from functools import wraps
import time
import logging

logger = logging.getLogger(__name__)


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
                start_time = self._get_timestamp()
                ret_val = function(*args, **kwargs)
                duration = self._get_timestamp() - start_time
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
        raise NotImplementedError("Set isn't implemented yet")

    @staticmethod
    def _get_timestamp():
        return time.time()

    def _build_tags(self, tags=None):
        tags_type = type(tags) if tags is not None else tuple
        return tags_type(itertools.chain(tags or (), self.default_tags))

    def _build_metric_name(self, metric_name):
        return '{0}.{1}'.format(self.service_prefix, metric_name)

    def _print_metric(self, metric_type, metric_name, value, tags):
        unix_epoch_timestamp = int(self._get_timestamp())
        metric = 'MONITORING|{0}|{1}|{2}|{3}'.format(
            unix_epoch_timestamp,
            value,
            metric_type,
            metric_name)
        if tags:
            metric += '|#{}'.format(','.join(tags))
        print(metric)
        return metric


class AggregatedDataDogMetrics(DataDogMetrics):
    def __init__(self, *args, **kwargs):
        DataDogMetrics.__init__(self, *args, **kwargs)
        self._counts = self._make_dict_of_dicts(int)
        self._gauges = collections.defaultdict(dict)
        self._histograms = self._make_dict_of_dicts(list)
        self._sets = self._make_dict_of_dicts(set)
        self.tag_orderings = collections.defaultdict(dict)
        return

    def __del__(self):
        n = self.flush_all()
        self._log_send_if_nonzero(n, "total remaining")

    def incr(self, metric_name, count=1, tags=None):
        idx = self._make_aggregation_index(metric_name, tags)
        counts_for_metric = self._counts[metric_name]
        counter = self._counts[metric_name][self._make_aggregation_index(metric_name, tags)]
        try:
            counts_for_metric[idx] += count
        except TypeError:
            # If the type of the count value doesn't match that of the existing counter value, cast
            # the current value to the type of the count and try again
            logger.debug("Type mismatch trying to increment %s counter currently at %s by %s", metric_name, counter, count)
            counts_for_metric[idx] = type(count)(counts_for_metric[idx]) + count
        return

    def gauge(self, metric_name, value, tags=None):
        self._gauges[metric_name][self._make_aggregation_index(metric_name, tags)] = value
        return

    def histogram(self, metric_name, value, tags=None):
        self._histograms[metric_name][self._make_aggregation_index(metric_name, tags)].append(value)
        return

    def set(self, metric_name, value, tags=None):
        self._sets[metric_name][self._make_aggregation_index(metric_name, tags)].add(value)
        return

    def flush(self):
        "Return an generator which yields each line to be printed and prints it"
        n = 0
        for metric_name, tags, count, n in self._append_count(self._consume_aggregate(self._counts)):
            yield DataDogMetrics.incr(self, metric_name, count, tags)
        self._log_send_if_nonzero(n, "count")

        n = 0
        for metric_name, tags, gauge, n in self._append_count(self._consume_aggregate(self._gauges)):
            yield DataDogMetrics.gauge(self, metric_name, gauge, tags)
        self._log_send_if_nonzero(n, "gauge")

        n = 0
        for metric_name, tags, set_, n in self._append_count(self._consume_aggregate(self._counts)):
            yield DataDogMetrics.set(self, metric_name, set_, tags)
        self._log_send_if_nonzero(n, "set")

        n = 0
        for metric_name, tags, hist, n in self._append_count(self._consume_aggregate(self._counts)):
            yield DataDogMetrics.histogram(self, metric_name, hist, tags)
        self._log_send_if_nonzero(n, "histogram")

        return

    def flush_all(self):
        "Flush all the buffered metrics and return a total count of metrics flushed"
        n = 0
        for n, _ in enumerate(self.flush(), start = 1):
            pass

        return n

    @staticmethod
    def _make_dict_of_dicts(leaf_type):
        "Make a defaultdict, whose values are defaultdicts, whose values in turn are of leaf_type."
        return collections.defaultdict(lambda: collections.defaultdict(leaf_type))

    def _consume_aggregate(self, nested_aggregate):
        "Return a generator which yields triplet of (metric_name, tags, aggreagte_value)"
        # FIXME: The use if items() could give very poor performance in Python 2.7.
        #We should probably use the 'six' compatibility library here, to get code that will
        # call iteritems() in Python 2.7
        for metric_name, aggr_by_tag in nested_aggregate.items():
            tags_mapping = self.tag_orderings[metric_name]
            for hashed_tags, aggr in aggr_by_tag.items():
                tags = tags_mapping[hashed_tags]
                yield (metric_name, tags, aggr)

            # FIXME: ideally we should be deleting individual entries as we consume them, as that would
            # be the most exception-safe thing to do.
            aggr_by_tag.clear()

        nested_aggregate.clear()

    def _make_aggregation_index(self, metric_name, tags):
        "Return a hashable, ordering-independent representation of the tags, and store the original ordering"
        user_tags = tags or frozenset()
        hashable_tags = frozenset(user_tags)
        if (len(hashable_tags) != len(user_tags)):
            raise ValueError("Duplicate tags in {}".format(tags))
        self.tag_orderings[metric_name][hashable_tags] = tags
        return hashable_tags

    @staticmethod
    def _append_count(gen):
        "Append a running count to the unpackable returned from a generator and yield the new object"
        for n, tp in enumerate(gen, start = 1):
            yield tuple(tp) + (n,)

    def _log_send_if_nonzero(self, n, name):
        if n != 0:
            logger.info("Sent %d %s %s metric(s).", n, self.service_prefix, name)
