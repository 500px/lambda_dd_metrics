## DataDog Metrics for Lambda Functions (python)

To install: `pip install lambda_dd_metrics`

To use:
```
import lambda_dd_metrics
METRICS = lambda_dd_metrics.DataDogMetrics('metric.name.prefix', 'group.name')
METRICS.incr('record.received', 1, ['tag_name:tag_value'])
```

Make sure that your DataDog account has active AWS and Lambda integrations enabled for this to take effect:
http://docs.datadoghq.com/integrations/awslambda/

## Note
This currently supports counters and gauges only as DataDog does not have Lambda integration for histograms or sets
unless you're willing to set up a statsd server for this (somewhat defeating the serverless nature of this).

## For Contributors
* Bump the version in `setup.py`
* Build the package: `python setup.py sdist`
* Upload using [twine](https://pypi.python.org/pypi/twine): `twine upload dist/*`
* If the above fails, make sure you have `~/.pypirc` with the following contents:
```
[pypi]
username = your_username
password = your_password
```
