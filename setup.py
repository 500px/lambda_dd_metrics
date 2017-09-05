from setuptools import setup

setup(name='lambda_dd_metrics',
      version='1.0',
      description='Wrapper around reporting metrics to DataDog from AWS Lambda python functions',
      url='http://github.com/500px/lambda_dd_metrics',
      author='500px Platform Team',
      author_email='platform@500px.com',
      license='Apache',
      py_modules=['lambda_dd_metrics'],
      zip_safe=True)
