from setuptools import setup
import streamsx.kafka
setup(
  name = 'streamsx.kafka',
  packages = ['streamsx.kafka'],
  include_package_data=True,
  version = streamsx.kafka.__version__,
  description = 'IBM Streams Kafka integration',
  long_description = open('DESC.txt').read(),
  author = 'IBM Streams @ github.com',
  author_email = 'rolef.heinrich@de.ibm.com',
  license='Apache License - Version 2.0',
  url = 'https://github.com/IBMStreams/pypi.streamsx.kafka',
  keywords = ['streams', 'ibmstreams', 'streaming', 'analytics', 'streaming-analytics', 'messaging', 'kafka'],
  classifiers = [
    'Development Status :: 4 - Beta',
    'License :: OSI Approved :: Apache Software License',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
  ],
  install_requires=['streamsx>=1.11.5a'],

  test_suite='nose.collector',
  tests_require=['nose']
)