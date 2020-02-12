from setuptools import setup
import streamsx.kafka
#import streamsx.kafka.scripts.makeproperties as makeproperties
setup(
    name = 'streamsx.kafka',
    packages = ['streamsx.kafka', 'streamsx.kafka.scripts'],
    include_package_data=True,
    version = streamsx.kafka.__version__,
    description = 'Kafka integration for IBM Streams topology applications',
    long_description = open('DESC.txt').read(),
    author = 'IBM Streams @ github.com',
    author_email = 'rolef.heinrich@de.ibm.com',
    license='Apache License - Version 2.0',
    url = 'https://github.com/IBMStreams/pypi.streamsx.kafka',
    keywords = ['streams', 'ibmstreams', 'streaming', 'analytics', 'streaming-analytics', 'messaging', 'kafka'],
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    install_requires=['streamsx>=1.14.3a0,<2.0', 'streamsx.toolkits>=1.2.0'],
    entry_points = {
        'console_scripts': [
            'streamsx-kafka-make-properties=streamsx.kafka.scripts.makeproperties:main'
        ],
    },
    test_suite='nose.collector',
    tests_require=['nose']
)
