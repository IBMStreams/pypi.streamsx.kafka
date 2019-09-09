## Python streamsx.kafka package.

This exposes SPL operators in the `com.ibm.streamsx.kafka` toolkit as Python methods.

Package is organized using standard packaging to upload to PyPi.

The package is uploaded to PyPi in the standard way:
```
cd package
rm -rf streamsx.kafka.egg-info/ build/ dist/
python3 setup.py sdist bdist_wheel upload -r pypi
```
**Note:** This is done using the `ibmstreams` account at pypi.org

Package details: https://pypi.python.org/pypi/streamsx.kafka

Documentation is using Sphinx and can be built locally using:
```
cd package/docs
make html
```
and viewed using
```
firefox build/html/index.html
```

The documentation is also setup at `readthedocs.io` under the account: `IBMStreams`

Documentation links:
* http://streamsxkafka.readthedocs.io

## Version update

To change the version information of the Python package, edit following files:

- ./package/docs/source/conf.py
- ./package/streamsx/kafka/\_\_init\_\_.py

When the development status changes, edit the *classifiers* in

- ./package/setup.py

When the documented sample must be changed, change it here:

- ./package/streamsx/kafka/\_\_init\_\_.py
- ./package/DESC.txt


## Test

The tests are run with a locally installed Streams installation and any Kafka broker.
Following environment variables must be set:

| Enveironment variable | content |
| --- | --- |
| STREAMS_INSTALL | must point to your Streams installation |
| STREAMS_USERNAME | The username of the Streams user |
| STREAMS_PASSWORD | The password of the Streams user |
| KAFKA_TOOLKIT_HOME | The directory where the Kafka toolkit is located |
| KAFKA_PROPERTIES | The name of a properties file with consumer properties |

The Streams runtime must be aware of the `PYTHONHOME` variable. The variable must be made
available to the runtime by using following *streamtool* command:

Uninstall the streamsx.kafka package from your Python environment before test:

`pip uninstall streamsx.kafka --yes`

```
streamtool setproperty --application-ev PYTHONHOME=path_to_python_install
```

For the tests, an application configuration with name `kafkatest` is required. It can be created
on instance or domain level and must contain the properties `bootstrap.servers` in the form
`kafka_server1:port,kafka_server2:port,...`.

For testing Kafka properties with dictionary type, a property file with the `bootstrap.servers`
configuration in it must be created anywhere in the file system and made be available with the
environmnet variable `KAFKA_PROPERTIES`.

In the broker, a topic with name `KAFKA_TEST` with a single partition must be created.

Run the tests with

```
cd package
python3 -u -m unittest streamsx.kafka.tests.test_kafka.TestSubscribeParams
python3 -u -m unittest streamsx.kafka.tests.test_kafka.TestKafka
```

or, to test all test cases

```
cd package
python3 -u -m unittest streamsx.kafka.tests.test_kafka
```
