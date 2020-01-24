# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

"""
Overview
++++++++

This module allows a Streams application to subscribe Kafka topics
as a stream and to publish messages on a Kafka topics from a stream
of tuples. To achieve this, this module provides the :py:class:`KafkaConsumer` and :py:class:`KafkaProducer` classes.

Connection to a Kafka broker
++++++++++++++++++++++++++++

To bootstrap servers of the Kafka broker can be defined using a Streams application configuration or
within the Python code by using a dictionary variable.
The name of the application configuration must be set as the ``app_config_name`` of the :py:class:`KafkaConsumer`
or :py:class:`KafkaProducer`. Alternative, or in addition to an application configuration, these classes have 
attributes ``consumer_config`` or ``producer_config`` to set a dictionary with configs.
The minimum set of properties in the application configuration or dictionary contains ``bootstrap.servers``, for example

.. csv-table::
    :header: config, value

    bootstrap.servers, "host1:port1,host2:port2,host3:port3"

Other configs for Kafka consumers or Kafka producers can be added to the application configuration or dictionary.
When configurations are specified, which are specific for consumers or producers only, it is recommended
to use different application configurations or variables of dict type for :py:class:`KafkaConsumer` and :py:class:`KafkaProducer`.

When the connection information contains sensitive information, like login information, it is also possible to store 
these information together with the server names in an application configuration and configure consumer or producer 
specific configs as attributes to :py:class:`KafkaConsumer` or :py:class:`KafkaProducer`.
The consumer and producer configs can be found in the `Kafka documentation <https://kafka.apache.org/23/documentation/>`_.
 
Please note, that the underlying SPL toolkit already adjusts several configurations.
Please review the `toolkit operator reference <https://ibmstreams.github.io/streamsx.kafka/doc/spldoc/html/>`_ 
for defaults and adjusted configurations.

Simple connection parameter example::

    from streamsx.kafka import KafkaConsumer
    from streamsx.topology.topology import Topology
    from streamsx.topology.schema import CommonSchema
    
    consumerProperties = {}
    consumerProperties['bootstrap.servers'] = 'kafka-host1.domain:9092,kafka-host2.domain:9092'
    consumerProperties['fetch.min.bytes'] = '1024'
    consumerProperties['max.partition.fetch.bytes'] = '4194304'
    
    consumer = KafkaConsumer(config=consumerProperties,
                             topic='Your_Topic',
                             schema=CommonSchema.String)
    consumer.consumer_config = consumerProperties
    
    topology = Topology()
    fromKafka = topo.source(consumer)

When trusted certificates, or client certificates, and private keys are required to connect with a Kafka cluster,
the function :py:func:`create_connection_properties <create_connection_properties>` helps to create stores for
certificates and keys, and to create the right properties.

In IBM Cloud Pak for Data it is also possible to create application configurations for consumer and 
producer properties. An application configuration is a safe place to store sensitive data. Use the
function :py:func:`configure_connection_from_properties <configure_connection_from_properties>` 
to create an application configuration for kafka properties.

Example with use of an application configuration::

    from icpd_core import icpd_util
    
    from streamsx.topology.topology import Topology
    from streamsx.topology.schema import CommonSchema
    from streamsx.rest import Instance
    import streamsx.topology.context
    
    import streamsx.kafka as kafka
    
    topology = Topology('ConsumeFromKafka')
    
    connection_properties = kafka.create_connection_properties(
        bootstrap_servers='kafka-bootstrap.192.168.42.183.nip.io:443',
        #use_TLS=True,
        #enable_hostname_verification=True,
        cluster_ca_cert='/tmp/secrets/cluster_ca_cert.pem',
        authentication = kafka.AuthMethod.SCRAM_SHA_512,
        username = 'user123',
        password = 'passw0rd', # not the very best choice for a password
        topology = topology)
    
    consumer_properties = dict()
    # In this example we read only transactionally committed messages
    consumer_properties['isolation.level'] = 'read_committed'
    # add connection specifc properties to the consumer properties
    consumer_properties.update(connection_properties)
    # get the streams instance in IBM Cloud Pak for Data
    instance_cfg = icpd_util.get_service_instance_details(name='instanceName')
    instance_cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False
    streams_instance = Instance.of_service(instance_cfg)
    
    # create the application configuration
    appconfig_name = kafka.configure_connection_from_properties(
        instance=streams_instance,
        name='kafkaConsumerProps',
        properties=consumer_properties,
        description='Consumer properties for authenticated access')
    
    consumer = kafka.KafkaConsumer(config=appconfig_name,
                                   topic='mytopic',
                                   schema=CommonSchema.String)
    
    topology = Topology()
    fromKafka = topology.source(consumer)

Messages
++++++++

The schema of the stream defines how messages are handled.

* ``CommonSchema.String`` - Each message is a UTF-8 encoded string.
* ``CommonSchema.Json`` - Each message is a UTF-8 encoded serialized JSON object.
* :py:const:`~schema.Schema.StringMessage` - structured schema with message and key
* :py:const:`~schema.Schema.BinaryMessage` - structured schema with message and key
* :py:const:`~schema.Schema.StringMessageMeta` - structured schema with message, key, and message meta data
* :py:const:`~schema.Schema.BinaryMessageMeta` - structured schema with message, key, and message meta data

No other formats are supported.

Sample
++++++

A simple hello world example of a Streams application publishing to
a topic and the same application consuming the same topic::

    from streamsx.topology.topology import Topology
    from streamsx.topology.schema import CommonSchema
    from streamsx.topology.context import submit, ContextTypes
    from streamsx.kafka import KafkaConsumer, KafkaProducer
    import time
    
    def delay(v):
        time.sleep(5.0)
        return True
    
    topology = Topology('KafkaHelloWorld')
    
    to_kafka = topology.source(['Hello', 'World!'])
    to_kafka = to_kafka.as_string()
    # delay tuple by tuple
    to_kafka = to_kafka.filter(delay)
    
    # Publish a stream to Kafka using TEST topic, the Kafka server is at localhost
    producer = KafkaProducer({'bootstrap.servers': 'localhost:9092'}, 'TEST')
    to_kafka.for_each(producer)
    
    # Subscribe to same topic as a stream
    consumer = KafkaConsumer({'bootstrap.servers': 'localhost:9092'}, 'TEST', CommonSchema.String)
    from_kafka = topology.source(consumer)
    
    # You'll find the Hello World! in stdout log file:
    from_kafka.print()
    
    submit(ContextTypes.DISTRIBUTED, topology)
    # The Streams job is kept running.

"""

__version__='1.8.0a2'

# controls sphinx documentation:
__all__ = [
    'AuthMethod',
    'KafkaConsumer',
    'KafkaProducer',
    'download_toolkit',
    'create_connection_properties',
    'create_connection_properties_for_eventstreams',
    'configure_connection',
    'configure_connection_from_properties',
    'publish',
    'subscribe'
    ]
from streamsx.kafka._kafka import AuthMethod, KafkaConsumer, KafkaProducer, create_connection_properties, create_connection_properties_for_eventstreams, configure_connection, configure_connection_from_properties, publish, subscribe, download_toolkit
