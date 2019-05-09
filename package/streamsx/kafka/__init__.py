# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

"""
Overview
++++++++

This module allows a Streams application to :py:func:`subscribe <subscribe>` a Kafka topic
as a stream and :py:func:`publish <publish>` messages on a Kafka topic from a stream
of tuples.

Connection to a Kafka broker
++++++++++++++++++++++++++++

To bootstrap servers of the Kafka broker can be defined using a Streams application configuration or
within the Python code by using a dictionary variable.
The name of the application configuration or the dictionary must be specified using the ``kafka_properties``
parameter to :py:func:`subscribe` or :py:func:`publish`.
The minimum set of properties in the application configuration or dictionary contains ``bootstrap.servers``, for example

.. csv-table::
    :header: config, value

    bootstrap.servers, "host1:port1,host2:port2,host3:port3"

Other configs for Kafka consumers or Kafka producers can be added to the application configuration.
When configurations are specified, which are specific for consumers or producers only, it is recommended
to use different application configurations for :py:func:`publish <publish>` and :py:func:`subscribe <subscribe>`.

Messages
++++++++

The schema of the stream defines how messages are handled.

* ``CommonSchema.String`` - Each message is a UTF-8 encoded string.
* ``CommonSchema.Json`` - Each message is a UTF-8 encoded serialized JSON object.

No other formats are supported.

Sample
++++++

A simple hello world example of a Streams application publishing to
a topic and the same application consuming the same topic::

    from streamsx.topology.topology import Topology
    from streamsx.topology.schema import CommonSchema
    from streamsx.topology.context import submit
    from streamsx.topology.context import ContextTypes
    import streamsx.kafka as kafka
    import time

    def delay (v):
    time.sleep (5.0)
    return True

    topo = Topology('KafkaHelloWorld')

    to_kafka = topo.source (['Hello', 'World!'])
    to_kafka = to_kafka.as_string()
    # delay tuple by tuple
    to_kafka = to_kafka.filter (delay)

    # Publish a stream to Kafka using TEST topic, the Kafka servers
    # (bootstrap.servers) are configured in the application configuration 'kafka_props'.
    kafka.publish (to_kafka, 'TEST', 'kafka_props')

    # Subscribe to same topic as a stream
    from_kafka = kafka.subscribe (topo, 'TEST', 'kafka_props', CommonSchema.String)

    # You'll find the Hello World! in stdout log file:
    from_kafka.print()

    submit (ContextTypes.DISTRIBUTED, topo)
    # The Streams job is kept running.

"""

__version__='1.1.0'

__all__ = ['subscribe', 'publish', 'configure_connection']
from streamsx.kafka._kafka import subscribe, publish, configure_connection
