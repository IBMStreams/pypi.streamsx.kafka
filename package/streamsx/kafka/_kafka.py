# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

import datetime
from tempfile import gettempdir
import json
import streamsx.spl.op
import streamsx.spl.types
from streamsx.topology.schema import CommonSchema


def _add_properties_file (topology, properties, file_name):
    """
    Adds properties as a dictionary as a file dependency to the topology

    Args:
        topology(Topology): the topology
        properties(dict):   the Kafka properties
        file_name(str):     the filename of the file dependency

    Returns:
        'etc/' + file_name
    """
    tmpdirname = gettempdir()
    tmpfile = tmpdirname + '/' + file_name
    with open (tmpfile, "w") as properties_file:
        for key, value in properties.items():
            properties_file.write (key + '=' + value + '\n')
    topology.add_file_dependency (tmpfile, 'etc')
    return 'etc/' + file_name


def subscribe (topology, topic, kafka_properties, schema, group=None, name=None):
    """Subscribe to messages from a Kafka broker for a single topic.

    Adds a Kafka consumer that subscribes to a topic
    and converts each message to a stream tuple.

    Args:
        topology(Topology): Topology that will contain the stream of messages.
        topic(str): Topic to subscribe messages from.
        kafka_properties(str|dict): Properties containing the consumer configurations, at minimum the ``bootstrap.servers`` property. When a string is given, it is the name of the application, which contains consumer configs. Must not be set to ``None``.
        schema(StreamSchema): Schema for returned stream.
        group(str): Kafka consumer group identifier. When not specified it default to the job name with `topic` appended separated by an underscore.
        name(str): Consumer name in the Streams context, defaults to a generated name.

    Returns:
         Stream: Stream containing messages.
    """
    if schema is CommonSchema.Json:
        msg_attr_name='jsonString'
    elif schema is CommonSchema.String:
        msg_attr_name='string'
    else:
        raise TypeError(schema)

    if group is None:
        group = streamsx.spl.op.Expression.expression('getJobName() + "_" + "' + str (topic) + '"')

    if name is None:
        name = topic
        fName = 'consumer-' + str(topic) + '.properties'
    else:
        fName = 'consumer-' + str(name) + '.' + str(topic) + '.properties'

    if isinstance (kafka_properties, dict):
        propsFilename = _add_properties_file (topology, kafka_properties, fName)
        _op = _KafkaConsumer (topology, schema=schema, outputMessageAttributeName=msg_attr_name, propertiesFile=propsFilename, topic=topic, groupId=group, name=name)
    else:
        _op = _KafkaConsumer (topology, schema=schema, outputMessageAttributeName=msg_attr_name, appConfigName=kafka_properties, topic=topic, groupId=group, name=name)
    return _op.stream


def publish(stream, topic, kafka_properties, name=None):
    """Publish messages to a topic in a Kafka broker.

    Adds a Kafka producer where each tuple on `stream` is
    published as a stream message.

    Args:
        stream(Stream): Stream of tuples to published as messages.
        topic(str): Topic to publish messages to.
        kafka_properties(str|dict): Properties containing the producer configurations, at minimum the ``bootstrap.servers`` property. When a string is given, it is the name of the application, which contains consumer configs. Must not be set to ``None``.
        name(str): Producer name in the Streams context, defaults to a generated name.

    Returns:
        streamsx.topology.topology.Sink: Stream termination.
    """
    if stream.oport.schema == CommonSchema.Json:
        msg_attr = 'jsonString'
    elif stream.oport.schema == CommonSchema.String:
        msg_attr = 'string'
    else:
        raise TypeError(schema)

    if isinstance (kafka_properties, dict):
        if name is None:
            fName = 'producer-' + str(topic) + '.properties'
        else:
            fName = 'producer-' + str(name) + '.' + str(topic) + '.properties'
        propsFilename = _add_properties_file (stream.topology, kafka_properties, fName)
        _op = _KafkaProducer (stream, propertiesFile=propsFilename, topic=topic)
    else:
        _op = _KafkaProducer (stream, appConfigName=kafka_properties, topic=topic)

    _op.params['messageAttribute'] = _op.attribute(stream, msg_attr)

    return streamsx.topology.topology.Sink(_op)


class _KafkaConsumer(streamsx.spl.op.Source):
  def __init__(self, topology, schema, vmArg=None, appConfigName=None, clientId=None, commitCount=None, groupId=None, outputKeyAttributeName=None, outputMessageAttributeName=None, outputTimestampAttributeName=None, outputOffsetAttributeName=None, outputPartitionAttributeName=None, outputTopicAttributeName=None, partition=None, propertiesFile=None, startPosition=None, startOffset=None, startTime=None, topic=None, triggerCount=None, userLib=None, name=None):
        kind="com.ibm.streamsx.kafka::KafkaConsumer"
        inputs=None
        schemas=schema
        params = dict()
        if vmArg is not None:
            params['vmArg'] = vmArg
        if appConfigName is not None:
            params['appConfigName'] = appConfigName
        if clientId is not None:
            params['clientId'] = clientId
        if commitCount is not None:
            params['commitCount'] = commitCount
        if groupId is not None:
           params['groupId'] = groupId
        if outputKeyAttributeName is not None:
            params['outputKeyAttributeName'] = outputKeyAttributeName
        if outputMessageAttributeName is not None:
            params['outputMessageAttributeName'] = outputMessageAttributeName
        if outputTimestampAttributeName is not None:
            params['outputTimestampAttributeName'] = outputTimestampAttributeName
        if outputOffsetAttributeName is not None:
            params['outputOffsetAttributeName'] = outputOffsetAttributeName
        if outputPartitionAttributeName is not None:
            params['outputPartitionAttributeName'] = outputPartitionAttributeName
        if outputTopicAttributeName is not None:
            params['outputTopicAttributeName'] = outputTopicAttributeName
        if partition is not None:
            params['partition'] = partition
        if propertiesFile is not None:
            params['propertiesFile'] = propertiesFile
        if startPosition is not None:
            params['startPosition'] = startPosition
        if startOffset is not None:
            params['startOffset'] = startOffset
        if startTime is not None:
            params['startTime'] = startTime
        if topic is not None:
            params['topic'] = topic
        if triggerCount is not None:
            params['triggerCount'] = triggerCount
        if userLib is not None:
            params['userLib'] = userLib
        super(_KafkaConsumer, self).__init__(topology,kind,schemas,params,name)


class _KafkaProducer(streamsx.spl.op.Sink):
    def __init__(self, stream, vmArg=None, appConfigName=None, clientId=None, keyAttribute=None, messageAttribute=None, partitionAttribute=None, propertiesFile=None, timestampAttribute=None, topicAttribute=None, topic=None, userLib=None, name=None):
        topology = stream.topology
        kind="com.ibm.streamsx.kafka::KafkaProducer"
        params = dict()
        if vmArg is not None:
            params['vmArg'] = vmArg
        if appConfigName is not None:
            params['appConfigName'] = appConfigName
        if keyAttribute is not None:
            params['keyAttribute'] = keyAttribute
        if clientId is not None:
            params['clientId'] = clientId
        if messageAttribute is not None:
            params['messageAttribute'] = messageAttribute
        if partitionAttribute is not None:
            params['partitionAttribute'] = partitionAttribute
        if propertiesFile is not None:
            params['propertiesFile'] = propertiesFile
        if timestampAttribute is not None:
            params['timestampAttribute'] = timestampAttribute
        if topicAttribute is not None:
            params['topicAttribute'] = topicAttribute
        if topic is not None:
            params['topic'] = topic
        if userLib is not None:
            params['userLib'] = userLib
        super(_KafkaProducer, self).__init__(kind,stream,params,name)
