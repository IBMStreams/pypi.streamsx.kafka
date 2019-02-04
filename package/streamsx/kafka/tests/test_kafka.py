from unittest import TestCase

import streamsx.kafka as kafka

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema

import streamsx.spl.toolkit

import datetime
import os
import time
import uuid
import json

##
## Test assumptions
##
## Streaming analytics service has:
##    application config 'kafkatest' configured for a Kafka broker. The minimum property is 'bootstrap.servers'
##
## The Kafka broker has:
##
##    topics T1 and KAFKA_TEST with one partition (1 hour retention)
##
##
## Locally the toolkit exists at and is at least 1.5.1
## $HOME/toolkits/com.ibm.streamsx.kafka

def _get_properties (properties_file):
    """
    Reads a property file and returns the key value pairs as dictionary.
    """
    with open (properties_file) as f:
        l = [line.strip().split("=", 1) for line in f.readlines() if not line.startswith('#') and line.strip()]
        d = {key.strip(): value.strip() for key, value in l}
    return d


class TestSubscribeParams(TestCase):
    def test_schemas_ok(self):
        topo = Topology()
        kafka.subscribe(topo, 'T1', 'kafkatest', CommonSchema.String)
        kafka.subscribe(topo, 'T1', 'kafkatest', CommonSchema.Json)

    def test_schemas_bad(self):
        topo = Topology()
        self.assertRaises(TypeError, kafka.subscribe, topo, 'T1', CommonSchema.Python)
        self.assertRaises(TypeError, kafka.subscribe, topo, 'T1', CommonSchema.Binary)
        self.assertRaises(TypeError, kafka.subscribe, topo, 'T1', CommonSchema.XML)
        self.assertRaises(TypeError, kafka.subscribe, topo, 'T1', StreamSchema('tuple<int32 a>'))
        self.assertRaises(TypeError, kafka.subscribe, topo, 'T1', 'tuple<int32 a>')

    def test_kafka_properties (self):
        properties_file = os.environ['KAFKA_PROPERTIES']
        properties = _get_properties (properties_file)
        topo = Topology()
        kafka.subscribe(topo, 'KAFKA_TEST', properties, CommonSchema.String)
        kafka.subscribe(topo, 'KAFKA_TEST', 'kafkatest', CommonSchema.String)

## Using a uuid to avoid concurrent test runs interferring
## with each other
class JsonData(object):
    def __init__(self, prefix, count, delay=True):
        self.prefix = prefix
        self.count = count
        self.delay = delay
    def __call__(self):
        # Since we are reading from the end allow
        # time to get the consumer started.
        if self.delay:
            time.sleep(10)
        for i in range(self.count):
            yield {'p': self.prefix + '_' + str(i), 'c': i}

class StringData(object):
    def __init__(self, prefix, count, delay=True):
        self.prefix = prefix
        self.count = count
        self.delay = delay
    def __call__(self):
        if self.delay:
            time.sleep(10)
        for i in range(self.count):
            yield self.prefix + '_' + str(i)

def add_kafka_toolkit(topo):
    streamsx.spl.toolkit.add_toolkit(topo, os.environ["KAFKA_TOOLKIT_HOME"])


class TestKafka(TestCase):
    def setUp(self):
        Tester.setup_distributed (self)
        self.test_config[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False


    def test_json_appconfig (self):
        n = 104
        topo = Topology()
        add_kafka_toolkit(topo)
        uid = str(uuid.uuid4())
        s = topo.source(JsonData(uid, n)).as_json()
        kafka.publish(s, 'KAFKA_TEST', kafka_properties='kafkatest')

        r = kafka.subscribe(topo, 'KAFKA_TEST', 'kafkatest', CommonSchema.Json)
        r = r.filter(lambda t : t['p'].startswith(uid))
        expected = list(JsonData(uid, n, False)())

        tester = Tester(topo)
        tester.contents(r, expected)
        tester.tuple_count(r, n)
        tester.test(self.test_ctxtype, self.test_config)

    def test_string_props_dict (self):
        n = 107
        topo = Topology()
        add_kafka_toolkit(topo)
        uid = str(uuid.uuid4())
        s = topo.source(StringData(uid, n)).as_string()
        kafka.publish(s, 'KAFKA_TEST', kafka_properties = _get_properties (os.environ['KAFKA_PROPERTIES']))

        r = kafka.subscribe(topo, 'KAFKA_TEST', _get_properties (os.environ['KAFKA_PROPERTIES']), CommonSchema.String)
        r = r.filter(lambda t : t.startswith(uid))
        expected = list(StringData(uid, n, False)())

        tester = Tester(topo)
        tester.contents(r, expected)
        tester.tuple_count(r, n)
        tester.test(self.test_ctxtype, self.test_config)
