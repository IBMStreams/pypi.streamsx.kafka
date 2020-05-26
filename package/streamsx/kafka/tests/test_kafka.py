# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

from unittest import TestCase

import streamsx.kafka as kafka
from streamsx.kafka.tests.x509_certs import TRUSTED_CERT_PEM, PRIVATE_KEY_PEM, CLIENT_CERT_PEM, CLIENT_CA_CERT_PEM

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.context import ContextTypes
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.kafka import KafkaConsumer, KafkaProducer
from streamsx.kafka.schema import Schema as MsgSchema
import streamsx.spl.toolkit

import datetime
import os
import time
import uuid
import json
import random
import string
from tempfile import gettempdir
import glob
import shutil
from posix import remove
import typing

##
## Test assumptions
##
## Streaming analytics service has:
##    application config 'kafkatest' configured for a Kafka broker. The minimum property is 'bootstrap.servers'
##
## Kafka configs in a property file denoted by $KAFKA_PROPERTIES. The minimum property is 'bootstrap.servers'
##
## The Kafka broker has:
##
##    topics T1 and KAFKA_TEST with one partition (1 hour retention)
##
##
## Locally the toolkit exists at and is at least 1.5.1
## $KAFKA_TOOLKIT_HOME

def _get_properties (properties_file):
    """
    Reads a property file and returns the key value pairs as dictionary.
    """
    with open (properties_file) as f:
        l = [line.strip().split("=", 1) for line in f.readlines() if not line.startswith('#') and line.strip()]
        d = {key.strip(): value.strip() for key, value in l}
    return d

def _write_text_file(text):
    filename = os.path.join(gettempdir(), 'pypi.streamsx.kafka.test-pem-' + ''.join(random.choice(string.digits) for _ in range(20)) + '.pem')
    f = open(filename, 'w')
    try:
        f.write (text)
        return filename
    finally:
        f.close()


#class StrConsumerSchema(typing.NamedTuple):
#    kafka_msg: str
#    kafka_key: str


class TestSubscribeParams(TestCase):
    def test_schemas_ok(self):
        topo = Topology()
        kafka.subscribe(topo, 'T1', 'kafkatest', CommonSchema.String)
        kafka.subscribe(topo, 'T1', 'kafkatest', CommonSchema.Json)
        kafka.subscribe(topo, 'T1', 'kafkatest', MsgSchema.StringMessage)
        kafka.subscribe(topo, 'T1', 'kafkatest', MsgSchema.BinaryMessage)
        kafka.subscribe(topo, 'T1', 'kafkatest', MsgSchema.StringMessageMeta)
        kafka.subscribe(topo, 'T1', 'kafkatest', MsgSchema.BinaryMessageMeta)

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

class TestPublishParams(TestCase):
    def test_schemas_ok(self):
        topo = Topology()
        pyObjStream = topo.source(['Hello', 'World!'])
        jsonStream = pyObjStream.as_json()
        stringStream = pyObjStream.as_string()
        binMsgStream = pyObjStream.map (func=lambda s: {'message': bytes(s, 'utf-8'), 'key': s}, schema=MsgSchema.BinaryMessage)
        strMsgStream = pyObjStream.map (func=lambda s: {'message': s, 'key': s}, schema=MsgSchema.StringMessage)
        kafka.publish (binMsgStream, "Topic", "AppConfig")
        kafka.publish (strMsgStream, "Topic", "AppConfig")
        kafka.publish (stringStream, "Topic", "AppConfig")
        kafka.publish (jsonStream, "Topic", "AppConfig")

    def test_schemas_bad(self):
        topo = Topology()
        pyObjStream = topo.source(['Hello', 'World!'])
        binStream = pyObjStream.map (func=lambda s: bytes ("ABC", utf-8), schema=CommonSchema.Binary)
        xmlStream = pyObjStream.map (schema=CommonSchema.XML)
        binMsgMetaStream = pyObjStream.map (func=lambda s: {'message': bytes(s, 'utf-8'), 'key': s}, schema=MsgSchema.BinaryMessageMeta)
        strMsgMetaStream = pyObjStream.map (func=lambda s: {'message': s, 'key': s}, schema=MsgSchema.StringMessageMeta)
        otherSplTupleStream1 = pyObjStream.map (schema=StreamSchema('tuple<int32 a>'))
        otherSplTupleStream2 = pyObjStream.map (schema='tuple<int32 a>')
        
        self.assertRaises(TypeError, kafka.publish, pyObjStream, "Topic", "AppConfig")
        self.assertRaises(TypeError, kafka.publish, binStream, "Topic", "AppConfig")
        self.assertRaises(TypeError, kafka.publish, xmlStream, "Topic", "AppConfig")
        self.assertRaises(TypeError, kafka.publish, binMsgMetaStream, "Topic", "AppConfig")
        self.assertRaises(TypeError, kafka.publish, strMsgMetaStream, "Topic", "AppConfig")
        self.assertRaises(TypeError, kafka.publish, otherSplTupleStream1, "Topic", "AppConfig")
        self.assertRaises(TypeError, kafka.publish, otherSplTupleStream2, "Topic", "AppConfig")

    def test_kafka_properties (self):
        # in this testcase we do not connect to the broker @localhost
        properties = {'bootstrap.servers': 'localhost:9092'}
        topo = Topology()
        jsonStream = topo.source(['Hello', 'World!']).as_json()
        kafka.publish (jsonStream, 'Topic', properties, name = 'Publish-1')
        kafka.publish (jsonStream, 'Topic', 'AppConfig', name = 'Publish-2')

class TestCreatePropertyFile(TestCase):
    def setUp(self):
        TestCase.setUp(self)
        self.filename = None
        
    def tearDown(self):
        TestCase.tearDown(self)
        if self.filename:
            os.remove(self.filename)
            pass

    def test_write_ints_and_strings(self):
        props = dict()
        buf_mem = 32000000
        props['bootstrap.servers'] = 'host1.domain:9092,host2.domain:9092'
        props['acks'] = -1
        props['buffer.memory'] = buf_mem
        props['batch.size'] = '16384'
        self.filename = os.path.join(gettempdir(), 'TestCreatePropertyFile-' + ''.join(random.choice(string.digits) for _ in range(20)) + '.properties')
        kafka._kafka._write_properties_file(props, self.filename)
        read_props = _get_properties(self.filename)
        # stringify values
        expected_props = dict()
        for k, v in props.items():
            expected_props[k] = str(v)
        self.assertDictEqual(read_props, expected_props)
        
        
class TestCreateConnectionProperties(TestCase):
    def setUp(self):
        TestCase.setUp(self)
        self.ca_crt_file = _write_text_file (TRUSTED_CERT_PEM)
        self.client_ca_crt_file = _write_text_file (CLIENT_CA_CERT_PEM)
        self.client_crt_file = _write_text_file (CLIENT_CERT_PEM)
        self.private_key_file = _write_text_file (PRIVATE_KEY_PEM)
    
    def tearDown(self):
        if os.path.isfile(self.ca_crt_file):
            os.remove(self.ca_crt_file)
        if os.path.isfile(self.client_ca_crt_file):
            os.remove(self.client_ca_crt_file)
        if os.path.isfile(self.client_crt_file):
            os.remove(self.client_crt_file)
        if os.path.isfile(self.private_key_file):
            os.remove(self.private_key_file)

        for storetype in ['truststore', 'keystore']:
            for f in glob.glob(os.path.join(gettempdir(), storetype) + '-*.jks'):
                try:
                    os.remove(f)
                    #print ('file removed: ' + f)
                except:
                    print('Error deleting file: ', f)


    def test_error_missing_params(self):
        t = Topology()
        self.assertRaises(TypeError, kafka.create_connection_properties, bootstrap_servers=None)
        # topology required for truststore file dependency
        self.assertRaises(ValueError, kafka.create_connection_properties, bootstrap_servers='host:9',
                          cluster_ca_cert='notused')
        # private key required for auth = TLS
        self.assertRaises(ValueError, kafka.create_connection_properties, bootstrap_servers='host:9',
                          authentication=kafka.AuthMethod.TLS, client_cert='notused', topology=t)
        # client certificate required for auth = TLS
        self.assertRaises(ValueError, kafka.create_connection_properties, bootstrap_servers='host:9',
                          authentication=kafka.AuthMethod.TLS, client_private_key='notused', topology=t)
        # topology required for keystore file dependency
        self.assertRaises(ValueError, kafka.create_connection_properties, bootstrap_servers='host:9',
                          authentication=kafka.AuthMethod.TLS, client_cert='notused', client_private_key='notused')
        # password required
        self.assertRaises(ValueError, kafka.create_connection_properties, bootstrap_servers='host:9',
                          authentication=kafka.AuthMethod.PLAIN)
        self.assertRaises(ValueError, kafka.create_connection_properties, bootstrap_servers='host:9',
                          authentication=kafka.AuthMethod.PLAIN, username='icke')
        self.assertRaises(ValueError, kafka.create_connection_properties, bootstrap_servers='host:9',
                          authentication=kafka.AuthMethod.SCRAM_SHA_512)
        self.assertRaises(ValueError, kafka.create_connection_properties, bootstrap_servers='host:9',
                          authentication=kafka.AuthMethod.SCRAM_SHA_512, username='icke')
        # username required 
        self.assertRaises(ValueError, kafka.create_connection_properties, bootstrap_servers='host:9',
                          authentication=kafka.AuthMethod.PLAIN, password='passwd')
        self.assertRaises(ValueError, kafka.create_connection_properties, bootstrap_servers='host:9',
                          authentication=kafka.AuthMethod.SCRAM_SHA_512, password='passwd')
        
    
    def test_plaintext_None_auth(self):
        props = kafka.create_connection_properties (bootstrap_servers='host:9', use_TLS=False, authentication=None)
        self.assertDictEqual(props, {'bootstrap.servers': 'host:9'})

    def test_tls_noca_noauth_defaults(self):
        props = kafka.create_connection_properties (bootstrap_servers='host:9')
        self.assertDictEqual(props, {'bootstrap.servers': 'host:9',
                                     'security.protocol': 'SSL',
                                     'ssl.endpoint.identification.algorithm': 'https'
                                     })
    def test_tls_noca_noauth_no_hostnameverify(self):
        props = kafka.create_connection_properties (bootstrap_servers='host:9', enable_hostname_verification=False)
        self.assertDictEqual(props, {'bootstrap.servers': 'host:9',
                                     'security.protocol': 'SSL',
                                     'ssl.endpoint.identification.algorithm': ''
                                     })
        
    def test_tls_ca_list_noauth(self):
        t = Topology('test_props')
        props = kafka.create_connection_properties (bootstrap_servers='host:9', 
                                                    cluster_ca_cert=[TRUSTED_CERT_PEM, CLIENT_CA_CERT_PEM],
                                                    enable_hostname_verification=True,
                                                    topology=t)
        self.assertSetEqual(set(props), set({'bootstrap.servers': 'host:9',
                                             'security.protocol': 'SSL',
                                             'ssl.endpoint.identification.algorithm': 'https',
                                             'ssl.truststore.location': '{applicationDir}/etc/truststore-RANDOM_TOKEN.jks',
                                             'ssl.truststore.password': '__RANDOM_PASSWORD__',
                                             'ssl.truststore.type': 'JKS'
                                             }))
        
    def test_tls_ca_empty_list_noauth(self):
        t = Topology('test_props')
        props = kafka.create_connection_properties (bootstrap_servers='host:9', 
                                                    cluster_ca_cert=[],
                                                    enable_hostname_verification=True,
                                                    topology=t)
        self.assertDictEqual(props, {'bootstrap.servers': 'host:9',
                                     'security.protocol': 'SSL',
                                     'ssl.endpoint.identification.algorithm': 'https'
                                     })
        
    def test_tls_ca_empty_string_noauth(self):
        t = Topology('test_props')
        props = kafka.create_connection_properties (bootstrap_servers='host:9', 
                                                    cluster_ca_cert='',
                                                    enable_hostname_verification=True,
                                                    topology=t)
        self.assertDictEqual(props, {'bootstrap.servers': 'host:9',
                                     'security.protocol': 'SSL',
                                     'ssl.endpoint.identification.algorithm': 'https'
                                     })

    def test_tls_ca_noauth(self):
        t = Topology('test_props')
        props = kafka.create_connection_properties (bootstrap_servers='host:9', 
                                                    cluster_ca_cert=TRUSTED_CERT_PEM, 
                                                    enable_hostname_verification=True,
                                                    topology=t)
        self.assertSetEqual(set(props), set({'bootstrap.servers': 'host:9',
                                             'security.protocol': 'SSL',
                                             'ssl.endpoint.identification.algorithm': 'https',
                                             'ssl.truststore.location': '{applicationDir}/etc/truststore-RANDOM_TOKEN.jks',
                                             'ssl.truststore.password': '__RANDOM_PASSWORD__',
                                             'ssl.truststore.type': 'JKS'
                                             }))
    def test_tls_ca_mutual_tls(self):
        t = Topology('test_props')
        props = kafka.create_connection_properties (bootstrap_servers='host:9', 
                                                    cluster_ca_cert=TRUSTED_CERT_PEM,
                                                    enable_hostname_verification=True,
                                                    authentication=kafka.AuthMethod.TLS,
                                                    client_cert=CLIENT_CERT_PEM,
                                                    client_private_key=PRIVATE_KEY_PEM,
                                                    topology=t)
        self.assertSetEqual(set(props), set({'bootstrap.servers': 'host:9',
                                             'security.protocol': 'SSL',
                                             'ssl.endpoint.identification.algorithm': 'https',
                                             'ssl.truststore.location': '{applicationDir}/etc/truststore-RANDOM_TOKEN.jks',
                                             'ssl.truststore.password': '__RANDOM_PASSWORD__',
                                             'ssl.truststore.type': 'JKS',
                                             'ssl.keystore.password': '__RANDOM_PASSWORD__',
                                             'ssl.key.password': '__RANDOM_PASSWORD__',
                                             'ssl.keystore.location': '{applicationDir}/etc/keystore-RANDOM_TOKEN.jks',
                                             'ssl.keystore.type': 'JKS'
                                             }))
        
    def test_tls_saslplain(self):
        self.maxDiff = None 
        props = kafka.create_connection_properties (bootstrap_servers='host:9',
                                                    authentication=kafka.AuthMethod.PLAIN,
                                                    username='user',
                                                    password='passw=rd')
        self.assertDictEqual(props, {'bootstrap.servers': 'host:9',
                                      'security.protocol': 'SASL_SSL',
                                      'ssl.endpoint.identification.algorithm': 'https',
                                      'sasl.jaas.config': 'org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="passw=rd";',
                                      'sasl.mechanism': 'PLAIN'
                                     })

    def test_plaintext_saslplain(self):
        self.maxDiff = None 
        props = kafka.create_connection_properties (bootstrap_servers='host:9',
                                                    use_TLS=False,
                                                    authentication=kafka.AuthMethod.PLAIN,
                                                    username='user',
                                                    password='passw=rd')
        self.assertDictEqual(props, {'bootstrap.servers': 'host:9',
                                      'security.protocol': 'SASL_PLAINTEXT',
                                      'sasl.jaas.config': 'org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="passw=rd";',
                                      'sasl.mechanism': 'PLAIN'
                                     })

    def test_tls_saslscram_sha_512(self):
        self.maxDiff = None 
        props = kafka.create_connection_properties (bootstrap_servers='host:9',
                                                    authentication=kafka.AuthMethod.SCRAM_SHA_512,
                                                    username='user',
                                                    password='passw=rd')
        self.assertDictEqual(props, {'bootstrap.servers': 'host:9',
                                      'security.protocol': 'SASL_SSL',
                                      'ssl.endpoint.identification.algorithm': 'https',
                                      'sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="user" password="passw=rd";',
                                      'sasl.mechanism': 'SCRAM-SHA-512'
                                     })

    def test_plaintext_saslscram_sha_512(self):
        self.maxDiff = None 
        props = kafka.create_connection_properties (bootstrap_servers='host:9',
                                                    use_TLS=False,
                                                    authentication=kafka.AuthMethod.SCRAM_SHA_512,
                                                    username='user',
                                                    password='passw=rd')
        self.assertDictEqual(props, {'bootstrap.servers': 'host:9',
                                      'security.protocol': 'SASL_PLAINTEXT',
                                      'sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="user" password="passw=rd";',
                                      'sasl.mechanism': 'SCRAM-SHA-512'
                                     })

    def test_tls_saslscram_sha_512_ignore_cert(self):
        self.maxDiff = None 
        props = kafka.create_connection_properties (bootstrap_servers='host:9',
                                                    authentication=kafka.AuthMethod.SCRAM_SHA_512,
                                                    client_cert=CLIENT_CERT_PEM,
                                                    client_private_key=PRIVATE_KEY_PEM,
                                                    username='user',
                                                    password='passw=rd')
        self.assertDictEqual(props, {'bootstrap.servers': 'host:9',
                                      'security.protocol': 'SASL_SSL',
                                      'ssl.endpoint.identification.algorithm': 'https',
                                      'sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="user" password="passw=rd";',
                                      'sasl.mechanism': 'SCRAM-SHA-512'
                                     })

    def test_tls_saslplain_ignore_cert(self):
        self.maxDiff = None 
        props = kafka.create_connection_properties (bootstrap_servers='host:9',
                                                    authentication=kafka.AuthMethod.PLAIN,
                                                    client_cert=CLIENT_CERT_PEM,
                                                    client_private_key=PRIVATE_KEY_PEM,
                                                    username='user',
                                                    password='passw=rd')
        self.assertDictEqual(props, {'bootstrap.servers': 'host:9',
                                      'security.protocol': 'SASL_SSL',
                                      'ssl.endpoint.identification.algorithm': 'https',
                                      'sasl.jaas.config': 'org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="passw=rd";',
                                      'sasl.mechanism': 'PLAIN'
                                     })
        
    def test_tls_ca_mutual_tls_ignore_username_password(self):
        t = Topology('test_props')
        props = kafka.create_connection_properties (bootstrap_servers='host:9',
                                                    cluster_ca_cert=TRUSTED_CERT_PEM,
                                                    enable_hostname_verification=True,
                                                    authentication=kafka.AuthMethod.TLS,
                                                    client_cert=CLIENT_CERT_PEM,
                                                    client_private_key=PRIVATE_KEY_PEM,
                                                    username='ignored',
                                                    password='ignored',
                                                    topology=t)
        self.assertSetEqual(set(props), set({'bootstrap.servers': 'host:9',
                                             'security.protocol': 'SSL',
                                             'ssl.endpoint.identification.algorithm': 'https',
                                             'ssl.truststore.location': '{applicationDir}/etc/truststore-RANDOM_TOKEN.jks',
                                             'ssl.truststore.password': '__RANDOM_PASSWORD__',
                                             'ssl.truststore.type': 'JKS',
                                             'ssl.keystore.password': '__RANDOM_PASSWORD__',
                                             'ssl.key.password': '__RANDOM_PASSWORD__',
                                             'ssl.keystore.location': '{applicationDir}/etc/keystore-RANDOM_TOKEN.jks',
                                             'ssl.keystore.type': 'JKS'
                                             }))

    def test_plaintext_noauth_ignore_all(self):
        props = kafka.create_connection_properties (bootstrap_servers='host:9',
                                                    use_TLS=False,
                                                    authentication=kafka.AuthMethod.NONE,
                                                    # from here everything is irrelevant
                                                    enable_hostname_verification=False,
                                                    cluster_ca_cert=TRUSTED_CERT_PEM,
                                                    client_cert=CLIENT_CERT_PEM,
                                                    client_private_key=PRIVATE_KEY_PEM,
                                                    username='ignored',
                                                    password='ignored',
                                                    topology=None)
        self.assertDictEqual(props, {'bootstrap.servers': 'host:9'})
    
    def test_read_certs_from_files(self):
        t = Topology('test_props')
        props = kafka.create_connection_properties (bootstrap_servers='host:9', 
                                                    cluster_ca_cert=self.ca_crt_file,
                                                    enable_hostname_verification=True,
                                                    authentication=kafka.AuthMethod.TLS,
                                                    client_cert=self.client_crt_file,
                                                    client_private_key=self.private_key_file,
                                                    topology=t)
        self.assertSetEqual(set(props), set({'bootstrap.servers': 'host:9',
                                             'security.protocol': 'SSL',
                                             'ssl.endpoint.identification.algorithm': 'https',
                                             'ssl.truststore.location': '{applicationDir}/etc/truststore-RANDOM_TOKEN.jks',
                                             'ssl.truststore.password': '__RANDOM_PASSWORD__',
                                             'ssl.truststore.type': 'JKS',
                                             'ssl.keystore.password': '__RANDOM_PASSWORD__',
                                             'ssl.key.password': '__RANDOM_PASSWORD__',
                                             'ssl.keystore.location': '{applicationDir}/etc/keystore-RANDOM_TOKEN.jks',
                                             'ssl.keystore.type': 'JKS'
                                             }))


class TestSubmissionParams(TestCase):
    def test_create_expression(self):
        expr = KafkaConsumer.submission_parameter("NAME")
        self.assertEqual(str(expr), 'getSubmissionTimeValue("NAME")')
        default = "abc" + str(56)
        expr = KafkaConsumer.submission_parameter("NAME", "default")
        self.assertEqual(str(expr), 'getSubmissionTimeValue("NAME", "default")')
        expr = KafkaProducer.submission_parameter("NAME")
        self.assertEqual(str(expr), 'getSubmissionTimeValue("NAME")')
        default = "abc" + str(56)
        expr = KafkaProducer.submission_parameter("NAME", "default")
        self.assertEqual(str(expr), 'getSubmissionTimeValue("NAME", "default")')


class TestKafkaConsumer(TestCase):
    def setUp(self):
        TestCase.setUp(self)
        self.toolkit_root = None
        self.jobConfigPath = None
        self.bundlePath = None
        self.schema = typing.NamedTuple('StrConsumerSchema', [('kafka_msg', str), ('kafka_key', str)])
        self.schema2 = typing.NamedTuple('StrConsumerSchema', [('kafka_msg', str), ('kafka_key', str), ('tpc', str), ('partn', int), ('ts', int), ('offs', int)])
        self.keep_results = False
        #self.keep_results = True

    def tearDown(self):
        if not self.keep_results:
            if self.toolkit_root:
                shutil.rmtree(self.toolkit_root)
            if self.jobConfigPath:
                os.remove(self.jobConfigPath)
            if self.bundlePath:
                os.remove(self.bundlePath)
        
    def _build_only(self, topo):
        result = streamsx.topology.context.submit(ContextTypes.TOOLKIT, topo)
        if self.keep_results:
            print(topo.name + ' (TOOLKIT):' + str(result))
        assert(result.return_code == 0)
        self.toolkit_root = result.toolkitRoot

        result = streamsx.topology.context.submit(ContextTypes.BUNDLE, topo)
        if self.keep_results:
            print(topo.name + ' (BUNDLE):' + str(result))
        assert(result.return_code == 0)
        self.jobConfigPath = result.jobConfigPath
        self.bundlePath = result.bundlePath

    def test_compile_submission_params(self):
        topo = Topology()
        binStream = topo.source(KafkaConsumer(config='appcfg',
                                              topic=KafkaConsumer.submission_parameter("TOPIC", "default_topic"),
                                              schema=MsgSchema.StringMessage,
                                              group_id=KafkaConsumer.submission_parameter("KAFKA_GROUP")))
        # tuple<blob message, rstring key>
        binStream.for_each(KafkaProducer(config='appcfg',
                                         topic=KafkaConsumer.submission_parameter("TOPIC", "default_topic")))
        self._build_only(topo)

    def test_instantiate_conf_dict(self):
        inst = KafkaConsumer({'bootstrap.servers':'localhost:9999'}, 'topic1', MsgSchema.BinaryMessage, message_attribute_name='kafka_msg ', key_attribute_name=' kafka_key')
        self.assertEqual(inst._topic, 'topic1')
        self.assertTrue(inst._schema is MsgSchema.BinaryMessage)
        self.assertIsNone(inst.app_config_name)
        self.assertIsNone(inst._msg_attr_name)
        self.assertIsNone(inst._key_attr_name)
        self.assertDictEqual(inst.consumer_config, {'bootstrap.servers':'localhost:9999'})

    def test_instantiate_conf_str(self):
        inst = KafkaConsumer('appconfig', 'topic1', [self.schema], message_attribute_name='kafka_msg ', key_attribute_name=' kafka_key')
        self.assertEqual(inst._topic, 'topic1')
        self.assertIsNotNone(inst._schema)
        self.assertEqual(inst.app_config_name, 'appconfig')
        self.assertEqual(inst._msg_attr_name, 'kafka_msg')
        self.assertEqual(inst._key_attr_name, 'kafka_key')
        self.assertIsNone(inst.consumer_config)

    def test_instantiate_conf_str_opt_consumer_config(self):
        inst = KafkaConsumer('appconfig', ['topic1', 'topic2'], MsgSchema.BinaryMessage, consumer_config={'max.partition.fetch.bytes':'65536'})
        self.assertListEqual(inst._topic, ['topic1', 'topic2'])
        self.assertTrue(inst._schema is MsgSchema.BinaryMessage)
        self.assertEqual(inst.app_config_name, 'appconfig')
        self.assertDictEqual(inst.consumer_config, {'max.partition.fetch.bytes':'65536'})

    def test_instantiate_conf_dict_opt_consumer_config(self):
        inst = KafkaConsumer({'bootstrap.servers':'localhost:9999', 'session.timeout.ms':12345}, ['topic1', 'topic2'], MsgSchema.BinaryMessage, consumer_config={'session.timeout.ms':240000, 'max.poll.records':2345})
        self.assertListEqual(inst._topic, ['topic1', 'topic2'])
        self.assertTrue(inst._schema is MsgSchema.BinaryMessage)
        self.assertIsNone(inst.app_config_name)
        self.assertDictEqual(inst.consumer_config, {'bootstrap.servers':'localhost:9999', 'max.poll.records':2345, 'session.timeout.ms':240000})
    
    def test_instantiate_kwargs(self):
        inst = KafkaConsumer({'bootstrap.servers':'localhost:9999'}, 'topic1', MsgSchema.BinaryMessage,
                             #kwargs except consumer_config
                             group_size=3,
                             group_id='gid333',
                             client_id='my_client_id',
                             vm_arg=['-Xms=345m', '-Xmx4G'],
                             ignored='must go into a warning',
                             app_config_name='appconfig',
                             ssl_debug=True)
        self.assertEqual(inst._topic, 'topic1')
        self.assertEqual(inst.app_config_name, 'appconfig')
        self.assertIsNotNone(inst._schema)
        self.assertIsNotNone(inst.consumer_config)
        self.assertEqual(inst.group_size, 3)
        self.assertEqual(inst.group_id, 'gid333')
        self.assertEqual(inst.client_id, 'my_client_id')
        self.assertListEqual(inst.vm_arg, ['-Xms=345m', '-Xmx4G'])
        self.assertEqual(inst.ssl_debug, True)

    def test_schema(self):
        #StrConsumerSchema = typing.NamedTuple('StrConsumerSchema', [('kafka_msg', str), ('kafka_key', str)])
        c = KafkaConsumer('appconfig', 'topic1', [self.schema], message_attribute_name='kafka_msg ', key_attribute_name=' kafka_key')
        Topology().source(c)
        self.assertEqual(c._op.params['outputMessageAttributeName'], 'kafka_msg')
        self.assertEqual(c._op.params['outputKeyAttributeName'], 'kafka_key')
        
        c = KafkaConsumer('appconfig', 'topic1', self.schema, message_attribute_name='kafka_msg ', key_attribute_name=' kafka_key')
        Topology().source(c)
        self.assertEqual(c._op.params['outputMessageAttributeName'], 'kafka_msg')
        self.assertEqual(c._op.params['outputKeyAttributeName'], 'kafka_key')

        c = KafkaConsumer('appconfig', 'topic1', MsgSchema.BinaryMessage)
        Topology().source(c)
        self.assertNotIn('outputMessageAttributeName', c._op.params)
        self.assertNotIn('outputKeyAttributeName', c._op.params)

        c = KafkaConsumer('appconfig', 'topic1', MsgSchema.BinaryMessageMeta)
        Topology().source(c)
        self.assertNotIn('outputMessageAttributeName', c._op.params)
        self.assertNotIn('outputKeyAttributeName', c._op.params)

        c = KafkaConsumer('appconfig', 'topic1', MsgSchema.StringMessage)
        Topology().source(c)
        self.assertNotIn('outputMessageAttributeName', c._op.params)
        self.assertNotIn('outputKeyAttributeName', c._op.params)

        c = KafkaConsumer('appconfig', 'topic1', MsgSchema.StringMessageMeta)
        Topology().source(c)
        self.assertNotIn('outputMessageAttributeName', c._op.params)
        self.assertNotIn('outputKeyAttributeName', c._op.params)

        c = KafkaConsumer('appconfig', 'topic1', CommonSchema.String)
        Topology().source(c)
        self.assertEqual(c._op.params['outputMessageAttributeName'], 'string')
        self.assertNotIn('outputKeyAttributeName', c._op.params)

        c = KafkaConsumer('appconfig', 'topic1', CommonSchema.Json)
        Topology().source(c)
        self.assertEqual(c._op.params['outputMessageAttributeName'], 'jsonString')
        self.assertNotIn('outputKeyAttributeName', c._op.params)

        c = KafkaConsumer('appconfig', 'topic1', CommonSchema.Binary)
        Topology().source(c)
        self.assertEqual(c._op.params['outputMessageAttributeName'], 'binary')
        self.assertNotIn('outputKeyAttributeName', c._op.params)

        c = KafkaConsumer('appconfig', 'topic1', StreamSchema('tuple<int32 key,rstring message>'), message_attribute_name='message', key_attribute_name='key')
        self.assertEqual(c._msg_attr_name, 'message')
        self.assertEqual(c._key_attr_name, 'key')
        Topology().source(c)
        self.assertEqual(c._op.params['outputMessageAttributeName'], 'message')
        self.assertEqual(c._op.params['outputKeyAttributeName'], 'key')

        c = KafkaConsumer('appconfig', 'topic1', 'tuple<rstring message,rstring key>', message_attribute_name='message', key_attribute_name='key')
        self.assertEqual(c._msg_attr_name, 'message')
        self.assertEqual(c._key_attr_name, 'key')
        Topology().source(c)
        self.assertEqual(c._op.params['outputMessageAttributeName'], 'message')
        self.assertEqual(c._op.params['outputKeyAttributeName'], 'key')
        
    def test_schema_bad(self):
        # constructor tests
        self.assertRaises(TypeError, KafkaConsumer, config='appconfig', topic='topic1', schema=CommonSchema.XML)
        self.assertRaises(TypeError, KafkaConsumer, config='appconfig', topic='topic1', schema=CommonSchema.Python)
        
    def test_compile_user_schema(self):
        c = KafkaConsumer('appconfig', 'topic1', [self.schema], message_attribute_name='kafka_msg ', key_attribute_name=' kafka_key')
        c.vm_arg = '-Xmx2G'
        c.ssl_debug = True
        c.group_size = 3
        c.consumer_config = {'bootstrap.servers':'localhost:9092'}
        topology = Topology('test_compile_user_schema')
        msgs = topology.source(c, 'Messages')
        msgs.end_parallel().print()
        self._build_only(topology)

    def test_compile_user_schema2(self):
        c = KafkaConsumer('appconfig', 'topic1', self.schema, message_attribute_name='kafka_msg ', key_attribute_name=' kafka_key')
        c.vm_arg = '-Xmx2G'
        c.ssl_debug = True
        c.group_size = 3
        c.consumer_config = {'bootstrap.servers':'localhost:9092'}
        topology = Topology('test_compile_user_schema')
        msgs = topology.source(c, 'Messages')
        msgs.end_parallel().print()
        self._build_only(topology)

    def test_compile_user_schema_all_meta_attrs(self):
        c = KafkaConsumer('appconfig', 'topic1', self.schema2,
                          message_attribute_name='kafka_msg ', key_attribute_name=' kafka_key',
                          topic_attribute_name='tpc',
                          partition_attribute_name='partn',
                          timestamp_attribute_name='ts',
                          offset_attribute_name='offs')
        c.vm_arg = '-Xmx2G'
        c.ssl_debug = True
        c.group_size = 3
        c.consumer_config = {'bootstrap.servers':'localhost:9092'}
        topology = Topology('test_compile_user_schema')
        msgs = topology.source(c, 'Messages')
        msgs.end_parallel().print()
        self._build_only(topology)

    def test_compile_commonschema_binary(self):
        c = KafkaConsumer('appconfig', 'topic1', CommonSchema.Binary)
        topology = Topology('test_compile_commonschema_binary')
        msgs = topology.source(c, 'Messages')
        self._build_only(topology)

    def test_compile_spl_schema(self):
        c = KafkaConsumer('appconfig', 'topic1', 'tuple<rstring m, int64 k>', message_attribute_name='m', key_attribute_name='k')
        topology = Topology('test_compile_spl_schema')
        msgs = topology.source(c, 'Messages')
        self._build_only(topology)


class TestKafkaProducer(TestCase):
    def setUp(self):
        TestCase.setUp(self)
        self.toolkit_root = None
        self.jobConfigPath = None
        self.bundlePath = None
        self.schema = typing.NamedTuple('StrProducerSchema', [('kafka_msg', str), ('kafka_key', str)])
        self.keep_results = False

    def tearDown(self):
        if not self.keep_results:
            if self.toolkit_root:
                shutil.rmtree(self.toolkit_root)
            if self.jobConfigPath:
                os.remove(self.jobConfigPath)
            if self.bundlePath:
                os.remove(self.bundlePath)
        
    def _build_only(self, topo):
        result = streamsx.topology.context.submit(ContextTypes.TOOLKIT, topo)
        if self.keep_results:
            print(topo.name + ' (TOOLKIT):' + str(result))
        assert(result.return_code == 0)
        self.toolkit_root = result.toolkitRoot

        result = streamsx.topology.context.submit(ContextTypes.BUNDLE, topo)
        if self.keep_results:
            print(topo.name + ' (BUNDLE):' + str(result))
        assert(result.return_code == 0)
        self.jobConfigPath = result.jobConfigPath
        self.bundlePath = result.bundlePath

    def test_instantiate_conf_dict(self):
        inst = KafkaProducer({'bootstrap.servers':'localhost:9999'}, 'topic1', message_attribute_name='kafka_msg ', key_attribute_name=' kafka_key')
        self.assertEqual(inst._topic, 'topic1')
        self.assertIsNone(inst.app_config_name)
        self.assertEqual(inst._msg_attr_name, 'kafka_msg')
        self.assertEqual(inst._key_attr_name, 'kafka_key')
        self.assertDictEqual(inst.producer_config, {'bootstrap.servers':'localhost:9999'})

    def test_instantiate_conf_str(self):
        inst = KafkaProducer('appconfig', 'topic1')
        self.assertEqual(inst._topic, 'topic1')
        self.assertEqual(inst.app_config_name, 'appconfig')
        self.assertIsNone(inst._msg_attr_name)
        self.assertIsNone(inst._key_attr_name)
        self.assertIsNone(inst.producer_config)

    def test_instantiate_conf_str_opt_consumer_config(self):
        inst = KafkaProducer('appconfig', ['topic1', 'topic2'], producer_config={'acks':'all'})
        self.assertListEqual(inst._topic, ['topic1', 'topic2'])
        self.assertEqual(inst.app_config_name, 'appconfig')
        self.assertDictEqual(inst.producer_config, {'acks':'all'})

    def test_instantiate_conf_dict_opt_consumer_config(self):
        inst = KafkaProducer({'bootstrap.servers':'localhost:9999', 'compression.type':'gzip'}, ['topic1', 'topic2'], producer_config={'compression.type':'snappy', 'retries':42})
        self.assertListEqual(inst._topic, ['topic1', 'topic2'])
        self.assertIsNone(inst.app_config_name)
        self.assertDictEqual(inst.producer_config, {'bootstrap.servers':'localhost:9999', 'compression.type':'snappy', 'retries':42})
    
    def test_instantiate_kwargs(self):
        inst = KafkaProducer({'bootstrap.servers':'localhost:9999'}, 'topic1',
                             #kwargs except producer_config
                             ignored='must go into a warning',
                             client_id='my_client_id',
                             vm_arg=['-Xms=345m', '-Xmx4G'],
                             app_config_name='appconfig',
                             ssl_debug=True)
        self.assertEqual(inst._topic, 'topic1')
        self.assertEqual(inst.app_config_name, 'appconfig')
        self.assertIsNotNone(inst.producer_config)
        self.assertIsNone(inst._msg_attr_name)
        self.assertIsNone(inst._key_attr_name)
        self.assertEqual(inst.client_id, 'my_client_id')
        self.assertListEqual(inst.vm_arg, ['-Xms=345m', '-Xmx4G'])
        self.assertEqual(inst.ssl_debug, True)

    def test_schema(self):
        topo = Topology()
        pyObjStream = topo.source(['Hello', 'World!'])
        
        jsonStream = pyObjStream.as_json()
        s = KafkaProducer('json', 'TOPIC', message_attribute_name=None, key_attribute_name=None)
        jsonStream.for_each(s)
        self.assertIsInstance(s._op.params['messageAttribute'], streamsx.spl.op.Expression)

        stringStream = pyObjStream.as_string()
        s = KafkaProducer('string', 'TOPIC', message_attribute_name=None, key_attribute_name=None)
        stringStream.for_each(s)
        self.assertIsInstance(s._op.params['messageAttribute'], streamsx.spl.op.Expression)
        
        binStream = pyObjStream.map (func=lambda s: bytes(s, utf-8), schema=CommonSchema.Binary)
        s = KafkaProducer('binary', 'TOPIC', message_attribute_name=None, key_attribute_name=None)
        binStream.for_each(s)
        self.assertIsInstance(s._op.params['messageAttribute'], streamsx.spl.op.Expression)
        
        binMsgStream = pyObjStream.map(func=lambda s: {'binary': bytes(s, 'utf-8'), 'key': s}, schema=MsgSchema.BinaryMessage)
        s = KafkaProducer('BinaryMessage', 'TOPIC', message_attribute_name='ignored', key_attribute_name='ignored')
        binMsgStream.for_each(s)
        self.assertNotIn('messageAttribute', s._op.params)
        self.assertNotIn('keyAttribute', s._op.params)
        
        strMsgStream = pyObjStream.map(func=lambda s: {'message': s, 'key': s}, schema=MsgSchema.StringMessage)
        s = KafkaProducer('StringMessage', 'TOPIC', message_attribute_name='ignored', key_attribute_name='ignored')
        strMsgStream.for_each(s)
        self.assertNotIn('messageAttribute', s._op.params)
        self.assertNotIn('keyAttribute', s._op.params)
        
        userMsgStream = pyObjStream.map(func=lambda s: {'kafka_msg': s, 'kafka_key':s}, schema=self.schema)
        s = KafkaProducer('namedTuple', 'TOPIC', message_attribute_name='kafka_msg', key_attribute_name='kafka_key')
        userMsgStream.for_each(s)
        self.assertIsInstance(s._op.params['messageAttribute'], streamsx.spl.op.Expression)
        self.assertIsInstance(s._op.params['keyAttribute'], streamsx.spl.op.Expression)
        
        splMsgStream = pyObjStream.map(func=lambda s: {'m':s, 'k':s}, schema='tuple<rstring m, int64 k>')
        s = KafkaProducer('spl', 'TOPIC', message_attribute_name='m', key_attribute_name='k')
        splMsgStream.for_each(s)
        self.assertIsInstance(s._op.params['messageAttribute'], streamsx.spl.op.Expression)
        self.assertIsInstance(s._op.params['keyAttribute'], streamsx.spl.op.Expression)

    def test_schema_bad(self):
        topo = Topology()
        pyObjStream = topo.source(['Hello', 'World!'])
        self.assertRaises(TypeError, pyObjStream.for_each, KafkaProducer('py', 'TOPIC'))

        xmlStream = pyObjStream.map (schema=CommonSchema.XML)
        self.assertRaises(TypeError, xmlStream.for_each, KafkaProducer('xml', 'TOPIC'))

    def test_compile_user_schema(self):
        topo = Topology()
        pyObjStream = topo.source(['Hello', 'World!'])
        
        userMsgStream = pyObjStream.map(func=lambda s: {'kafka_msg': s, 'kafka_key':s}, schema=self.schema)
        userMsgStream.for_each(KafkaProducer('namedTuple', 'TOPIC', message_attribute_name='kafka_msg', key_attribute_name='kafka_key'))
        
        splMsgStream = pyObjStream.map(func=lambda s: {'m':s, 'k':s}, schema='tuple<rstring m, int64 k>')
        splMsgStream.for_each(KafkaProducer('spl', 'TOPIC', message_attribute_name='m', key_attribute_name='k'))
        self._build_only(topo)

    def test_compile_BinaryMessage(self):
        topo = Topology()
        binStream = topo.source(KafkaConsumer(config='appcfg',
                                              topic=['t1', 't2', 't3'],
                                              schema=MsgSchema.BinaryMessage))
        # tuple<blob message, rstring key>
        binStream.for_each(KafkaProducer('BinaryMessage', 'dest_TOPIC'))
        self._build_only(topo)

    def test_compile_StringMessage(self):
        topo = Topology()
        binStream = topo.source(KafkaConsumer(config='appcfg',
                                              topic=['t1', 't2', 't3'],
                                              schema=MsgSchema.StringMessage))
        # tuple<blob message, rstring key>
        binStream.for_each(KafkaProducer('StringMessageMeta', 'dest_TOPIC'))
        self._build_only(topo)

    def test_compile_commonschema_binary(self):
        topo = Topology()
        binStream = topo.source(KafkaConsumer(config='appcfg',
                                              topic=['t1', 't2', 't3'],
                                              schema=CommonSchema.Binary))
        # tuple<blob message, rstring key>
        binStream.for_each(KafkaProducer('Binary', 'dest_TOPIC'))
        self._build_only(topo)

    def test_compile_commonschema_string_json(self):
        topo = Topology()
        pyObjStream = topo.source(['Hello', 'World!'])
        
        jsonStream = pyObjStream.as_json()
        jsonStream.for_each(KafkaProducer('json', 'TOPIC', message_attribute_name=None, key_attribute_name=None))

        stringStream = pyObjStream.as_string()
        stringStream.for_each(KafkaProducer('string', 'TOPIC', message_attribute_name=None, key_attribute_name=None))

        self._build_only(topo)


class TestDownloadToolkit(TestCase):
    @classmethod
    def tearDownClass(cls):
        # delete downloaded *.tgz (should be deleted in _download_toolkit(...)
        for f in glob.glob(gettempdir() + '/toolkit-[0-9]*.tgz'):
            try:
                os.remove(f)
                print ('file removed: ' + f)
            except:
                print('Error deleting file: ', f)
        # delete unpacked toolkits
        for d in glob.glob(gettempdir() + '/pypi.streamsx.kafka.tests-*'):
            if os.path.isdir(d):
                shutil.rmtree(d)
        for d in glob.glob(gettempdir() + '/com.ibm.streamsx.kafka'):
            if os.path.isdir(d):
                shutil.rmtree(d)

    def test_download_latest(self):
        topology = Topology()
        location = kafka.download_toolkit()
        print('toolkit location: ' + location)
        streamsx.spl.toolkit.add_toolkit(topology, location)

    def test_download_with_url(self):
        topology = Topology()
        ver = '1.8.0'
        url = 'https://github.com/IBMStreams/streamsx.kafka/releases/download/v' + ver + '/com.ibm.streamsx.kafka-' + ver + '.tgz'
        location = kafka.download_toolkit(url=url)
        print('toolkit location: ' + location)
        streamsx.spl.toolkit.add_toolkit(topology, location)

    def test_download_latest_with_target_dir(self):
        topology = Topology()
        target_dir = 'pypi.streamsx.kafka.tests-' + str(uuid.uuid4()) + '/kafka-toolkit'
        location = kafka.download_toolkit(target_dir=target_dir)
        print('toolkit location: ' + location)
        streamsx.spl.toolkit.add_toolkit(topology, location)

    def test_download_with_url_and_target_dir(self):
        topology = Topology()
        target_dir = 'pypi.streamsx.kafka.tests-' + str(uuid.uuid4()) + '/kafka-toolkit'
        ver = '1.9.0'
        url = 'https://github.com/IBMStreams/streamsx.kafka/releases/download/v' + ver + '/com.ibm.streamsx.kafka-' + ver + '.tgz'
        location = kafka.download_toolkit(url=url, target_dir=target_dir)
        print('toolkit location: ' + location)
        streamsx.spl.toolkit.add_toolkit(topology, location)


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

def add_pip_toolkits(topo):
    topo.add_pip_package('streamsx.toolkits')


class TestKafka(TestCase):
    def setUp(self):
        TestCase.setUp(self)
        Tester.setup_distributed (self)
        self.test_config[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False

    def test_json_appconfig (self):
        n = 104
        topo = Topology()
        add_kafka_toolkit(topo)
        add_pip_toolkits(topo)
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
        add_pip_toolkits(topo)
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

    def test_string_eventstreams_creds (self):
        n = 107
        topo = Topology()
        add_kafka_toolkit(topo)
        add_pip_toolkits(topo)
        uid = str(uuid.uuid4())
        s = topo.source(StringData(uid, n)).as_string()
        kafka.publish(stream=s,
                      topic='MH_TEST',
                      kafka_properties=kafka.create_connection_properties_for_eventstreams (os.environ['EVENTSTREAMS_CREDENTIALS']))

        r = kafka.subscribe(topology=topo,
                            topic='MH_TEST',
                            kafka_properties=kafka.create_connection_properties_for_eventstreams (os.environ['EVENTSTREAMS_CREDENTIALS']),
                            schema=CommonSchema.String)
        r = r.filter(lambda t : t.startswith(uid))
        expected = list(StringData(uid, n, False)())

        tester = Tester(topo)
        tester.contents(r, expected)
        tester.tuple_count(r, n)
        tester.test(self.test_ctxtype, self.test_config)
