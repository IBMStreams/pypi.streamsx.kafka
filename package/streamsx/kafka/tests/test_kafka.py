# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019

from unittest import TestCase

import streamsx.kafka as kafka
from streamsx.kafka.tests.x509_certs import TRUSTED_CERT_PEM, PRIVATE_KEY_PEM, CLIENT_CERT_PEM, CLIENT_CA_CERT_PEM

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema
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


class TestCreateConnectionProperties(TestCase):
    def setUp(self):
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
