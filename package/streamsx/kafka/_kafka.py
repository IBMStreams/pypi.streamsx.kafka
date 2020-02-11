# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
import streamsx.spl.op
import streamsx.spl.types
from streamsx.topology.composite import Source as AbstractSource
from streamsx.topology.composite import ForEach as AbstractSink
from streamsx.topology.schema import CommonSchema
import streamsx.spl.toolkit as tk

from streamsx.kafka.schema import Schema

from tempfile import gettempdir, NamedTemporaryFile
import json
from builtins import str
from enum import Enum
import datetime
import os
import sys
import warnings
import string
import random
from fileinput import filename
from streamsx.toolkits import download_toolkit, create_keystore, create_truststore

_TOOLKIT_NAME = 'com.ibm.streamsx.kafka'

class AuthMethod(Enum):
    """Defines authentication methods for Kafka.
    """
    NONE = 0
    """No authentication

    .. versionadded:: 1.3
    """
    
    TLS = 1
    """Mutual TLS authentication with X.509 certificates.
    Client certificate and client private key is required

    .. versionadded:: 1.3
    """
    
    PLAIN = 2
    """PLAIN, or SASL/PLAIN, is a simple username/password authentication mechanism that is
    typically used with TLS for encryption to implement secure authentication.
    SASL/PLAIN should only be used with SSL as transport layer to ensure that clear
    passwords are not transmitted on the wire without encryption.

    .. versionadded:: 1.3
    """
    
    SCRAM_SHA_512 = 3
    """Authentication with SASL/SCRAM-SHA-512 method. A username and a password is required.

    .. versionadded:: 1.3
    """


class KafkaConsumer(AbstractSource):
    """
    Represents a source for messages read from Kafka, which can be passed to 
    ``Topology.source()`` to create a stream.
    
    A KafkaConsumer subscribes to one or more topics and can build a consumer 
    group by setting :attr:`group_size` to a value greater than one. In this case,
    the KafkaConsumer is the begin of a *parallel region*.
    
    The KafkaConsumer can also be the begin of a periodic consistent region::
    
        from streamsx.topology.state import ConsistentRegionConfig
        from streamsx.topology.topology import Topology
        from streamsx.kafka.schema import Schema
        
        consumer = KafkaConsumer(config={'bootstrap.servers': 'kafka-server.domain:9092'},
                                 topic='myTopic',
                                 schema=Schema.StringMessageMeta,
                                 group_size = 3,
                                 group_id = "my-consumer-group")
    
        topology = Topology("KafkaConsumer")
        from_kafka = topology.source(consumer, "SourceName").set_consistent(ConsistentRegionConfig.periodic(period=60))

    Operator driven consistent region is not supported by the KafkaConsumer.

    Args:
        config(str|dict): The name of an application configuration (str) with consumer configs or a dict with consumer configs
        topic(str|list): Single topic or list of topics to subscribe messages from
        schema(StreamSchema): Schema for the output stream
        
            Valid schemas are:
            
            * ``CommonSchema.String`` - pre-defined, each message is a UTF-8 encoded string.
            * ``CommonSchema.Json`` - pre-defined, each message is a UTF-8 encoded serialized JSON object.
            * ``CommonSchema.Binary`` - pre-defined, each message is binary object (byte array).
            * :py:const:`~schema.Schema.StringMessage` - pre-defined, structured schema with message and key
            * :py:const:`~schema.Schema.BinaryMessage` - pre-defined, structured schema with message and key
            * :py:const:`~schema.Schema.StringMessageMeta` - pre-defined, structured schema with message, key, and message meta data
            * :py:const:`~schema.Schema.BinaryMessageMeta` - pre-defined, structured schema with message, key, and message meta data
            * User defined schemas. When user defined schemas are used, the attributes names for message and key must be given
              if they differ from the defaults ``message`` and ``key``. Receiving the key is optional.
              
        message_attribute_name(str): The attribute name in the stream that receives the message content of the Kafka message.
            Required for user-defined schema when the attribute name is different from ``message``.
        key_attribute_name(str): The attribute name in the stream that receives the key of the Kafka message.
            Required for user-defined schema when the attribute name is different from ``key``.
        
        **options(kwargs): optional arguments as keyword arguments. Following arguments are supported:
        
            * group_id
            * group_size
            * client_id
            * app_config_name
            * consumer_config - these configs override the configs given as the ``config`` parameter by being merged with them.
              This applies also for configs stored in an application configuration.
            * ssl_debug
            * vm_arg
        
    .. versionadded:: 1.8.0
    """
    
    def __init__(self, config, topic, schema, message_attribute_name=None, key_attribute_name=None, **options):
        if isinstance(config, str):
            self._app_config_name = config
            self.consumer_config = None
        elif isinstance(config, dict):
            self._app_config_name = None
            self.consumer_config = config
        else:
            raise TypeError(config)
        if topic is None:
            raise TypeError(topic)
        self._topic = topic
        
        self._key_attr_name = None
        self._msg_attr_name = None
        if schema is CommonSchema.Python:
            self._msg_attr_name = '__spl_po'
            raise TypeError('CommonSchema.Python is not supported by the KafkaConsumer')
        elif schema is CommonSchema.XML:
            self._msg_attr_name = 'document'
            raise TypeError('CommonSchema.XML is not supported by the KafkaConsumer')
        elif schema is CommonSchema.Json:
            self._msg_attr_name = 'jsonString'
        elif schema is CommonSchema.String:
            self._msg_attr_name = 'string'
        elif schema is CommonSchema.Binary:
            self._msg_attr_name = 'binary'
        elif schema is Schema.BinaryMessage:
            # self._msg_attr_name = 'message'
            pass
        elif schema is Schema.StringMessage:
            # self._msg_attr_name = 'message'
            pass
        elif schema is Schema.BinaryMessageMeta:
            # self._msg_attr_name = 'message'
            pass
        elif schema is Schema.StringMessageMeta:
            # self._msg_attr_name = 'message'
            pass
        else:
            if message_attribute_name:
                self._msg_attr_name = _check_non_whitespace_string(message_attribute_name)
            if key_attribute_name:
                self._key_attr_name =  _check_non_whitespace_string(key_attribute_name)
        
        self._schema = schema
        self._vm_arg = None
        self._group_id = None
        self._group_size = 1
        self._client_id = None
        self._ssl_debug = False
        super(KafkaConsumer, self).__init__()
        opts = dict(options)
        if 'vm_arg' in options:
            self.vm_arg = options.get('vm_arg')
            del opts['vm_arg']
        if 'ssl_debug' in options:
            self.ssl_debug = options.get('ssl_debug')
            del opts['ssl_debug']
        if 'client_id' in options:
            self.client_id = options.get('client_id')
            del opts['client_id']
        if 'group_id' in options:
            self.group_id = options.get('group_id')
            del opts['group_id']
        if 'group_size' in options:
            self.group_size = options.get('group_size')
            del opts['group_size']
        if 'app_config_name' in options:
            self.app_config_name = options.get('app_config_name')
            del opts['app_config_name']
        if 'consumer_config' in options:
            _option_cfg = options.get('consumer_config')
            if self._consumer_config is _option_cfg:
                # object identity
                pass
            else:
                if self._consumer_config is None:
                    self._consumer_config = _option_cfg
                else:
                    self._consumer_config.update(_option_cfg)
            del opts['consumer_config']
        if opts:
            warnings.warn('ignored options: {}'.format(str(opts)), category=None, stacklevel=3)
        self._op = None

    @property
    def ssl_debug(self):
        """
        bool: When ``True`` the property enables verbose SSL debug output at runtime.
        """
        return self._ssl_debug
    
    @ssl_debug.setter
    def ssl_debug(self, ssl_debug:bool):
        self._ssl_debug = ssl_debug

    @property
    def vm_arg(self):
        """
         str|list: Arguments for the Java Virtual Machine used at Runtime, for example ``-Xmx2G``.
             For multiple arguments, use a list::
             
                 consumer.vm_arg = ["-Xmx=2G", "-Xms=512M"]
        """
        return self._vm_arg

    @vm_arg.setter
    def vm_arg(self, vm_arg):
        if vm_arg:
            self._vm_arg = vm_arg

    @property
    def app_config_name(self):
        """
        str: The name of an application configuration with consumer configurations.
        The application configuration must exist when the topology is submitted.
        """
        return self._app_config_name

    @app_config_name.setter
    def app_config_name(self, app_config_name):
        self._app_config_name = app_config_name

    @property
    def consumer_config(self):
        """
        dict: The consumer configuration
        
        The consumer configuration must be a ``dict`` in which the keys are the 
        `consumer configs defined by Kafka <https://kafka.apache.org/23/documentation/#consumerconfigs>`_.
        A consumer configuration can be created with :func:`create_connection_properties`.
        The properties given as `consumer_config` override the same properties in an application
        configuration if present.
        """
        return self._consumer_config

    @consumer_config.setter
    def consumer_config(self, consumer_config):
        self._consumer_config = consumer_config

    @property
    def group_id(self):
        """
        str: The identifier of a Kafka consumer group. This attribute goes into the ``group.id``
        consumer config and overrides the same property if given as :attr:`consumer_config`.
        When no group identifier is given, a group identifier is generated from the job name and
        the subscribed topics.
        """
        return self._group_id

    @group_id.setter
    def group_id(self, group_id):
        self._group_id = group_id

    @property
    def group_size(self):
        """
        int: The size of a Kafka consumer group, in which multiple consumers share 
        the partitions of the subscribed topics.
        
        With ``group_size`` greater than 1, the 
        source stream is split into multiple channels as the start of a parallel region.
        
        This is effectively the same as ``Stream.set_parallel(width=group_size)``.
        
        The parallel region can be ended, for example, with ``Stream.end_parallel()``.
        """
        return self._group_size

    @group_size.setter
    def group_size(self, group_size):
        self._group_size = group_size

    @property
    def client_id(self):
        """
        str: An optional client identifier for Kafka. The client identifier has no 
        function for the application, but allows to identify the Kafka client within the Kafka server.
        
        The client identifier given here overrides the consumer config ``client.id`` if given
        in an application configuration or within the consumer configuration.
         
        .. note:: Each instance of :py:class:`~KafkaConsumer` and :py:class:`~KafkaProducer` must
            have a different client identifier.
       """
        return self._client_id

    @client_id.setter
    def client_id(self, client_id):
        self._client_id = client_id

    def populate(self, topology, name, **options) -> streamsx.topology.topology.Stream:
        # test mandatory parameters 
        if self._topic is None:
            raise TypeError(self._topic)
        
        if isinstance(self._topic, str):
            _topic_tok = self._topic.replace("-", "_")
        elif isinstance(self._topic, list):
            # topic list must not be empty
            if self._topic:
                _topic_tok = "_".join(self._topic).replace("-", "_")
            else:
                raise ValueError('topic must not be an empty list')
        else:
            raise TypeError('trusted_cert must be of str or list type')

        if self._app_config_name is None and not self._consumer_config:
            raise TypeError('one of app_config_name or non-empty consumer_config must be set')
        if self._group_id is None:
            self._group_id = streamsx.spl.op.Expression.expression('getJobName() + "_" + "' + str(_topic_tok) + '"')

        propsFilename = None
        if self._consumer_config:
            tmpf = NamedTemporaryFile(suffix='.properties', prefix='consumer-', delete=False)
            propsFilename = _add_properties_file(topology, self._consumer_config, tmpf.name)

        if name is None:
            name = "KafkaConsumed_" + _topic_tok
        vmargs = None
        if self._vm_arg or self._ssl_debug:
            vmargs = []
            if isinstance(self._vm_arg, list):
                vmargs.extend(self._vm_arg)
            elif isinstance(self._vm_arg, str):
                vmargs.append(self._vm_arg)
            if self._ssl_debug:
                vmargs.append('-Djavax.net.debug=all')

        self._op = _KafkaConsumer(topology,
                            schema=self._schema,
                            vmArg=vmargs,
                            appConfigName=self._app_config_name,
                            outputMessageAttributeName=self._msg_attr_name,
                            outputKeyAttributeName=self._key_attr_name,
                            propertiesFile=propsFilename,
                            topic=self._topic, 
                            groupId=self._group_id,
                            clientId=self._client_id,
                            name=name)
        oStream = self._op.stream
        if self._group_size > 1:
            return oStream.set_parallel(self._group_size)
        else:
            return oStream


class KafkaProducer(AbstractSink):
    """
    The ``KafkaProducer`` represents a stream termination that publishes each tuple as a message to one or more Kafka topics.
    Instances can be passed to ``Stream.for_each()`` to create a sink (stream termination).

    Trivial example::
    
        producer = KafkaProducer(config={'bootstrap.servers': 'kafka-server.domain:9092'},
                                 topic="topic")
        stream_to_publish.for_each(producer)

    Example with two topics in IBM Event Streams cloud service::
    
        eventstreams_credentials_json = "..."
        producer = KafkaProducer(create_connection_properties_for_eventstreams(eventstreams_credentials_json),
                                 topic=["topic1", "topic2"])
        stream_to_publish.for_each(producer)
    
    Args:
        config(str|dict): The name of an application configuration (str) with producer configs or a dict with producer configs
        topic(str|list): Topic or topics to publish messages
        message_attribute_name(str): the attribute name in the schema that is used as the message to publish.
            Required for user-defined schema when the attribute name is different from ``message``.
        key_attribute_name(str): the attribute name in the schema that is used as the key for the Kafka message to publish.
            Required for user-defined schema when the attribute name is different from ``key``.
        **options(kwargs): optional arguments as keyword arguments. Following arguments are supported:
        
            * client_id
            * app_config_name
            * producer_config - these configs override the configs given as the ``config`` parameter by being merged with them.
              This applies also for configs stored in an application configuration.
            * ssl_debug
            * vm_arg

    .. versionadded:: 1.8.0
    """
    
    def __init__(self, config, topic, message_attribute_name=None, key_attribute_name=None, **options):
        """
        Args:
            config(str|dict): The name of an application configuration (str) with producer configs or a dict with producer configs
            topic(str|list): Topic or topics to publish messages
        """
        if isinstance(config, str):
            self._app_config_name = config
            self.producer_config = None
        elif isinstance(config, dict):
            self._app_config_name = None
            self.producer_config = config
        else:
            raise TypeError(config)
        if topic is None:
            raise TypeError(topic)
        self._msg_attr_name = None
        self._key_attr_name = None
        if message_attribute_name:
            self._msg_attr_name = _check_non_whitespace_string(message_attribute_name)
        if key_attribute_name:
            self._key_attr_name = _check_non_whitespace_string(key_attribute_name)
        self._topic = topic
        self._vm_arg = None
        self._client_id = None
        self._ssl_debug = False
        super(KafkaProducer, self).__init__()
        opts = dict(options)
        if 'vm_arg' in options:
            self.vm_arg = options.get('vm_arg')
            del opts['vm_arg']
        if 'ssl_debug' in options:
            self.ssl_debug = options.get('ssl_debug')
            del opts['ssl_debug']
        if 'client_id' in options:
            self.client_id = options.get('client_id')
            del opts['client_id']
        if 'app_config_name' in options:
            self.app_config_name = options.get('app_config_name')
            del opts['app_config_name']
        if 'producer_config' in options:
            _option_cfg = options.get('producer_config')
            if self._producer_config is _option_cfg:
                # object identity
                pass
            else:
                if self._producer_config is None:
                    self._producer_config = _option_cfg
                else:
                    self._producer_config.update(_option_cfg)
            del opts['producer_config']
        if opts:
            warnings.warn('ignored options: {}'.format(str(opts)), category=None, stacklevel=3)
        self._op = None

    @property
    def ssl_debug(self):
        """
        bool: enables verbose SSL debug output at runtime when SSL connections are used.
        """
        return self._ssl_debug
    
    @ssl_debug.setter
    def ssl_debug(self, ssl_debug:bool):
        self._ssl_debug = ssl_debug

    @property
    def vm_arg(self):
        """
         str|list: Arguments for the Java Virtual Machine used at Runtime, for example ``-Xmx2G``.
             For multiple arguments, use a list::
             
                 producer.vm_arg = ["-Xmx=2G", "-Xms=512M"]
        """
        return self._vm_arg

    @vm_arg.setter
    def vm_arg(self, vm_arg):
        if vm_arg:
            self._vm_arg = vm_arg

    @property
    def app_config_name(self):
        """
        str: The name of an application configuration with producer configurations.
        The application configuration must exist when the topology is submitted.
        """
        return self._app_config_name

    @app_config_name.setter
    def app_config_name(self, app_config_name):
        self._app_config_name = app_config_name

    @property
    def producer_config(self):
        """
        dict: The producer configuration
        
        The producer configuration must be a ``dict`` in which the keys are the 
        `producer configs defined by Kafka <https://kafka.apache.org/23/documentation/#producerconfigs>`_.
        A producer configuration can be created with :func:`create_connection_properties`.
        The properties given as `producer_config` override the same properties in an application
        configuration if present.
        """
        return self._producer_config

    @producer_config.setter
    def producer_config(self, config):
        self._producer_config = config

    @property
    def client_id(self):
        """
        str: An optional client identifier for Kafka. The client identifier has no 
        function for the application, but allows to identify the Kafka client within the Kafka server.
        
        The client identifier given here overrides the producer config ``client.id`` if given
        in an application configuration or within the producer configuration.
        
        .. note:: Each instance of :py:class:`~KafkaConsumer` and :py:class:`~KafkaProducer` must
            have a different client identifier.
        """
        return self._client_id

    @client_id.setter
    def client_id(self, client_id):
        self._client_id = client_id

    def populate(self, topology, stream, name, **options) -> streamsx.topology.topology.Sink:
        # test mandatory parameters 
        if self._topic is None:
            raise TypeError(self._topic)
        
        if isinstance(self._topic, str):
            _topic_tok = self._topic.replace("-", "_")
        elif isinstance(self._topic, list):
            # topic list must not be empty
            if self._topic:
                _topic_tok = "_".join(self._topic).replace("-", "_")
            else:
                raise ValueError('topic must not be an empty list')
        else:
            raise TypeError('topic must be of str or list type')

        if self._app_config_name is None and not self._producer_config:
            raise TypeError('one of app_config_name or non-empty producer_config must be set')

        msg_attr_name = None
        key_attr_name = None
        streamSchema = stream.oport.schema
        if streamSchema is CommonSchema.Python:
            msg_attr_name = '__spl_po'
            raise TypeError('CommonSchema.Python is not supported by the KafkaProducer')
        elif streamSchema is CommonSchema.XML:
            msg_attr_name = 'document'
            raise TypeError('CommonSchema.XML is not supported by the KafkaProducer')
        elif streamSchema is CommonSchema.Json:
            msg_attr_name = 'jsonString'
        elif streamSchema is CommonSchema.String:
            msg_attr_name = 'string'
        elif streamSchema is CommonSchema.Binary:
            msg_attr_name = 'binary'
        elif streamSchema is Schema.BinaryMessage:
            # msg_attr_name = 'message'
            pass
        elif streamSchema is Schema.StringMessage:
            # msg_attr_name = 'message'
            pass
        elif streamSchema is Schema.BinaryMessageMeta:
            # msg_attr_name = 'message'
            pass
        elif streamSchema is Schema.StringMessageMeta:
            # msg_attr_name = 'message'
            pass
        else:
            msg_attr_name = self._msg_attr_name
            key_attr_name = self._key_attr_name

        propsFilename = None
        if self._producer_config:
            tmpf = NamedTemporaryFile(suffix='.properties', prefix='producer-', delete=False)
            propsFilename = _add_properties_file(topology, self._producer_config, tmpf.name)

        if name is None:
            name = "KafkaProduced_" + _topic_tok
        vmargs = None
        if self._vm_arg or self._ssl_debug:
            vmargs = []
            if isinstance(self._vm_arg, list):
                vmargs.extend(self._vm_arg)
            elif isinstance(self._vm_arg, str):
                vmargs.append(self._vm_arg)
            if self._ssl_debug:
                vmargs.append('-Djavax.net.debug=all')

        self._op = _KafkaProducer(stream,
                            propertiesFile=propsFilename,
                            vmArg=vmargs,
                            appConfigName=self._app_config_name,
                            topic=self._topic,
                            clientId=self._client_id,
                            name=name)

        # create the input attribute expressions after operator op initialization
        if msg_attr_name is not None:
            self._op.params['messageAttribute'] = self._op.attribute(stream, msg_attr_name)
        if key_attr_name is not None:
            self._op.params['keyAttribute'] = self._op.attribute(stream, key_attr_name)
    #    if partitionAttributeName is not None:
    #        self._op.params['partitionAttribute'] = self._op.attribute(stream, partitionAttributeName)
    #    if timestampAttributeName is not None:
    #        self._op.params['timestampAttribute'] = self._op.attribute(stream, timestampAttributeName)
    #    if tself._opicAttributeName is not None:
    #        self._op.params['topicAttribute'] = self._op.attribute(stream, topicAttributeName)
        return streamsx.topology.topology.Sink(self._op)


def _check_non_whitespace_string(s):
    """checks s for being a non-space-only or non-empty string.
    Returns:
         s.trim()
    """
    if not isinstance(s, str):
        raise TypeError('unexpected type {}. Must be str.'.format(type(s)))
    _s = s.strip()
    if not _s:
        raise ValueError(_s)
    return _s


def _try_read_from_file (potential_filename):
    """
    Reads data from a file or returns the data 'as is'.
    Args:
        potential_filename(str): the data, which can also be a filename.

    Returns:
        str: data read from file or the content of parameter 'potential_filename'

    """
    _data = potential_filename
    if os.path.exists (potential_filename) and os.path.isfile (potential_filename):
        with open(potential_filename, 'r') as data_file:
            _data = data_file.read()
#            print ("data read from file " + _data)
#    else:
#        print ("using data literally")
    return _data.strip()

def _generate_password(len=16):
    return ''.join(random.choice(string.ascii_uppercase + string.digits + string.ascii_lowercase) for _ in range(len))


def _generate_random_digits(len=10):
    return ''.join(random.choice(string.digits) for _ in range(len))

def _test_dsx_dataset_dir_and_get(default_dir):
    if 'DSX_PROJECT_DIR' in os.environ:
        _dsxdir = os.environ['DSX_PROJECT_DIR'].strip()
        if _dsxdir:
            _dir = _dsxdir + '/datasets'
            if os.path.isdir(_dir):
                return _dir
    return default_dir


def _create_keystore_properties(client_cert, client_private_key, store_passwd=None, store_suffix=None, store_dir=None, topology=None):
    """Creates a keystore, adds it as file dependency to the topology, and creates
    the required properties to use a keystore.

    Args:
        client_cert(str): the filename with a client certificate in PEM format or the certificate itself
        client_private_key(str): the filename with a private key in PEM format or the private key itself
        store_passwd(str): the password for the keystore and the private key. When None, a password is generated.
        store_suffix(str): A suffix for the keystore file name. The filename will be ``'keystore-<suffix>.jks'``.
            If the suffix is None, a suffix will be generated.
        store_dir(str): directory where the truststore file is created
        topology(Topology): the topology to which the keystore is added as file dependency in 'etc' directory.
            If None, the key store file is generated in current working directory and not added to a topology.

    Returns:
        dict: A set of keystore relevant properties. They can be used for consumers and producers.
    """
    _storeSuffix = store_suffix
    if _storeSuffix is None:
        _storeSuffix = _generate_random_digits()
    else:
        _storeSuffix = _storeSuffix.strip()
    # an empty string evaluates to False
    if _storeSuffix:
        _store_filename = 'keystore-' + _storeSuffix + '.jks'
    else:
        _store_filename = 'keystore.jks'
    if store_dir:
        _dir = store_dir
    else:
        if topology is None:
            _dir = '.'
        else:
            _dir = _test_dsx_dataset_dir_and_get(gettempdir())
    _filename = os.path.join(_dir, _store_filename)
    _passwd = store_passwd
    if _passwd is None or not _passwd.strip():
        _passwd = _generate_password()
    streamsx.toolkits.create_keystore(client_cert, client_private_key, _filename, _passwd)
    print('keystore file ' + _filename + ' generated. Store and key password is ' + _passwd)
    if topology is None:
        print('Copy this file into the etc/ directory of your Streams application.')
    else:
        topology.add_file_dependency(_filename, 'etc')
        print("Keystore file etc/" + _store_filename + " added to the topology " + topology.name)
    
    _props = dict()
    _props['ssl.keystore.type'] = 'JKS'
    _props['ssl.keystore.password'] = _passwd
    _props['ssl.key.password'] = _passwd
    _props['ssl.keystore.location'] = '{applicationDir}/etc/' + _store_filename
    return _props


def _create_truststore_properties(trusted_cert, store_passwd=None, store_suffix=None, store_dir=None, topology=None):
    """Creates a keystore, adds it as file dependency to the topology, and creates 
    the required properties to use a truststore.

    Args:
        trusted_cert(list|str): a list of filenames with trusted certificates in PEM format or the certificates themselves
        store_passwd(str): the password for the keystore. When None, a password is generated.
        store_suffix(str): A suffix for the keystore file name. The filename will be ``'truststore-<suffix>.jks'``.
            If the suffix is None, a suffix will be generated.
        store_dir(str): directory where the truststore file is created
        topology(Topology): the topology to which the keystore is added as file dependency in 'etc' directory.
            If None, the key store file is generated in current working directory and not added to a topology.

    Returns:
        dict: A set of truststore relevant properties. They can be used for consumers and producers.
    """
    _storeSuffix = store_suffix
    if _storeSuffix is None:
        _storeSuffix = _generate_random_digits()
    else:
        _storeSuffix = _storeSuffix.strip()
    # an empty string evaluates to False
    if _storeSuffix:
        _store_filename = 'truststore-' + _storeSuffix + '.jks'
    else:
        _store_filename = 'truststore.jks'
    if store_dir:
        _dir = store_dir
    else:
        if topology is None:
            _dir = '.'
        else:
            _dir = _test_dsx_dataset_dir_and_get(gettempdir())
    _filename = os.path.join(_dir, _store_filename)
    _passwd = store_passwd
    if _passwd is None or not _passwd.strip():
        _passwd = _generate_password()
    streamsx.toolkits.create_truststore(trusted_cert, _filename, _passwd)
    print('truststore file ' + _filename + ' generated. Store password is ' + _passwd)
    if topology is None:
        print('Copy this file into the etc/ directory of your Streams application.')
    else:
        topology.add_file_dependency(_filename, 'etc')
        print("Truststore file etc/" + _store_filename + " added to the topology " + topology.name)
    
    _props = dict()
    _props['ssl.truststore.type'] = 'JKS'
    _props['ssl.truststore.password'] = _passwd
    _props['ssl.truststore.location'] = '{applicationDir}/etc/' + _store_filename
    return _props


def _create_connection_properties(bootstrap_servers, use_TLS=True, enable_hostname_verification=True,
                    cluster_ca_cert=None, authentication=AuthMethod.NONE, 
                    client_cert=None, client_private_key=None, username=None, password=None, topology=None,
                    store_dir=None, store_pass=None, store_suffix=None):
    """Internal method. Assumes all parameters are validated.
    """ 
    props = dict()
    _storePasswd = store_pass
    if _storePasswd is None or not _storePasswd.strip():
        _storePasswd = _generate_password()
    _storeSuffix = store_suffix
    if _storeSuffix is None:
        _storeSuffix = _generate_random_digits()
    props['bootstrap.servers'] = bootstrap_servers
    if authentication == AuthMethod.NONE:
        if use_TLS:
            props['security.protocol'] = 'SSL'
    elif authentication == AuthMethod.PLAIN:
        props['security.protocol'] = 'SASL_SSL' if use_TLS else 'SASL_PLAINTEXT'
        props['sasl.mechanism'] = 'PLAIN'
        props['sasl.jaas.config'] = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="' + username + '" password="' + password + '";'
    elif authentication == AuthMethod.TLS:
        props['security.protocol'] = 'SSL'
        props.update(_create_keystore_properties(client_cert,
                                                 client_private_key,
                                                 _storePasswd,
                                                 _storeSuffix,
                                                 store_dir,
                                                 topology))
    elif authentication == AuthMethod.SCRAM_SHA_512:
        props['security.protocol'] = 'SASL_SSL' if use_TLS else 'SASL_PLAINTEXT'
        props['sasl.mechanism'] = 'SCRAM-SHA-512'
        props['sasl.jaas.config'] = 'org.apache.kafka.common.security.scram.ScramLoginModule required username="' + username + '" password="' + password + '";'
    else:
        raise NotImplementedError(authentication)

    if use_TLS:
        props['ssl.endpoint.identification.algorithm'] = 'https' if enable_hostname_verification else ''
        if cluster_ca_cert:
            props.update(_create_truststore_properties(cluster_ca_cert,
                                                       _storePasswd,
                                                       _storeSuffix,
                                                       store_dir,
                                                       topology))
    return props


def _write_properties_file(properties, file_name=None):
    """
    writes properties as a dictionary into a file

    Args:
        properties(dict):       the Kafka properties
        file_name(str):         the filename of the property file. If None, stdout is written
    """
    with sys.stdout if file_name is None else open(file_name, "w") as properties_file:
        for key, value in properties.items():
            properties_file.write(key + '=' + str(value) + '\n')


def _add_properties_file(topology, properties, file_name):
    """
    Adds properties as a dictionary as a file dependency to the topology

    Args:
        topology(Topology):     the topology
        properties(dict):       the Kafka properties
        file_name(str):         the filename of the file dependency

    Returns:
        str: 'etc/' + basename(file_name)
    """
    if properties is None:
        raise TypeError(properties)
    if file_name is None:
        raise TypeError(file_name)

    if len(properties.keys()) == 0:
        raise ValueError ("properties(dict) is empty. Please add at least the property 'bootstrap.servers'.")
    _write_properties_file (properties, file_name)
    print('properties file ' + file_name + ' generated.')
    topology.add_file_dependency(file_name, 'etc')
    fName = 'etc/' + os.path.basename(file_name)
    print("Properties file " + fName + " added to the topology " + topology.name)
    return fName


def download_toolkit(url=None, target_dir=None):
    r"""Downloads the latest Kafka toolkit from GitHub.

    Example for updating the Kafka toolkit for your topology with the latest toolkit from GitHub::

        import streamsx.kafka as kafka
        # download Kafka toolkit from GitHub
        kafka_toolkit_location = kafka.download_toolkit()
        # add the toolkit to topology
        streamsx.spl.toolkit.add_toolkit(topology, kafka_toolkit_location)

    Example for updating the topology with a specific version of the Kafka toolkit using a URL::

        import streamsx.kafka as kafka
        url201 = 'https://github.com/IBMStreams/streamsx.kafka/releases/download/v2.0.1/com.ibm.streamsx.kafka-2.0.1.tgz'
        kafka_toolkit_location = kafka.download_toolkit(url=url201)
        streamsx.spl.toolkit.add_toolkit(topology, kafka_toolkit_location)

    Args:
        url(str): Link to toolkit archive (\*.tgz) to be downloaded. Use this parameter to 
            download a specific version of the toolkit.
        target_dir(str): the directory where the toolkit is unpacked to. If a relative path is given,
            the path is appended to the system temporary directory, for example to /tmp on Unix/Linux systems.
            If target_dir is ``None`` a location relative to the system temporary directory is chosen.

    Returns:
        str: the location of the downloaded Kafka toolkit

    .. note:: This function requires an outgoing Internet connection
    .. versionadded:: 1.3
    """
    _toolkit_location = streamsx.toolkits.download_toolkit (toolkit_name=_TOOLKIT_NAME, url=url, target_dir=target_dir)
    return _toolkit_location


def configure_connection(instance, name, bootstrap_servers, ssl_protocol = None, enable_hostname_verification = True):
    """Configures IBM Streams for a connection with a Kafka broker.

    Creates an application configuration object containing the required properties
    with connection information. The application configuration contains following properties:
    
    - bootstrap.servers
    - security.protocol (when **ssl_protocol** is not ``None``)
    - ssl.protocol (when **ssl_protocol** is not ``None``)
    - ssl.endpoint.identification.algorithm (when **enable_hostname_verification** is ``False``)

    Example for creating a configuration for a Streams instance with connection details::


        from streamsx.rest import Instance
        import streamsx.topology.context
        from icpd_core import icpd_util
        
        cfg = icpd_util.get_service_instance_details(name='your-streams-instance')
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False
        instance = Instance.of_service(cfg)
        bootstrap_servers = 'kafka-server-1.domain:9093,kafka-server-2.domain:9093,kafka-server-3.domain:9093'
        app_cfg_name = configure_connection(instance, 'my_app_cfg1', bootstrap_servers, 'TLSv1.2')

    Args:
        instance(streamsx.rest_primitives.Instance): IBM Streams instance object.
        name(str): Name of the application configuration.
        bootstrap_servers(str): Comma separated List of *hostname:TCPport* of the 
            Kafka-bootstrap-servers, for example ``'server1:9093'``, or 
            ``'server1:9093,server2:9093,server3:9093'``.

        ssl_protocol(str): One of None, 'TLS', 'TLSv1', 'TLSv1.1', or 'TLSv1.2'.
            If None is used, TLS is not configured. If unsure, use 'TLS',
            which is Kafka's default.

        enable_hostname_verification(bool): ``True`` (default) enables hostname 
            verification of the server certificate, ``False`` disables hostname 
            verification. The parameter is ignored, when ``ssl_protocol`` is ``None``.
    Returns:
        str: Name of the application configuration, i.e. the same value as given in the ``name`` parameter

    .. warning:: The function can be used only in IBM Cloud Pak for Data
    .. deprecated:: 1.6.1
        Use :func:`create_connection_properties` and :func:`configure_connection_from_properties`
    .. versionadded:: 1.1
    """
    warnings.warn("Use 'create_connection_properties' and 'configure_connection_from_properties'", DeprecationWarning, stacklevel=2)
    if name is None:
        raise TypeError(name)
    if bootstrap_servers is None:
        raise TypeError(bootstrap_servers)
    if not isinstance(name, str):
        raise TypeError(name)
    if not isinstance(bootstrap_servers, str):
        raise TypeError(bootstrap_servers)
    if not (isinstance(ssl_protocol, str) or ssl_protocol is None):
        raise TypeError(ssl_protocol)
    if not isinstance(enable_hostname_verification, bool):
        raise TypeError(enable_hostname_verification)
    
    
    kafkaProperties = {}
    kafkaProperties['bootstrap.servers'] = bootstrap_servers
    if ssl_protocol:
        kafkaProperties['security.protocol'] = 'SSL'
        if ssl_protocol in ('TLS', 'TLSv1', 'TLSv1.1', 'TLSv1.2'):
            kafkaProperties['ssl.protocol'] = ssl_protocol
        else:
            warnings.warn('ignoring invalid sslProtocol ' + ssl_protocol + '. Using Kafkas default value')
        if not enable_hostname_verification:
            kafkaProperties['ssl.endpoint.identification.algorithm'] = ''
            
    return configure_connection_from_properties(instance, name, kafkaProperties)


def configure_connection_from_properties(instance, name, properties, description=None):
    """
    Configures IBM Streams for a connection with a Kafka broker.

    Creates an application configuration object containing the required 
    properties with connection information. The application configuration 
    contains the properties given as key-value pairs in the `properties` 
    dictionary. The keys must be valid 
    `consumer <https://kafka.apache.org/22/documentation/#consumerconfigs>`_ 
    or `producer configurations <https://kafka.apache.org/22/documentation/#producerconfigs>`_.

    Example for creating a configuration for a Streams instance with connection details::

        from streamsx.rest import Instance
        import streamsx.topology.context
        from streamsx.kafka import create_connection_properties, configure_connection
        from icpd_core import icpd_util
        
        cfg = icpd_util.get_service_instance_details(name='your-streams-instance')
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False
        instance = Instance.of_service(cfg)
        bootstrap_servers = 'kafka-server-1.domain:9093,kafka-server-2.domain:9093,kafka-server-3.domain:9093'
        consumer_properties = create_connection_properties (bootstrap_servers = bootstrap_servers)
        app_cfg_name = configure_connection_from_properties(instance, 'my_app_cfg1', consumer_properties, 'KafkaConsumerConfig')

    Args:
        instance(streamsx.rest_primitives.Instance): IBM Streams instance object.
        name(str): Name of the application configuration.
        properties(dict): Properties containing valid consumer or producer configurations.
        description(str): Description of the application configuration. If no 
            descrition is given, a description is generated.
    Returns:
        str: Name of the application configuration, i.e. the same value as given in the ``name`` parameter

    .. warning:: The function can be used only in IBM Cloud Pak for Data
    .. versionadded:: 1.3
    """
    if name is None or not isinstance(name, str):
        raise TypeError(name)
    if properties is None or not isinstance(properties, dict):
        raise TypeError(properties)
    # stringify property values:
    _properties = dict()
    for k, v in properties.items():
        _properties[k] = str(v)
    _description = description
    if _description is None:
        _description = 'Kafka configurations'

    # check if application configuration exists
    app_config = instance.get_application_configurations(name = name)
    if app_config:
        # set the values to None for the keys which are not present in the new property set any more...
        oldProperties = app_config[0].properties
        for key, value in oldProperties.items():
            if not key in _properties:
                # set None to delete the property in the application config
                _properties[key] = None
        print('update application configuration: ' + name)
        app_config[0].update(_properties)
    else:
        print('create application configuration: ' + name)
        instance.create_application_configuration(name, _properties, _description)
    
    return name


def create_connection_properties_for_eventstreams(credentials):
    """Create Kafka configuration properties from service credentials for IBM event streams to connect 
    with IBM event streams. The resulting properties can be used for example in 
    :func:`configure_connection_from_properties`, as ``consumer_properties`` in :py:class:`~KafkaConsumer`,
    and as ``producer_properties`` in :py:class:`~KafkaProducer`.
    
    Args:
        credentials(dict|str): The service credentials for IBM event streams as a JSON dict or as string.
            When a string is given, the parameter must be the name of an existing JSON file with credentials or 
            must contain the raw JSON credentials.
        
    Returns:
        dict: Kafka properties that contain the connection information.
        
    .. warning:: The returned properties contain sensitive data. Storing the properties in
        an application configuration is a good idea to avoid exposing sensitive information.
        On IBM Cloud Pak for Data use :func:`configure_connection_from_properties` to do this.
        
    .. versionadded:: 1.7
    """
    if credentials is None:
        raise TypeError(credentials)

    if isinstance(credentials, str):
        _creds = json.loads(_try_read_from_file(credentials))
    else:
        _creds = credentials
    _cfg = dict()
    _cfg['bootstrap.servers'] = ','.join(_creds['kafka_brokers_sasl'])
    _cfg['sasl.mechanism'] = 'PLAIN'
    _cfg['ssl.endpoint.identification.algorithm'] = 'https'
    _cfg['security.protocol'] = 'SASL_SSL'
    _cfg['sasl.jaas.config'] = 'org.apache.kafka.common.security.plain.PlainLoginModule required username="{}" password="{}";'.format (_creds['user'], _creds['password'])
    return _cfg


def create_connection_properties(bootstrap_servers, use_TLS=True, enable_hostname_verification=True,
                    cluster_ca_cert=None, authentication=AuthMethod.NONE, 
                    client_cert=None, client_private_key=None, username=None, password=None, topology=None):
    """Create Kafka properties that can be used to connect a consumer or a producer with a Kafka cluster
    when certificates and keys or authentication is required. The resulting properties can be used
    for example in :func:`configure_connection_from_properties`, as ``consumer_properties`` in :py:class:`~KafkaConsumer`,
    and as ``producer_properties`` in :py:class:`~KafkaProducer`.
    
    When certificates are given, the function will create a truststore and/or a keystore, 
    which are added as file dependencies to the topology, which must not be ``None`` in this case.
    
    Certificates and keys are given as strings. The arguments can be the name of an existinig PEM formatted file,
    which the content is read, or the PEM formatted certificate or key directly. The PEM format is a text format
    with base64 encoded content between anchors::
    
        -----BEGIN CERTIFICATE-----
        ...
        -----END CERTIFICATE-----
    
    or::
    
        -----BEGIN PRIVATE KEY-----
        ...
        -----END PRIVATE KEY-----

    **Example**, in which the brokers are configured for SCRAM-SHA-512 authentication over a TLS connection,
    and where the server certificates are not signed by a public CA. In the example, the CA certificate 
    for the cluster is stored in the file */tmp/secrets/cluster_ca.crt*::
    
        from streamsx.topology.topology import Topology
        from streamsx.topology.schema import CommonSchema
        import streamsx.kafka as kafka
        
        consumerTopology = Topology('ConsumeFromKafka')
        
        consumerProperties = dict()
        
        # In this example we read only transactionally committed messages
        consumerProperties['isolation.level'] = 'read_committed'
        connectionProps = kafka.create_connection_properties(
            bootstrap_servers = 'kafka-cluster1-kafka-bootstrap-myproject.192.168.42.183.nip.io:443',
            #use_TLS = True,
            #enable_hostname_verification = True,
            cluster_ca_cert = '/tmp/secrets/cluster_ca.crt',
            authentication = kafka.AuthMethod.SCRAM_SHA_512,
            username = 'user123',
            password = 'passw0rd', # not the very best choice for a password
            topology = consumerTopology)
        
        # add connection specifc properties to the consumer properties
        consumerProperties.update(connectionProps)
        messages = kafka.subscribe (consumerTopology, 'mytopic', consumerProperties, CommonSchema.String)
    
    Args:
        bootstrap_servers(str): The bootstrap address of the Kafka cluster.
            This is a single *hostname:TCPport* pair or a comma separated List of *hostname:TCPport*,
            for example ``'server1:9093'``, or ``'server1:9093,server2:9093,server3:9093'``.
            
        use_TLS(bool): When ``True`` (default), the client connects via encrypted connections with the Kafka brokers.
            In this case it may also be necessary to provide the CA certificate of the cluster 
            within the **cluster_ca_cert** parameter.
            When the parameter is ``False``, the traffic to the Kafka brokers is not encrypted.
            
        enable_hostname_verification(bool): When ``True`` (default), the hostname verification of the
            presented server certificate is enabled. For example, some methods to expose a 
            containerized Kafka cluster do not support hostname verification.
            In these cases hostname verification must be disabled.
            
            The parameter is ignored when **use_TLS** is ``False``.
            
        cluster_ca_cert(str|list): The CA certificate of the broker certificates or a list of them. This certificate is 
            required when the cluster does not use certificates signed by a public CA authority.
            The parameter must be the name of an existing PEM formatted file or the PEM formatted certificate itself, 
            or a list of filenames or PEM strings. These certificates are treated as trusted and go into a truststore.
            
            A trusted certificate must have a text format like this::
            
                -----BEGIN CERTIFICATE-----
                ...
                -----END CERTIFICATE-----
           
            The parameter is ignored when **use_TLS** is ``False``.
           
        authentication(AuthMethod): The authentication method used by the brokers.
            
            * None, AuthMethod.NONE: clients are not authenticated. The parameters 
              **client_cert**, **client_private_key**, **username**, and **password** are ignored.
            * AuthMethod.PLAIN: PLAIN, or SASL/PLAIN, is a simple username/password authentication
              mechanism that is typically used with TLS for encryption to implement secure authentication.
              SASL/PLAIN should only be used with SSL as transport layer to ensure that clear
              passwords are not transmitted on the wire without encryption.
            * AuthMethod.TLS: clients are authorized with client certificates. This authentication method
              can only be used when the client uses a TLS connection, i.e. when **use_TLS** is ``True``.
              The client certificate must be trusted by the server, and must be and given as the 
              **client_cert** parameter together with the corresponding private key as 
              the **client_private_key** parameter.
            * AuthMethod.SCRAM_SHA_512: SCRAM (Salted Challenge Response Authentication Mechanism) is an
              authentication protocol that can establish authentication using usernames and passwords.
              It can be used with or without a TLS client connection. This authentication 
              method requires that the parameters **username** and **password** are used.
              
        client_cert(str): The client certificate, i.e. the public part of a key pair signed by an
            authority that is trusted by the brokers. The parameter must be the name of 
            an existing PEM formatted file or the PEM formatted certificate itself.
            The client certificate must have a text format like this::
            
                -----BEGIN CERTIFICATE-----
                ...
                -----END CERTIFICATE-----
            
            The parameter is ignored when **authentication** is not 'TLS'.
            
        client_private_key(str): The private part of the RSA key pair on which the client certificate is based.
            The parameter must be the name of an existing PEM formatted file or the PEM 
            formatted key itself. The private key must have a text format like this::
            
                -----BEGIN PRIVATE KEY-----
                ...
                -----END PRIVATE KEY-----

            The parameter is ignored when **authentication** is not 'TLS'.
            
        username(str): The username for SCRAM authentication.
            The parameter is ignored when **authentication** is not 'SCRAM-SHA-512'.

        password(str): The password for SCRAM authentication.
            The parameter is ignored when **authentication** is not 'SCRAM-SHA-512'.

        topology(Topology): The topology to which a truststore and or keystore as 
            file dependencies are added. It must be this Topology instance, which 
            the created Kafka properties are used. The parameter must not be
            None when one of the parameters **cluster_ca_cert**, **client_cert**, 
            or **client_private_key** is not ``None``.
            
    Returns:
        dict: Kafka properties 
        
    .. note:: When certificates are needed, this function can be used only with 
        the **streamsx.kafka** toolkit version 1.9.2 and higher.
        The function will add a toolkit dependency to the topology.
        When the toolkit dependency cannot be satisfied, use a newer toolkit version.
        A newer version of the toolkit can be downloaded from GitHub with :func:`download_toolkit`.
    .. warning:: The returned properties can contain sensitive data. Storing the properties in
        an application configuration is a good idea to avoid exposing sensitive information.
        On IBM Cloud Pak for Data use :func:`configure_connection_from_properties` to do this.
        
    .. versionadded:: 1.3
    """
    if bootstrap_servers is None:
        raise TypeError ('bootstrap_servers must not be None')
    _auth = authentication
    if _auth is None:
        _auth = AuthMethod.NONE
    if _auth == AuthMethod.TLS:
        if client_cert is None:
            raise ValueError('client_cert must not be None when AuthMethod.TLS is used')
        if client_private_key is None:
            raise ValueError('client_private_key must not be None when AuthMethod.TLS is used')
        if not use_TLS:
            raise ValueError('When AuthMethod.TLS is used, the broker must be connected by using TLS. use_TLS must be True.')
        if topology is None:
            raise ValueError('topology must not be None when TLS authentication is used')
    if _auth == AuthMethod.SCRAM_SHA_512:
        if username is None:
            raise ValueError('username must not be None when AuthMethod.SCRAM_SHA_512 is used')
        if password is None:
            raise ValueError('password must not be None when AuthMethod.SCRAM_SHA_512 is used')
    if _auth == AuthMethod.PLAIN:
        if username is None:
            raise ValueError('username must not be None when AuthMethod.PLAIN is used')
        if password is None:
            raise ValueError('password must not be None when AuthMethod.PLAIN is used')
    if use_TLS and (not cluster_ca_cert is None) and (topology is None):
        raise ValueError('topology must not be None when a CA certificate is given')

    if cluster_ca_cert or client_cert or client_private_key:
        if topology:
            # add toolkit dependency to support '{applicationDir}' replacement in properties
            tk.add_toolkit_dependency(topology, _TOOLKIT_NAME, '[1.9.2,99.0.0]')

    props = _create_connection_properties(bootstrap_servers=bootstrap_servers, use_TLS=use_TLS, enable_hostname_verification=enable_hostname_verification,
                                          cluster_ca_cert=cluster_ca_cert,
                                          authentication=_auth, 
                                          client_cert=client_cert, client_private_key=client_private_key,
                                          username=username, password=password,
                                          topology=topology,
                                          # the following are generated or automatically chosen when None:
                                          store_dir=None, store_pass=None, store_suffix=None)
    return props


def subscribe(topology, topic, kafka_properties, schema, group=None, name=None):
    """Subscribe to messages from a Kafka broker for a single topic.

    Adds a Kafka consumer that subscribes to a topic
    and converts each message to a stream tuple.

    Args:
        topology(Topology): Topology that will contain the stream of messages.
        topic(str): Topic to subscribe messages from.
        kafka_properties(dict|str): Properties containing the consumer configurations, at 
            minimum the ``bootstrap.servers`` property. When a string is given, it is the 
            name of the application configuration, which contains consumer configs.
            Must not be ``None``.
        schema(StreamSchema): Schema for returned stream.
        group(str): Kafka consumer group identifier. When not specified it default to the
            job name with ``topic`` appended separated by an underscore, so that all ``subscribe``
            invocations within the topology to the same topic form a Kafka consumer group.
        name(str): Consumer name in the Streams context, defaults to a generated name.

    Returns:
        streamsx.topology.topology.Stream: Stream containing messages.

    .. deprecated:: 1.8.0
        Use the :py:class:`~KafkaConsumer` to create a source stream from a Kafka subscription.
    """
    if topic is None:
        raise TypeError(topic)
    msg_attr_name = None
    if schema is CommonSchema.Json:
        msg_attr_name='jsonString'
    elif schema is CommonSchema.String:
        msg_attr_name='string'
    elif schema is Schema.BinaryMessage:
        # msg_attr_name = 'message'
        pass
    elif schema is Schema.StringMessage:
        # msg_attr_name = 'message'
        pass
    elif schema is Schema.BinaryMessageMeta:
        # msg_attr_name = 'message'
        pass
    elif schema is Schema.StringMessageMeta:
        # msg_attr_name = 'message'
        pass
    else:
        raise TypeError(schema)

    if group is None:
        group = streamsx.spl.op.Expression.expression('getJobName() + "_" + "' + str(topic) + '"')

    if name is None:
        name = topic

    if isinstance(kafka_properties, dict):
        tmpf = NamedTemporaryFile(suffix='.properties', prefix='consumer-', delete=False)
        propsFilename = _add_properties_file(topology, kafka_properties, tmpf.name)
        _op = _KafkaConsumer(topology, schema=schema,
                             outputMessageAttributeName=msg_attr_name,
                             propertiesFile=propsFilename, 
                             topic=topic, 
                             groupId=group, 
                             name=name)
    elif isinstance(kafka_properties, str):
        _op = _KafkaConsumer(topology, schema=schema,
                             outputMessageAttributeName=msg_attr_name,
                             appConfigName=kafka_properties,
                             topic=topic,
                             groupId=group,
                             name=name)
    else:
        raise TypeError(kafka_properties)

    return _op.stream


def publish(stream, topic, kafka_properties, name=None):
    """Publish messages to a topic in a Kafka broker.

    Adds a Kafka producer where each tuple on `stream` is
    published as a stream message.

    Args:
        stream(Stream): Stream of tuples to be published as messages.
        topic(str): Topic to publish messages to.
        kafka_properties(dict|str): Properties containing the producer configurations, 
            at minimum the ``bootstrap.servers`` property. When a string is given, it 
            is the name of the application configuration, which contains producer configs.
            Must not be ``None``.
        name(str): Producer name in the Streams context, defaults to a generated name.

    Returns:
        streamsx.topology.topology.Sink: Stream termination.

    .. deprecated:: 1.8.0
        Use the :py:class:`~KafkaProducer` to terminate a stream by publishing messages to Kafka.
    """
    if topic is None:
        raise TypeError(topic)
    msg_attr_name = None
    streamSchema = stream.oport.schema
    if streamSchema is CommonSchema.Json:
        msg_attr_name = 'jsonString'
    elif streamSchema is CommonSchema.String:
        msg_attr_name = 'string'
    elif streamSchema is Schema.BinaryMessage:
        # msg_attr_name = 'message'
        pass
    elif streamSchema is Schema.StringMessage:
        # msg_attr_name = 'message'
        pass
    else:
        raise TypeError(streamSchema)

    if isinstance(kafka_properties, dict):
        tmpf = NamedTemporaryFile(suffix='.properties', prefix='producer-', delete=False)
        propsFilename = _add_properties_file(stream.topology, kafka_properties, tmpf.name)
        _op = _KafkaProducer(stream,
                             propertiesFile=propsFilename,
                             topic=topic,
                             name = name)
    elif isinstance(kafka_properties, str):
        _op = _KafkaProducer(stream,
                             appConfigName=kafka_properties,
                             topic=topic,
                             name = name)
    else:
        raise TypeError(kafka_properties)

    # create the input attribute expressions after operator _op initialization
    if msg_attr_name is not None:
        _op.params['messageAttribute'] = _op.attribute(stream, msg_attr_name)
#    if keyAttributeName is not None:
#        params['keyAttribute'] = _op.attribute(stream, keyAttributeName)
#    if partitionAttributeName is not None:
#        params['partitionAttribute'] = _op.attribute(stream, partitionAttributeName)
#    if timestampAttributeName is not None:
#        params['timestampAttribute'] = _op.attribute(stream, timestampAttributeName)
#    if topicAttributeName is not None:
#        params['topicAttribute'] = _op.attribute(stream, topicAttributeName)

    return streamsx.topology.topology.Sink(_op)


class _KafkaConsumer(streamsx.spl.op.Source):
  def __init__(self, topology, schema,
               vmArg=None,
               appConfigName=None,
               clientId=None,
               commitCount=None,
               groupId=None,
               outputKeyAttributeName=None,
               outputMessageAttributeName=None,
               outputTimestampAttributeName=None,
               outputOffsetAttributeName=None,
               outputPartitionAttributeName=None,
               outputTopicAttributeName=None,
               partition=None,
               propertiesFile=None,
               startPosition=None,
               startOffset=None,
               startTime=None,
               topic=None,
               triggerCount=None,
               userLib=None,
               name=None):
        kind = "com.ibm.streamsx.kafka::KafkaConsumer"
        inputs = None
        schemas = schema
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
        super(_KafkaConsumer, self).__init__(topology, kind, schemas, params, name)


class _KafkaProducer(streamsx.spl.op.Sink):
    def __init__(self, stream,
                 vmArg=None,
                 appConfigName=None,
                 clientId=None,
                 propertiesFile=None,
                 topic=None,
                 userLib=None,
                 name=None):
        # topology = stream.topology
        kind="com.ibm.streamsx.kafka::KafkaProducer"
        params = dict()
        if vmArg is not None:
            params['vmArg'] = vmArg
        if appConfigName is not None:
            params['appConfigName'] = appConfigName
        if clientId is not None:
            params['clientId'] = clientId
        if propertiesFile is not None:
            params['propertiesFile'] = propertiesFile
        if topic is not None:
            params['topic'] = topic
        if userLib is not None:
            params['userLib'] = userLib
        super(_KafkaProducer, self).__init__(kind, stream, params, name)
