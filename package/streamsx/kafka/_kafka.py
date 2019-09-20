# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2019
import streamsx.spl.op
import streamsx.spl.types
from streamsx.topology.schema import CommonSchema
import streamsx.spl.toolkit as tk

from streamsx.kafka.schema import Schema

from tempfile import gettempdir
import json
from builtins import str
from enum import Enum
import datetime
import jks
import OpenSSL
import os
import sys
import warnings
import string
import random
from fileinput import filename
from streamsx.toolkits import download_toolkit

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
    if not '---BEGIN' in _data:
        warnings.warn('Certificate or key data does not look like in PEM format; no BEGIN anchor found', stacklevel=3)
    return _data.strip()

def _generate_password(len=16):
    return ''.join(random.choice(string.ascii_uppercase + string.digits + string.ascii_lowercase) for _ in range(len))


def _generate_store_suffix(len=10):
    return ''.join(random.choice(string.digits) for _ in range(10))

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
    _client_cert_pem = _try_read_from_file(client_cert)
    _client_key_pem = _try_read_from_file(client_private_key)
    _storeSuffix = store_suffix
    if _storeSuffix is None:
        _storeSuffix = _generate_store_suffix()
    else:
        _storeSuffix = _storeSuffix.strip()
    _passwd = store_passwd
    if _passwd is None:
        _passwd = _generate_password()
    try:
        _cert = OpenSSL.crypto.load_certificate (OpenSSL.crypto.FILETYPE_PEM, bytes(_client_cert_pem, 'utf-8'))
        _client_cer_der = OpenSSL.crypto.dump_certificate (OpenSSL.crypto.FILETYPE_ASN1, _cert)
    except OpenSSL.crypto.Error as e:
        print('Error: Processing client certificate failed.', file=sys.stderr)
        raise
    try:
        _key = OpenSSL.crypto.load_privatekey (OpenSSL.crypto.FILETYPE_PEM, bytes(_client_key_pem, 'utf-8'))
        _client_key_der = OpenSSL.crypto.dump_privatekey (OpenSSL.crypto.FILETYPE_ASN1, _key)
    except OpenSSL.crypto.Error as e:
        print('Error: Processing client private key failed.', file=sys.stderr)
        raise

    try:
        privateKeyEntry = jks.PrivateKeyEntry.new("client_cert", [_client_cer_der], _client_key_der, 'rsa_raw')
    except Exception:
        print('Error: Processing client private key failed. Not RSA format?', file=sys.stderr)
        raise
    
    if privateKeyEntry.is_decrypted():
        privateKeyEntry.encrypt(_passwd)
    keystore = jks.KeyStore.new('jks', [privateKeyEntry])
    # an empty string evaluates to False
    if _storeSuffix:
        _store_filename = 'keystore-' + _storeSuffix + '.jks'
    else:
        _store_filename = 'keystore.jks'
    if topology is None:
        if store_dir is None:
            # save the keystore in current directory
            _dir = '.'
        else:
            _dir = store_dir
        filename = os.path.join(_dir, _store_filename)
        keystore.save(filename, _passwd)
        print('keystore file ' + filename + ' generated. Store and key password is ' + _passwd)
        print('Copy this file into the etc/ directory of your Streams application.')
    else:
        if store_dir is None:
            _dir = _test_dsx_dataset_dir_and_get(gettempdir())
        else:
            _dir = store_dir
        filename = os.path.join(_dir, _store_filename)
        keystore.save(filename, _passwd)
        print('keystore file ' + filename + ' generated. Store and key password is ' + _passwd)
        topology.add_file_dependency(filename, 'etc')
        fName = 'etc/' + _store_filename
        print("Keystore file " + fName + " added to the topology " + topology.name)
    
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
    if isinstance(trusted_cert, str):
        _cert_list = [trusted_cert]
    elif isinstance(trusted_cert, list):
        # must not be empty
        if trusted_cert:
            _cert_list = trusted_cert
        else:
            raise ValueError('trusted_cert must not be an empty list')
    else:
        raise TypeError('trusted_cert must be of str or list type')
    
    _storeSuffix = store_suffix
    if _storeSuffix is None:
        _storeSuffix = _generate_store_suffix()
    else:
        _storeSuffix = _storeSuffix.strip()
    _passwd = store_passwd
    if _passwd is None:
        _passwd = _generate_password()
    _storeEntries = list()
    i = 0
    for _crt in _cert_list:
        _ca_cert_pem = _try_read_from_file(_crt)
        try:
            _cert = OpenSSL.crypto.load_certificate (OpenSSL.crypto.FILETYPE_PEM, bytes(_ca_cert_pem, 'utf-8'))
            _ca_cert_der = OpenSSL.crypto.dump_certificate (OpenSSL.crypto.FILETYPE_ASN1, _cert)
        except OpenSSL.crypto.Error as e:
            print('Error: Processing trusted certificate failed.', file=sys.stderr)
            raise
        _alias = 'ca_cert-' + str(i)
        _store_entry = jks.TrustedCertEntry.new (_alias, _ca_cert_der)
        _storeEntries.append(_store_entry)
        i = i + 1

    keystore = jks.KeyStore.new('jks', _storeEntries)
    # an empty string evaluates to False
    if _storeSuffix:
        _store_filename = 'truststore-' + _storeSuffix + '.jks'
    else:
        _store_filename = 'truststore.jks'
    if topology is None:
        if store_dir is None:
            # save the keystore in current directory
            _dir = '.'
        else:
            _dir = store_dir
        filename = os.path.join(_dir, _store_filename)
        keystore.save(filename, _passwd)
        print('truststore file ' + filename + ' generated. Store password is ' + _passwd)
        print('Copy this file into the etc/ directory of your Streams application.')
    else:
        if store_dir is None:
            _dir = _test_dsx_dataset_dir_and_get(gettempdir())
        else:
            _dir = store_dir
        filename = os.path.join(_dir, _store_filename)
        keystore.save(filename, _passwd)
        print('truststore file ' + filename + ' generated. Store password is ' + _passwd)
        topology.add_file_dependency(filename, 'etc')
        fName = 'etc/' + _store_filename
        print("Truststore file " + fName + " added to the topology " + topology.name)
    
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
    if _storePasswd is None:
        _storePasswd = _generate_password()
    _storeSuffix = store_suffix
    if _storeSuffix is None:
        _storeSuffix = _generate_store_suffix()
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
            properties_file.write(key + '=' + value + '\n')

def _add_properties_file(topology, properties, file_name):
    """
    Adds properties as a dictionary as a file dependency to the topology

    Args:
        topology(Topology):     the topology
        properties(dict):       the Kafka properties
        file_name(str):         the filename of the file dependency

    Returns:
        str: 'etc/' + file_name
    """
    if properties is None:
        raise TypeError(properties)
    if file_name is None:
        raise TypeError(file_name)

    if len(properties.keys()) == 0:
        raise ValueError ("properties(dict) is empty. Please add at least the property 'bootstrap.servers'.")
    tmpfile = os.path.join(gettempdir(), file_name)
    _write_properties_file (properties, tmpfile)
    print('properties file ' + tmpfile + ' generated.')
    topology.add_file_dependency(tmpfile, 'etc')
    fName = 'etc/' + file_name
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
    
    _description = description
    if _description is None:
        _description = 'Kafka configurations'

    # check if application configuration exists
    app_config = instance.get_application_configurations(name = name)
    if app_config:
        # set the values to None for the keys which are not present in the new property set any more...
        oldProperties = app_config[0].properties
        for key, value in oldProperties.items():
            if not key in properties:
                # set None to delete the property in the application config
                properties[key] = None
        print('update application configuration: ' + name)
        app_config[0].update(properties)
    else:
        print('create application configuration: ' + name)
        instance.create_application_configuration(name, properties, _description)
    
    return name


def create_connection_properties(bootstrap_servers, use_TLS=True, enable_hostname_verification=True,
                    cluster_ca_cert=None, authentication=AuthMethod.NONE, 
                    client_cert=None, client_private_key=None, username=None, password=None, topology=None):
    """Create Kafka properties that can be used to connect a consumer or a producer with a Kafka cluster
    when certificates and keys or authentication is required. The resulting properties can be used
    for example in :func:`configure_connection_from_properties`, :func:`subscribe`, or :func:`publish`.
    
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
            job name with ``topic`` appended separated by an underscore.
        name(str): Consumer name in the Streams context, defaults to a generated name.

    Returns:
        streamsx.topology.topology.Stream: Stream containing messages.
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
        fName = 'consumer-' + str(topic) + '.properties'
    else:
        fName = 'consumer-' + str(name) + '.' + str(topic) + '.properties'

    if isinstance(kafka_properties, dict):
        propsFilename = _add_properties_file(topology, kafka_properties, fName)
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
    """
    if topic is None:
        raise TypeError(topic)
    msg_attr_name = None
    streamSchema = stream.oport.schema
    if streamSchema == CommonSchema.Json:
        msg_attr_name = 'jsonString'
    elif streamSchema == CommonSchema.String:
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
        if name is None:
            fName = 'producer-' + str(topic) + '.properties'
        else:
            fName = 'producer-' + str(name) + '.' + str(topic) + '.properties'
        propsFilename = _add_properties_file(stream.topology, kafka_properties, fName)
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
