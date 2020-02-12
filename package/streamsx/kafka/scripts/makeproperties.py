# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2018

import sys
import os
import traceback
import argparse
import streamsx.kafka as kafka
import streamsx.kafka._kafka as kafka_int
from tempfile import gettempdir
from streamsx.topology.topology import Topology

#_CMD_NAME_ = 'streamsx-kafka-make-properties'

class _BootstrapArg(argparse.Action):
    """De-composes comma-separated bootstrap servers
    """
    def __call__(self, parser, namespace, values, option_string=None):
        servers = list()
        for value in values:
            for s in value.split(','):
                host_port = s.strip()
                # no validation is we really have hostname:port. What to do with IPv6 addresses?
                servers.append(host_port)
        setattr(namespace, 'bootstrap_servers', servers)


def _auth_str_to_enum(value):
    """Transform token into enum value
    """
    auth = kafka.AuthMethod.NONE
    if value == 'NONE':
        pass
    elif value == 'PLAIN':
        auth = kafka.AuthMethod.PLAIN
    elif value == 'TLS':
        auth = kafka.AuthMethod.TLS
    elif value == 'SCRAM-SHA-512':
        auth = kafka.AuthMethod.SCRAM_SHA_512
    else:
        raise NotImplementedError("AuthMethod parsing not implemented: " + value)
    return auth


def _verify_dir_writable(parser, filepath):
    if not os.path.exists(filepath):
        parser.error(filepath + ' directory does not exist')
    if not os.path.isdir(filepath):
        parser.error(filepath + ' is not a directory')
    if not os.access(filepath, os.W_OK | os.X_OK):
        parser.error(filepath + ' directory has no permissions to create file(s)')
    else:
        return filepath
    

def _verify_file_readable(parser, filepath):
    if not os.path.exists(filepath):
        parser.error(filepath + ' does not exist')
    if not os.path.isfile(filepath):
        parser.error(filepath + ' is not a file')
    if not os.access(filepath, os.R_OK):
        parser.error(filepath + ' is not readable')
    else:
        return filepath


def _verify_trusted_cert_args(parser, args):
    """Verify readability of trusted cert files
    """
    if args.no_tls:
        pass
    elif args.trusted_cert is not None:
        for cert_file in args.trusted_cert:
            _verify_file_readable(parser, cert_file)

def _verify_auth_args(parser, args):
    """Verify presence of parameters required for the various authentication methods
    """
    a = _auth_str_to_enum(args.auth)
    if a == kafka.AuthMethod.NONE:
        pass
    elif a in {kafka.AuthMethod.PLAIN, kafka.AuthMethod.SCRAM_SHA_512}:
        if args.password is None:
            parser.error('missing --password for authentication method ' + args.auth)
        if args.username is None:
            parser.error('missing --username for authentication method ' + args.auth)
    elif a == kafka.AuthMethod.TLS:
        if args.no_tls:
            parser.error('option --no-tls is incompatible with authentication method ' + args.auth)
        if args.client_cert is None:
            parser.error ('missing --client-cert for authentication method ' + args.auth)
        else:
            _verify_file_readable(parser, args.client_cert)
        if args.client_private_key is None:
            parser.error ('missing --client-private-key for authentication method ' + args.auth)
        else:
            _verify_file_readable(parser, args.client_private_key)
    else:
        raise NotImplementedError("AuthMethod parsing not implemented: " + value)


def _parse_args(args=None):
    """
    Command line argument parsing
    """
    parser = argparse.ArgumentParser(description='Create Kafka consumer or producer configuration for use with the SPL streamsx.kafka toolkit.',
                                     allow_abbrev=False
                                     )
    parser.add_argument('-v', '--version', action='version', version='%(prog)s ' + kafka.__version__)

    conn_grp = parser.add_argument_group(title='Connection', description='connection details')
    conn_grp.add_argument('--bootstrap', '-b', nargs='+', required=True, action=_BootstrapArg, metavar='hostname:port', help='one or more bootstrap servers in the form hostname:port')
    conn_grp.add_argument('--no-tls', action='store_true', default=False, help='connect over an unencrypted plaintext connection, default is to use TLS')
    conn_grp.add_argument('--no-hostname-verify', action='store_true', default=False, help='disable hostname verification of server certificates when TLS is used for the connection')
    conn_grp.add_argument('--trusted-cert', '-T', nargs='+', metavar='file', help='one or more paths to files with certificates in PEM format, which are trusted by the client')

    auth_grp = parser.add_argument_group(title='Authentication', description='options for client authentication')
    auth_grp.add_argument('--auth', '-A', default='NONE', choices=['NONE', 'TLS', 'PLAIN', 'SCRAM-SHA-512'], help='authentication method, defaults to NONE')
    auth_grp.add_argument('--client-cert', '-C', metavar='file', help='path to a file that contains the client certificate in PEM format, required for TLS authentication')
    auth_grp.add_argument('--client-private-key', '-K', metavar='file', help='path to a file that contains the private key of the client certificate in PEM format, required for TLS authentication')
    auth_grp.add_argument('--username', '-U', help='user name, required for PLAIN and SCRAM-SHA-512 authentication')
    auth_grp.add_argument('--password', '-P', help='password, required for PLAIN and SCRAM-SHA-512 authentication')
    #tmp = gettempdir()
    #cwd = os.getcwd()
    out_grp = parser.add_argument_group(title='Output', description='options for output file generation')
    out_grp.add_argument('--out-keystore-dir', '-d', metavar='directory', default='.', help='directory where keystores files are created; the directory must be an existing directory - default is the current working directory')
    out_grp.add_argument('--out-propfile', '-o', metavar='file', help='The filename of an output property file. If not specified, properties are written to stdout together with other information')
    
    cmd_args = parser.parse_args(args)
    _verify_auth_args(parser, cmd_args)
    _verify_trusted_cert_args(parser, cmd_args)
    is_tls = not cmd_args.no_tls
    if (is_tls and cmd_args.trusted_cert) or (_auth_str_to_enum(cmd_args.auth) == kafka.AuthMethod.TLS):
        # must create truststor or keystore
        _verify_dir_writable(parser, cmd_args.out_keystore_dir)
    return cmd_args


def makeproperties(args=None):
    cmd_args = _parse_args(args)
    #print ('===================================================================================')
    #print(cmd_args)
    #print ('===================================================================================')
    is_tls = not cmd_args.no_tls
    tls_hostname_verify = not cmd_args.no_hostname_verify
    bootstrap = ','.join(cmd_args.bootstrap_servers)
    try:
        rc = 0
        props = kafka_int._create_connection_properties(bootstrap_servers=bootstrap,
                                                        use_TLS=is_tls,
                                                        enable_hostname_verification=tls_hostname_verify,
                                                        cluster_ca_cert=cmd_args.trusted_cert,
                                                        authentication=_auth_str_to_enum(cmd_args.auth),
                                                        client_cert=cmd_args.client_cert,
                                                        client_private_key=cmd_args.client_private_key,
                                                        username=cmd_args.username,
                                                        password=cmd_args.password,
                                                        store_dir=cmd_args.out_keystore_dir,
                                                        store_pass=None,   # generate a password
                                                        store_suffix=None) # creates a random suffix
    except:
        print('Error: failed to create properties: {0}'.format(traceback.format_exc()), file=sys.stderr)
        rc = 4
    else:
        if cmd_args.out_propfile is None:
            print('\nKafka properties:\n')
        try:
            kafka_int._write_properties_file(props, cmd_args.out_propfile)
        except OSError as err:
            errno, strerror = err.args
            print('Error: failed to write {0}: ({1}): {2}'.format(cmd_args.out_propfile, errno, strerror), file=sys.stderr)
            rc = 3
        except Exception as e:
            print('Error: failed to write {0}: {1}'.format(cmd_args.out_propfile, traceback.format_exc()), file=sys.stderr)
            rc = 5
    return rc


def main(args=None):
    return makeproperties(args)

if __name__ == '__main__':
    sys.exit(main())
