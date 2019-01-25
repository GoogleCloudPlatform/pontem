# Copyright 2018 The Pontem Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Cloud SQL Replicator CLI main."""

from __future__ import print_function

from builtins import input
import getpass
import json
import logging as std_logging
import re
import shlex
import sys
import time
import uuid
import pprint

from absl import app
from absl.flags import argparse_flags
from absl import logging

# Used for Python 2/3 compatibility
from future.utils import iteritems

from google.cloud.pontem.sql import replicator
from google.cloud.pontem.sql.replicator.util import cloudsql
from google.cloud.pontem.sql.replicator.util import compute
from google.cloud.pontem.sql.replicator.util import mysql_util
from google.cloud.pontem.sql.replicator.util import storage


class MissingRequiredParameterError(ValueError):
    """Raised when a required parameter is missing."""


class SSLConfiguration(object):
    """SSL Configuration"""
    def __init__(self,
                 ca_certificate=None,
                 client_certificate=None,
                 client_key=None):
        """Constructor.

        Args:
            ca_certificate (str): PEM representation of CA X509 certificate.
            client_certificate (str): PEM representation of
                client X509 certificate.
            client_key (str): key for client certificate.
        """
        self.ca_certificate = ca_certificate
        self.client_certificate = client_certificate
        self.client_key = client_key


class MasterConfiguration(object):
    """Configuration for External Master Representation."""
    master_config_defaults = frozenset({
        'user': None,
        'password': None,
        'master_ip': None,
        'master_port': cloudsql.DEFAULT_REPLICATION_PORT,
        'databases': None,
        'database_version': cloudsql.DEFAULT_2ND_GEN_DB_VERSION,
        'region': cloudsql.DEFAULT_2ND_GEN_REGION,
        'source_name': None,
    }.items())

    def __init__(self, **kwargs):
        """Constructor.

        Args:
           kwargs: Parameters that are used to initialize master configuration.
        Raises:
             TypeError: If unrecognized properties are passed.
             MissingRequiredParameterError: If required attributes are not set.
             ValueError: If one or more configuration values is not allowed
                (e.g. 5.6 and 5.7 are the only allowed database versions).
        """
        # pylint: disable=access-member-before-definition

        # Check for required properties are present
        if not set(['user', 'password', 'master_ip']).issubset(kwargs):
            raise MissingRequiredParameterError(
                'Required property missing for master configuration.'
            )

        # Set recognized properties
        for (key, value) in (
                iteritems(dict(MasterConfiguration.master_config_defaults))
        ):
            setattr(self, key, kwargs.get(key) or value)

        if self.database_version not in cloudsql.SUPPORTED_VERSIONS:
            raise ValueError(
                'Database version {} not supported'.format(
                    self.database_version
                )
            )

        if self.source_name is None:
            self.source_name = (
                cloudsql.DEFAULT_EXT_MASTER_FORMAT_STRING.format(uuid.uuid4())
            )

    def to_json(self):
        """Converts MasterConfiguration into a dict.

        Converts object instance into dictionary that can be used with
            Cloud SQL Admin API.

        Returns:
            dict: JSON object to be used as  body argument for
                instance.insert method of Cloud SQL Admin API.
        """
        master_config = {
            'host': self.master_ip,
            'port': self.master_port,
            'user': self.user,
            'password': self.password,
            'databases': self.databases,
            'sourceRepresentationBody': {
                'name': self.source_name,
                'databaseVersion': self.database_version,
                'region': self.region,
                'onPremisesConfiguration': {
                    'kind': 'sql#onPremisesConfiguration',
                    'hostPort': '{}:{}'.format(
                        self.master_ip,
                        self.master_port
                    )
                }
            }
        }
        return master_config


class ReplicaConfiguration(object):
    """Configuration for External Master Representation."""
    replica_config_defaults = frozenset({
        'master_instance_name': None,
        'bucket': None,
        'dumpfile_path': None,
        'user': None,
        'password': None,
        'database_version': cloudsql.DEFAULT_2ND_GEN_DB_VERSION,
        'tier': cloudsql.DEFAULT_2ND_GEN_TIER,
        'region': cloudsql.DEFAULT_2ND_GEN_REGION,
        'replica_name': None,
        'caCertificate': None,
        'client_certificate': None,
        'client_key': None

    }.items())

    def __init__(self, **kwargs):
        """Constructor.

        Args:
            kwargs: Parameters that are used to initialize master configuration.
        Raises:
             MissingRequiredParameterError: If required attributes are not set.
             TypeError: If unrecognized properties are passed.
             ValueError: If one or more configuration values is not allowed
                (e.g. 5.6 and 5.7 are the only allowed database versions).
        """
        # pylint: disable=access-member-before-definition

        # Check for required properties are present
        if not set(
                [
                    'master_instance_name',
                    'dumpfile_path',
                    'user',
                    'password'
                ]
        ).issubset(kwargs):
            raise MissingRequiredParameterError(
                'Required property missing for master configuration.'
            )
        for (key, value) in (
                iteritems(dict(ReplicaConfiguration.replica_config_defaults))
        ):
            setattr(self, key, kwargs.get(key) or value)

        if self.database_version not in cloudsql.SUPPORTED_VERSIONS:
            raise ValueError(
                'Database version {} not supported'.format(
                    self.database_version
                )
            )
        if self.dumpfile_path and not self.dumpfile_path.startswith('gs://'):
            raise ValueError(
                'dumpfile_path must start with gs://'
            )
        if self.replica_name is None:
            self.replica_name = (
                cloudsql.DEFAULT_REPLICA_FORMAT_STRING.format(uuid.uuid4())
            )

    def to_json(self):
        """Converts ReplicaConfiguration into a dict.

        Converts object instance into dictionary that can be used with
            Cloud SQL Admin API.

        Returns:
            dict: JSON object to be used as  body argument for
                instance.insert method of Cloud SQL Admin API.
        """
        replica_config = {
            'bucket': self.bucket,
            'replicaInstanceBody': {
                'name': self.replica_name,
                'settings': {
                    'tier': self.tier,

                },
                'databaseVersion': self.database_version,
                'masterInstanceName': self.master_instance_name,
                'region': self.region,
                'replicaConfiguration': {
                    'kind': 'sql#replicaConfiguration',
                    'mysqlReplicaConfiguration': {
                        'kind': 'sql#mysqlReplicaConfiguration',
                        'dumpFilePath': self.dumpfile_path,
                        'username': self.user,
                        'password': self.password,
                        'caCertificate': self.caCertificate,
                        'clientCertificate': self.client_certificate,
                        'clientKey': self.client_key
                    }

                }
            }
        }
        return replica_config


class ReplicationConfiguration(object):
    """Configuration for setting up replication to external master."""

    def __init__(self, master_configuration, replica_configuration):
        """Constructor.

        Args:
            master_configuration (MasterConfiguration): Source representation
                of external master.
            replica_configuration (ReplicaConfiguration): Instance configuration
                information for replica.
        Raises:
            KeyError: If either configuration is missing an error is raised.
        """
        if master_configuration is None or replica_configuration is None:
            raise KeyError(
                'Master config is {}. Replica config is {}'.format(
                    'missing' if master_configuration is None else 'present',
                    'missing' if replica_configuration is None else 'present'
                )
            )
        self.master_configuration = master_configuration
        self.replica_configuration = replica_configuration

    def to_json(self):
        """Converts replication configuration into a JSON object.

        Returns:
            JSON: JSON Configuration object for replication.
        """
        return {
            'masterConfiguration': self.master_configuration.to_json(),
            'replicaConfiguration': self.replica_configuration.to_json()
        }


def get_replicate_config_from_file(config_file_path):
    """Creates a Replication Configuration object from a file.

    Args:
        config_file_path (str): File path for .json file.

    Returns:
        ReplicationConfiguration: Configuration object to be used with
            Cloud SQL Admin API.
    """
    with open(config_file_path) as f:
        config = json.load(f)
        return ReplicationConfiguration(
            master_configuration=config['masterConfiguration'],
            replica_configuration=config['replicaConfiguration']
        )


def get_master_config_from_user():
    """Gets master configuration from user.

    Returns:
        MasterConfiguration: configuration for creating source representation.
    """
    master_ip = (
        input(
            ('Enter IP address of external master '
             '(leave blank for 127.0.0.1):')
        )
        or '127.0.0.1'
    )
    master_port = input(
        'Enter replication port (leave blank for {}):'.format(
            cloudsql.DEFAULT_REPLICATION_PORT
        )
    )
    user = input('Enter user to perform MySQLDump:')
    password = getpass.getpass()
    databases = shlex.split(input('Enter database(s) to replicate:').strip())
    database_version = input(
        'Enter database version (leave blank for {}):'.format(
            cloudsql.DEFAULT_2ND_GEN_DB_VERSION
        )
    )
    region = input(
        'Enter region of master (leave blank for {}):'.format(
            cloudsql.DEFAULT_2ND_GEN_REGION
        )
    )
    source_name = (
        input(
            'Enter source name (leave blank for generated name):'
        ) or
        cloudsql.DEFAULT_EXT_MASTER_FORMAT_STRING.format(uuid.uuid4())
    )

    master_config = MasterConfiguration(master_ip=master_ip,
                                        master_port=master_port,
                                        user=user,
                                        password=password,
                                        databases=databases,
                                        database_version=database_version,
                                        region=region,
                                        source_name=source_name)

    return master_config


def get_ssl_config_from_user():
    """Gets SSL configuration from user.

    Returns:
        SSLConfiguration: SSL Configuration with the following fields
            CA Certificate - PEM representation of CA X509 certificate.
            Client Certificate - PEM representation of client
                X509 certificate.
            Client Certificate key - key for client certificate.
    """
    using_ssl = re.match('y',
                         input('Will replication use SSL (Recommended)? y/n:'),
                         re.I)

    ca_certificate = input('Enter PEM representation of the CA\'s '
                           'x509 certificate:') if using_ssl else None
    client_certificate = input('Enter PEM representation of the replica\'s '
                               'x509 certificate:') if ca_certificate else None
    client_key = (
        input(
            'Enter client certificate key:'
        ) if client_certificate else None
    )

    return SSLConfiguration(ca_certificate, client_certificate, client_key)


def get_replica_config_from_user(master_config):
    """Gets replica configuration from user.

    Args:
        master_config (MasterConfiguration): Configuration from
            source representation.
    Raises:
        ValueError: Error if dumpfile specified does not exist.
    Returns:
        ReplicaConfiguration: Configuration for creating replica instance.
    """
    master_instance_name = master_config.source_name
    bucket = None
    dumpfile_path = input('Enter dumpfile path (must start with gs://):')
    if dumpfile_path:
        result = re.search('gs://(.*)/(.*)', dumpfile_path)
        bucket = result.group(1)
        blob = result.group(2)
        if not storage.blob_exists(bucket_name=bucket, blob_name=blob):
            raise ValueError('Dumpfile missing {}'.format(dumpfile_path))
    if not dumpfile_path:
        bucket = input(
            ('Enter the bucket path where the dumpfile should be created.\n'
             'If it does not exist it will be created:')
        )
    if bucket:
        if not dumpfile_path and not storage.bucket_exists(bucket):
            create_bucket = input(
                ('{} does not exist.  '
                 'Would you like to create it? (y/n):'.format(bucket)
                )
            )
            if re.match('y', create_bucket, re.I):
                storage.create_bucket(bucket)
                logging.info('Bucket {} created.'.format(bucket))

    user = input(
        'Enter replication user name (leave blank to use external master user):'
    )
    password = None
    if not user:
        user = master_config.user
        password = master_config.password
    else:
        password = getpass.getpass()

    database_version = master_config.database_version
    tier = input(
        'Enter tier of replica (leave blank for {}):'.format(
            cloudsql.DEFAULT_2ND_GEN_TIER)
    )
    region = master_config.region
    replica_name = (
        input(
            'Enter replica name (leave blank for generated name):'
        ) or
        cloudsql.DEFAULT_REPLICA_FORMAT_STRING.format(
            uuid.uuid4()
        )
    )

    ssl_config = get_ssl_config_from_user()

    replica_config = ReplicaConfiguration(
        bucket=bucket,
        master_instance_name=master_instance_name,
        dumpfile_path=dumpfile_path,
        user=user,
        password=password,
        database_version=database_version,
        tier=tier,
        region=region,
        replica_name=replica_name,
        ca_certificate=ssl_config.ca_certificate,
        client_certificate=ssl_config.client_certificate,
        client_key=ssl_config.client_key
    )

    return replica_config


def get_replicate_config_from_user():
    """Sets up replica interactively.

    Returns:
        ReplicationConfiguration: Replication configuration information to
            provide to Cloud SQL Admin API calls.
    """
    master_config = get_master_config_from_user()
    replica_config = get_replica_config_from_user(master_config)

    return ReplicationConfiguration(master_configuration=master_config,
                                    replica_configuration=replica_config)


def create_source_representation(source_body):
    """Creates source representation.

    Args:
        source_body (JSON): Config object for instances.insert()
            method of sql admin service.
    """
    response = cloudsql.create_source_representation(
        source_body=source_body)
    pprint.pprint(response)
    operation_id = response['name']

    # Wait for the source representation to be created
    while not cloudsql.is_sql_operation_done(operation_id):
        logging.info('Waiting for source representation to be created.')
        time.sleep(5)

    logging.info(
        'Source representation {} has been created.'.format(
            source_body['name']
        )
    )


def create_replica_instance(replica_configuration):
    """Creates a replica instance.

    Args:
        replica_configuration (ReplicationConfiguration):
            Config object for instances.insert() method of sql admin service.
    """
    replica_instance_body = (
        replica_configuration.to_json()
    )['replicaInstanceBody']
    response = cloudsql.create_replica_instance(
        replica_body=replica_instance_body)
    operation_id = response['name']
    outgoing_ip_address_provisioned = False
    service_account_available = False
    replica_name = replica_instance_body['name']
    is_firewall_rule_active = False
    firewall_rule_name = 'replication-{}'.format(uuid.uuid4())
    ip_address = None
    # Wait for replica to be created.
    while not cloudsql.is_sql_operation_done(operation_id):

        # Try to get outgoing ip address
        if not outgoing_ip_address_provisioned or not service_account_available:
            ip_address, service_account = (
                cloudsql.get_ip_and_service_account(replica_name)
            )
            if ip_address:
                outgoing_ip_address_provisioned = True
            else:
                print('Outgoing IP address not available.')
            if service_account:
                if not service_account_available:
                    logging.info(
                        'Service account email is {}'.format(service_account)
                    )
                    service_account_available = True
                    storage.grant_read_access_to_bucket(
                        bucket_name=replica_configuration.bucket,
                        email='serviceAccount:{}'.format(service_account)
                    )
            else:
                print('Service account not available.')
        elif not is_firewall_rule_active:
            # Create firewall rule to allow replica to access master.
            _ = compute.create_firewall_rule(
                name=firewall_rule_name,
                description='replication from {}'.format(replica_name),
                source_ip_range=[ip_address])
            is_firewall_rule_active = True
        else:
            # Verify firewall rule is still active
            is_firewall_rule_active = compute.is_firewall_rule_active(
                firewall_rule_name
            )
        logging.info('Waiting for replica instance to be created.')
        time.sleep(5)

    logging.info('Replica instance {} has been created.'.format(
        replica_name)
                )


def replicate(replication_configuration):
    """Replicate master to replica

    Args:
        replication_configuration (ReplicationConfiguration):
            configuration for replication operation
    """

    # Create our source representation
    source_body = (
        replication_configuration.master_configuration.to_json()
    )['sourceRepresentationBody']
    create_source_representation(source_body=source_body)

    # Check if we have a dumpfile for the replica
    if not replication_configuration.replica_configuration.dumpfile_path:
        # Perform a MySQLDump
        if replication_configuration.replica_configuration.bucket:
            bucket_name = replication_configuration.replica_configuration.bucket
            external_master_db = mysql_util.MySQL(
                host=replication_configuration.master_configuration.master_ip,
                user=replication_configuration.master_configuration.user,
                password=replication_configuration.master_configuration.password
            )
            bucket_url = (
                'gs://{}/replication-{}.sql.gz'.format(
                    bucket_name, uuid.uuid4()
                )
            )
            external_master_db.dump_sql(
                databases=(
                    replication_configuration.master_configuration.databases
                ),
                bucket_url=bucket_url

            )
            replication_configuration.replica_configuration.dumpfile_path = (
                bucket_url
            )
    # Create our replica instance.
    create_replica_instance(replication_configuration.replica_configuration)


def replicate_dispatcher(interactive=False, config=None):
    """Replicate dispatcher.

    Calls the appropriate function based on flags to collect replication
        configuration, then calls replicate.

    Args:
        interactive (bool): Whether to run replicate in interactive mode.
        config (str): Path to configuration file.
    """
    if interactive:
        config = get_replicate_config_from_user()
    elif config is not None:
        config = get_replicate_config_from_file(config)

    replicate(config)


def allow_host(host_ip):
    """Allows client host to connect to default VPC.

    Args:
        host_ip (str): IPV4 address of client host.
    """

    compute.create_firewall_rule(
        name='client-connection-{}'.format(uuid.uuid4()),
        description='Allow {} to connect to VPC'.format(host_ip),
        source_ip_range=[host_ip])


def configure(argv):
    """Configures Cloud SQL Replicator CLI behavior via commands and flags.

    Parses sub-command for replicator and any associated flags.

    Args:
      argv (List): Command line arguments.

    Returns:
      Namespace: Parsed arguments for Replicator CLI.
    """
    parser = argparse_flags.ArgumentParser(
        description='Cloud SQL Replicator CLI.'
    )
    subparsers = parser.add_subparsers(help='The command to execute.')
    replicate_parser = subparsers.add_parser(
        'replicate',
        help='Replicate an external master.'
    )
    replicate_parser.add_argument('-i', '--interactive',
                                  action='store_true',
                                  help='Run replicate in interactive mode.')
    replicate_parser.add_argument('-c',
                                  '--config',
                                  help='Path to configuration file.')
    replicate_parser.set_defaults(command=replicate_dispatcher)

    firewall_parser = subparsers.add_parser(
        'allow-host',
        help='Add firewall rule to allow host ingress access to default VPC.'
    )
    firewall_parser.add_argument('-i', '--host_ip', help='IP address of host.')
    firewall_parser.set_defaults(command=allow_host)

    # todo(chrisdrake): Add sub parser for status command
    args = parser.parse_args(argv[1:])
    return args


def main(argv):
    """Main entry point for Cloud SQL Replicator CLI.

    Args:
        argv (Namespace): parsed commandline flags.
    """
    #Suppress warnings from logger
    logger = std_logging.getLogger()
    logger.setLevel(logging.ERROR)

    logging.info('Running under Python {0[0]}.{0[1]}.{0[2]}'
                 .format(sys.version_info))
    logging.info('Running version {} of replicator'
                 .format(replicator.__version__))
    # command is set by the sub-parser of the sub-command.
    if argv.command is not None:
        command_args = (
            {
                key: value for (key, value) in iteritems(vars(argv))
                if key != 'command'
            }
        )
        argv.command(**command_args)


def run():
    """Entry point for console app."""
    app.run(main, flags_parser=configure)


if __name__ == '__main__':
    run()
