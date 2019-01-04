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
import sys
import uuid

from absl import app
from absl.flags import argparse_flags
from absl import logging

from google.cloud.pontem.sql import replicator
from google.cloud.pontem.sql.replicator.util import cloudsql


class MasterConfiguration(object):
    """Configuration for External Master Representation."""
    # pylint: disable=too-many-instance-attributes
    def __init__(self,
                 user,
                 password,
                 master_ip,
                 master_port=cloudsql.DEFAULT_REPLICATION_PORT,
                 database_version=cloudsql.DEFAULT_2ND_GEN_DB_VERSION,
                 region=cloudsql.DEFAULT_2ND_GEN_REGION,
                 source_name=None,
                 ca_certificate=None):
        """Constructor.

        Args:
            user (str): User that will be used to perform MySQLDump.
            password (str): Password of user.
            master_ip (str): IP address of external master.
            master_port (str): Replication port.
            database_version (str): Database version of external master.
            region (str): Region where external master resides.
            source_name (str): Name that will be given to the source
                representation instance.
            ca_certificate (str): x509 PEM-encoded certificate of the CA that
                signed the source database server's certificate.
        Raises:
             ValueError: If one or more configuration values is not allowed
                (e.g. 5.6 and 5.7 are the only allowed database versions).
        """
        if database_version not in cloudsql.SUPPORTED_VERSIONS:
            raise ValueError(
                'Database version {} not supported'.format(
                    database_version
                )
            )

        self._master_ip = master_ip
        self._master_port = master_port
        self._user = user
        self._ca_certificate = ca_certificate
        self._password = password
        self._database_version = database_version
        self._region = region
        self._source_name = source_name


class ReplicaConfiguration(object):
    """Configuration for External Master Representation."""
    # pylint: disable=too-many-instance-attributes, too-many-arguments
    def __init__(self,
                 user,
                 password,
                 master_instance_name,
                 dumpfile_path,
                 database_version,
                 tier,
                 region,
                 replica_name,
                 client_certificate,
                 client_key):
        """Constructor.

        Args:
            user (str): User that will be used to perform MySQLDump.
            password (str): Password of user.
            master_instance_name (str): Name of external master.
            dumpfile_path (str): Path to dumpfile (must start with gs://).
            database_version (str): Database version of replica.
            tier (str): Tier of replica
            region (str): Region where external master resides.
            replica_name (str): Name that will be given to the
                replica instance.
            client_certificate (str): Certificate that will be used to
                authenticate during replication.
            client_key (str): PEM-encoded private key associated with
                the client_certificate

        Raises:
             ValueError: If one or more configuration values is not allowed
                (e.g. 5.6 and 5.7 are the only allowed database versions).
        """
        if database_version not in cloudsql.SUPPORTED_VERSIONS:
            raise ValueError(
                'Database version {} not supported'.format(
                    database_version
                )
            )

        self._master_instance_name = master_instance_name
        self._dumpfile_path = dumpfile_path
        self._user = user
        self._password = password
        self._client_certificate = client_certificate
        self._client_key = client_key
        self._database_version = database_version
        self._tier = tier
        self._region = region
        self._replica_name = replica_name

    def __dict__(self):
        """Converts ReplicaConfiguration into a dict.

        Converts object instance into dictionary that can be used with
            Cloud SQL Admin API.

        Returns:
            dict: JSON object to be used as  body argument for
                instance.insert method of Cloud SQL Admin API.
        """
        replica_body = {
            'name': self._replica_name,
            'settings': {
                'tier': self._tier,

            },
            'databaseVersion': self._database_version,
            'masterInstanceName': self._master_instance_name,
            'region': self._region,
            'replicaConfiguration': {
                'mysqlReplicaConfiguration': {
                    'dumpFilePath': self._dumpfile_path,
                    'username': self._user,
                    'password': self._password,
                    'clientCertificate': self._client_certificate,
                    'clientKey': self._client_key
                }

            }
        }
        return replica_body


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
        self._master_configuration = master_configuration
        self._replica_configuration = replica_configuration


def replicate_interactive():
    """Sets up replica interactively.

    Returns:
        ReplicationConfiguration: Replication configuration information to
            provide to Cloud SQL Admin API calls.
    """
    # pylint: disable=too-many-locals
    # Collect master representation configuration
    master_ip = input('Enter IP address of external master:')
    master_port = input(
        'Enter replication port (leave blank for {}):'.format(
            cloudsql.DEFAULT_REPLICATION_PORT
        )
    )
    user = input('Enter user to perform MySQLDump')
    password = getpass.getpass()
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
    ca_certificate = input('Enter CA of SSL certificate for master:')
    master_config = MasterConfiguration(master_ip=master_ip,
                                        master_port=master_port,
                                        user=user,
                                        password=password,
                                        database_version=database_version,
                                        region=region,
                                        source_name=source_name,
                                        ca_certificate=ca_certificate)

    # Collect replica configuration
    master_instance_name = input('Enter name of source representation')
    dumpfile_path = input('Enter dumpfile path (must start with gs://):')
    user = input('Enter replication user name:')
    password = getpass.getpass()
    database_version = input(
        'Enter database version (leave blank for {}):'.format(
            cloudsql.DEFAULT_2ND_GEN_DB_VERSION
        )
    )
    tier = input(
        'Enter tier of replica (leave blank for {}):'.format(
            cloudsql.DEFAULT_2ND_GEN_TIER)
    )
    region = input(
        'Enter region of replica (leave blank for {}):'.format(
            cloudsql.DEFAULT_2ND_GEN_REGION
        )
    )
    replica_name = (
        input(
            'Enter replica name (leave blank for generated name):'
        ) or
        cloudsql.DEFAULT_REPLICA_FORMAT_STRING.format(
            uuid.uuid4()
        )
    )
    client_certificate = input('Enter PEM representation of the replica\'s '
                               'x509 certificate:')
    client_key = input('Enter client certificate key:')
    replica_config = ReplicaConfiguration(
        master_instance_name=master_instance_name,
        dumpfile_path=dumpfile_path,
        user=user,
        password=password,
        database_version=database_version,
        tier=tier,
        region=region,
        replica_name=replica_name,
        client_certificate=client_certificate,
        client_key=client_key
    )
    return ReplicationConfiguration(master_configuration=master_config,
                                    replica_configuration=replica_config)


def replicate_from_config(config):
    """Sets up replica from a config file

    Args:
        config (str): Path to configuration file.

    Returns:
        ReplicationConfiguration: Replication configuration information to
            provide to Cloud SQL Admin API calls.
    Raises:
        ValueError: If the file contains an incorrect value.
        KeyError: If the file lacks a required field.
    """
    # todo(chrisdrake): parse config file
    del config
    return ReplicationConfiguration(master_configuration=None,
                                    replica_configuration=None)


def replicate(replication_configuration):
    """Replicate master to replica

    Args:
        replication_configuration (ReplicationConfiguration):
            configuration for replication operation
    """
    # todo(chrisdrake): implement function
    del replication_configuration


def replicate_dispatcher(interactive=False, config=None):
    """Replicate dispatcher.

    Calls the appropriate function based on flags to collect replication
        configuration, then calls replicate.

    Args:
        interactive (bool): Whether to run replicate in interactive mode.
        config (str): Path to configuration file.
    """
    if interactive:
        config = replicate_interactive()
    elif config is not None:
        config = replicate_from_config(config)

    replicate(config)


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

    # todo(chrisdrake): Add sub parser for status command
    args = parser.parse_args(argv[1:])
    return args


def main(argv):
    """Main entry point for Cloud SQL Replicator CLI.

    Args:
        argv (Namespace): parsed commandline flags.
    """
    logging.info('Running under Python {0[0]}.{0[1]}.{0[2]}'
                 .format(sys.version_info))
    logging.info('Running version {} of replicator'
                 .format(replicator.__version__))
    # command is set by the sub-parser of the sub-command.
    if argv.command is not None:
        command_args = (
            {key: value for (key, value) in vars(argv) if key != 'command'}
        )
        argv.command(**command_args)


if __name__ == '__main__':

    app.run(main, flags_parser=configure)
