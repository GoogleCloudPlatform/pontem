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

# Used for Python 2/3 compatibility
from future.utils import iteritems # pylint: disable=unused-import

from google.cloud.pontem.sql import replicator
from google.cloud.pontem.sql.replicator.util import cloudsql


class MissingRequiredParameterError(ValueError):
    """Raised when a required parameter is missing."""


class MasterConfiguration(object):
    """Configuration for External Master Representation."""
    master_config_defaults = frozenset({
        'user': None,
        'password': None,
        'master_ip': None,
        'master_port': cloudsql.DEFAULT_REPLICATION_PORT,
        'database_version': cloudsql.DEFAULT_2ND_GEN_DB_VERSION,
        'region': cloudsql.DEFAULT_2ND_GEN_REGION,
        'source_name': None,
        'ca_certificate': None
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
        if 'user' or 'password' or 'master_ip' not in kwargs:
            raise MissingRequiredParameterError(
                'Required property missing for master configuration.'
            )

        # Set recognized properties
        for (key, value) in (
                dict(MasterConfiguration.master_config_defaults).iteritems()
        ):
            if key not in MasterConfiguration.master_config_defaults:
                raise TypeError(
                    ('Master Configuration does not have a '
                     '{} property'.format(key)
                    )
                )
            setattr(self, '_{}'.format(key), kwargs.get(key, value))

        if self._database_version not in cloudsql.SUPPORTED_VERSIONS:
            raise ValueError(
                'Database version {} not supported'.format(
                    self.database_version
                )
            )

        if self._source_name is None:
            self._source_name = (
                cloudsql.DEFAULT_EXT_MASTER_FORMAT_STRING.format(uuid.uuid4())
            )


class ReplicaConfiguration(object):
    """Configuration for External Master Representation."""
    replica_config_defaults = frozenset({
        'master_instance_name': None,
        'dumpfile_path': None,
        'user': None,
        'password': None,
        'database_version': cloudsql.DEFAULT_2ND_GEN_DB_VERSION,
        'tier': cloudsql.DEFAULT_2ND_GEN_TIER,
        'region': cloudsql.DEFAULT_2ND_GEN_REGION,
        'replica_name': None,
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
        for (key, value) in (
                dict(ReplicaConfiguration.replica_config_defaults).iteritems()
        ):
            if (
                    'master_instance_name' or
                    'dumpfile_path' or
                    'user' or
                    'password'
            ) not in kwargs:
                setattr(self, '_{}'.format(key), kwargs.get(key, value))

        if self._database_version not in cloudsql.SUPPORTED_VERSIONS:
            raise ValueError(
                'Database version {} not supported'.format(
                    self._database_version
                )
            )

        if self._replica_name is None:
            self._replica_name = (
                cloudsql.DEFAULT_REPLICA_FORMAT_STRING.format(uuid.uuid4())
            )

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


def get_master_config_from_user():
    """Gets master configuration from user.

    Returns:
        MasterConfiguration: configuration for creating source representation.
    """
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

    return master_config


def get_replica_config_from_user():
    """Gets master configuration from user.

    Returns:
        ReplicaConfiguration: configuration for creating replica instance.
    """
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

    return replica_config


def replicate_interactive():
    """Sets up replica interactively.

    Returns:
        ReplicationConfiguration: Replication configuration information to
            provide to Cloud SQL Admin API calls.
    """
    master_config = get_master_config_from_user()
    replica_config = get_replica_config_from_user()

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
