# Copyright 2019 The Pontem Authors. All rights reserved.
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

"""Cloud SQL Replicator configuration."""

import uuid

# Used for Python 2/3 compatibility
from future.utils import iteritems
import yaml

from google.cloud.pontem.sql.replicator.util import cloudsql
from google.cloud.pontem.sql.replicator.util import kms


class MissingRequiredParameterError(ValueError):
    """Raised when a required parameter is missing."""


class EncryptionConfiguration(object):
    """Encryption Configuration."""
    def __init__(self,
                 key_id,
                 key_ring_id,
                 location='global',
                 project=None):
        """Constructor.

        Args:
            key_id (str): Key id for encryption key.
            key_ring_id (str): Keyring where encryption key exists.
            location (str): Location where keyring exists.
            project (str): Project id where key/keyring exists.
        """
        self.key_id = key_id
        self.key_ring_id = key_ring_id
        self.location = location
        self.project = project


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
    master_config_defaults = frozenset(
        {
            'user': None,
            'password': None,
            'encrypted_password': None,
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
    replica_config_defaults = frozenset(
        {
            'master_instance_name': None,
            'bucket': None,
            'dumpfile_path': None,
            'user': None,
            'password': None,
            'encrypted_password': None,
            'database_version': cloudsql.DEFAULT_2ND_GEN_DB_VERSION,
            'tier': cloudsql.DEFAULT_2ND_GEN_TIER,
            'region': cloudsql.DEFAULT_2ND_GEN_REGION,
            'replica_name': None,
            'caCertificate': None,
            'client_certificate': None,
            'client_key': None,
            'encrypted_client_key': None

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
        if not {'master_instance_name',
                'dumpfile_path',
                'user',
                'password'}.issubset(kwargs):
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

    def __init__(self,
                 master_configuration,
                 replica_configuration,
                 run_uuid=None):
        """Constructor.

        Args:
            master_configuration (MasterConfiguration): Source representation
                of external master.
            replica_configuration (ReplicaConfiguration): Instance configuration
                information for replica.
            run_uuid (str): Unique id for this replication run.
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
        self._run_uuid = run_uuid or str(uuid.uuid4())
        self.key = None
        self.key_ring = None
        self.project = None

    @property
    def run_uuid(self):
        """Property that uniquely identifies this replication run.

        Returns:
            str: Universally unique identifier of replication configuration.
        """
        return self._run_uuid

    @run_uuid.setter
    def run_uuid(self, value):
        """Setter for run uuid property.

        Args:
            value (str): New run uuid to reassign master and replica names.
        """
        self._run_uuid = value
        self.master_configuration.source_name = (
            cloudsql.DEFAULT_EXT_MASTER_FORMAT_STRING.format(self._run_uuid)
        )
        self.replica_configuration.replica_name = (
            cloudsql.DEFAULT_REPLICA_FORMAT_STRING.format(self._run_uuid)
        )

    def to_json(self):
        """Converts replication configuration into a JSON object.

        Returns:
            JSON: JSON Configuration object for replication.
        """
        return {
            'masterConfiguration': self.master_configuration.to_json(),
            'replicaConfiguration': self.replica_configuration.to_json()
        }


def export_config_to_file(config, key_id=None, key_ring_id=None, project=None,
                          file_path=None):
    """Serializes config to config to a .yaml file.

    Args:
        config (ReplicationConfiguration): Config object.
        key_id (str): Key used for encryption.
        key_ring_id (str): Key ring for encryption key.
        project (str): Project id where key exists.
        file_path (str): Where to serialize the config.
    """
    config_file_path = file_path or '{}.yaml'.format(config.run_uuid)
    # If we encryption parameters, use those to encrypt the password
    # and private key
    master_config_password = config.master_configuration.password
    replica_config_password = config.replica_configuration.password
    replica_config_client_key = config.replica_configuration.client_key

    if key_id and key_ring_id:
        config.key = key_id
        config.key_ring = key_ring_id
        config.project = project

        # master configuration
        if master_config_password:
            config.master_configuration.encrypted_password = (
                kms.encrypt(master_config_password, key_id, key_ring_id,
                            project=project)
            )
            config.master_configuration.password = None

        # replica configuration
        if replica_config_password:
            config.replica_configuration.encrypted_password = (
                kms.encrypt(replica_config_password, key_id, key_ring_id)
            )
            config.replica_configuration.password = None
        if replica_config_client_key:
            config.replica_configuration.encrypted_client_key = (
                kms.encrypt(replica_config_client_key, key_id, key_ring_id)
            )
            config.replica_configuration.client_key = None

    with open(config_file_path, 'w+') as f:
        yaml.dump(config, f)

    # Restore passwords after we have saved the config to file
    config.master_configuration.password = master_config_password
    config.replica_configuration.password = replica_config_password
    config.replica_configuration.client_key = replica_config_client_key
