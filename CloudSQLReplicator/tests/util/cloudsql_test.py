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

"""Tests GCP API Cloud SQL utility functions."""

import unittest
import mock
from mock import MagicMock
import uuid

import google.auth
from google.oauth2 import credentials

from google.cloud.pontem.sql.replicator.util import cloudsql
from google.cloud.pontem.sql.replicator.util import gcp_api_util

TEST_PROJECT_NAME = 'test-project'
TEST_UUID = uuid.UUID('{12345678-1234-5678-1234-567812345678}')
TEST_HOST = '127.0.0.1'
TEST_PORT = '1234'
TEST_MASTER = 'test-master'
TEST_DUMPFILE_PATH = 'gs://test_db/employees.sql.gz'
TEST_REPL_USER = 'repl'
TEST_PASSWORD = 'password'

@mock.patch.object(
    google.auth, 'default',
    return_value=(mock.Mock(spec_set=credentials.Credentials),
                  TEST_PROJECT_NAME)
)
@mock.patch.object(
    gcp_api_util, 'build_authorized_service'
)
class TestSQLAdminMethods(unittest.TestCase):
    """Test Cloud SQL Admin methods."""
    def test_build_sql_admin_service(self, mock_build, mock_auth_default):
        """Test build sql admin service."""
        _ = cloudsql.build_sql_admin_service()
        mock_build.assert_called_with(
            cloudsql.SQL_ADMIN_SERVICE,
            cloudsql.SQL_ADMIN_SERVICE_VERSION,
            mock_auth_default.return_value[0]
        )

    @mock.patch.object(
        uuid, 'uuid4',
        return_value=TEST_UUID
    )
    def test_create_cloudsql_instance_defaults(self,
                                               mock_uuid,
                                               mock_build,
                                               mock_auth_default):
        """Test create_cloudsql_instance util function with defaults."""
        del mock_uuid, mock_auth_default  # unused in body of test
        mock_build.return_value = MagicMock()
        mock_insert_instance = (
            mock_build.return_value.instances.return_value.insert
        )
        cloudsql.create_cloudsql_instance()
        # Verify that instances.insert is called with arguments and defaults
        mock_insert_instance.assert_called_with(
            project=TEST_PROJECT_NAME,
            body={
                'name': 'cloudsql-db-{}'.format(TEST_UUID),
                'settings': {
                    'tier': cloudsql.DEFAULT_2ND_GEN_TIER,
                }
            }
        )

    def test_create_cloudsql_instance(self,
                                      mock_build,
                                      mock_auth_default):
        """Test create_cloudsql_instance util function with instance body."""
        del mock_auth_default  # unused in body of test
        mock_build.return_value = MagicMock()
        mock_insert_instance = (
            mock_build.return_value.instances.return_value.insert
        )
        test_db_name = 'test-db'
        test_db_tier = cloudsql.DEFAULT_1ST_GEN_TIER

        test_database_instance_body = {
            'name': test_db_name,
            'settings': {
                'tier': test_db_tier
            }
        }
        cloudsql.create_cloudsql_instance(test_database_instance_body)
        # Verify that instances.insert is called with specified arguments
        mock_insert_instance.assert_called_with(
            project=TEST_PROJECT_NAME,
            body={
                'name': test_db_name,
                'settings': {
                    'tier': test_db_tier,
                }
            }
        )

    @mock.patch.object(
        uuid, 'uuid4',
        return_value=TEST_UUID
    )
    def test_create_source_representation_defaults(self,
                                                   mock_uuid,
                                                   mock_build,
                                                   mock_auth_default):
        """Test create_source_representation util function with defaults."""
        del mock_uuid, mock_auth_default  # unused in body of test
        mock_build.return_value = MagicMock()
        mock_insert_instance = (
            mock_build.return_value.instances.return_value.insert
        )
        cloudsql.create_source_representation(TEST_HOST, TEST_PORT)
        # Verify that instances.insert is called with arguments and defaults
        mock_insert_instance.assert_called_with(
            project=TEST_PROJECT_NAME,
            body={
                'name': 'external-mysql-representation-{}'.format(TEST_UUID),
                'databaseVersion': cloudsql.DEFAULT_2ND_GEN_DB_VERSION,
                'region': cloudsql.DEFAULT_2ND_GEN_REGION,
                'onPremisesConfiguration': {
                    'kind': 'sql#onPremisesConfiguration',
                    'hostPort': '{}:{}'.format(TEST_HOST, TEST_PORT)
                }
            }
        )

    def test_create_source_representation(self,
                                          mock_build,
                                          mock_auth_default):
        """Test create_source_representation util function with defaults."""
        del mock_auth_default  # unused in body of test
        mock_build.return_value = MagicMock()
        mock_insert_instance = (
            mock_build.return_value.instances.return_value.insert
        )
        test_db_name = 'test-external-mysql-representation'
        test_host_port = '192.161.0.1/4321'
        test_body = {
            'name': test_db_name,
            'databaseVersion': cloudsql.DEFAULT_2ND_GEN_DB_VERSION,
            'region': cloudsql.DEFAULT_2ND_GEN_REGION,
            'onPremisesConfiguration': {
                'kind': 'sql#onPremisesConfiguration',
                'hostPort': test_host_port
            }
        }
        cloudsql.create_source_representation(None,
                                              None,
                                              source_body=test_body)
        # Verify that instances.insert is called with specified arguments
        mock_insert_instance.assert_called_with(
            project=TEST_PROJECT_NAME,
            body=test_body
        )

    @mock.patch.object(
        uuid, 'uuid4',
        return_value=TEST_UUID
    )
    def test_create_replica_instance_defaults(self,
                                              mock_uuid,
                                              mock_build,
                                              mock_auth_default):
        """Test create_replica_instance util function with defaults."""
        del mock_uuid, mock_auth_default  # unused in body of test
        mock_build.return_value = MagicMock()
        mock_insert_instance = (
            mock_build.return_value.instances.return_value.insert
        )

        cloudsql.create_replica_instance(TEST_MASTER,
                                         TEST_DUMPFILE_PATH,
                                         TEST_REPL_USER,
                                         TEST_PASSWORD)

        # Verify that instances.insert is called with arguments and defaults
        mock_insert_instance.assert_called_with(
            project=TEST_PROJECT_NAME,
            body={
                'name': 'cloudsql-replica-{}'.format(TEST_UUID),
                'settings': {
                    'tier': cloudsql.DEFAULT_2ND_GEN_TIER,

                },
                'databaseVersion': cloudsql.DEFAULT_2ND_GEN_DB_VERSION,
                'masterInstanceName': TEST_MASTER,
                'region': cloudsql.DEFAULT_2ND_GEN_REGION,
                'replicaConfiguration': {
                    'mysqlReplicaConfiguration': {
                        'dumpFilePath': TEST_DUMPFILE_PATH,
                        'username': TEST_REPL_USER,
                        'password': TEST_PASSWORD,
                    }

                }
            }
        )
        
    def test_create_replica_instance(self,
                                     mock_build,
                                     mock_auth_default):
        """Test create_replica_instance util function with arguments."""
        del mock_auth_default
        mock_build.return_value = MagicMock()
        mock_insert_instance = (
            mock_build.return_value.instances.return_value.insert
        )
        test_replica_name = 'test_replica'
        test_tier = cloudsql.DEFAULT_1ST_GEN_TIER
        test_db_version = cloudsql.DEFAULT_1ST_GEN_DB_VERSION
        test_region = cloudsql.DEFAULT_1ST_GEN_REGION
        test_master = 'test_master'
        test_dumpfile = 'gs://test_dumpfile/test.sql.gz'
        test_repl_user = 'test_repl_user'
        test_password = 'test_password'

        test_replica_body = {
            'name': test_replica_name,
            'settings': {
                'tier': test_tier,

            },
            'databaseVersion': test_db_version,
            'masterInstanceName': test_master,
            'region': test_region,
            'replicaConfiguration': {
                'mysqlReplicaConfiguration': {
                    'dumpFilePath': test_dumpfile,
                    'username': test_repl_user,
                    'password': test_password,
                }

            }
        }

        cloudsql.create_replica_instance(
            None, None, None, None,
            replica_body=test_replica_body
        )

        # Verify that instances.insert is called with arguments and defaults
        mock_insert_instance.assert_called_with(
            project=TEST_PROJECT_NAME,
            body={
                'name': test_replica_name,
                'settings': {
                    'tier': test_tier,

                },
                'databaseVersion': test_db_version,
                'masterInstanceName': test_master,
                'region': test_region,
                'replicaConfiguration': {
                    'mysqlReplicaConfiguration': {
                        'dumpFilePath': test_dumpfile,
                        'username': test_repl_user,
                        'password': test_password,
                    }

                }
            }
        )

    def test_import_sql_database(self,
                                 mock_build,
                                 mock_auth_default):
        """Test create_replica_instance util function with arguments."""
        del mock_auth_default
        mock_build.return_value = MagicMock()
        mock_insert_instance = (
            mock_build.return_value.instances.return_value.import_
        )

        test_import_file_uri = 'gs://test/db.sql'

        cloudsql.import_sql_database(TEST_MASTER, test_import_file_uri)

        mock_insert_instance.assert_called_with(
            project=TEST_PROJECT_NAME,
            instance=TEST_MASTER,
            body={
                'importContext': {
                    'kind': 'sql#importContext',
                    'fileType': 'SQL',
                    'uri': test_import_file_uri,
                }
            }
        )

    def test_is_sql_operation_done(self,
                                   mock_build,
                                   mock_auth_default):
        """Tests is_sql_operation_done."""
        del mock_auth_default
        test_operation = '12345678'
        mock_build.return_value = MagicMock()
        mock_get_operation = (
            mock_build.return_value.operations.return_value.get
        )

        cloudsql.is_sql_operation_done(test_operation)

        mock_get_operation.assert_called_with(
            project=TEST_PROJECT_NAME,
            operation=test_operation
        )

if __name__ == '__main__':
    unittest.main()