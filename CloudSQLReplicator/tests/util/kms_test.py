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

"""Tests GCP API KMS utility functions."""


import unittest
import mock

from google.api_core.gapic_v1 import client_info
import google.auth
from google.cloud import kms_v1
from google.cloud.kms_v1 import enums
from google.oauth2 import credentials

from google.cloud.pontem.sql.replicator.util import gcp_api_util
from google.cloud.pontem.sql.replicator.util import kms

USER_AGENT = 'pontem,cloudsql-replication/0.0.1 (gzip)'
TEST_PROJECT_ID = 'test-project'
DEFAULT_LOCATION = 'global'
TEST_KEY_RING_ID = 'test_key_ring_id'
TEST_KEY = 'test_key'
TEST_KEYRING_PARENT_PATH = 'projects/{}/locations/{}/keyRings'.format(
    TEST_PROJECT_ID, DEFAULT_LOCATION)
TEST_KEY_RING_PATH = '{}/{}'.format(
    TEST_KEYRING_PARENT_PATH, TEST_KEY_RING_ID)
TEST_KEY_PATH = '{}/{}'.format(TEST_KEY_RING_PATH, TEST_KEY)


@mock.patch.object(
    google.auth, 'default',
    return_value=(mock.Mock(spec_set=credentials.Credentials),
                  TEST_PROJECT_ID)
)
class TestKMSMethods(unittest.TestCase):
    """Test Storage methods."""

    @mock.patch.object(
        kms_v1, 'KeyManagementServiceClient'
    )
    @mock.patch.object(gcp_api_util, 'get_user_agent',
                       return_value=USER_AGENT)
    @mock.patch.object(client_info, 'ClientInfo')
    def test_build_kms_client(self, mock_client_info_init, mock_get_user_agent, mock_kms_client, mock_auth_default):
        """Test that code creates KMS client correctly."""

        _ = kms.build_kms_client()
        
        # Verify that method calls default during build_kms_client
        mock_auth_default.assert_called_once_with()
        # Verify that get_user_agent is called
        mock_get_user_agent.assert_called_once_with()
        # Verify that client info is populated with custom user agent
        mock_client_info_init.assert_called_with(client_library_version=USER_AGENT)
        # Verify constructor is called for KMS client
        mock_kms_client.assert_called()

    @mock.patch.object(kms, 'build_kms_client',
                       return_value=mock.Mock(
                           spec_set=kms_v1.KeyManagementServiceClient)
                       )
    def test_create_keyring(self, mock_build_client, mock_auth_default):
        """Test that create_keyring calls client correctly."""
        mock_location_path = mock_build_client.return_value.location_path
        mock_location_path.return_value = TEST_KEYRING_PARENT_PATH
        mock_key_ring_path = mock_build_client.return_value.key_ring_path
        mock_key_ring_path.return_value = TEST_KEY_RING_PATH
        mock_create_key_ring = mock_build_client.return_value.create_key_ring

        _ = kms.create_key_ring(TEST_KEY_RING_ID)

        mock_auth_default.assert_called_once_with()
        mock_location_path.assert_called_with(TEST_PROJECT_ID, DEFAULT_LOCATION)
        mock_key_ring_path.assert_called_with(TEST_PROJECT_ID, DEFAULT_LOCATION, TEST_KEY_RING_ID)
        mock_create_key_ring.assert_called_with(TEST_KEYRING_PARENT_PATH, TEST_KEY_RING_ID,
                                                {'name': TEST_KEY_RING_PATH})

    @mock.patch.object(kms, 'build_kms_client',
                       return_value=mock.Mock(
                           spec_set=kms_v1.KeyManagementServiceClient)
                       )
    def test_create_key(self, mock_build_client, mock_auth_default):
        """Test that create_key calls client correctly."""
        key_id = TEST_KEY
        mock_key_ring_path = mock_build_client.return_value.key_ring_path
        mock_key_ring_path.return_value = TEST_KEY_RING_PATH
        mock_create_key = mock_build_client.return_value.create_crypto_key

        _ = kms.create_key(key_id, TEST_KEY_RING_ID)

        mock_auth_default.assert_called_once_with()
        mock_key_ring_path.assert_called_with(
            TEST_PROJECT_ID, DEFAULT_LOCATION, TEST_KEY_RING_ID)
        mock_create_key.assert_called_with(TEST_KEY_RING_PATH, key_id,
                                           {'purpose': enums.CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT})

    @mock.patch.object(kms, 'build_kms_client',
                       return_value=mock.Mock(
                           spec_set=kms_v1.KeyManagementServiceClient)
                       )
    def test_encrypt(self, mock_build_client, mock_auth_default):
        """Tests that encrypt prepares and calls client correctly."""
        mock_encrypt = mock_build_client.return_value.encrypt
        mock_crypto_key_ring_path = mock_build_client.return_value.crypto_key_path_path
        mock_crypto_key_ring_path.return_value = TEST_KEY_PATH
        plain_text = 'password'

        _ = kms.encrypt(plain_text, TEST_KEY, TEST_KEY_RING_ID)

        mock_auth_default.assert_called_once_with()
        mock_crypto_key_ring_path.assert_called_with(
            TEST_PROJECT_ID, DEFAULT_LOCATION, TEST_KEY_RING_ID, TEST_KEY)
        mock_encrypt.assert_called_with(TEST_KEY_PATH, plain_text)

    @mock.patch.object(kms, 'build_kms_client',
                       return_value=mock.Mock(
                           spec_set=kms_v1.KeyManagementServiceClient)
                       )
    def test_decrypt(self, mock_build_client, mock_auth_default):
        """Tests that decrypt prepares and calls client correctly."""
        mock_decrypt = mock_build_client.return_value.decrypt
        mock_crypto_key_ring_path = mock_build_client.return_value.crypto_key_path_path
        mock_crypto_key_ring_path.return_value = TEST_KEY_PATH
        cipher_text = '==cipher_text'

        _ = kms.decrypt(cipher_text, TEST_KEY, TEST_KEY_RING_ID)

        mock_auth_default.assert_called_once_with()
        mock_crypto_key_ring_path.assert_called_with(
            TEST_PROJECT_ID, DEFAULT_LOCATION, TEST_KEY_RING_ID, TEST_KEY)
        mock_decrypt.assert_called_with(TEST_KEY_PATH, cipher_text)

    @mock.patch.object(kms, 'build_kms_client',
                       return_value=mock.Mock(
                           spec_set=kms_v1.KeyManagementServiceClient)
                       )
    def test_add_member_to_crypto_key_policy(self,mock_build_client, mock_auth_default):
        """Tests that add_member_to_crypto_key_policy prepares and calls client correctly."""
        mock_crypto_key_ring_path = mock_build_client.return_value.crypto_key_path_path
        mock_crypto_key_ring_path.return_value = TEST_KEY_PATH
        mock_get_iam_policy = mock_build_client.return_value.get_iam_policy
        mock_set_iam_policy = mock_build_client.return_value.set_iam_policy
        mock_bindings_add = mock_get_iam_policy.return_value.bindings.add
        test_member = 'test_member@google.com'
        test_role = 'test_role'

        kms.add_member_to_crypto_key_policy(
            test_member, test_role, TEST_KEY, TEST_KEY_RING_ID)

        mock_auth_default.assert_called_once_with()
        mock_crypto_key_ring_path.assert_called_with(
            TEST_PROJECT_ID, DEFAULT_LOCATION, TEST_KEY_RING_ID, TEST_KEY)
        mock_bindings_add.assert_called_with(role=test_role, members=[test_member])
        mock_set_iam_policy.assert_called_once_with(TEST_KEY_PATH, mock_get_iam_policy.return_value)


if __name__ == '__main__':
    unittest.main()

