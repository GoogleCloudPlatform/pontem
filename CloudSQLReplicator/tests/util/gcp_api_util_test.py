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

"""Tests GCP API utility functions."""

import unittest
import mock
from mock import MagicMock

import google.auth
from google.cloud import storage
from google.oauth2 import credentials

from google.cloud.pontem.sql.replicator.util import storage as replicator_storage

GCP_STORAGE_SCOPE = frozenset(
    ['https://www.googleapis.com/auth/devstorage.read_write']
)


class TestSQLAdminMethods(unittest.TestCase):
  """Test SQL Admin methods."""

@mock.patch.object(
    storage, 'Client'
)
@mock.patch.object(
    google.auth, 'default',
    return_value=(mock.Mock(spec_set=credentials.Credentials),
                  'test-project')
)
class TestStorageMethods(unittest.TestCase):
  """Test Storage methods."""

  def test_create_bucket(self, mock_auth_default, mock_storage_client):
    """Test that create bucket calls client correctly."""
    bucket_name = 'test_bucket'
    mock_storage_client.return_value = MagicMock()
    mock_create_bucket = mock_storage_client.return_value.create_bucket
    mock_create_bucket.return_value.name = 'test_bucket'

    replicator_storage.create_bucket(bucket_name)

    mock_auth_default.assert_called_with(scopes=GCP_STORAGE_SCOPE)
    mock_create_bucket.assert_called_with(bucket_name)


  def test_delete_bucket(self, mock_auth_default, mock_storage_client):
    """Test that delete bucket calls client correctly."""
    bucket_name = 'test_bucket'
    mock_storage_client.return_value = MagicMock()
    mock_get_bucket = mock_storage_client.return_value.get_bucket
    mock_get_bucket.return_value.name = 'test_bucket'
    mock_get_bucket.return_value.delete = MagicMock()
    mock_delete_bucket = mock_get_bucket.return_value.delete

    replicator_storage.delete_bucket(bucket_name)

    mock_auth_default.assert_called_with(scopes=GCP_STORAGE_SCOPE)
    mock_get_bucket.assert_called_with(bucket_name)
    mock_delete_bucket.assert_called_once_with()

  def test_delete_blob(self, mock_auth_default, mock_storage_client):
    """Test that delete bucket calls client correctly."""
    bucket_name = 'test_bucket'
    blob_name = 'test_blob'
    mock_storage_client.return_value = MagicMock()
    mock_get_bucket = mock_storage_client.return_value.get_bucket
    mock_get_bucket.return_value.name = 'test_bucket'
    mock_get_bucket.return_value.blob = MagicMock()
    mock_blob = mock_get_bucket.return_value.blob
    mock_blob.return_value.delete = MagicMock()
    mock_delete_blob = mock_blob.return_value.delete

    replicator_storage.delete_blob(bucket_name, blob_name)

    mock_auth_default.assert_called_with(scopes=GCP_STORAGE_SCOPE)
    mock_get_bucket.assert_called_with(bucket_name)
    mock_blob.assert_called_once_with(blob_name)
    mock_delete_blob.assert_called_once_with()

if __name__ == '__main__':
  unittest.main()
