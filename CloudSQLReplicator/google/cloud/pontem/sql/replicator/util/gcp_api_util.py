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

"""GCP API utility functions."""
import logging
import google.auth
from google.cloud import storage

from google.cloud.pontem.sql import replicator

STORAGE_SCOPE = ['https://www.googleapis.com/auth/devstorage.read_write']

def _get_user_agent():
  """Returns user agent based on packagage info."""

  user_agent = 'pontem,{}/{} (gzip)'.format(replicator.__package_name__, replicator.__version__)
  return user_agent


# Storage
def build_storage_client(project=None, credentials=None):
  """Authorize Storage.Client and set custom CloudSQL Replicator user agent

    Args:
      project (str): Project ID
      credentials (google.auth.Credentials): credentials to authorize client

    Returns:
      Storage.Client: returns authorized storage client with CloudSQL Replicator
        user agent.

  """
  google.cloud._http.Connection.USER_AGENT = _get_user_agent()
  default_credentials, default_project = google.auth.default(scopes=STORAGE_SCOPE)
  storage_client = (
    storage.Client(project=project or default_project,
                   credentials=credentials or default_credentials)
  )
  
  return storage_client

def create_bucket(bucket_name,
    project=None,
    credentials=None):
  """Creates a new bucket.

  Creates a new Cloud Storage Bucket.

  Args:
      project (str): Project ID where bucket will be created.
      credentials (google.auth.Credentials): credentials to authorize client.

  """
  storage_client = build_storage_client(project, credentials)
  bucket = storage_client.create_bucket(bucket_name)
  logging.info('Created bucket {}'.format(bucket.name))

def delete_bucket(bucket_name,
    project=None,
    credentials=None):
  """Deletes a bucket.

  Deletes a Cloud Storage Bucket.

  Args:
      project (str): Project ID where bucket will be deleted.
      credentials (google.auth.Credentials): credentials to authorize client.
  """

  storage_client = build_storage_client(project, credentials)
  bucket = storage_client.get_bucket(bucket_name)
  bucket.delete()
  logging.info('Deleted bucket {}'.format(bucket.name))


def delete_blob(bucket_name,
    blob_name,
    project=None,
    credentials=None):
  """Deletes a blob from the bucket.

  Deletes a Cloud Storage Blob.

  Args:
      project (str): Project ID where blob will be deleted.
      credentials (google.auth.Credentials): credentials to authorize client.

  """

  storage_client = build_storage_client(project, credentials)
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(blob_name)
  blob.delete()
  logging.info('Deleted {}'.format(blob_name))
