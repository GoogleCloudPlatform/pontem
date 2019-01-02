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

"""Wrapper for Storage API client."""
import logging

import google.auth
from google.cloud import storage

from google.cloud.pontem.sql.replicator.util import gcp_api_util

GCP_STORAGE_SCOPE = frozenset(
    ['https://www.googleapis.com/auth/devstorage.read_write']
)

def build_storage_client(project=None, credentials=None):
    """Builds Authorized Storage.Client with custom user agent.

      Args:
        project (str): Project ID.
        credentials (google.auth.Credentials): credentials to authorize client.

      Returns:
        Storage.Client: returns authorized storage client with CloudSQL
        Replicator user agent.
    """

    google.cloud._http.Connection.USER_AGENT = gcp_api_util.get_user_agent()  # pylint: disable=protected-access
    default_credentials, default_project = google.auth.default(
        scopes=GCP_STORAGE_SCOPE
    )
    storage_client = (
        storage.Client(project=project or default_project,
                       credentials=credentials or default_credentials)
    )

    return storage_client


def create_bucket(bucket_name,
                  project=None,
                  credentials=None):
    """Creates a new Cloud Storage bucket.

    Args:
        bucket_name (str): Name of bucket to create.
        project (str): Project ID where bucket will be created.
        credentials (google.auth.Credentials): credentials to authorize client.

    """
    storage_client = build_storage_client(project, credentials)
    bucket = storage_client.create_bucket(bucket_name)
    logging.info('Created bucket %s', bucket.name)


def delete_bucket(bucket_name,
                  project=None,
                  credentials=None):
    """Deletes a Cloud Storage bucket.



    Args:
        bucket_name (str): name of bucket that will be deleted.
        project (str): Project ID where bucket will be deleted.
        credentials (google.auth.Credentials): credentials to authorize client.
    """
    storage_client = build_storage_client(project, credentials)
    bucket = storage_client.get_bucket(bucket_name)
    bucket.delete()
    logging.info('Deleted bucket %s', bucket.name)


def delete_blob(bucket_name,
                blob_name,
                project=None,
                credentials=None):
    """Deletes a blob in the Cloud Storage bucket.

    Args:
        bucket_name (str): where blob will be deleted.
        blob_name (str): blob that will be deleted.
        project (str): Project ID where blob will be deleted.
        credentials (google.auth.Credentials): credentials to authorize client.
    """
    storage_client = build_storage_client(project, credentials)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.delete()
    logging.info('Deleted %s', blob_name)


def grant_read_access_to_bucket(bucket_name,
                                email,
                                project=None,
                                credentials=None
                               ):
    """Grants read access to a specific bucket.

      Args:
          bucket_name (str): name of bucket that the user will be able to read.
          email (str): email of user who will be granted read privileges.
          project (str): Project ID where blob will be deleted.
          credentials (google.auth.Credentials): credentials to authorize
            client.
    """
    storage_client = build_storage_client(project, credentials)
    bucket = storage_client.get_bucket(bucket_name)
    acl = bucket.acl
    acl.user(email).grant_read()
    acl.save()
