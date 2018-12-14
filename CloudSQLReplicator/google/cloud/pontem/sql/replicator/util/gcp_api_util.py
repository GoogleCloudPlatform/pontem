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
import httplib2
import logging
import uuid

import google.auth
from google.cloud import storage
import google_auth_httplib2
from googleapiclient import discovery

from google.cloud.pontem.sql import replicator

DEFAULT_TIER = 'db-n1-standard-2'

def _get_user_agent():
  """Returns user agent based on packagage info."""

  user_agent = 'pontem,{}/{} (gzip)'.format(replicator.__package_name__, replicator.__version__)
  return user_agent

def _get_user_agent_header():
  """Returns custom User-Agent header."""

  user_agent = _get_user_agent()
  headers = {'User-Agent': user_agent}
  return headers

# SQL Admin
def build_sql_admin_service(credentials=None):
  """Authorizes and sets custom CloudSQL Replicator user agent.

    Args:
      project (str): Project ID
      credentials (google.auth.Credentials): Credentials to authorize client

    Returns:
      Authorized sqladmin service with CloudSQL Replicator user agent.
  """
  headers = _get_user_agent_header()
  httplib2.Http.request.__func__.func_defaults = ('GET', None, headers, 5, None)
  default_credentials, _ = google.auth.default()
  authorized_http = google_auth_httplib2.AuthorizedHttp(
      credentials or default_credentials
  )

  service = discovery.build(
      'sqladmin',
      'v1beta4',
      http=authorized_http
  )

  return service

def create_cloudsql_instance(database_instance_body=None,
                             project=None,
                             credentials=None):
  """Provisions a Cloud SQL instance.

    Args:
      database_instance_body(JSON): Creation options
      project(str): Project ID
      credentials (google.auth.Credentials): Credentials to authorize client

    Returns:
      JSON: response from sqladmin.instances().insert() call
  """

  default_credentials, default_project = google.auth.default()
  default_database_intance_body = {
    'project': default_project,
    'name': 'cloudsql-db-{}'.format(uuid.uuid4()),
    'settings': {
      'tier': DEFAULT_TIER
    }
  }
  service = build_sql_admin_service(credentials or default_credentials)
  request = service.instances().insert(
      project=project or default_project,
      body=database_instance_body or default_database_intance_body
  )
  response = request.execute()

  return response

def import_sql_database(database_instance,
                        import_file_uri,
                        project=None,
                        credentials=None):
  """Import database from SQL import file.

    Args:
      database_instance(str): Database instance id
      project(str): Project ID
      credentials (google.auth.Credentials): Credentials to authorize client

    Returns:
      JSON: response from sqladmin.instances().insert() call
  """
  default_credentials, default_project = google.auth.default()
  service = build_sql_admin_service(credentials or default_credentials)
  instances_import_request_body = {
    "importContext": {
      "kind": "sql#importContext",
      "fileType": 'SQL',
      "uri": import_file_uri,
    }
  }

  request = service.instances().import_(
      project=project or default_project,
      instance=database_instance,
      body=instances_import_request_body
  )
  response = request.execute()

  return response


def is_sql_operation_done(operation, project=None, credentials=None):
  """Returns True if a SQL operation is done."""
  default_credentials, default_project = google.auth.default()
  service = build_sql_admin_service(credentials or default_credentials)
  request = service.operations().get(
      project=project or default_project,
      operation=operation)
  response = request.execute()

  return response['status'] == 'DONE'

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
  scopes = ['https://www.googleapis.com/auth/devstorage.read_write']
  default_credentials, default_project = google.auth.default(scopes=scopes)
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
