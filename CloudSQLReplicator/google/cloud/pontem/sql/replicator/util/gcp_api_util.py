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
import re
import uuid

import google_auth_httplib2
from googleapiclient import discovery
import httplib2


import google.auth
from google.cloud import storage


from google.cloud.pontem.sql import replicator

# GCP API constants
GCP_STORAGE_SCOPE = frozenset(
    ['https://www.googleapis.com/auth/devstorage.read_write']
)
COMPUTE_SVC = 'compute'
COMPUTE_SVC_VERSION = 'v1'
SQL_ADMIN_SVC = 'sqladmin'
SQL_ADMIN_SVC_VERSION = 'v1beta4'

# Defaults for Cloud SQL instances
DEFAULT_1ST_GEN_DB_VERSION = 'MYSQL_5_6'
DEFAULT_2ND_GEN_DB_VERSION = 'MYSQL_5_7'
DEFAULT_1ST_GEN_TIER = 'd2'
DEFAULT_2ND_GEN_TIER = 'db-n1-standard-2'
DEFAULT_1ST_GEN_REGION = 'us-central'
DEFAULT_2ND_GEN_REGION = 'us-central1'

#REGEX
RFC1035_REGEX = r'(?:[a-z]([-a-z0-9]*[a-z0-9])?)\Z'

def _get_user_agent():
    """Returns user agent based on packagage info.

    Returns:
        str: User agent based on package info
    """

    user_agent = 'pontem,{}/{} (gzip)'.format(
        replicator.__package_name__, replicator.__version__
    )
    return user_agent


def _get_user_agent_header():
    """Returns custom User-Agent header.

    Returns:
        dict: Key value pair for user agent based on package info
    """

    user_agent = _get_user_agent()
    headers = {'User-Agent': user_agent}
    return headers


def build_authorized_svc(service, version, credentials=None):
    """Builds an authorized service proxy with customer user agent.

    Args:
        service (str): name of service requested.
        version (str): version of service requested.
        credentials (google.auth.Credentials): Credentials to authorize client.
    Returns:
        Resource: Authorized compute service proxy with custom user agent.
    """
    headers = _get_user_agent_header()
    httplib2.Http.request.__func__.func_defaults = (
        'GET', None, headers, 5, None
    )
    default_credentials, _ = google.auth.default()
    authorized_http = google_auth_httplib2.AuthorizedHttp(
        credentials or default_credentials
    )

    service = discovery.build(
        service,
        version,
        http=authorized_http
    )

    return service

# Compute
def build_compute_service(credentials=None):
    """Authorizes and sets custom CloudSQL Replicator user agent.

    Args:
        credentials (google.auth.Credentials): Credentials to authorize client.

    Returns:
        Resource: Authorized compute service proxy with custom user agent.
    """
    service = build_authorized_svc(
        COMPUTE_SVC,
        COMPUTE_SVC_VERSION,
        credentials
    )

    return service


def create_firewall_rule(name, source_ip_range, protocol='TCP',
                         is_allowed=True, ports=frozenset(['3306']),
                         is_ingress=True,
                         project=None, credentials=None):
    """Creates a firewall rule in a Google Cloud Project.

    Args:
        name (str): name of resource, must comply with RFC1035.
        source_ip_range (list): list of source IP ranges in CIDR format.
        protocol (str): protocol allowed.
        is_allowed (bool): whether traffic allowed or denied.
        ports (str): port for traffic.
        is_ingress (bool): whether traffic is ingress or egress.
        project (str): Project ID where replica will be created.
        credentials (google.auth.Credentials): Credentials to authorize client.

    Returns:
        JSON: Response from request.

    Raises:
        ValueError: Exception if name does not conform to RFC1035.
    """
    if re.match(RFC1035_REGEX, name) is None:
        raise ValueError('{} does not conform to RFC1035'.format(name))
    default_credentials, default_project = google.auth.default()
    service = build_compute_service(credentials or default_credentials)
    request_body = {
        'name': name,
        'sourceRanges': source_ip_range,
        'allowed' if is_allowed else 'denied': {
            'IPProtocol': protocol,
            'ports': ports,
        },
        'direction': 'INGRESS' if is_ingress else 'EGRESS',
    }

    request = service.firewalls().insert(
        project=project or default_project,
        body=request_body
    )
    response = request.execute()

    return response

# SQL Admin
def build_sql_admin_service(credentials=None):
    """Authorizes and sets custom CloudSQL Replicator user agent.

      Args:
        credentials (google.auth.Credentials): Credentials to authorize client

      Returns:
        Resource: Authorized sqladmin service proxy with custom user agent.
    """
    service = discovery.build(
        SQL_ADMIN_SVC,
        SQL_ADMIN_SVC_VERSION,
        credentials
    )

    return service


def create_cloudsql_instance(database_instance_body=None,
                             project=None,
                             credentials=None):
    """Provisions a Cloud SQL instance.

      Args:
        database_instance_body(JSON): Cloud SQL instance creation options.
        project(str): Project ID
        credentials (google.auth.Credentials): Credentials to authorize client

      Returns:
        JSON: response from sqladmin.instances().insert() call
    """

    default_credentials, default_project = google.auth.default()
    default_database_intance_body = {
        'name': 'cloudsql-db-{}'.format(uuid.uuid4()),
        'settings': {
            'tier': DEFAULT_2ND_GEN_TIER
        }
    }
    service = build_sql_admin_service(credentials or default_credentials)
    request = service.instances().insert(
        project=project or default_project,
        body=database_instance_body or default_database_intance_body
    )
    response = request.execute()

    return response


def create_source_representation(
        ip_address,
        port,
        database_version=DEFAULT_2ND_GEN_DB_VERSION,
        region=DEFAULT_2ND_GEN_REGION,
        source_name=None,
        source_body=None,
        project=None,
        credentials=None):
    """Creates a source representation of an external master.


    If source_body is included source_representation_name,
    ip_address, port, db_version and region are ignored.

    Args:
      source_name (str): The instance name of the
        external master.
      ip_address (str): The ip address of the external master.
      port (str): Port that will be used for replication.
      region (str): Region source representation will be created.
      database_version (str): MySQL database version
      source_body (JSON): Creation options for source
        representation.
      project (str): Project ID where replica will be created.
      credentials (google.auth.Credentials): Credentials to authorize
        client.

    Returns:
        JSON: response from sqladmin.instances().insert() call.
    """
    default_source_body = {
        'name': source_name or
                'external-mysql-representation-{}'.format(uuid.uuid4()),
        'databaseVersion': database_version,
        'region': region,
        'onPremisesConfiguration': {
            'kind': 'sql#onPremisesConfiguration',
            'hostPort': '{}:{}'.format(ip_address, port)
        }

    }

    response = create_cloudsql_instance(
        source_body or default_source_body,
        project,
        credentials
    )

    return response


def create_replica_instance(
        master_instance_name,
        dumpfile_path,
        replica_user,
        replica_pwd,
        replica_name=None,
        replica_body=None,
        project=None,
        credentials=None):
    """Provisions a Cloud SQL Replica instance.

      Will create a second generation replica by default, specify tier and
      region if creating a first generation replica.

      If replica_instance_body is supplied, master_instance_name, dumpfile_path
        replica_user, replica_pwd, and replica_instance_name will be ignored.

      Args:
        master_instance_name (str): Instance name of master that will be
          replicated.
        dumpfile_path (str): SQL file path (possibly gzipped) that contains dump
          from master.
        replica_user (str): User name of replica user.
        replica_pwd (str): Password of replica user.
        replica_name (str): Name of replica instance to create.
        replica_body (JSON): Options for replica instance creation.
        project (str): Project ID where replica will be created.
        credentials (google.auth.Credentials): Credentials to authorize client

      Returns:
        JSON: response from sqladmin.instances().insert() call
    """

    default_replica_body = {
        'name': replica_name or
                'cloudsql-replica-{}'.format(uuid.uuid4()),
        'settings': {
            'tier': DEFAULT_2ND_GEN_TIER,

        },
        'databaseVersion': DEFAULT_2ND_GEN_DB_VERSION,
        'masterInstanceName': master_instance_name,
        'region': DEFAULT_2ND_GEN_REGION,
        'replicaConfiguration': {
            'mysqlReplicaConfiguration': {
                'dumpFilePath': dumpfile_path,
                'username': replica_user,
                'password': replica_pwd,
            }

        }
    }

    response = create_cloudsql_instance(
        replica_body or default_replica_body,
        project,
        credentials
    )

    return response


def import_sql_database(database_instance,
                        import_file_uri,
                        project=None,
                        credentials=None):
    """Import database from SQL import file.

      Args:
        database_instance (str): Database instance id.
        import_file_uri (str): URI of sql file to import.
        project(str): Project ID
        credentials (google.auth.Credentials): Credentials to authorize client.

      Returns:
        JSON: response from sqladmin.instances().insert() call.
    """
    default_credentials, default_project = google.auth.default()
    service = build_sql_admin_service(credentials or default_credentials)
    instances_import_request_body = {
        'importContext': {
            'kind': 'sql#importContext',
            'fileType': 'SQL',
            'uri': import_file_uri,
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
    """Returns True if a SQL operation is done.

    Args:
        operation (str): operation id to check.
        project(str): Project ID
        credentials (google.auth.Credentials): Credentials to authorize client.

    Returns:
          bool: whether operation is done.
    """
    default_credentials, default_project = google.auth.default()
    service = build_sql_admin_service(credentials or default_credentials)
    request = service.operations().get(
        project=project or default_project,
        operation=operation)
    response = request.execute()

    return response['status'] == 'DONE'


# Storage
def build_storage_client(project=None, credentials=None):
    """Authorize Storage.Client and set custom CloudSQL Replicator user agent.

      Args:
        project (str): Project ID.
        credentials (google.auth.Credentials): credentials to authorize client.

      Returns:
        Storage.Client: returns authorized storage client with CloudSQL
        Replicator user agent.
    """

    google.cloud._http.Connection.USER_AGENT = _get_user_agent() # pylint: disable=protected-access
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
    """Creates a new bucket.

    Creates a new Cloud Storage Bucket.

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
    """Deletes a bucket.

    Deletes a Cloud Storage Bucket.

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
    """Deletes a blob from the bucket.

    Deletes a Cloud Storage Blob.

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
