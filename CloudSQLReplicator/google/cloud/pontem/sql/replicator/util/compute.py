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

"""Wrapper for Compute API service proxy."""
import re
import subprocess
import time
import uuid

from googleapiclient import errors
from httplib2 import HttpLib2Error


import google.auth

from google.cloud.pontem.sql.replicator.util import gcp_api_util

COMPUTE_SERVICE = 'compute'
COMPUTE_SERVICE_VERSION = 'v1'
ALLOW_SSH_FIREWALL_RULE = 'allow_replicator_ssh'
# RFC 1035 regex (https://www.ietf.org/rfc/rfc1035.txt) for firewall rule names
RFC1035_REGEX = r'(?:[a-z]([-a-z0-9]*[a-z0-9])?)\Z'
# Amount of seconds to wait prior to throwing error
FIREWALL_RULE_TIMEOUT = 300
FIREWALL_RULE_INTERVAL = 10


class TimeoutError(Exception):
    """Timeout error raised if operation takes too long."""


class SCPError(Exception):
    """Error raised if there was a problem copying a file with SCP."""


class FirewallRule(object):
    """Configuration for firewall rule."""
    def __init__(self, name=None, description=None):
        """Constructor.

        Args:
            name (str): Name for firewall rule.  Must comply with RFC 1035.
            description (str): Description of firewall rule.

        Raises:
            ValueError: Exception if name does not conform to RFC1035.
        """
        self.name = name or 'firewall_rule_{}'.format(uuid.uuid4())
        if re.match(RFC1035_REGEX, name) is None:
            raise ValueError('{} does not conform to RFC1035'.format(name))
        self.description = description or 'User defined firewall rule.'
        self.protocol = 'TCP'
        self.is_allowed = True,
        self.ports = None
        self.is_ingress = True
        self.network = None


def build_compute_service(credentials=None):
    """Builds an authorized compute service proxy with a custom user agent.

    Args:
        credentials (google.auth.Credentials): Credentials to authorize client.

    Returns:
        Resource: Authorized compute service proxy with custom user agent.
    """
    service = gcp_api_util.build_authorized_service(
        COMPUTE_SERVICE,
        COMPUTE_SERVICE_VERSION,
        credentials
    )

    return service


def create_firewall_rule(firewall_rule,
                         project=None, credentials=None):
    """Creates a firewall rule in a Google Cloud Project.

    Args:
        firewall_rule (FirewallRule): Configuration for firewall rule.
        project (str): Project ID where firewall rule will be created.
        credentials (google.auth.Credentials): Credentials to authorize client.

    Returns:
        JSON: Response from request.
    """

    default_credentials, default_project = google.auth.default()
    service = build_compute_service(credentials or default_credentials)

    if firewall_rule.ports is None:
        ports = ['3306']
    else:
        ports = firewall_rule.ports

    request_body = {
        'name': firewall_rule.name,
        'description': firewall_rule.description,
        'sourceRanges': firewall_rule.source_ip_range,
        'allowed' if firewall_rule.is_allowed else 'denied': [{
            'IPProtocol': firewall_rule.protocol,
            'ports': ports,
        }],
        'network': firewall_rule.network,
        'direction': 'INGRESS' if firewall_rule.is_ingress else 'EGRESS',
    }

    request = service.firewalls().insert(
        project=project or default_project,
        body=request_body
    )
    response = request.execute()

    return response


def is_firewall_rule_active(firewall, project=None, credentials=None):
    """Checks if a firewall rule active.

    Args:
        firewall (str): The name of the firewall rule.
        project (str): Project ID where firewall rule may be active.
        credentials (google.auth.Credentials): Credentials to authorize client.

    Returns:
        bool: True if firewall rule is active, False otherwise.
    """
    default_credentials, default_project = google.auth.default()
    service = build_compute_service(credentials or default_credentials)
    try:
        _ = service.firewalls().get(project=project or default_project,
                                    firewall=firewall)

    except (errors.HttpError, HttpLib2Error) as e:
        if isinstance(e, errors.HttpError) and e.resp.status == 404:
            return False
    return True


def enable_ssh(network=None, project=None, credentials=None):
    """Enables SSH on the network if rule note enabled.

    Args:
        network (str): Network to allow SSH on.
        project (str): Project ID where firewall rule will be activated.
        credentials (google.auth.Credentials): Credentials to authorize client.

    Raises:
        TimeoutError: Operation took too long.
    """
    if not is_firewall_rule_active(ALLOW_SSH_FIREWALL_RULE,
                                   project, credentials):
        firewall_rule = FirewallRule(ALLOW_SSH_FIREWALL_RULE,
                                     description='Allow SSH.')
        firewall_rule.ports = ['22']
        firewall_rule.network = network
        create_firewall_rule(firewall_rule, project, credentials)

        start_time = time.time()
        while not is_firewall_rule_active(ALLOW_SSH_FIREWALL_RULE,
                                          project, credentials):
            elapsed_time = time.time() - start_time
            if elapsed_time > FIREWALL_RULE_TIMEOUT:
                raise TimeoutError(
                    'Creating Allow SSH Firewall Rule took too long.')
            time.sleep(FIREWALL_RULE_INTERVAL)
    return


def get_remote_file(remote_host, remote_file_path, zone, local_dir='.'):
    """Copies file from remote GCP host to local directory.

    Args:
        remote_host (str): Remote host file exists on.
        remote_file_path (str): File to copy
        zone (str): Zone remote host resides in.
        local_dir (str): Local directory where file will be copied.

    Raises:
        SCPError: Something went wrong during the SCP operation.
    """
    gcloud = subprocess.Popen(('gcloud',
                               'compute',
                               'scp',
                               '{}:{}'.format(remote_host, remote_file_path),
                               local_dir, '--zone', zone))
    _, error = gcloud.communicate()
    if error:
        raise SCPError(error.strip())
    return


def write_remote_file(remote_host, remote_dir, zone, local_file_path):
    """Copies a local file to a remote host directory.

    Args:
        remote_host (str): Remote host file exists on.
        remote_dir (str): Remote directory to copy file to.
        zone (str): Zone remote host resides in.
        local_file_path (str): Local file to be copied.

    Raises:
        SCPError: Something went wrong during the SCP operation.
    """
    gcloud = subprocess.Popen(('gcloud',
                               'compute',
                               'scp',
                               local_file_path,
                               '{}:{}'.format(remote_host, remote_dir),
                               '--zone', zone))
    _, error = gcloud.communicate()
    if error:
        raise SCPError(error.strip())
    return
