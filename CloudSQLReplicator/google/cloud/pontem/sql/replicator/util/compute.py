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

import google.auth

from google.cloud.pontem.sql.replicator.util import gcp_api_util

COMPUTE_SERVICE = 'compute'
COMPUTE_SERVICE_VERSION = 'v1'

# RFC 1035 regex (https://www.ietf.org/rfc/rfc1035.txt) for firewall rule names
RFC1035_REGEX = r'(?:[a-z]([-a-z0-9]*[a-z0-9])?)\Z'


def build_compute_service(credentials=None):
    """Builds an authorized compute service proxy with a custom user agent.

    Args:
        credentials (google.auth.Credentials): Credentials to authorize client.

    Returns:
        Resource: Authorized compute service proxy with custom user agent.
    """
    service = gcp_api_util.build_authorized_svc(
        COMPUTE_SERVICE,
        COMPUTE_SERVICE_VERSION,
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
