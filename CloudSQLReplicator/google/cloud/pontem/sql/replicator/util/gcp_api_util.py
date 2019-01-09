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
import sys

import google_auth_httplib2
from googleapiclient import discovery
import httplib2


import google.auth

from google.cloud.pontem.sql import replicator

PY3 = sys.version_info[0] == 3
PY2 = sys.version_info[0] == 2


def get_user_agent():
    """Returns user agent based on package info.

    Returns:
        str: User agent based on package info
    """

    user_agent = 'pontem,{}/{} (gzip)'.format(
        replicator.__package_name__, replicator.__version__
    )
    return user_agent


def get_user_agent_header():
    """Returns custom User-Agent header.

    Returns:
        dict: Key value pair for user agent based on package info
    """

    user_agent = get_user_agent()
    headers = {'User-Agent': user_agent}
    return headers


def build_authorized_service(service, version, credentials=None):
    """Builds an authorized service proxy with custom user agent.

    Args:
        service (str): name of service requested.
        version (str): version of service requested.
        credentials (google.auth.Credentials): Credentials to authorize client.
    Returns:
        Resource: Authorized compute service proxy with custom user agent.
    """
    headers = get_user_agent_header()
    if PY2:
        httplib2.Http.request.__func__.func_defaults = (
            'GET', None, headers, 5, None
        )
    elif PY3:
        httplib2.Http.request.func_defaults = (
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
