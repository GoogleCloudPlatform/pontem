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

"""Tests GCP API helper utility functions."""

import unittest
import mock

import google_auth_httplib2
from googleapiclient import discovery
import httplib2

import google.auth
from google.oauth2 import credentials

from google.cloud.pontem.sql import replicator
from google.cloud.pontem.sql.replicator.util import gcp_api_util


class TestGCPAPIMethods(unittest.TestCase):
    """Test GCP API helper methods."""

    def setUp(self):
        """Initializes instance attributes."""
        self.user_agent = 'pontem,{}/{} (gzip)'.format(
            replicator.__package_name__, replicator.__version__
        )

    def test_get_user_agent(self):
        """Tests that correct user agent is returned."""
        self.assertEqual(self.user_agent, gcp_api_util.get_user_agent())

    def test_get_user_agent_header(self):
        """Tests that correct user agent header is returned."""
        self.assertEqual(self.user_agent,
                         gcp_api_util.get_user_agent_header()['User-Agent'])

    @mock.patch.object(
        google.auth, 'default',
        return_value=(mock.Mock(spec_set=credentials.Credentials),
                      'test-project')
    )
    @mock.patch.object(
        google_auth_httplib2, 'AuthorizedHttp'
    )
    @mock.patch.object(
        discovery, 'build'
    )
    def test_build_authorized_svc_defaults(self,
                                           mock_build,
                                           mock_authorized_http,
                                           mock_auth_default):
        """Tests build_authorized_svc with default credentials and project."""
        test_svc = 'test-svc'
        test_version = '0.0.1'
        _ = gcp_api_util.build_authorized_service(test_svc, test_version)
        # Verify that default headers have been set correctly
        self.assertEqual(
            httplib2.Http.request.__func__.func_defaults[2]['User-Agent'],
            self.user_agent
        )
        mock_auth_default.assert_called_with()
        mock_authorized_http.assert_called_with(
            mock_auth_default.return_value[0]
        )
        mock_build.assert_called_with(test_svc,
                                      test_version,
                                      http=mock_authorized_http.return_value
                                     )

    @mock.patch.object(
        google.auth, 'default',
        return_value=(mock.Mock(spec_set=credentials.Credentials),
                      'test-project')
    )
    @mock.patch.object(
        google_auth_httplib2, 'AuthorizedHttp'
    )
    @mock.patch.object(
        discovery, 'build'
    )
    def test_build_authorized_svc(self,
                                  mock_build,
                                  mock_authorized_http,
                                  mock_auth_default):
        """Tests build_authorized_svc with non-default credentials."""
        test_svc = 'test-svc'
        test_version = '0.0.1'
        mock_credentials = mock.Mock(spec_set=credentials.Credentials)

        _ = gcp_api_util.build_authorized_service(test_svc,
                                                  test_version,
                                                  credentials=mock_credentials)
        # Verify that default headers have been set correctly
        self.assertEqual(
            httplib2.Http.request.__func__.func_defaults[2]['User-Agent'],
            self.user_agent
        )
        mock_auth_default.assert_called_with()
        mock_authorized_http.assert_called_with(
            mock_credentials
        )
        mock_build.assert_called_with(test_svc,
                                      test_version,
                                      http=mock_authorized_http.return_value
                                      )

if __name__ == '__main__':
    unittest.main()