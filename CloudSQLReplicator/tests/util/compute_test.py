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
from google.oauth2 import credentials


from google.cloud.pontem.sql.replicator.util import compute
from google.cloud.pontem.sql.replicator.util import gcp_api_util

TEST_PROJECT = 'test-project'


@mock.patch.object(
    google.auth, 'default',
    return_value=(mock.Mock(spec_set=credentials.Credentials),
                  TEST_PROJECT)
)
@mock.patch.object(
    gcp_api_util, 'build_authorized_service'
)
class TestComputeMethods(unittest.TestCase):
    """Tests compute util methods."""

    def test_raises_rfc1035_exception(self,
                                      mock_build,
                                      mock_auth_default
                                     ):
        """Tests that create firewall rule raises exception correctly.

        Verifies that create firewall rule will raise an ValueError exception
        for firewall rule names that do not adhere to RFC 1035.
        """
        del mock_auth_default
        self.assertRaises(
            ValueError,
            compute.create_firewall_rule,
            '_bad_firewall_rule_name',
            frozenset(['127.0.0.1'])
        )
        mock_build.assert_not_called()

    def test_create_firewall(self,
                             mock_build,
                             mock_auth_default,
                            ):
        """Tests that create firewall is called correctly"""
        good_firewall_rule = 'good-firewall-rule'
        source_ip_range = frozenset(['127.0.0.1'])
        mock_build.return_value = MagicMock()
        mock_insert_firewall = (
            mock_build.return_value.firewalls.return_value.insert
        )
        compute.create_firewall_rule(
            good_firewall_rule,
            source_ip_range
        )
        mock_build.assert_called_with(
            compute.COMPUTE_SERVICE,
            compute.COMPUTE_SERVICE_VERSION,
            mock_auth_default.return_value[0]
        )
        # Verify that firewalls.insert is called with arguments and defaults
        mock_insert_firewall.assert_called_with(
            project=TEST_PROJECT,
            body={
                'name': good_firewall_rule,
                'sourceRanges': source_ip_range,
                'allowed': {
                    'IPProtocol': 'TCP',
                    'ports': frozenset(['3306'])
                },
                'direction': 'INGRESS'
            }
        )

