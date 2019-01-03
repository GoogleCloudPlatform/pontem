#!/usr/bin/env python
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for the Checksum module."""
from __future__ import absolute_import

import unittest
import unittest.mock as mock

import checksum
import constants_mock

EXPECTED_CHECKSUMS = "table1=123\ntable2=456\ntable3=789\n"


class ChecksumTest(unittest.TestCase):

  def setUp(self):
    super(ChecksumTest, self).setUp()
    self.constants_mock = constants_mock.configure()

  @mock.patch("checksum.sql_wrapper")
  def test_checksum(self, sql_wrapper_mock):
    database_tables = "table1\ntable2\ntable3\n"
    tables_to_checksum = list(filter(bool, database_tables.split("\n")))

    sql_wrapper_mock.call_show_tables.return_value = database_tables
    sql_wrapper_mock.call_checksum.return_value = EXPECTED_CHECKSUMS

    actual_checksums = checksum.get_checksum(self.constants_mock)
    self.assertEqual(EXPECTED_CHECKSUMS, actual_checksums)
    sql_wrapper_mock.call_show_tables.assert_called_with(self.constants_mock)
    sql_wrapper_mock.call_checksum.assert_called_with(tables_to_checksum,
                                                      self.constants_mock)

  @mock.patch("checksum.sql_wrapper")
  def test_checksum_raises_list_tables_error(self, sql_wrapper_mock):
    sql_wrapper_mock.call_show_tables.side_effect = RuntimeError("Failed")

    self.assertRaises(RuntimeError, checksum.get_checksum, self.constants_mock)
    sql_wrapper_mock.call_show_tables.assert_called_with(self.constants_mock)

  @mock.patch("checksum.sql_wrapper")
  def test_checksum_raises_checksum_error(self, sql_wrapper_mock):
    database_tables = "table1\ntable2\ntable3\n"
    tables_to_checksum = list(filter(bool, database_tables.split("\n")))

    sql_wrapper_mock.call_show_tables.return_value = database_tables
    sql_wrapper_mock.call_checksum.side_effect = RuntimeError("Failed")

    self.assertRaises(RuntimeError, checksum.get_checksum, self.constants_mock)
    sql_wrapper_mock.call_show_tables.assert_called_with(self.constants_mock)
    sql_wrapper_mock.call_checksum.assert_called_with(tables_to_checksum,
                                                      self.constants_mock)

  @mock.patch("checksum.filecmp")
  def test_compare_checksum_files(self, filecmp_mock):
    this_checksum_file = EXPECTED_CHECKSUMS
    that_checksum_file = EXPECTED_CHECKSUMS

    filecmp_mock.cmp.return_value = True

    checksum.compare_checksum_files(this_checksum_file, that_checksum_file)
    filecmp_mock.cmp.assert_called_with(
        this_checksum_file, that_checksum_file, shallow=False)

  @mock.patch("checksum.filecmp")
  def test_compare_checksum_files_raises_comparisson_error(self, filecmp_mock):
    this_checksum_file = EXPECTED_CHECKSUMS
    that_checksum_file = "Another checksum=987654321"

    filecmp_mock.cmp.return_value = False

    self.assertRaises(RuntimeError, checksum.compare_checksum_files,
                      this_checksum_file, that_checksum_file)
    filecmp_mock.cmp.assert_called_with(
        this_checksum_file, that_checksum_file, shallow=False)


if __name__ == "__main__":
  unittest.main()
