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
r"""End to end tests for mysql_exporter.py.

Please note that our End to End tests use predefined defaults for
'database_name' and 'backup_path' so whatever you pass in those fields will be
ignored all other args should work normally.

Sample usage:
    $ ./mySqlExporter _ _ --login-path=<login_path>
"""
from __future__ import absolute_import

import filecmp
import logging
import os

import end_to_end_runner
import mysql_exporter
import mysql_exporter_constants
import sql_wrapper


def compare_dumps():
  """Compares mysql_exporter's dumps versus the raw export ones."""
  expected_exported_files = os.listdir(
      end_to_end_runner.EXPORTER_DUMP_RESOURCES_FOLDER)

  logging.info(
      "Checking that the following files are equal to the expected dump: %s",
      expected_exported_files)

  matches, mismatches, errors = filecmp.cmpfiles(
      end_to_end_runner.EXPORTER_DUMP_RESOURCES_FOLDER,
      mysql_exporter_constants.MYSQL_EXPORT_DATABASE_OUTPUT_FOLDER,
      expected_exported_files,
      shallow=False)

  if (len(expected_exported_files) == len(matches)
     ) and not mismatches and not errors:
    logging.info("All files match!")
    logging.info("Cleaning temp files")
    end_to_end_runner.clean_temp_folder(
        mysql_exporter_constants.MYSQL_EXPORT_DATABASE_OUTPUT_FOLDER)
  else:
    logging.error("Not all files match!")
    logging.info("Not cleaning temp files so you can compare files.")
    logging.error("Matches: %s\nMismatches: %s\nErrors: %s", matches,
                  mismatches, errors)

  assert len(expected_exported_files) == len(matches)
  assert not mismatches
  assert not errors


def test_mysql_exporter():
  logging.info("Creating database from raw dump and calling the exporter.")
  sql_wrapper.call_create_database(mysql_exporter_constants)
  end_to_end_runner.load_database_from_raw_dump(mysql_exporter_constants)
  mysql_exporter.main()
  compare_dumps()


def configure_exporter_test_defaults():
  """Reconfigure the constants so as to use predefined defaults for tests."""
  mysql_exporter_constants.DATABASE_NAME = (
      end_to_end_runner.END_TO_END_DATABASE_NAME)
  mysql_exporter_constants.MYSQL_EXPORT_DATABASE_OUTPUT_FOLDER = (
      end_to_end_runner.EXPORTER_OUTPUT_FOLDER)
  mysql_exporter_constants.SKIP_DATE = True
  mysql_exporter_constants.CHECKSUM = True


if __name__ == "__main__":
  configure_exporter_test_defaults()
  end_to_end_runner.test(test_mysql_exporter, mysql_exporter_constants)
