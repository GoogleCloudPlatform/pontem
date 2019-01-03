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
r"""End to end tests for cloudsql_importer.py.

Please note that our End to End tests use predefined defaults for
'database_nanme' and 'backup_path' so whatever you pass in those fields will be
ignored all other args should work normally.

Sample usage:
    $ ./cloudsql_importer.py _ _ --hostname=<cloudsql_hostname> \
      --port=<cloudsql_port>
"""
from __future__ import absolute_import

import logging
import os

import checksum
import cloudsql_importer
import cloudsql_importer_constants
import end_to_end_runner
import sql_wrapper


def test_cloudsql_importer():
  """Compares cloudsql_importer's checksum versus the raw import's checksum."""

  logging.info("Creating database from raw dump and getting checksum")
  sql_wrapper.call_create_database(cloudsql_importer_constants)
  end_to_end_runner.load_database_from_raw_dump(cloudsql_importer_constants)
  raw_checksum = checksum.get_checksum(cloudsql_importer_constants)
  sql_wrapper.call_drop_database(cloudsql_importer_constants)

  cloudsql_importer.main()
  importer_checksum = checksum.get_checksum(cloudsql_importer_constants)

  if raw_checksum == importer_checksum:
    logging.info("Checksums are equal!")
  else:
    logging.error("Checksums don't match!")
    logging.error("Raw Checksum: %s", raw_checksum)
    logging.error("Importer Checksum: %s", importer_checksum)

  logging.info("Cleaning temp files")
  end_to_end_runner.clean_temp_folder(cloudsql_importer_constants.TEMP_FOLDER)

  assert raw_checksum == importer_checksum


def configure_importer_test_defaults():
  """Reconfigure the constants so as to use predefined defaults for tests."""
  cloudsql_importer_constants.DATABASE_NAME = (
      end_to_end_runner.END_TO_END_DATABASE_NAME)

  cloudsql_importer_constants.MYSQL_BACKUP_FOLDER = (
      end_to_end_runner.EXPORTER_DUMP_RESOURCES_FOLDER)

  cloudsql_importer_constants.INITIAL_EXECUTION_STAGE = (
      cloudsql_importer_constants.NO_CHECKPOINT)

  cloudsql_importer_constants.TEMP_FOLDER = (
      end_to_end_runner.IMPORTER_TEMP_FOLDER)

  cloudsql_importer_constants.SCHEMA_PATH = os.path.join(
      cloudsql_importer_constants.MYSQL_BACKUP_FOLDER,
      cloudsql_importer_constants.DATABASE_NAME +
      cloudsql_importer_constants.SCHEMA_FILE_SUFFIX)

  cloudsql_importer_constants.DATABASE_TEMP_FOLDER = os.path.join(
      cloudsql_importer_constants.TEMP_FOLDER,
      cloudsql_importer_constants.DATABASE_NAME)

  cloudsql_importer_constants.LOAD_GROUP_FOLDER = os.path.join(
      cloudsql_importer_constants.DATABASE_TEMP_FOLDER, "load_group")

  cloudsql_importer_constants.SECONDARY_INDEXES_SCRIPTS_FOLDER = os.path.join(
      cloudsql_importer_constants.DATABASE_TEMP_FOLDER,
      "secondary_indexes_scripts")

  cloudsql_importer_constants.DROP_INDEXES_TEMPLATE_PATH = os.path.join(
      cloudsql_importer_constants.SECONDARY_INDEXES_SCRIPT_TEMPLATES_FOLDER,
      cloudsql_importer_constants.DROP_INDEXES_TEMPLATE_FILE)

  cloudsql_importer_constants.RESTORE_INDEXES_TEMPLATE_PATH = os.path.join(
      cloudsql_importer_constants.SECONDARY_INDEXES_SCRIPT_TEMPLATES_FOLDER,
      cloudsql_importer_constants.RESTORE_INDEXES_TEMPLATE_FILE)

  cloudsql_importer_constants.RESTORE_INDEXES_COMMANDS_FOLDER = os.path.join(
      cloudsql_importer_constants.DATABASE_TEMP_FOLDER,
      "restore_indexes_commands")


if __name__ == "__main__":
  configure_importer_test_defaults()
  end_to_end_runner.test(test_cloudsql_importer, cloudsql_importer_constants)
