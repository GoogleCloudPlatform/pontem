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
"""Common code for End to End tests."""
from __future__ import absolute_import

import os
import logging
import shutil
import subprocess

import logger
import sql_wrapper

END_TO_END_DATABASE_NAME = "cloudsql_import_optimization_end_to_end_database"
END_TO_END_RESOURCES_FOLDER = "end_to_end_resources/"

RAW_DUMP_RESOURCES_FOLDER = os.path.join(END_TO_END_RESOURCES_FOLDER,
                                         "raw_dump/")
RAW_DUMP_FILE = "raw_dump.sql"

EXPORTER_DUMP_RESOURCES_FOLDER = os.path.join(END_TO_END_RESOURCES_FOLDER,
                                              "exporter_dump/")

EXPORTER_OUTPUT_FOLDER = "/tmp/cloudsql_import_optimization_end_to_end_exporter"
IMPORTER_TEMP_FOLDER = "/tmp/cloudsql_import_optimization_end_to_end_importer"


def load_database_from_raw_dump(constants):
  """Creates a database to dump from the raw dump files."""
  raw_dump_path = os.path.join(RAW_DUMP_RESOURCES_FOLDER, RAW_DUMP_FILE)

  with open(raw_dump_path, "r") as raw_dump:
    sql_wrapper.call_mysql_with_stdin_command(raw_dump, constants)


def clean_temp_folder(folder_name):
  """Removes the temp folder and all it's contents."""
  logging.info("Removing contents from temp folder '%s'", folder_name)

  if os.path.exists(folder_name):
    shutil.rmtree(folder_name)


def test(test_fn, constants):
  """Runs tests based on the provided function."""
  logger.configure(constants.VERBOSE)

  try:
    test_fn()
    logging.info("Test Passed")
  except Exception as e:
    logging.error("Test Failed")
    logging.exception(e)

    if hasattr(e, "output"):
      logging.error("Exception output: %s", e.output)
  finally:
    sql_wrapper.call_drop_database(constants)
