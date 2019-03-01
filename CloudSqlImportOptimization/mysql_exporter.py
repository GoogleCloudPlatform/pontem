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
r"""Script to dump MySQL database s in a delimited format.

Sample usage:
    $ ./mySqlExporter <database_name> <path_to_mysql_exports_folder> \
      --login-path=<login_path>
"""
from __future__ import absolute_import
from distutils import dir_util

import logging
import os
import shutil
import subprocess

import checksum
import file_writer
import mysql_exporter_constants
import logger
import sql_wrapper


def setup_database_folder_in_output_path():
  """Creates the temp export data directory with correct permissions set.

  Ensures that the temp directory has 'rwx' permissions for the mysql user, this
  is necessary for mysqldump to run with the --tab option.

  For databases using --secure-file-priv the setup phase is ignored as the
  output folder is already configured properly by MySQL.
  """
  if not mysql_exporter_constants.USING_SECURE_FILE_PRIV:
    logging.info("Creating temporary export folder '%s'",
                 mysql_exporter_constants.TEMP_FOLDER)

    _delete_directory(mysql_exporter_constants.TEMP_FOLDER)

    try:
      original_umask = os.umask(0)
      # Ensure 'rwx' permissions for the mysql user
      dir_util.mkpath(mysql_exporter_constants.TEMP_FOLDER, 0o757, 1)
    finally:
      os.umask(original_umask)


def export_schema():
  """Exports the database schema into a sql file."""
  logging.info("Exporting the schema for database '%s'",
               mysql_exporter_constants.DATABASE_NAME)

  options = ["--no-data"]
  schema_file = sql_wrapper.call_mysqldump(mysql_exporter_constants, options)

  logging.debug("Schema dump output:\n%s", schema_file)

  schema_path = os.path.join(
      mysql_exporter_constants.TEMP_FOLDER,
      mysql_exporter_constants.DATABASE_NAME +
      mysql_exporter_constants.SCHEMA_FILE_SUFFIX)
  file_writer.write(schema_path, schema_file)


def export_data():
  """Exports database table data into a text file ordered by primary key."""
  logging.info("Exporting data for '%s' tables",
               mysql_exporter_constants.DATABASE_NAME)

  options = [
      "--order-by-primary", "--tab=" + mysql_exporter_constants.TEMP_FOLDER
  ]
  sql_wrapper.call_mysqldump(mysql_exporter_constants, options)


def checksum_tables():
  """Get the checksums for the Database's tables and stores them in a file."""
  logging.info("Checksumming exported tables")

  checksumed_tables = checksum.get_checksum(mysql_exporter_constants)
  checksum_path = os.path.join(
      mysql_exporter_constants.TEMP_FOLDER,
      mysql_exporter_constants.DATABASE_NAME +
      mysql_exporter_constants.CHECKSUM_FILE_SUFFIX)
  file_writer.write(checksum_path, checksumed_tables)


def copy_from_temp_to_output():
  """Copies dumped files from the temp directory to the output directory.

  Databases using --secure-file-priv write directly to the output folder so,
  no copying between directories is necessary.
  """
  if not mysql_exporter_constants.USING_SECURE_FILE_PRIV:
    logging.info("Copying exported data to it's destination folder %s",
                 mysql_exporter_constants.MYSQL_EXPORT_DATABASE_OUTPUT_FOLDER)

    _delete_directory(
        mysql_exporter_constants.MYSQL_EXPORT_DATABASE_OUTPUT_FOLDER)
    shutil.move(mysql_exporter_constants.TEMP_FOLDER,
                mysql_exporter_constants.MYSQL_EXPORT_DATABASE_OUTPUT_FOLDER)
  else:
    logging.info("Exported data already in it's destination folder %s",
                 mysql_exporter_constants.MYSQL_EXPORT_DATABASE_OUTPUT_FOLDER)


def _delete_directory(directory):
  if os.path.exists(directory):
    logging.debug("Removing directory '%s'", directory)
    shutil.rmtree(directory)


def main():
  logger.configure(mysql_exporter_constants.VERBOSE)

  try:
    setup_database_folder_in_output_path()
    export_schema()
    export_data()

    if mysql_exporter_constants.CHECKSUM:
      checksum_tables()

    copy_from_temp_to_output()
  except Exception as e:
    logging.exception("Caught exception!")

    if hasattr(e, "output") and e.output:
      logging.error("Exception output: %s", e.output)
  finally:
    if not mysql_exporter_constants.USING_SECURE_FILE_PRIV:
      _delete_directory(mysql_exporter_constants.TEMP_FOLDER)


if __name__ == "__main__":
  main()
