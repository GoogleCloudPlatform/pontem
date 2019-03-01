#!/usr/bin/env python
# Copyright 2018 Google LLC
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
"""Slim wrapper that calls MySql CLI tools via Subprocess."""
from __future__ import absolute_import

import MySQLdb
import subprocess
import sys

MYSQL = "mysql"
MYSQLDUMP = "mysqldump"
MYSQLIMPORT = "mysqlimport"

IS_PY3 = sys.version_info >= (3, 0)


def call_mysqldump(constants, options=None):
  """Calls MySqlDump and returns any result.

  Args:
    constants: Application constants that contain the target database,
      connection settings, credentials, etc.
    options: List of options to pass to the CLI tool.

  Returns:
    The result of the dump if any.
  """
  command = [constants.DATABASE_NAME]

  if constants.SKIP_DATE:
    command += ["--skip-dump-date"]

  subprocess_command = _build_command_to_run(MYSQLDUMP, command, constants,
                                             options)
  return _call_subprocess(subprocess_command)


def call_mysqlimport(data_filepath, constants, options=None):
  """Calls MySqlImport and returns any result.

  Args:
    data_filepath: The path containing the data to be loaded by MySqlImport.
    constants: Application constants that contain the target database,
      connection settings, credentials, etc.
    options: List of options to pass to the CLI tool.

  Returns:
    The result of the data load if any.
  """
  subprocess_command = _build_command_to_run(
      MYSQLIMPORT, [constants.DATABASE_NAME, data_filepath], constants, options)
  return _call_subprocess(subprocess_command)


def call_mysql(command, constants, stdin=None):
  """Calls MySql with the provided command and returns any result.

  Args:
    command: List of commands or queries along any desired options.
    constants: Application constants that contain the target database,
      connection settings, credentials, etc.
    stdin: The stdin file to run against MySql.

  Returns:
    The result if any.
  """
  subprocess_command = _build_command_to_run(MYSQL, command, constants)
  return _call_subprocess(subprocess_command, stdin)


def call_mysql_query(query, constants, options=None):
  """Queries MySql and returns any result.

  Args:
    query: The query to run against MySql
    constants: Application constants that contain the target database,
      connection settings, credentials, etc.
    options: List of options to pass to the CLI tool.

  Returns:
    The result of the query if any.
  """
  subprocess_command = _build_command_to_run(MYSQL, ["--execute=" + query],
                                             constants, options)
  return _call_subprocess(subprocess_command)


def call_mysql_with_stdin_command(stdin, constants, options=None):
  """Calls MySql with a command provided from stdin.

  Args:
    stdin: The stdin file to run against MySql.
    constants: Application constants that contain the target database,
      connection settings, credentials, etc.
    options: List of options to pass to the CLI tool.

  Returns:
    The result of the data load if any.
  """
  subprocess_command = _build_command_to_run(MYSQL, [constants.DATABASE_NAME],
                                             constants, options)
  return _call_subprocess(subprocess_command, stdin)


def call_checksum(tables_to_checksum, constants):
  """Calculates checksums for the provided tables list.

  Args:
    tables_to_checksum: The list of tables to checksum
    constants: Application constants that contain the target database,
      connection settings, credentials, etc.

  Returns:
    The checksum calculation for all provided tables
  """
  tables = [_back_quote(table) for table in tables_to_checksum]
  tables = ",".join(tables).rstrip(",")

  return call_mysql_query("CHECKSUM TABLE " + tables, constants,
                          [constants.DATABASE_NAME])


def call_create_database(constants):
  """Creates a Database in MySQL.

  Args:
    constants: Application constants that contain the target database,
      connection settings, credentials, etc.

  Returns:
    The result if any.
  """
  back_quoted_database_name = _back_quote(constants.DATABASE_NAME)
  return call_mysql_query("CREATE DATABASE " + back_quoted_database_name,
                          constants)


def call_drop_database(constants):
  """Drops a Database in MySQL.

  Args:
    constants: Application constants that contain the target database,
      connection settings, credentials, etc.

  Returns:
    The result if any.
  """
  back_quoted_database_name = _back_quote(constants.DATABASE_NAME)
  return call_mysql_query("DROP DATABASE " + back_quoted_database_name,
                          constants)


def call_show_databases_like(constants):
  """Tries to match the Database name in MySQL.

  Args:
    constants: Application constants that contain the target database,
      connection settings, credentials, etc.

  Returns:
    The result if any.
  """
  escaped_database_name = escape_string(constants.DATABASE_NAME,
                                        constants.DATABASE_ENCODING)
  quoted_database_name = _quote(escaped_database_name)

  return call_mysql_query("SHOW DATABASES LIKE " + quoted_database_name,
                          constants)


def call_show_tables(constants):
  """Show all Tables present in a Database in MySQL.

  Args:
    constants: Application constants that contain the target database,
      connection settings, credentials, etc.

  Returns:
    The result if any.
  """
  back_quoted_database_name = _back_quote(constants.DATABASE_NAME)
  return call_mysql_query("SHOW TABLES FROM " + back_quoted_database_name,
                          constants, ["--skip_column_names"])


def _call_subprocess(command, stdin=None, stderr=subprocess.STDOUT):
  output = subprocess.check_output(command, stdin=stdin, stderr=stderr)
  if IS_PY3:
    output = output.decode(sys.stdout.encoding)

  return output


def _build_command_to_run(cli_tool, command, constants, extra_options=None):
  options = _get_options_from_constants(constants)
  if extra_options:
    options += extra_options

  return [cli_tool] + options + command


def _get_options_from_constants(constants):
  options = []

  if constants.DATABASE_LOGIN_PATH:
    options += ["--login-path=" + constants.DATABASE_LOGIN_PATH]
  elif constants.DO_NOT_USE_CREDENTIALS_FILE:
    options += [
        "--user=" + constants.DATABASE_USER,
        "--password=" + constants.DATABASE_PASSWORD
    ]

  options += [
      "--host=" + constants.DATABASE_HOSTNAME,
      "--port=" + constants.DATABASE_PORT
  ]

  if constants.DATABASE_USE_SSL_MODE:
    options += [
        "--ssl-mode=REQUIRED", "--ssl-ca=" + constants.DATABASE_SSL_CA,
        "--ssl-cert=" + constants.DATABASE_SSL_CERT,
        "--ssl-key=" + constants.DATABASE_SSL_KEY
    ]
  elif constants.DATABASE_USE_SSL:
    options += [
        "--ssl=1", "--ssl-ca=" + constants.DATABASE_SSL_CA,
        "--ssl-cert=" + constants.DATABASE_SSL_CERT,
        "--ssl-key=" + constants.DATABASE_SSL_KEY
    ]

  return options


def _quote(string):
  """Quotes the provided string."""
  return "'{}'".format(string)


def _back_quote(string):
  """Back quotes the provided string."""
  return "`{}`".format(string)


def escape_string(string, encoding):
  """Escapes string to be MySQL compliant."""
  escaped = MySQLdb.escape_string(string)

  if IS_PY3:
    escaped = escaped.decode(encoding)

  return escaped
