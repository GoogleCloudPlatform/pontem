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
"""Tests for the Sql Wrapper module."""
from __future__ import absolute_import

import subprocess
import unittest
import unittest.mock as mock

import constants_mock
import sql_wrapper

MYSQL = ["mysql"]
MYSQLDUMP = ["mysqldump"]
MYSQLIMPORT = ["mysqlimport"]

DATA_PATH = "/fake/data/path"
QUERY = "SELECT * FROM `FOO_TABLE`"
SKIP_COLUMNS_NAMES = "--skip_column_names"

MYSQL_COMMANDS = ["--execute=" + QUERY, SKIP_COLUMNS_NAMES]
STDIN_COMMAND = "ALTER TABLE `FOO_TABLE`; ALTER TABLE `BAR_TABLE`;"

OPTIONS = ["--tab", "--no-data", SKIP_COLUMNS_NAMES]


class SqlWrapperTest(unittest.TestCase):

  def setUp(self):
    super(SqlWrapperTest, self).setUp()
    self.constants_mock = constants_mock.configure()

  @mock.patch("sql_wrapper.subprocess")
  def test_call_sql_wrapper_with_creds_file(self, subprocess_mock):
    self.constants_mock.DO_NOT_USE_CREDENTIALS_FILE = False

    defaults = self._get_default_options_from_mock()
    command = MYSQLDUMP + defaults + [self.constants_mock.DATABASE_NAME]

    sql_wrapper.call_mysqldump(self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_sql_wrapper_with_ssl_mode(self, subprocess_mock):
    self.constants_mock.DATABASE_USE_SSL_MODE = True

    defaults = self._get_default_options_from_mock()
    command = MYSQLDUMP + defaults + [self.constants_mock.DATABASE_NAME]

    sql_wrapper.call_mysqldump(self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_sql_wrapper_with_ssl(self, subprocess_mock):
    self.constants_mock.DATABASE_USE_SSL = True

    defaults = self._get_default_options_from_mock()
    command = MYSQLDUMP + defaults + [self.constants_mock.DATABASE_NAME]

    sql_wrapper.call_mysqldump(self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_sql_wrapper_with_creds_file_and_login_path(
      self, subprocess_mock):
    self.constants_mock.DO_NOT_USE_CREDENTIALS_FILE = False
    self.constants_mock.DATABASE_LOGIN_PATH = "login_path"

    defaults = self._get_default_options_from_mock()
    command = MYSQLDUMP + defaults + [self.constants_mock.DATABASE_NAME]

    sql_wrapper.call_mysqldump(self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_sql_wrapper_login_path_wins_over_do_not_use_creds(
      self, subprocess_mock):
    self.constants_mock.DO_NOT_USE_CREDENTIALS_FILE = False

    defaults = self._get_default_options_from_mock()
    command = MYSQLDUMP + defaults + [self.constants_mock.DATABASE_NAME]

    sql_wrapper.call_mysqldump(self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_mysqldump(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    command = MYSQLDUMP + defaults + [self.constants_mock.DATABASE_NAME]

    sql_wrapper.call_mysqldump(self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_mysqldump_skips_date(self, subprocess_mock):
    self.constants_mock.SKIP_DATE = True
    defaults = self._get_default_options_from_mock()
    command = MYSQLDUMP + defaults + [
        self.constants_mock.DATABASE_NAME, "--skip-dump-date"
    ]

    sql_wrapper.call_mysqldump(self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_mysqldump_with_options(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    command = MYSQLDUMP + defaults + OPTIONS + [
        self.constants_mock.DATABASE_NAME
    ]

    sql_wrapper.call_mysqldump(self.constants_mock, OPTIONS)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_mysqldump_raises_subprocess_error(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    command = MYSQLDUMP + defaults + [self.constants_mock.DATABASE_NAME]

    subprocess_mock.check_output.side_effect = subprocess.CalledProcessError(
        1, command, "Failed mysqldump")

    self.assertRaises(subprocess.CalledProcessError, sql_wrapper.call_mysqldump,
                      self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_mysqlimport(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    command = MYSQLIMPORT + defaults + [
        self.constants_mock.DATABASE_NAME, DATA_PATH
    ]

    sql_wrapper.call_mysqlimport(DATA_PATH, self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_mysqlimport_with_options(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    command = MYSQLIMPORT + defaults + OPTIONS + [
        self.constants_mock.DATABASE_NAME, DATA_PATH
    ]

    sql_wrapper.call_mysqlimport(DATA_PATH, self.constants_mock, OPTIONS)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_mysqlimport_raises_subprocess_error(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    command = MYSQLIMPORT + defaults + [
        self.constants_mock.DATABASE_NAME, DATA_PATH
    ]

    subprocess_mock.check_output.side_effect = subprocess.CalledProcessError(
        1, command, "Failed mysqldump")

    self.assertRaises(subprocess.CalledProcessError,
                      sql_wrapper.call_mysqlimport, DATA_PATH,
                      self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_mysql(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    subprocess_command = MYSQL + defaults + MYSQL_COMMANDS

    sql_wrapper.call_mysql(MYSQL_COMMANDS, self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, subprocess_command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_mysql_with_stdin_param(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    subprocess_command = MYSQL + defaults + MYSQL_COMMANDS
    stdin_mock = mock.Mock()

    sql_wrapper.call_mysql(MYSQL_COMMANDS, self.constants_mock, stdin_mock)
    self.assert_subprocess_call(subprocess_mock, subprocess_command, stdin_mock)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_mysql_raises_subprocess_error(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    subprocess_command = MYSQL + defaults + MYSQL_COMMANDS

    subprocess_mock.check_output.side_effect = subprocess.CalledProcessError(
        1, subprocess_command, "Failed mysqldump")

    self.assertRaises(subprocess.CalledProcessError, sql_wrapper.call_mysql,
                      MYSQL_COMMANDS, self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, subprocess_command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_mysql_query(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    command = MYSQL + defaults + ["--execute=" + QUERY]

    sql_wrapper.call_mysql_query(QUERY, self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_mysql_query_with_options(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    command = MYSQL + defaults + OPTIONS + ["--execute=" + QUERY]

    sql_wrapper.call_mysql_query(QUERY, self.constants_mock, OPTIONS)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_mysql_query_raises_subprocess_error(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    command = MYSQL + defaults + ["--execute=" + QUERY]

    subprocess_mock.check_output.side_effect = subprocess.CalledProcessError(
        1, command, "Failed mysqldump")

    self.assertRaises(subprocess.CalledProcessError,
                      sql_wrapper.call_mysql_query, QUERY, self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_mysql_with_stdin_command(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    command = MYSQL + defaults + [self.constants_mock.DATABASE_NAME]

    sql_wrapper.call_mysql_with_stdin_command(STDIN_COMMAND,
                                              self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command, STDIN_COMMAND)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_mysql_with_stdin_command_and_options(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    command = MYSQL + defaults + OPTIONS + [self.constants_mock.DATABASE_NAME]

    sql_wrapper.call_mysql_with_stdin_command(STDIN_COMMAND,
                                              self.constants_mock, OPTIONS)
    self.assert_subprocess_call(subprocess_mock, command, STDIN_COMMAND)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_mysql_with_stdin_command_raises_subprocess_error(
      self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    command = MYSQL + defaults + [self.constants_mock.DATABASE_NAME]

    subprocess_mock.check_output.side_effect = subprocess.CalledProcessError(
        1, command, "Failed mysqldump")

    self.assertRaises(subprocess.CalledProcessError,
                      sql_wrapper.call_mysql_with_stdin_command, STDIN_COMMAND,
                      self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command, STDIN_COMMAND)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_checksum(self, subprocess_mock):
    tables = ["table1", "table2"]
    defaults = self._get_default_options_from_mock()
    command = MYSQL + defaults + [
        self.constants_mock.DATABASE_NAME,
        "--execute=CHECKSUM TABLE `table1`,`table2`"
    ]

    sql_wrapper.call_checksum(tables, self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_checksum_raises_subprocess_error(
      self, subprocess_mock):
    tables = ["table1", "table2"]
    defaults = self._get_default_options_from_mock()
    command = MYSQL + defaults + [
        self.constants_mock.DATABASE_NAME,
        "--execute=CHECKSUM TABLE `table1`,`table2`"
    ]

    subprocess_mock.check_output.side_effect = subprocess.CalledProcessError(
        1, command, "Failed mysqldump")

    self.assertRaises(subprocess.CalledProcessError, sql_wrapper.call_checksum,
                      tables, self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_create_database(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    command = MYSQL + defaults + [
        "--execute=CREATE DATABASE `" + self.constants_mock.DATABASE_NAME + "`"
    ]

    sql_wrapper.call_create_database(self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_create_database_raises_subprocess_error(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    command = MYSQL + defaults + [
        "--execute=CREATE DATABASE `" + self.constants_mock.DATABASE_NAME + "`"
    ]

    subprocess_mock.check_output.side_effect = subprocess.CalledProcessError(
        1, command, "Failed mysqldump")

    self.assertRaises(subprocess.CalledProcessError,
                      sql_wrapper.call_create_database, self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_drop_database(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    command = MYSQL + defaults + [
        "--execute=DROP DATABASE `" + self.constants_mock.DATABASE_NAME + "`"
    ]

    sql_wrapper.call_drop_database(self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_drop_database_raises_subprocess_error(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    command = MYSQL + defaults + [
        "--execute=DROP DATABASE `" + self.constants_mock.DATABASE_NAME + "`"
    ]

    subprocess_mock.check_output.side_effect = subprocess.CalledProcessError(
        1, command, "Failed mysqldump")

    self.assertRaises(subprocess.CalledProcessError,
                      sql_wrapper.call_drop_database, self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_show_databases_like_database(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    command = MYSQL + defaults + [
        "--execute=SHOW DATABASES LIKE '" + self.constants_mock.DATABASE_NAME +
        "'"
    ]

    sql_wrapper.call_show_databases_like(self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_show_databases_like_raises_subprocess_error(
      self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    command = MYSQL + defaults + [
        "--execute=SHOW DATABASES LIKE '" + self.constants_mock.DATABASE_NAME +
        "'"
    ]

    subprocess_mock.check_output.side_effect = subprocess.CalledProcessError(
        1, command, "Failed mysqldump")

    self.assertRaises(subprocess.CalledProcessError,
                      sql_wrapper.call_show_databases_like, self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_show_tables(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    command = MYSQL + defaults + [
        SKIP_COLUMNS_NAMES,
        "--execute=SHOW TABLES FROM `" + self.constants_mock.DATABASE_NAME + "`"
    ]

    sql_wrapper.call_show_tables(self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  @mock.patch("sql_wrapper.subprocess")
  def test_call_show_tables_raises_subprocess_error(self, subprocess_mock):
    defaults = self._get_default_options_from_mock()
    command = MYSQL + defaults + [
        SKIP_COLUMNS_NAMES,
        "--execute=SHOW TABLES FROM `" + self.constants_mock.DATABASE_NAME + "`"
    ]

    subprocess_mock.check_output.side_effect = subprocess.CalledProcessError(
        1, command, "Failed mysqldump")

    self.assertRaises(subprocess.CalledProcessError,
                      sql_wrapper.call_show_tables, self.constants_mock)
    self.assert_subprocess_call(subprocess_mock, command)

  def _get_default_options_from_mock(self):
    defaults = []

    if self.constants_mock.DATABASE_LOGIN_PATH:
      defaults += ["--login-path=" + self.constants_mock.DATABASE_LOGIN_PATH]
    elif self.constants_mock.DO_NOT_USE_CREDENTIALS_FILE:
      defaults += [
          "--user=" + self.constants_mock.DATABASE_USER,
          "--password=" + self.constants_mock.DATABASE_PASSWORD
      ]

    defaults += [
        "--host=" + self.constants_mock.DATABASE_HOSTNAME,
        "--port=" + self.constants_mock.DATABASE_PORT
    ]

    if self.constants_mock.DATABASE_USE_SSL_MODE:
      defaults += [
          "--ssl-mode=REQUIRED",
          "--ssl-ca=" + self.constants_mock.DATABASE_SSL_CA,
          "--ssl-cert=" + self.constants_mock.DATABASE_SSL_CERT,
          "--ssl-key=" + self.constants_mock.DATABASE_SSL_KEY
      ]
    elif self.constants_mock.DATABASE_USE_SSL:
      defaults += [
          "--ssl=1", "--ssl-ca=" + self.constants_mock.DATABASE_SSL_CA,
          "--ssl-cert=" + self.constants_mock.DATABASE_SSL_CERT,
          "--ssl-key=" + self.constants_mock.DATABASE_SSL_KEY
      ]

    return defaults

  def assert_subprocess_call(self,
                             subprocess_mock,
                             subprocess_command,
                             stdin=None):
    subprocess_mock.check_output.assert_called_with(
        subprocess_command, stdin=stdin, stderr=-2)


if __name__ == "__main__":
  unittest.main()
