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
"""Tests for CloudSql Importer."""
from __future__ import absolute_import

import os
import unittest
import unittest.mock as mock
import subprocess

import constants_mock


class CloudSqlImporterTest(unittest.TestCase):

  def setUp(self):
    super(CloudSqlImporterTest, self).setUp()

    self.constants_mock = constants_mock.configure()
    modules = {"cloudsql_importer_constants": self.constants_mock}
    self.modules_patch = mock.patch.dict("sys.modules", modules)
    self.modules_patch.start()

    mock.patch("checkpoint_manager.manage_checkpoint", lambda x: x).start()
    mock.patch("pipeline_manager.manage_pipeline", lambda x: x).start()

    import cloudsql_importer
    self.cloudsql_importer = cloudsql_importer

  def tearDown(self):
    super(CloudSqlImporterTest, self).tearDown()
    self.modules_patch.stop()
    mock.patch.stopall()

  @mock.patch("cloudsql_importer.file_writer")
  @mock.patch("cloudsql_importer.replace_pattern_in_file")
  def test_prepare_secondary_index_script(self, replace_mock, writer_mock):
    test_input = "input.txt"
    test_output = "output.txt"
    test_pattern = {"<test_pattern>": "REPLACED"}
    test_replaced = "Lorem REPLACED Ipsum"

    replace_mock.return_value = test_replaced

    self.cloudsql_importer.prepare_secondary_index_script(
        test_input, test_output, test_pattern)
    replace_mock.assert_called_with(test_input, test_pattern)
    writer_mock.write.assert_called_with(test_output, test_replaced)

  @mock.patch("cloudsql_importer.file_writer")
  @mock.patch("cloudsql_importer.replace_pattern_in_file")
  def test_prepare_secondary_index_script_raises_replace_error(
      self, replace_mock, writer_mock):
    test_input = "input.txt"
    test_output = "output.txt"
    test_pattern = {"<test_pattern>": "REPLACED"}

    replace_mock.side_effect = IOError("Failed to replace")

    self.assertRaises(IOError,
                      self.cloudsql_importer.prepare_secondary_index_script,
                      test_input, test_output, test_pattern)
    replace_mock.assert_called_with(test_input, test_pattern)
    writer_mock.write.assert_not_called()

  @mock.patch("cloudsql_importer.file_writer")
  @mock.patch("cloudsql_importer.replace_pattern_in_file")
  def test_prepare_secondary_index_script_raises_write_error(
      self, replace_mock, writer_mock):
    test_input = "input.txt"
    test_output = "output.txt"
    test_pattern = {"<test_pattern>": "REPLACED"}
    test_replaced = "Lorem 'REPLACED' Ipsum"

    replace_mock.return_value = test_replaced

    writer_mock.write.side_effect = IOError("Failed to replace")

    self.assertRaises(IOError,
                      self.cloudsql_importer.prepare_secondary_index_script,
                      test_input, test_output, test_pattern)
    replace_mock.assert_called_with(test_input, test_pattern)
    writer_mock.write.assert_called_with(test_output, test_replaced)

  @mock.patch("cloudsql_importer.open")
  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_replace_pattern_in_file(self, sql_wrapper_mock, open_mock):
    test_file = "test_file.txt"
    test_replacement = "REPLACED"
    test_pattern = {"<test_pattern>": test_replacement}
    test_contents = "Lorem <test_pattern> Ipsum"
    expected_contents = "Lorem REPLACED Ipsum"

    output_mock = mock.Mock()
    open_mock.return_value.__enter__.return_value = output_mock
    output_mock.read.return_value = test_contents

    sql_wrapper_mock.escape_string.return_value = test_replacement

    replaced = self.cloudsql_importer.replace_pattern_in_file(
        test_file, test_pattern)
    self.assertEqual(expected_contents, replaced)
    open_mock.assert_called_with(test_file, "r")
    output_mock.read.assert_any_call()
    sql_wrapper_mock.escape_string.assert_called_with(
        test_replacement, self.constants_mock.DATABASE_ENCODING)

  @mock.patch("cloudsql_importer.open")
  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_replace_pattern_in_file_escapes_string(self, sql_wrapper_mock,
                                                  open_mock):
    test_file = "test_file.txt"
    test_replacement = "RE'PLA'CED"
    test_pattern = {"<test_pattern>": test_replacement}
    test_contents = "Lorem <test_pattern> Ipsum"
    expected_contents = "Lorem RE\\'PLA\\'CED Ipsum"

    output_mock = mock.Mock()
    open_mock.return_value.__enter__.return_value = output_mock
    output_mock.read.return_value = test_contents

    sql_wrapper_mock.escape_string.return_value = "RE\\'PLA\\'CED"

    replaced = self.cloudsql_importer.replace_pattern_in_file(
        test_file, test_pattern)
    self.assertEqual(expected_contents, replaced)
    open_mock.assert_called_with(test_file, "r")
    output_mock.read.assert_any_call()
    sql_wrapper_mock.escape_string.assert_called_with(
        test_replacement, self.constants_mock.DATABASE_ENCODING)

  @mock.patch("cloudsql_importer.open")
  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_replace_pattern_in_file_multiple_patterns(self, sql_wrapper_mock,
                                                     open_mock):
    test_file = "test_file.txt"
    test_patterns = {
        "<test_pattern>": "REPLACED",
        "<another_test_pattern>": "REPLACED_AGAIN",
        "<yet_another_test_pattern>": "REPLACED_YET_AGAIN"
    }
    test_contents = ("Lorem <test_pattern> Ipsum <another_test_pattern> Dolor"
                     " Sit Amet <yet_another_test_pattern>")
    expected_contents = ("Lorem REPLACED Ipsum REPLACED_AGAIN Dolor Sit Amet"
                         " REPLACED_YET_AGAIN")

    output_mock = mock.Mock()
    open_mock.return_value.__enter__.return_value = output_mock

    output_mock.read.return_value = test_contents

    sql_wrapper_mock.escape_string.side_effect = test_patterns.values()
    wrapper_calls = []
    for pattern in test_patterns.values():
      wrapper_calls.append(
          mock.call(pattern, self.constants_mock.DATABASE_ENCODING))

    replaced = self.cloudsql_importer.replace_pattern_in_file(
        test_file, test_patterns)
    self.assertEqual(expected_contents, replaced)
    open_mock.assert_called_with(test_file, "r")
    output_mock.read.assert_any_call()
    sql_wrapper_mock.escape_string.assert_has_calls(wrapper_calls)

  @mock.patch("cloudsql_importer.open")
  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_replace_pattern_in_file_ignores_non_matching_pattern(
      self, sql_wrapper_mock, open_mock):
    test_file = "test_file.txt"
    test_pattern = {"<wrong_pattern>": "REPLACED"}
    test_replacement = "REPLACED"
    test_contents = "Lorem <test_pattern> Ipsum"

    output_mock = mock.Mock()
    open_mock.return_value.__enter__.return_value = output_mock

    output_mock.read.return_value = test_contents

    sql_wrapper_mock.escape_string.return_value = test_replacement

    not_replaced = self.cloudsql_importer.replace_pattern_in_file(
        test_file, test_pattern)
    self.assertEqual(test_contents, not_replaced)
    open_mock.assert_called_with(test_file, "r")
    output_mock.read.assert_any_call()
    sql_wrapper_mock.escape_string.assert_called_with(
        test_replacement, self.constants_mock.DATABASE_ENCODING)

  @mock.patch("cloudsql_importer.open")
  def test_replace_pattern_in_file_raises_open_error(self, open_mock):
    test_file = "test_file.txt"
    test_pattern = {"<test_pattern>": "REPLACED"}
    test_replacement = "REPLACED"

    open_mock.return_value.__enter__.side_effect = IOError("Failed to open")

    self.assertRaises(IOError, self.cloudsql_importer.replace_pattern_in_file,
                      test_file, test_pattern)
    open_mock.assert_called_with(test_file, "r")

  @mock.patch("cloudsql_importer.open")
  def test_replace_pattern_in_file_raises_read_error(self, open_mock):
    test_file = "test_file.txt"
    test_patterns = {"<test_pattern>": "REPLACED"}
    test_replacement = "REPLACED"

    output_mock = mock.Mock()
    open_mock.return_value.__enter__.return_value = output_mock

    output_mock.read.side_effect = IOError("Failed to read")

    self.assertRaises(IOError, self.cloudsql_importer.replace_pattern_in_file,
                      test_file, test_patterns)
    open_mock.assert_called_with(test_file, "r")
    output_mock.read.assert_any_call()

  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_force_drop_database(self, sql_wrapper_mock):
    self.constants_mock.FORCE_DROP_DATABASE = True

    self.drop_database_successfully(self.cloudsql_importer.force_drop_database,
                                    sql_wrapper_mock)

  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_force_drop_database_raises_sql_error(self, sql_wrapper_mock):
    self.constants_mock.FORCE_DROP_DATABASE = True

    self.drop_database_raises_sql_error(
        self.cloudsql_importer.force_drop_database, sql_wrapper_mock)

  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_force_drop_database_is_skipped_if_disabled(self, sql_wrapper_mock):
    self.constants_mock.FORCE_DROP_DATABASE = False

    self.drop_database_is_skipped_if_disabled(
        self.cloudsql_importer.force_drop_database, sql_wrapper_mock)

  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_fail_if_database_exists(self, sql_wrapper_mock):
    sql_wrapper_mock.call_show_databases_like.return_value = None

    self.cloudsql_importer.fail_if_database_exists()
    sql_wrapper_mock.call_show_databases_like.assert_called_with(
        self.constants_mock)

  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_fail_if_database_exists_raises_error(self, sql_wrapper_mock):
    sql_wrapper_mock.call_show_databases_like.return_value = (
        "Found a table: " + self.constants_mock.DATABASE_NAME)

    self.assertRaises(RuntimeError,
                      self.cloudsql_importer.fail_if_database_exists)
    sql_wrapper_mock.call_show_databases_like.assert_called_with(
        self.constants_mock)

  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_create_database(self, sql_wrapper_mock):
    self.cloudsql_importer.create_database()
    sql_wrapper_mock.call_create_database.assert_called_with(
        self.constants_mock)

  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_create_database_raises_error(self, sql_wrapper_mock):
    sql_wrapper_mock.call_create_database.side_effect = RuntimeError("Failed")

    self.assertRaises(RuntimeError, self.cloudsql_importer.create_database)
    sql_wrapper_mock.call_create_database.assert_called_with(
        self.constants_mock)

  @mock.patch("cloudsql_importer.open")
  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_load_schema(self, sql_wrapper_mock, open_mock):
    test_schema = "my test schema"
    open_mock.return_value.__enter__.return_value = test_schema

    self.cloudsql_importer.load_schema()
    open_mock.assert_called_with(self.constants_mock.SCHEMA_PATH, "r")
    sql_wrapper_mock.call_mysql_with_stdin_command.assert_called_with(
        test_schema, self.constants_mock)

  @mock.patch("cloudsql_importer.open")
  def test_load_schema_raises_file_error(self, open_mock):
    open_mock.return_value.__enter__.side_effect = IOError("Failed file read")

    self.assertRaises(IOError, self.cloudsql_importer.load_schema)
    self.assertRaises(OSError, self.cloudsql_importer.load_schema)
    open_mock.assert_called_with(self.constants_mock.SCHEMA_PATH, "r")

  @mock.patch("cloudsql_importer.open")
  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_load_schema_raises_sql_error(self, sql_wrapper_mock, open_mock):
    test_schema = "my test schema"
    open_mock.return_value.__enter__.return_value = test_schema

    sql_wrapper_mock.call_mysql_with_stdin_command.side_effect = RuntimeError(
        "Failed")

    self.assertRaises(RuntimeError, self.cloudsql_importer.load_schema)
    open_mock.assert_called_with(self.constants_mock.SCHEMA_PATH, "r")
    sql_wrapper_mock.call_mysql_with_stdin_command.assert_called_with(
        test_schema, self.constants_mock)

  @mock.patch("cloudsql_importer.dir_util")
  @mock.patch("cloudsql_importer.os")
  def test_create_temp_folder(self, os_mock, dir_util_mock):
    self.constants_mock.TEMP_FOLDER = "/test/path/"

    os_mock.path.exists.return_value = False

    self.cloudsql_importer.create_temp_folder()
    os_mock.path.exists.assert_called_with(self.constants_mock.TEMP_FOLDER)
    dir_util_mock.mkpath.assert_called_with(self.constants_mock.TEMP_FOLDER)

  @mock.patch("cloudsql_importer.dir_util")
  @mock.patch("cloudsql_importer.os")
  def test_create_temp_folder_not_called_for_empty_path(self, os_mock,
                                                        dir_util_mock):
    self.constants_mock.TEMP_FOLDER = "/existing/test/path"

    os_mock.path.exists.return_value = True

    self.cloudsql_importer.create_temp_folder()
    os_mock.path.exists.assert_called_with(self.constants_mock.TEMP_FOLDER)
    dir_util_mock.mkpath.assert_not_called()

  @mock.patch("cloudsql_importer.dir_util")
  @mock.patch("cloudsql_importer.os")
  def test_create_temp_folder_raises_mkpath_error(self, os_mock, dir_util_mock):
    self.constants_mock.TEMP_FOLDER = "/invalid_!@#%$#%$?///@@@path/"

    os_mock.path.exists.return_value = False
    dir_util_mock.mkpath.side_effect = IOError("Failed to make path")

    self.assertRaises(IOError, self.cloudsql_importer.create_temp_folder)
    os_mock.path.exists.assert_called_with(self.constants_mock.TEMP_FOLDER)
    dir_util_mock.mkpath.assert_called_with(self.constants_mock.TEMP_FOLDER)

  @mock.patch("cloudsql_importer.file_writer")
  @mock.patch("cloudsql_importer.open")
  @mock.patch("cloudsql_importer.prepare_secondary_index_script")
  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_generate_restore_secondary_indexes_commands(
      self, sql_wrapper_mock, prepare_mock, open_mock, writer_mock):
    database_tables = "table1\ntable2\ntable3\n"
    test_tables = ("table1", "table2", "table3")
    sql_wrapper_mock.call_show_tables.return_value = database_tables

    prepare_calls = []
    write_calls = []
    for test_table in test_tables:
      setup_script_path = os.path.join(
          self.constants_mock.SECONDARY_INDEXES_SCRIPTS_FOLDER,
          test_table + self.constants_mock.RESTORE_INDEXES_SCRIPT_SUFFIX)
      prepare_script_patterns = {
          "<source_database_name>": self.constants_mock.DATABASE_NAME,
          "<source_database_table>": test_table
      }
      prepare_calls += [
          mock.call(self.constants_mock.RESTORE_INDEXES_TEMPLATE_PATH,
                    setup_script_path, prepare_script_patterns)
      ]

      restore_indexes_script = "my restore indexes script"
      open_mock.return_value.__enter__.return_value = restore_indexes_script

      options = ["--skip_column_names"]
      restore_commands = "do this to restore indexes"
      sql_wrapper_mock.call_mysql_with_stdin_command.return_value = (
          restore_commands)

      commands_path = os.path.join(
          self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER,
          test_table + self.constants_mock.RESTORE_INDEXES_COMMANDS_SUFFIX)
      write_calls = [mock.call(commands_path, restore_commands)]

    self.cloudsql_importer.generate_restore_secondary_indexes_commands()
    sql_wrapper_mock.call_show_tables.assert_called_with(self.constants_mock)
    prepare_mock.assert_has_calls(prepare_calls)
    open_mock.assert_called_with(setup_script_path, "r")
    sql_wrapper_mock.call_mysql_with_stdin_command.assert_called_with(
        restore_indexes_script, self.constants_mock, options)
    writer_mock.write.assert_has_calls(write_calls)

  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_generate_restore_secondary_indexes_commands_show_tables_error(
      self, sql_wrapper_mock):
    database_tables = "table1\ntable2\ntable3\n"
    sql_wrapper_mock.call_show_tables.side_effect = RuntimeError("Failed")

    self.assertRaises(
        RuntimeError,
        self.cloudsql_importer.generate_restore_secondary_indexes_commands)
    sql_wrapper_mock.call_show_tables.assert_called_with(self.constants_mock)

  @mock.patch("cloudsql_importer.prepare_secondary_index_script")
  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_generate_restore_secondary_indexes_commands_raises_prepare_error(
      self, sql_wrapper_mock, prepare_mock):
    database_tables = "table1\ntable2\ntable3\n"
    sql_wrapper_mock.call_show_tables.return_value = database_tables

    test_table = "table1"
    setup_script_path = os.path.join(
        self.constants_mock.SECONDARY_INDEXES_SCRIPTS_FOLDER,
        test_table + self.constants_mock.RESTORE_INDEXES_SCRIPT_SUFFIX)
    prepare_script_patterns = {
        "<source_database_name>": self.constants_mock.DATABASE_NAME,
        "<source_database_table>": test_table
    }

    prepare_mock.side_effect = IOError("Failed to read file")

    self.assertRaises(
        IOError,
        self.cloudsql_importer.generate_restore_secondary_indexes_commands)
    sql_wrapper_mock.call_show_tables.assert_called_with(self.constants_mock)
    prepare_mock.assert_called_with(
        self.constants_mock.RESTORE_INDEXES_TEMPLATE_PATH, setup_script_path,
        prepare_script_patterns)

  @mock.patch("cloudsql_importer.open")
  @mock.patch("cloudsql_importer.prepare_secondary_index_script")
  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_generate_restore_secondary_indexes_commands_raises_file_error(
      self, sql_wrapper_mock, prepare_mock, open_mock):
    database_tables = "table1\ntable2\ntable3\n"
    sql_wrapper_mock.call_show_tables.return_value = database_tables

    test_table = "table1"
    setup_script_path = os.path.join(
        self.constants_mock.SECONDARY_INDEXES_SCRIPTS_FOLDER,
        test_table + self.constants_mock.RESTORE_INDEXES_SCRIPT_SUFFIX)
    prepare_script_patterns = {
        "<source_database_name>": self.constants_mock.DATABASE_NAME,
        "<source_database_table>": test_table
    }

    open_mock.return_value.__enter__.side_effect = IOError("Failed file read")

    self.assertRaises(
        IOError,
        self.cloudsql_importer.generate_restore_secondary_indexes_commands)
    self.assertRaises(
        OSError,
        self.cloudsql_importer.generate_restore_secondary_indexes_commands)
    sql_wrapper_mock.call_show_tables.assert_called_with(self.constants_mock)
    prepare_mock.assert_called_with(
        self.constants_mock.RESTORE_INDEXES_TEMPLATE_PATH, setup_script_path,
        prepare_script_patterns)
    open_mock.assert_called_with(setup_script_path, "r")

  @mock.patch("cloudsql_importer.open")
  @mock.patch("cloudsql_importer.prepare_secondary_index_script")
  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_generate_restore_secondary_indexes_commands_raises_sql_error(
      self, sql_wrapper_mock, prepare_mock, open_mock):
    database_tables = "table1\ntable2\ntable3\n"
    sql_wrapper_mock.call_show_tables.return_value = database_tables

    test_table = "table1"
    setup_script_path = os.path.join(
        self.constants_mock.SECONDARY_INDEXES_SCRIPTS_FOLDER,
        test_table + self.constants_mock.RESTORE_INDEXES_SCRIPT_SUFFIX)
    prepare_script_patterns = {
        "<source_database_name>": self.constants_mock.DATABASE_NAME,
        "<source_database_table>": test_table
    }

    restore_indexes_script = "my restore indexes script"
    open_mock.return_value.__enter__.return_value = restore_indexes_script

    options = ["--skip_column_names"]
    sql_wrapper_mock.call_mysql_with_stdin_command.side_effect = RuntimeError(
        "Failed")

    self.assertRaises(
        RuntimeError,
        self.cloudsql_importer.generate_restore_secondary_indexes_commands)
    prepare_mock.assert_called_with(
        self.constants_mock.RESTORE_INDEXES_TEMPLATE_PATH, setup_script_path,
        prepare_script_patterns)
    open_mock.assert_called_with(setup_script_path, "r")
    sql_wrapper_mock.call_mysql_with_stdin_command.assert_called_with(
        restore_indexes_script, self.constants_mock, options)

  @mock.patch("cloudsql_importer.open")
  @mock.patch("cloudsql_importer.prepare_secondary_index_script")
  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_drop_secondary_indexes(self, sql_wrapper_mock, prepare_mock,
                                  open_mock):
    setup_script_path = os.path.join(
        self.constants_mock.SECONDARY_INDEXES_SCRIPTS_FOLDER,
        self.constants_mock.DATABASE_NAME +
        self.constants_mock.DROP_INDEXES_SCRIPT_SUFFIX)
    prepare_script_patterns = {
        "<source_database_name>": self.constants_mock.DATABASE_NAME
    }

    drop_indexes_script = "test drop indexes script"
    open_mock.return_value.__enter__.return_value = drop_indexes_script

    sql_wrapper_mock.call_mysql_with_stdin_command.return_value = (
        "drop indexes instructions")

    options = ["--skip_column_names"]
    drop_command = [
        "--execute=" +
        sql_wrapper_mock.call_mysql_with_stdin_command.return_value,
        self.constants_mock.DATABASE_NAME
    ]

    self.cloudsql_importer.drop_secondary_indexes()
    prepare_mock.assert_called_with(
        self.constants_mock.DROP_INDEXES_TEMPLATE_PATH, setup_script_path,
        prepare_script_patterns)
    open_mock.assert_called_with(setup_script_path, "r")
    sql_wrapper_mock.call_mysql_with_stdin_command.assert_called_with(
        drop_indexes_script, self.constants_mock, options)
    sql_wrapper_mock.call_mysql.assert_called_with(drop_command,
                                                   self.constants_mock)

  @mock.patch("cloudsql_importer.prepare_secondary_index_script")
  def test_drop_secondary_indexes_raises_prepare_error(self, prepare_mock):
    setup_script_path = os.path.join(
        self.constants_mock.SECONDARY_INDEXES_SCRIPTS_FOLDER,
        self.constants_mock.DATABASE_NAME +
        self.constants_mock.DROP_INDEXES_SCRIPT_SUFFIX)
    prepare_script_patterns = {
        "<source_database_name>": self.constants_mock.DATABASE_NAME
    }

    prepare_mock.side_effect = IOError("Failed to read file")

    self.assertRaises(IOError, self.cloudsql_importer.drop_secondary_indexes)
    prepare_mock.assert_called_with(
        self.constants_mock.DROP_INDEXES_TEMPLATE_PATH, setup_script_path,
        prepare_script_patterns)

  @mock.patch("cloudsql_importer.open")
  @mock.patch("cloudsql_importer.prepare_secondary_index_script")
  def test_drop_secondary_indexes_raises_file_error(self, prepare_mock,
                                                    open_mock):
    setup_script_path = os.path.join(
        self.constants_mock.SECONDARY_INDEXES_SCRIPTS_FOLDER,
        self.constants_mock.DATABASE_NAME +
        self.constants_mock.DROP_INDEXES_SCRIPT_SUFFIX)
    prepare_script_patterns = {
        "<source_database_name>": self.constants_mock.DATABASE_NAME
    }

    open_mock.return_value.__enter__.side_effect = IOError("Failed file read")

    self.assertRaises(IOError, self.cloudsql_importer.drop_secondary_indexes)
    prepare_mock.assert_called_with(
        self.constants_mock.DROP_INDEXES_TEMPLATE_PATH, setup_script_path,
        prepare_script_patterns)
    open_mock.assert_called_with(setup_script_path, "r")

  @mock.patch("cloudsql_importer.open")
  @mock.patch("cloudsql_importer.prepare_secondary_index_script")
  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_drop_secondary_indexes_raises_subprocess_setup_error(
      self, sql_wrapper_mock, prepare_mock, open_mock):
    setup_script_path = os.path.join(
        self.constants_mock.SECONDARY_INDEXES_SCRIPTS_FOLDER,
        self.constants_mock.DATABASE_NAME +
        self.constants_mock.DROP_INDEXES_SCRIPT_SUFFIX)
    prepare_script_patterns = {
        "<source_database_name>": self.constants_mock.DATABASE_NAME
    }

    drop_indexes_script = "test drop indexes script"
    open_mock.return_value.__enter__.return_value = drop_indexes_script

    options = ["--skip_column_names"]

    sql_wrapper_mock.call_mysql_with_stdin_command.side_effect = RuntimeError(
        "Failed")

    self.assertRaises(RuntimeError,
                      self.cloudsql_importer.drop_secondary_indexes)
    prepare_mock.assert_called_with(
        self.constants_mock.DROP_INDEXES_TEMPLATE_PATH, setup_script_path,
        prepare_script_patterns)
    open_mock.assert_called_with(setup_script_path, "r")
    sql_wrapper_mock.call_mysql_with_stdin_command.assert_called_with(
        drop_indexes_script, self.constants_mock, options)
    sql_wrapper_mock.call_mysql.assert_not_called()

  @mock.patch("cloudsql_importer.open")
  @mock.patch("cloudsql_importer.prepare_secondary_index_script")
  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_drop_secondary_indexes_raises_subprocess_execute_error(
      self, sql_wrapper_mock, prepare_mock, open_mock):
    setup_script_path = os.path.join(
        self.constants_mock.SECONDARY_INDEXES_SCRIPTS_FOLDER,
        self.constants_mock.DATABASE_NAME +
        self.constants_mock.DROP_INDEXES_SCRIPT_SUFFIX)
    prepare_script_patterns = {
        "<source_database_name>": self.constants_mock.DATABASE_NAME
    }

    drop_indexes_script = "test drop indexes script"
    open_mock.return_value.__enter__.return_value = drop_indexes_script

    sql_wrapper_mock.call_mysql_with_stdin_command.return_value = (
        "drop indexes instructions")

    options = ["--skip_column_names"]
    drop_command = [
        "--execute=" +
        sql_wrapper_mock.call_mysql_with_stdin_command.return_value,
        self.constants_mock.DATABASE_NAME
    ]

    sql_wrapper_mock.call_mysql.side_effect = RuntimeError("Failed")

    self.assertRaises(RuntimeError,
                      self.cloudsql_importer.drop_secondary_indexes)
    prepare_mock.assert_called_with(
        self.constants_mock.DROP_INDEXES_TEMPLATE_PATH, setup_script_path,
        prepare_script_patterns)
    open_mock.assert_called_with(setup_script_path, "r")
    sql_wrapper_mock.call_mysql_with_stdin_command.assert_called_with(
        drop_indexes_script, self.constants_mock, options)
    sql_wrapper_mock.call_mysql.assert_called_with(drop_command,
                                                   self.constants_mock)

  @mock.patch("cloudsql_importer.chunker")
  def test_chunk_data(self, chunker_mock):
    chunker_mock.get_files_to_chunk.return_value = [
        "file1.txt", "file2.txt", "file3.txt"
    ]

    chunk_by_line_calls = [
        mock.call("file1.txt", self.constants_mock),
        mock.call("file2.txt", self.constants_mock),
        mock.call("file3.txt", self.constants_mock)
    ]

    self.cloudsql_importer.chunk_data()
    chunker_mock.delete_preexisting_load_group_folders.assert_called_with(
        self.constants_mock)
    chunker_mock.get_files_to_chunk.assert_called_with(self.constants_mock)
    chunker_mock.chunk_by_line.assert_has_calls(chunk_by_line_calls)

  @mock.patch("cloudsql_importer.chunker")
  def test_chunk_data_skips_non_txt_files(self, chunker_mock):
    chunker_mock.get_files_to_chunk.return_value = []

    self.cloudsql_importer.chunk_data()
    chunker_mock.delete_preexisting_load_group_folders.assert_called_with(
        self.constants_mock)
    chunker_mock.get_files_to_chunk.assert_called_with(self.constants_mock)
    chunker_mock.chunk_by_line.assert_not_called()

  @mock.patch("cloudsql_importer.chunker")
  def test_chunk_data_raises_error(self, chunker_mock):
    chunker_mock.get_files_to_chunk.return_value = [
        "file1.txt", "file2.txt", "file3.txt"
    ]

    chunker_mock.chunk_by_line.side_effect = IOError("Failed to chunk file")

    self.assertRaises(IOError, self.cloudsql_importer.chunk_data)
    chunker_mock.delete_preexisting_load_group_folders.assert_called_with(
        self.constants_mock)
    chunker_mock.get_files_to_chunk.assert_called_with(self.constants_mock)
    chunker_mock.chunk_by_line.assert_called_with("file1.txt",
                                                  self.constants_mock)

  @mock.patch("cloudsql_importer.os")
  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_load_data(self, sql_wrapper_mock, os_mock):
    load_group_folders = [
        "test_load_group1", "test_load_group2", "test_load_group3"
    ]
    load_group_files = ["test_file1", "test_file2", "test_file3"]

    options = [
        "--local",
        "--use-threads=" + str(self.constants_mock.DATABASE_IMPORT_THREADS)
    ]

    join_tmp_to_load_group_values = []
    join_load_group_to_file_values = []
    listdir_calls = [mock.call(self.constants_mock.DATABASE_TEMP_FOLDER)]
    join_calls = []
    import_calls = []

    for load_group in load_group_folders:
      join_call = mock.call(self.constants_mock.DATABASE_TEMP_FOLDER,
                            load_group)
      join_calls.append(join_call)
      join_value = self.constants_mock.DATABASE_TEMP_FOLDER + "/" + load_group
      join_tmp_to_load_group_values.append(join_value)

    for load_group_path in join_tmp_to_load_group_values:
      listdir_calls.append(mock.call(load_group_path))
      for data_file in load_group_files:
        join_calls.append(mock.call(load_group_path, data_file))
        join_value = load_group_path + "/" + data_file
        join_load_group_to_file_values.append(join_value)

        import_call = mock.call(join_value, self.constants_mock, options)
        import_calls.append(import_call)

    os_mock.listdir.side_effect = [
        load_group_folders, load_group_files, load_group_files, load_group_files
    ]

    os_mock.path.join.side_effect = (
        join_tmp_to_load_group_values + join_load_group_to_file_values)
    self.cloudsql_importer.load_data()

    os_mock.listdir.assert_has_calls(listdir_calls)
    os_mock.path.join.assert_has_calls(join_calls)
    sql_wrapper_mock.call_mysqlimport.assert_has_calls(import_calls)

  @mock.patch("cloudsql_importer.os")
  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_load_data_ignores_empty_folders(self, sql_wrapper_mock, os_mock):
    load_group_folder = "test_load_group1"
    load_group_folder_path = (
        self.constants_mock.DATABASE_TEMP_FOLDER + "/" + load_group_folder)
    listdir_calls = [
        mock.call(self.constants_mock.DATABASE_TEMP_FOLDER),
        mock.call(load_group_folder_path)
    ]
    os_mock.listdir.side_effect = [[load_group_folder], []]
    os_mock.path.join.return_value = load_group_folder_path

    self.cloudsql_importer.load_data()
    os_mock.listdir.assert_has_calls(listdir_calls)
    os_mock.path.join.assert_called_with(
        self.constants_mock.DATABASE_TEMP_FOLDER, load_group_folder)
    sql_wrapper_mock.assert_not_called()

  @mock.patch("cloudsql_importer.os")
  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_load_data_raises_sql_error(self, sql_wrapper_mock, os_mock):
    data_file = "file1"
    load_group_folder = "test_load_group1"
    load_group_folder_path = (
        self.constants_mock.DATABASE_TEMP_FOLDER + "/" + load_group_folder)
    load_group_data_file_path = load_group_folder_path + "/" + data_file

    listdir_calls = [
        mock.call(self.constants_mock.DATABASE_TEMP_FOLDER),
        mock.call(load_group_folder_path)
    ]
    os_mock.listdir.side_effect = [[load_group_folder], [data_file]]

    join_calls = [
        mock.call(self.constants_mock.DATABASE_TEMP_FOLDER, load_group_folder),
        mock.call(load_group_folder_path, data_file)
    ]
    os_mock.path.join.side_effect = [
        load_group_folder_path, load_group_data_file_path
    ]

    options = [
        "--local",
        "--use-threads=" + str(self.constants_mock.DATABASE_IMPORT_THREADS)
    ]
    sql_wrapper_mock.call_mysqlimport.side_effect = RuntimeError("Failed")

    self.assertRaises(RuntimeError, self.cloudsql_importer.load_data)
    os_mock.listdir.assert_has_calls(listdir_calls)
    os_mock.path.join.assert_has_calls(join_calls)
    sql_wrapper_mock.call_mysqlimport.assert_called_with(
        load_group_data_file_path, self.constants_mock, options)

  @mock.patch("cloudsql_importer.open")
  @mock.patch("cloudsql_importer.os")
  @mock.patch("cloudsql_importer.thread_pool")
  def test_restore_secondary_indexes(self, thread_pool_mock, os_mock,
                                     open_mock):
    commands_and_files = {
        "restore index 1;\nrestore index 2;\n": "file1.sql",
        "restore index 3;\nrestore index 4;\n": "file2.sql",
        "restore index 5;\nrestore index 6;\n": "file3.sql"
    }
    os_mock.listdir.return_value = commands_and_files.values()

    join_calls = []
    join_values = []
    open_calls = []
    file_mocks = []
    for commands in commands_and_files:
      restore_file = commands_and_files[commands]
      restore_path = os.path.join(
          self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER, restore_file)
      join_calls += [
          mock.call(self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER,
                    restore_file)
      ]
      join_values += [restore_path]

      open_calls += [mock.call(restore_path, "r")]

      file_mock = mock.Mock()
      file_mock.read.return_value = commands
      file_mocks += [file_mock]

    os_mock.path.join.side_effect = join_values
    open_mock.side_effect = file_mocks

    pool_mock = mock.Mock()
    thread_pool_mock.return_value = pool_mock
    pool_mock.map.return_value = []

    self.cloudsql_importer.restore_secondary_indexes()
    os_mock.listdir.assert_called_with(
        self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER)
    os_mock.path.join.assert_has_calls(join_calls)
    open_mock.assert_has_calls(open_calls)

    for file_mock in file_mocks:
      file_mock.read.assert_called_once_with()

    thread_pool_mock.assert_called_with(len(commands_and_files.keys()))
    self.assertTrue(pool_mock.map.called)
    self.assertEqual(1, pool_mock.map.call_count)

    for file_mock in file_mocks:
      file_mock.close.assert_called_once_with()

  @mock.patch("cloudsql_importer.open")
  @mock.patch("cloudsql_importer.os")
  def test_restore_secondary_indexes_raises_read_error(self, os_mock,
                                                       open_mock):
    test_file = "file1.sql"
    commands_and_files = {
        "restore index 1;\nrestore index 2;\n": "file1.sql",
        "restore index 3;\nrestore index 4;\n": "file2.sql",
        "restore index 5;\nrestore index 6;\n": "file3.sql"
    }

    restore_path = os.path.join(
        self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER, test_file)

    os_mock.listdir.return_value = [test_file]
    os_mock.path.join.return_value = restore_path

    open_mock.side_effect = IOError("Failed to read")

    self.assertRaises(IOError, self.cloudsql_importer.restore_secondary_indexes)
    os_mock.listdir.assert_called_with(
        self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER)
    os_mock.path.join.assert_called_with(
        self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER, test_file)
    open_mock.assert_called_with(restore_path, "r")

  @mock.patch("cloudsql_importer.handle_restore_secondary_indexes_failures")
  @mock.patch("cloudsql_importer.open")
  @mock.patch("cloudsql_importer.os")
  @mock.patch("cloudsql_importer.thread_pool")
  def test_restore_secondary_indexes_has_pool_errors(
      self, thread_pool_mock, os_mock, open_mock, handler_mock):
    commands_and_files = {
        "restore index 1;\nrestore index 2;\n": "file1.sql",
        "restore index 3;\nrestore index 4;\n": "file2.sql",
        "restore index 5;\nrestore index 6;\n": "file3.sql"
    }
    os_mock.listdir.return_value = commands_and_files.values()

    join_calls = []
    join_values = []
    open_calls = []
    file_mocks = []
    pool_failures = []
    dict_for_handler = {}
    for commands in commands_and_files:
      restore_file = commands_and_files[commands]
      restore_path = os.path.join(
          self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER, restore_file)
      join_calls += [
          mock.call(self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER,
                    restore_file)
      ]
      join_values += [restore_path]

      open_calls += [mock.call(restore_path, "r")]

      dict_for_handler[commands] = restore_path

      file_mock = mock.Mock()
      file_mock.read.return_value = commands
      file_mocks += [file_mock]

      pool_failures += [commands]

    os_mock.path.join.side_effect = join_values
    open_mock.side_effect = file_mocks

    pool_mock = mock.Mock()
    thread_pool_mock.return_value = pool_mock
    pool_mock.map.return_value = pool_failures

    writer_calls = []
    for failure in pool_failures:
      writer_calls += mock.call(
          self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER, failure)

    self.assertRaises(RuntimeError,
                      self.cloudsql_importer.restore_secondary_indexes)
    os_mock.listdir.assert_called_with(
        self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER)
    os_mock.path.join.assert_has_calls(join_calls)
    open_mock.assert_has_calls(open_calls)

    for file_mock in file_mocks:
      file_mock.read.assert_called_once_with()

    thread_pool_mock.assert_called_with(len(commands_and_files.keys()))
    self.assertTrue(pool_mock.map.called)
    self.assertEqual(1, pool_mock.map.call_count)

    for file_mock in file_mocks:
      file_mock.close.assert_called_once_with()

    handler_mock.assert_called_with(pool_failures, dict_for_handler)

  @mock.patch("cloudsql_importer.handle_restore_secondary_indexes_failures")
  @mock.patch("cloudsql_importer.open")
  @mock.patch("cloudsql_importer.os")
  @mock.patch("cloudsql_importer.thread_pool")
  def test_restore_secondary_indexes_raises_handler_error(
      self, thread_pool_mock, os_mock, open_mock, handler_mock):
    commands_and_files = {
        "restore index 1;\nrestore index 2;\n": "file1.sql",
        "restore index 3;\nrestore index 4;\n": "file2.sql",
        "restore index 5;\nrestore index 6;\n": "file3.sql"
    }
    os_mock.listdir.return_value = commands_and_files.values()

    join_calls = []
    join_values = []
    open_calls = []
    file_mocks = []
    pool_failures = []
    dict_for_handler = {}
    for commands in commands_and_files:
      restore_file = commands_and_files[commands]
      restore_path = os.path.join(
          self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER, restore_file)
      join_calls += [
          mock.call(self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER,
                    restore_file)
      ]
      join_values += [restore_path]

      open_calls += [mock.call(restore_path, "r")]

      dict_for_handler[commands] = restore_path

      file_mock = mock.Mock()
      file_mock.read.return_value = commands
      file_mocks += [file_mock]

      pool_failures += [commands]

    os_mock.path.join.side_effect = join_values
    open_mock.side_effect = file_mocks

    pool_mock = mock.Mock()
    thread_pool_mock.return_value = pool_mock
    pool_mock.map.return_value = pool_failures

    handler_mock.side_effect = IOError("Failed")

    self.assertRaises(IOError, self.cloudsql_importer.restore_secondary_indexes)
    os_mock.listdir.assert_called_with(
        self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER)
    os_mock.path.join.assert_has_calls(join_calls)
    open_mock.assert_has_calls(open_calls)

    for file_mock in file_mocks:
      file_mock.read.assert_called_once_with()

    thread_pool_mock.assert_called_with(len(commands_and_files.keys()))
    self.assertTrue(pool_mock.map.called)
    self.assertEqual(1, pool_mock.map.call_count)

    for file_mock in file_mocks:
      file_mock.close.assert_called_once_with()

    handler_mock.assert_called_with(pool_failures, dict_for_handler)

  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_restore(self, sql_wrapper_mock):
    command = "do thingies"
    sql_command = ["--execute=" + command, self.constants_mock.DATABASE_NAME]

    failed_commands = self.cloudsql_importer.restore(command)
    self.assertIsNone(failed_commands)
    sql_wrapper_mock.call_mysql.assert_called_with(sql_command,
                                                   self.constants_mock)

  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_restore_has_errors(self, sql_wrapper_mock):
    command = "do thingies"
    sql_command = ["--execute=" + command, self.constants_mock.DATABASE_NAME]

    sql_wrapper_mock.call_mysql.side_effect = subprocess.CalledProcessError(
        1, sql_command, "Failed")

    failed_commands = self.cloudsql_importer.restore(command)
    self.assertEqual(command, failed_commands)
    sql_wrapper_mock.call_mysql.assert_called_with(sql_command,
                                                   self.constants_mock)

  @mock.patch("cloudsql_importer.file_writer")
  @mock.patch("cloudsql_importer.dir_util")
  @mock.patch("cloudsql_importer.os")
  def test_handle_restore_secondary_indexes_failures(
      self, os_mock, dir_util_mock, writer_mock):
    commands_and_files = {
        "restore index 1;\nrestore index 2;\n": "file1.sql",
        "restore index 3;\nrestore index 4;\n": "file2.sql",
        "restore index 5;\nrestore index 6;\n": "file3.sql"
    }
    failed_commands = [
        "restore index 3;\nrestore index 4;\n",
        "restore index 5;\nrestore index 6;\n"
    ]

    join_calls = []
    join_values = []
    writer_calls = []
    for commands in failed_commands:
      commands_path = os.path.join(
          self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER,
          commands_and_files[commands])

      join_calls += [
          mock.call(self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER,
                    commands_and_files[commands])
      ]
      join_values += [commands_path]

      writer_calls += [mock.call(commands_path, commands)]

    os_mock.path.join.side_effect = join_values

    self.cloudsql_importer.handle_restore_secondary_indexes_failures(
        failed_commands, commands_and_files)
    dir_util_mock.remove_tree.assert_called_with(
        self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER)
    dir_util_mock.mkpath.assert_called_with(
        self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER)
    os_mock.path.join.assert_has_calls(join_calls)
    writer_mock.write.assert_has_calls(writer_calls)

  @mock.patch("cloudsql_importer.dir_util")
  def test_handle_restore_secondary_indexes_failures_raises_directory_error(
      self, dir_util_mock):
    commands_and_files = {
        "restore index 1;\nrestore index 2;\n": "file1.sql",
        "restore index 3;\nrestore index 4;\n": "file2.sql",
        "restore index 5;\nrestore index 6;\n": "file3.sql"
    }
    failed_commands = [
        "restore index 3;\nrestore index 4;\n",
        "restore index 5;\nrestore index 6;\n"
    ]

    dir_util_mock.remove_tree.side_effect = IOError("Failed")

    self.assertRaises(
        IOError,
        self.cloudsql_importer.handle_restore_secondary_indexes_failures,
        failed_commands, commands_and_files)
    dir_util_mock.remove_tree.assert_called_with(
        self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER)
    dir_util_mock.mkpath.assert_not_called()

  @mock.patch("cloudsql_importer.file_writer")
  @mock.patch("cloudsql_importer.dir_util")
  @mock.patch("cloudsql_importer.os")
  def test_handle_restore_secondary_indexes_failures_raises_writer_error(
      self, os_mock, dir_util_mock, writer_mock):
    commands_and_files = {
        "restore index 1;\nrestore index 2;\n": "file1.sql",
        "restore index 3;\nrestore index 4;\n": "file2.sql",
        "restore index 5;\nrestore index 6;\n": "file3.sql"
    }
    failed_commands = ["restore index 1;\nrestore index 2;\n"]

    join_calls = []
    join_values = []
    commands_path = os.path.join(
        self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER,
        commands_and_files[failed_commands[0]])

    for commands in failed_commands:
      join_calls += [
          mock.call(self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER,
                    commands_and_files[commands])
      ]
      join_values += [commands_path]

    os_mock.path.join.side_effect = join_values

    writer_mock.write.side_effect = IOError("Failed")

    self.assertRaises(
        IOError,
        self.cloudsql_importer.handle_restore_secondary_indexes_failures,
        failed_commands, commands_and_files)
    dir_util_mock.remove_tree.assert_called_with(
        self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER)
    dir_util_mock.mkpath.assert_called_with(
        self.constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER)
    os_mock.path.join.assert_has_calls(join_calls)
    writer_mock.write.assert_called_with(commands_path, failed_commands[0])

  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_drop_database(self, sql_wrapper_mock):
    self.constants_mock.DROP_DATABASE = True

    self.drop_database_successfully(self.cloudsql_importer.drop_database,
                                    sql_wrapper_mock)

  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_drop_database_raises_sql_error(self, sql_wrapper_mock):
    self.constants_mock.DROP_DATABASE = True

    self.drop_database_raises_sql_error(self.cloudsql_importer.drop_database,
                                        sql_wrapper_mock)

  @mock.patch("cloudsql_importer.sql_wrapper")
  def test_drop_database_is_skipped_if_disabled(self, sql_wrapper_mock):
    self.constants_mock.DROP_DATABASE = False

    self.drop_database_is_skipped_if_disabled(
        self.cloudsql_importer.drop_database, sql_wrapper_mock)

  @mock.patch("cloudsql_importer.os")
  @mock.patch("cloudsql_importer.shutil")
  def test_cleanup(self, shutil_mock, os_mock):
    os_mock.path.exists.return_value = True

    self.cloudsql_importer.cleanup()
    os_mock.path.exists.assert_called_with(self.constants_mock.TEMP_FOLDER)
    shutil_mock.rmtree.assert_called_with(self.constants_mock.TEMP_FOLDER)

  @mock.patch("cloudsql_importer.os")
  @mock.patch("cloudsql_importer.shutil")
  def test_cleanup_ignores_nonexistent_path(self, shutil_mock, os_mock):
    os_mock.path.exists.return_value = False

    self.cloudsql_importer.cleanup()
    os_mock.path.exists.assert_called_with(self.constants_mock.TEMP_FOLDER)
    shutil_mock.assert_not_called()

  @mock.patch("cloudsql_importer.os")
  @mock.patch("cloudsql_importer.shutil")
  def test_cleanup_raises_deletion_error(self, shutil_mock, os_mock):
    os_mock.path.exists.return_value = True

    shutil_mock.rmtree.side_effect = IOError("Deletion failed")

    self.assertRaises(IOError, self.cloudsql_importer.cleanup)
    os_mock.path.exists.assert_called_with(self.constants_mock.TEMP_FOLDER)
    shutil_mock.rmtree.assert_called_with(self.constants_mock.TEMP_FOLDER)

  def drop_database_successfully(self, drop_func, sql_wrapper_mock):
    drop_func()
    sql_wrapper_mock.call_drop_database.assert_called_with(self.constants_mock)

  def drop_database_raises_sql_error(self, drop_func, sql_wrapper_mock):
    sql_wrapper_mock.call_drop_database.side_effect = RuntimeError("Failed")

    self.assertRaises(RuntimeError, drop_func)
    sql_wrapper_mock.call_drop_database.assert_called_with(self.constants_mock)

  def drop_database_is_skipped_if_disabled(self, drop_func, sql_wrapper_mock):
    drop_func()
    sql_wrapper_mock.assert_not_called()

  def get_file_path(self, file_name):
    return os.path.join("resources", file_name)


if __name__ == "__main__":
  unittest.main()
