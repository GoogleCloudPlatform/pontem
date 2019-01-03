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
"""Tests for MySql Exporter."""
from __future__ import absolute_import

import os
import unittest
import unittest.mock as mock

import constants_mock


class MySqlExporterTest(unittest.TestCase):

  def setUp(self):
    super(MySqlExporterTest, self).setUp()

    self.constants_mock = constants_mock.configure()
    modules = {"mysql_exporter_constants": self.constants_mock}
    self.patches = mock.patch.dict("sys.modules", modules)
    self.patches.start()

    import mysql_exporter
    self.mysql_exporter = mysql_exporter

  def tearDown(self):
    super(MySqlExporterTest, self).tearDown()
    self.patches.stop()

  @mock.patch("mysql_exporter._delete_directory")
  @mock.patch("mysql_exporter.dir_util")
  @mock.patch("mysql_exporter.os")
  def test_setup_database_folder_in_output_path(self, os_mock, dir_util_mock,
                                                delete_mock):
    test_umask = "123"

    os_mock.umask.return_value = test_umask

    umask_calls = [mock.call(0), mock.call(test_umask)]

    self.mysql_exporter.setup_database_folder_in_output_path()
    delete_mock.assert_called_with(self.constants_mock.TEMP_FOLDER)
    os_mock.umask.assert_has_calls(umask_calls)
    dir_util_mock.mkpath.assert_called_with(self.constants_mock.TEMP_FOLDER,
                                            0o757, 1)
    os_mock.umask.assert_called_with(test_umask)

  @mock.patch("mysql_exporter._delete_directory")
  @mock.patch("mysql_exporter.dir_util")
  @mock.patch("mysql_exporter.os")
  def test_setup_database_folder_in_output_path_file_priv_ignores_setup(
      self, os_mock, dir_util_mock, delete_mock):
    self.constants_mock.USING_SECURE_FILE_PRIV = True

    self.mysql_exporter.setup_database_folder_in_output_path()
    delete_mock.assert_not_called()
    os_mock.umask.assert_not_called()
    dir_util_mock.mkpath.assert_not_called()

  @mock.patch("mysql_exporter._delete_directory")
  @mock.patch("mysql_exporter.dir_util")
  @mock.patch("mysql_exporter.os")
  def test_setup_database_folder_in_output_path_raises_delete_error(
      self, os_mock, dir_util_mock, delete_mock):
    delete_mock.side_effect = IOError("Failed")
    self.assertRaises(IOError,
                      self.mysql_exporter.setup_database_folder_in_output_path)

    delete_mock.assert_called_with(self.constants_mock.TEMP_FOLDER)
    os_mock.umask.assert_not_called()
    dir_util_mock.mkpath.assert_not_called()

  @mock.patch("mysql_exporter._delete_directory")
  @mock.patch("mysql_exporter.dir_util")
  @mock.patch("mysql_exporter.os")
  def test_setup_database_folder_in_output_path_raises_umask_error(
      self, os_mock, dir_util_mock, delete_mock):
    test_umask = "123"

    os_mock.umask.return_value = test_umask

    dir_util_mock.mkpath.side_effect = IOError("Mkpath failed")

    umask_calls = [mock.call(0), mock.call(test_umask)]

    self.assertRaises(IOError,
                      self.mysql_exporter.setup_database_folder_in_output_path)
    delete_mock.assert_called_with(self.constants_mock.TEMP_FOLDER)
    os_mock.umask.assert_has_calls(umask_calls)
    dir_util_mock.mkpath.assert_called_with(self.constants_mock.TEMP_FOLDER,
                                            0o757, 1)
    os_mock.umask.assert_called_with(test_umask)

  @mock.patch("mysql_exporter.file_writer")
  @mock.patch("mysql_exporter.os")
  @mock.patch("mysql_exporter.sql_wrapper")
  def test_export_schema(self, sql_wrapper_mock, os_mock, writer_mock):
    options = ["--no-data"]
    sql_wrapper_mock.call_mysqldump.return_value = "my exported schema"

    database_schema_file = (
        self.constants_mock.DATABASE_NAME +
        self.constants_mock.SCHEMA_FILE_SUFFIX)
    os_mock.path.join.return_value = (
        self.constants_mock.TEMP_FOLDER + "/" + database_schema_file)

    self.mysql_exporter.export_schema()
    sql_wrapper_mock.call_mysqldump.assert_called_with(self.constants_mock,
                                                       options)
    os_mock.path.join.assert_called_with(self.constants_mock.TEMP_FOLDER,
                                         database_schema_file)
    writer_mock.write.assert_called_with(
        os_mock.path.join.return_value,
        sql_wrapper_mock.call_mysqldump.return_value)

  @mock.patch("mysql_exporter.file_writer")
  @mock.patch("mysql_exporter.sql_wrapper")
  def test_export_schema_raises_sql_error(self, sql_wrapper_mock, writer_mock):
    options = ["--no-data"]
    sql_wrapper_mock.call_mysqldump.side_effect = RuntimeError("Dump Failed")

    self.assertRaises(RuntimeError, self.mysql_exporter.export_schema)
    sql_wrapper_mock.call_mysqldump.assert_called_with(self.constants_mock,
                                                       options)
    writer_mock.write.assert_not_called()

  @mock.patch("mysql_exporter.file_writer")
  @mock.patch("mysql_exporter.sql_wrapper")
  def test_export_schema_raises_write_error(self, sql_wrapper_mock,
                                            writer_mock):
    options = ["--no-data"]
    sql_wrapper_mock.call_mysqldump.return_value = "my exported schema"

    expected_schema_path = os.path.join(
        self.constants_mock.TEMP_FOLDER,
        self.constants_mock.DATABASE_NAME + "_schema.sql")

    writer_mock.write.side_effect = IOError("Write failed")

    self.assertRaises(IOError, self.mysql_exporter.export_schema)
    sql_wrapper_mock.call_mysqldump.assert_called_with(self.constants_mock,
                                                       options)
    writer_mock.write.assert_called_with(
        expected_schema_path, sql_wrapper_mock.call_mysqldump.return_value)

  @mock.patch("mysql_exporter.sql_wrapper")
  def test_export_data(self, sql_wrapper_mock):
    options = ["--order-by-primary", "--tab=" + self.constants_mock.TEMP_FOLDER]

    self.mysql_exporter.export_data()
    sql_wrapper_mock.call_mysqldump.assert_called_with(self.constants_mock,
                                                       options)

  @mock.patch("mysql_exporter.sql_wrapper")
  def test_export_data_raises_sql_error(self, sql_wrapper_mock):
    options = ["--order-by-primary", "--tab=" + self.constants_mock.TEMP_FOLDER]

    sql_wrapper_mock.call_mysqldump.side_effect = RuntimeError("Dump failed")

    self.assertRaises(RuntimeError, self.mysql_exporter.export_data)
    sql_wrapper_mock.call_mysqldump.assert_called_with(self.constants_mock,
                                                       options)

  @mock.patch("mysql_exporter._delete_directory")
  @mock.patch("mysql_exporter.shutil")
  def test_copy_from_temp_to_output(self, shutil_mock, delete_mock):
    self.mysql_exporter.copy_from_temp_to_output()
    delete_mock.assert_called_with(
        self.constants_mock.MYSQL_EXPORT_DATABASE_OUTPUT_FOLDER)
    shutil_mock.move.assert_called_with(
        self.constants_mock.TEMP_FOLDER,
        self.constants_mock.MYSQL_EXPORT_DATABASE_OUTPUT_FOLDER)

  @mock.patch("mysql_exporter._delete_directory")
  @mock.patch("mysql_exporter.shutil")
  def test_copy_from_temp_to_output_priv_ignores_copy(self, shutil_mock,
                                                      delete_mock):
    self.constants_mock.USING_SECURE_FILE_PRIV = True

    self.mysql_exporter.copy_from_temp_to_output()
    delete_mock.assert_not_called()
    shutil_mock.assert_not_called()

  @mock.patch("mysql_exporter._delete_directory")
  @mock.patch("mysql_exporter.shutil")
  def test_copy_from_temp_to_output_raises_delete_error(self, shutil_mock,
                                                        delete_mock):
    delete_mock.side_effect = IOError("Failed")

    self.assertRaises(IOError, self.mysql_exporter.copy_from_temp_to_output)
    delete_mock.assert_called_with(
        self.constants_mock.MYSQL_EXPORT_DATABASE_OUTPUT_FOLDER)
    shutil_mock.move.assert_not_called()

  @mock.patch("mysql_exporter._delete_directory")
  @mock.patch("mysql_exporter.shutil")
  def test_copy_from_temp_to_output_raises_move_error(self, shutil_mock,
                                                      delete_mock):
    shutil_mock.move.side_effect = IOError("Failed")

    self.assertRaises(IOError, self.mysql_exporter.copy_from_temp_to_output)
    delete_mock.assert_called_with(
        self.constants_mock.MYSQL_EXPORT_DATABASE_OUTPUT_FOLDER)
    shutil_mock.move.assert_called_with(
        self.constants_mock.TEMP_FOLDER,
        self.constants_mock.MYSQL_EXPORT_DATABASE_OUTPUT_FOLDER)

  @mock.patch("mysql_exporter.os")
  @mock.patch("mysql_exporter.shutil")
  def test_delete_directory(self, shutil_mock, os_mock):
    test_directory = "/test/directory/"
    os_mock.path.exists.return_value = True

    self.mysql_exporter._delete_directory(test_directory)
    os_mock.path.exists.assert_called_with(test_directory)
    shutil_mock.rmtree.assert_called_with(test_directory)

  @mock.patch("mysql_exporter.os")
  @mock.patch("mysql_exporter.shutil")
  def test_cleanup_ignores_nonexistent_path(self, shutil_mock, os_mock):
    test_directory = "/test/directory/"
    os_mock.path.exists.return_value = False

    self.mysql_exporter._delete_directory(test_directory)
    os_mock.path.exists.assert_called_with(test_directory)
    shutil_mock.assert_not_called()

  @mock.patch("mysql_exporter.os")
  @mock.patch("mysql_exporter.shutil")
  def test_cleanup_raises_deletion_error(self, shutil_mock, os_mock):
    test_directory = "/test/directory/"
    os_mock.path.exists.return_value = True
    shutil_mock.rmtree.side_effect = IOError("Deletion failed")

    self.assertRaises(IOError, self.mysql_exporter._delete_directory, test_directory)
    os_mock.path.exists.assert_called_with(test_directory)
    shutil_mock.rmtree.assert_called_with(test_directory)


if __name__ == "__main__":
  unittest.main()
