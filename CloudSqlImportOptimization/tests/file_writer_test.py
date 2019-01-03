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
"""Tests for File Writer."""
from __future__ import absolute_import

import unittest
import unittest.mock as mock

import file_writer


class FileWriterTest(unittest.TestCase):

  @mock.patch("file_writer.dir_util")
  @mock.patch("file_writer.open")
  @mock.patch("file_writer.os")
  def test_write_file(self, os_mock, open_mock, dir_util_mock):
    file_dirname = "/test_dir/"
    test_file = file_dirname + "test_file.txt"
    test_contents = "Lorem Ipsum"

    os_mock.path.dirname.return_value = file_dirname

    output_mock = mock.Mock()
    open_mock.return_value.__enter__.return_value = output_mock

    file_writer.write(test_file, test_contents)
    os_mock.path.dirname.assert_called_with(test_file)
    dir_util_mock.mkpath.assert_called_with(file_dirname)
    open_mock.assert_called_with(test_file, "w")
    output_mock.write.assert_called_with(test_contents)

  @mock.patch("file_writer.dir_util")
  @mock.patch("file_writer.open")
  @mock.patch("file_writer.os")
  def test_write_file_with_no_path(self, os_mock, open_mock, dir_util_mock):
    file_dirname = ""
    test_file = file_dirname + "test_file.txt"
    test_contents = "Lorem Ipsum"

    os_mock.path.dirname.return_value = file_dirname

    output_mock = mock.Mock()
    open_mock.return_value.__enter__.return_value = output_mock

    file_writer.write(test_file, test_contents)
    os_mock.path.dirname.assert_called_with(test_file)
    dir_util_mock.mkpath.assert_called_with(file_dirname)
    open_mock.assert_called_with(test_file, "w")
    output_mock.write.assert_called_with(test_contents)

  @mock.patch("file_writer.dir_util")
  @mock.patch("file_writer.open")
  @mock.patch("file_writer.os")
  def test_write_file_raises_open_error(self, os_mock, open_mock,
                                        dir_util_mock):
    file_dirname = ""
    test_file = file_dirname + "test_file.txt"
    test_contents = "Lorem Ipsum"

    os_mock.path.dirname.return_value = file_dirname

    open_mock.return_value.__enter__.side_effect = IOError("Failed to open")

    self.assertRaises(IOError, file_writer.write, test_file,
                      test_contents)
    os_mock.path.dirname.assert_called_with(test_file)
    dir_util_mock.mkpath.assert_called_with(file_dirname)
    open_mock.assert_called_with(test_file, "w")

  @mock.patch("file_writer.dir_util")
  @mock.patch("file_writer.open")
  @mock.patch("file_writer.os")
  def test_write_file_raises_write_error(self, os_mock, open_mock,
                                         dir_util_mock):
    file_dirname = ""
    test_file = file_dirname + "test_file.txt"
    test_contents = "Lorem Ipsum"

    os_mock.path.dirname.return_value = file_dirname

    output_mock = mock.Mock()
    open_mock.return_value.__enter__.return_value = output_mock

    output_mock.write.side_effect = IOError("Failed to write")

    self.assertRaises(IOError, file_writer.write, test_file,
                      test_contents)
    os_mock.path.dirname.assert_called_with(test_file)
    dir_util_mock.mkpath.assert_called_with(file_dirname)
    open_mock.assert_called_with(test_file, "w")
    output_mock.write.assert_called_with(test_contents)

if __name__ == "__main__":
  unittest.main()
