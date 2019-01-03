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
"""Tests for the Chunker module."""
from __future__ import absolute_import

import unittest
import unittest.mock as mock

import chunker
import constants_mock


class ChunkerTest(unittest.TestCase):

  def setUp(self):
    super(ChunkerTest, self).setUp()
    self.constants_mock = constants_mock.configure()

  @mock.patch("chunker.get_chunk")
  @mock.patch("chunker.file_writer")
  @mock.patch("chunker.open")
  @mock.patch("chunker.os")
  def test_chunk_by_line_with_5_complete_chunks(
      self, os_mock, open_mock, file_writer_mock, get_chunk_mock):
    self.constants_mock.CHUNKS = 5
    expected_file_size = 768 * 1024 * 1024 # 768MB
    chunk_size = 161061274  # expected_size / chunks

    file_name = "chunk_me.txt"
    file_path = self.constants_mock.MYSQL_BACKUP_FOLDER + file_name

    os_mock.path.getsize.return_value = expected_file_size

    file_mock = mock.Mock()
    open_mock.return_value.__enter__.return_value = file_mock

    get_chunk_calls = [
        mock.call(file_mock, chunk_size),
        mock.call(file_mock, chunk_size),
        mock.call(file_mock, chunk_size),
        mock.call(file_mock, chunk_size),
        mock.call(file_mock, chunk_size)
    ]

    get_chunk_mock.side_effect = [("12", chunk_size), ("34", chunk_size),
                                  ("56", chunk_size), ("78", chunk_size),
                                  ("910", chunk_size)]

    join_calls = [
        mock.call(self.constants_mock.MYSQL_BACKUP_FOLDER, file_name),
        mock.call(self.constants_mock.LOAD_GROUP_FOLDER + "1", file_name),
        mock.call(self.constants_mock.LOAD_GROUP_FOLDER + "2", file_name),
        mock.call(self.constants_mock.LOAD_GROUP_FOLDER + "3", file_name),
        mock.call(self.constants_mock.LOAD_GROUP_FOLDER + "4", file_name),
        mock.call(self.constants_mock.LOAD_GROUP_FOLDER + "5", file_name)
    ]

    os_mock.path.join.side_effect = [
        file_path, "load_group1/chunk_me.txt", "load_group2/chunk_me.txt",
        "load_group3/chunk_me.txt", "load_group4/chunk_me.txt",
        "load_group5/chunk_me.txt"
    ]

    file_writer_calls = [
        mock.call("load_group1/chunk_me.txt", "12", "a"),
        mock.call("load_group2/chunk_me.txt", "34", "a"),
        mock.call("load_group3/chunk_me.txt", "56", "a"),
        mock.call("load_group4/chunk_me.txt", "78", "a"),
        mock.call("load_group5/chunk_me.txt", "910", "a")
    ]

    chunker.chunk_by_line(file_name, self.constants_mock)
    os_mock.path.join.assert_has_calls(join_calls)
    os_mock.path.getsize.assert_called_with(file_path)
    get_chunk_mock.assert_has_calls(get_chunk_calls)
    file_writer_mock.write.assert_has_calls(file_writer_calls)

  @mock.patch("chunker.get_chunk")
  @mock.patch("chunker.file_writer")
  @mock.patch("chunker.open")
  @mock.patch("chunker.os")
  def test_chunk_by_line_with_max_buffer_and_5_chunks(
      self, os_mock, open_mock, file_writer_mock, get_chunk_mock):
    self.constants_mock.CHUNKS = 5
    expected_file_size = 3 * 1024 * 1024 * 1024  #  3GB
    chunk_size = 644245095  # expected_size / chunks
    buffer_size = 512 * 1024 * 1024 # 512MB
    buffer_remainder = chunk_size - buffer_size

    file_name = "chunk_me.txt"
    file_path = self.constants_mock.MYSQL_BACKUP_FOLDER + file_name

    os_mock.path.getsize.return_value = expected_file_size

    file_mock = mock.Mock()
    open_mock.return_value.__enter__.return_value = file_mock

    get_chunk_calls = [
        mock.call(file_mock, buffer_size),
        mock.call(file_mock, buffer_remainder),
        mock.call(file_mock, buffer_size),
        mock.call(file_mock, buffer_remainder),
        mock.call(file_mock, buffer_size),
        mock.call(file_mock, buffer_remainder),
        mock.call(file_mock, buffer_size),
        mock.call(file_mock, buffer_remainder),
        mock.call(file_mock, buffer_size),
        mock.call(file_mock, buffer_remainder)
    ]

    get_chunk_mock.side_effect = [("12", buffer_size), ("12", buffer_remainder),
                                  ("34", buffer_size), ("34", buffer_remainder),
                                  ("56", buffer_size), ("56", buffer_remainder),
                                  ("78", buffer_size), ("78", buffer_remainder),
                                  ("910", buffer_size), ("910",
                                                         buffer_remainder)]

    join_calls = [
        mock.call(self.constants_mock.MYSQL_BACKUP_FOLDER, file_name),
        mock.call(self.constants_mock.LOAD_GROUP_FOLDER + "1", file_name),
        mock.call(self.constants_mock.LOAD_GROUP_FOLDER + "1", file_name),
        mock.call(self.constants_mock.LOAD_GROUP_FOLDER + "2", file_name),
        mock.call(self.constants_mock.LOAD_GROUP_FOLDER + "2", file_name),
        mock.call(self.constants_mock.LOAD_GROUP_FOLDER + "3", file_name),
        mock.call(self.constants_mock.LOAD_GROUP_FOLDER + "3", file_name),
        mock.call(self.constants_mock.LOAD_GROUP_FOLDER + "4", file_name),
        mock.call(self.constants_mock.LOAD_GROUP_FOLDER + "4", file_name),
        mock.call(self.constants_mock.LOAD_GROUP_FOLDER + "5", file_name),
        mock.call(self.constants_mock.LOAD_GROUP_FOLDER + "5", file_name)
    ]

    os_mock.path.join.side_effect = [
        file_path, "load_group1/chunk_me.txt", "load_group1/chunk_me.txt",
        "load_group2/chunk_me.txt", "load_group2/chunk_me.txt",
        "load_group3/chunk_me.txt", "load_group3/chunk_me.txt",
        "load_group4/chunk_me.txt", "load_group4/chunk_me.txt",
        "load_group5/chunk_me.txt", "load_group5/chunk_me.txt"
    ]

    file_writer_calls = [
        mock.call("load_group1/chunk_me.txt", "12", "a"),
        mock.call("load_group1/chunk_me.txt", "12", "a"),
        mock.call("load_group2/chunk_me.txt", "34", "a"),
        mock.call("load_group2/chunk_me.txt", "34", "a"),
        mock.call("load_group3/chunk_me.txt", "56", "a"),
        mock.call("load_group3/chunk_me.txt", "56", "a"),
        mock.call("load_group4/chunk_me.txt", "78", "a"),
        mock.call("load_group4/chunk_me.txt", "78", "a"),
        mock.call("load_group5/chunk_me.txt", "910", "a"),
        mock.call("load_group5/chunk_me.txt", "910", "a")
    ]

    chunker.chunk_by_line(file_name, self.constants_mock)
    os_mock.path.join.assert_has_calls(join_calls)
    os_mock.path.getsize.assert_called_with(file_path)
    get_chunk_mock.assert_has_calls(get_chunk_calls)
    file_writer_mock.write.assert_has_calls(file_writer_calls)

  @mock.patch("chunker.get_chunk")
  @mock.patch("chunker.file_writer")
  @mock.patch("chunker.open")
  @mock.patch("chunker.os")
  def test_chunk_by_line_with_3_complete_chunks(
      self, os_mock, open_mock, file_writer_mock, get_chunk_mock):
    self.constants_mock.CHUNKS = 3
    expected_file_size = 768 * 1024 * 1024 # 768MB
    chunk_size = 268435456  # expected_size / chunks

    file_name = "chunk_me.txt"
    file_path = self.constants_mock.MYSQL_BACKUP_FOLDER + file_name

    os_mock.path.getsize.return_value = expected_file_size

    file_mock = mock.Mock()
    open_mock.return_value.__enter__.return_value = file_mock

    get_chunk_calls = [
        mock.call(file_mock, chunk_size),
        mock.call(file_mock, chunk_size),
        mock.call(file_mock, chunk_size)
    ]

    get_chunk_mock.side_effect = [("12", chunk_size), ("34", chunk_size),
                                  ("56", chunk_size)]

    join_calls = [
        mock.call(self.constants_mock.MYSQL_BACKUP_FOLDER, file_name),
        mock.call(self.constants_mock.LOAD_GROUP_FOLDER + "1", file_name),
        mock.call(self.constants_mock.LOAD_GROUP_FOLDER + "2", file_name),
        mock.call(self.constants_mock.LOAD_GROUP_FOLDER + "3", file_name),
    ]

    os_mock.path.join.side_effect = [
        file_path, "load_group1/chunk_me.txt", "load_group2/chunk_me.txt",
        "load_group3/chunk_me.txt"
    ]

    file_writer_calls = [
        mock.call("load_group1/chunk_me.txt", "12", "a"),
        mock.call("load_group2/chunk_me.txt", "34", "a"),
        mock.call("load_group3/chunk_me.txt", "56", "a"),
    ]

    chunker.chunk_by_line(file_name, self.constants_mock)
    os_mock.path.join.assert_has_calls(join_calls)
    os_mock.path.getsize.assert_called_with(file_path)
    get_chunk_mock.assert_has_calls(get_chunk_calls)
    file_writer_mock.write.assert_has_calls(file_writer_calls)

  @mock.patch("chunker.get_chunk")
  @mock.patch("chunker.file_writer")
  @mock.patch("chunker.open")
  @mock.patch("chunker.os")
  def test_chunk_by_line_fails_to_read_file(self, os_mock, open_mock,
                                            file_writer_mock, get_chunk_mock):
    self.constants_mock.CHUNKS = 5
    expected_file_size = 768 * 1024 * 1024 # 768MB

    file_folder = "my/file/path/"
    file_name = "chunk_me.txt"
    file_path = file_folder + file_name

    os_mock.path.join.return_value = file_path
    os_mock.path.getsize.return_value = expected_file_size

    open_mock.return_value.__enter__.side_effect = IOError("Failed to read")

    self.assertRaises(IOError, chunker.chunk_by_line, file_name,
                      self.constants_mock)
    os_mock.path.join.assert_called_with(
        self.constants_mock.MYSQL_BACKUP_FOLDER, file_name)
    os_mock.path.getsize.assert_called_with(file_path)
    get_chunk_mock.assert_not_called()
    file_writer_mock.write.assert_not_called()

  @mock.patch("chunker.get_chunk")
  @mock.patch("chunker.file_writer")
  @mock.patch("chunker.open")
  @mock.patch("chunker.os")
  def test_chunk_by_line_fails_to_write_file(self, os_mock, open_mock,
                                             file_writer_mock, get_chunk_mock):
    self.constants_mock.CHUNKS = 5
    expected_file_size = 768 * 1024 * 1024 # 768MB
    chunk_size = 161061274  # expected_size / chunks

    file_folder = "my/file/path/"
    file_name = "chunk_me.txt"
    file_path = file_folder + file_name

    os_mock.path.getsize.return_value = expected_file_size

    file_mock = mock.Mock()
    open_mock.return_value.__enter__.return_value = file_mock

    get_chunk_mock.return_value = ("12", chunk_size)

    join_calls = [
        mock.call(self.constants_mock.MYSQL_BACKUP_FOLDER, file_name),
        mock.call(self.constants_mock.LOAD_GROUP_FOLDER + "1", file_name),
    ]

    os_mock.path.join.side_effect = [
        file_path, "load_group1/chunk_me.txt", "load_group2/chunk_me.txt",
        "load_group3/chunk_me.txt", "load_group4/chunk_me.txt",
        "load_group5/chunk_me.txt"
    ]

    file_writer_mock.write.side_effect = IOError("Failed to write chunk")

    self.assertRaises(IOError, chunker.chunk_by_line, file_name,
                      self.constants_mock)
    os_mock.path.join.assert_has_calls(join_calls)
    os_mock.path.getsize.assert_called_with(file_path)
    get_chunk_mock.assert_called_with(file_mock, chunk_size)
    file_writer_mock.write.assert_called_with("load_group1/chunk_me.txt", "12",
                                              "a")

  def test_get_chunk(self):
    lines = [
        "This is the first line \\\n",
        "This is the second line \\\n",
        "This is the third line \\\n",
        "This is the last line \n\n",
    ]
    bytes_to_read = len(lines)
    chunk = "".join(lines)
    expected_chunk_tuple = (chunk, len(chunk))

    file_mock = mock.Mock()
    file_mock.readlines.return_value = lines

    actual_chunk_tuple = chunker.get_chunk(file_mock, bytes_to_read)
    self.assertEqual(expected_chunk_tuple, actual_chunk_tuple)
    file_mock.readlines.assert_called_with(bytes_to_read)
    file_mock.readline.assert_not_called()

  def test_get_chunk_keeps_looking_for_end_of_line(self):
    initial_lines = [
        "This is the first line \\\n",
        "This is the second line \\\n",
    ]
    bytes_to_read = len(initial_lines)
    final_lines = [
        "This is the third line \\\n",
        "This is the last line \n\n",
    ]

    chunk = "".join(initial_lines + final_lines)
    expected_chunk_tuple = (chunk, len(chunk))

    file_mock = mock.Mock()
    file_mock.readlines.return_value = initial_lines
    file_mock.readline.side_effect = final_lines

    actual_chunk_tuple = chunker.get_chunk(file_mock, bytes_to_read)
    self.assertEqual(expected_chunk_tuple, actual_chunk_tuple)
    file_mock.readlines.assert_called_with(bytes_to_read)
    file_mock.readline.assert_any_call()

  def test_get_chunk_has_no_data(self):
    lines = []
    bytes_to_read = 1000
    expected_chunk_tuple = ("", 0)

    file_mock = mock.Mock()
    file_mock.readlines.return_value = lines

    actual_chunk_tuple = chunker.get_chunk(file_mock, bytes_to_read)
    self.assertEqual(expected_chunk_tuple, actual_chunk_tuple)
    file_mock.readlines.assert_called_with(bytes_to_read)

  def test_get_chunk_fails_readlines(self):
    bytes_to_read = 1000
    file_mock = mock.Mock()
    file_mock.readlines.side_effect = IOError("Failed to read lines")

    self.assertRaises(IOError, chunker.get_chunk, file_mock, bytes_to_read)
    file_mock.readlines.assert_called_with(bytes_to_read)
    file_mock.readline.assert_not_called()

  @mock.patch("chunker.os")
  @mock.patch("chunker.shutil")
  def test_delete_preexisting_load_group_folders(self, shutil_mock, os_mock):
    exists_calls = []
    rmtree_calls = []
    for i in range(1, self.constants_mock.CHUNKS + 1):
      call = mock.call(self.constants_mock.LOAD_GROUP_FOLDER + str(i))
      exists_calls.append(call)
      rmtree_calls.append(call)

    os_mock.path.exists.return_value = True

    chunker.delete_preexisting_load_group_folders(self.constants_mock)
    os_mock.path.exists.assert_has_calls(exists_calls)
    shutil_mock.rmtree.assert_has_calls(rmtree_calls)

  @mock.patch("chunker.os")
  @mock.patch("chunker.shutil")
  def test_delete_preexisting_load_group_folders_skips_non_existing_folders(
      self, shutil_mock, os_mock):
    exists_calls = []
    for i in range(1, self.constants_mock.CHUNKS + 1):
      call = mock.call(self.constants_mock.LOAD_GROUP_FOLDER + str(i))
      exists_calls.append(call)

    os_mock.path.exists.return_value = False

    chunker.delete_preexisting_load_group_folders(self.constants_mock)
    os_mock.path.exists.assert_has_calls(exists_calls)
    shutil_mock.rmtree.assert_not_called()

  @mock.patch("chunker.os")
  @mock.patch("chunker.shutil")
  def test_delete_preexisting_load_group_folders_fails_to_remove_folder(
      self, shutil_mock, os_mock):
    path = self.constants_mock.LOAD_GROUP_FOLDER + "1"

    os_mock.path.exists.return_value = True

    shutil_mock.rmtree.side_effect = IOError("Failed to delete file")

    self.assertRaises(IOError, chunker.delete_preexisting_load_group_folders,
                      self.constants_mock)
    os_mock.path.exists.assert_called_with(path)
    shutil_mock.rmtree.assert_called_with(path)

  @mock.patch("chunker.os")
  def test_get_files_to_chunk(self, os_mock):

    files = ["file1.txt", "file2.txt", "file3.txt"]
    os_mock.listdir.return_value = files

    splittext_calls = []
    for backup_file in os_mock.listdir.return_value:
      splittext_calls.append(mock.call(backup_file))
      os_mock.path.splitext.return_value = ("file", ".txt")

    actual_files = chunker.get_files_to_chunk(self.constants_mock)
    self.assertEqual(files, actual_files)
    os_mock.listdir.assert_called_with(self.constants_mock.MYSQL_BACKUP_FOLDER)
    os_mock.path.splitext.assert_has_calls(splittext_calls)

  @mock.patch("chunker.os")
  def test_get_files_to_chunk_has_no_files(self, os_mock):

    os_mock.listdir.return_value = []

    files = chunker.get_files_to_chunk(self.constants_mock)
    self.assertFalse(files)
    os_mock.listdir.assert_called_with(self.constants_mock.MYSQL_BACKUP_FOLDER)
    os_mock.path.splitext.assert_not_called()

  @mock.patch("chunker.os")
  def test_get_files_to_chunk_skips_non_txt_files(self, os_mock):
    os_mock.listdir.return_value = ["file1.gif", "file2.crc", "file3"]

    splittext_calls = []
    for backup_file in os_mock.listdir.return_value:
      splittext_calls.append(mock.call(backup_file))
    os_mock.path.splitext.side_effect = [("file1", ".gif"), ("file2", ".crc"),
                                         ("file3", "")]

    files = chunker.get_files_to_chunk(self.constants_mock)
    self.assertFalse(files)
    os_mock.listdir.assert_called_with(self.constants_mock.MYSQL_BACKUP_FOLDER)
    os_mock.path.splitext.assert_has_calls(splittext_calls)

  @mock.patch("chunker.os")
  def test_get_files_to_chunk_fails_to_read_files(self, os_mock):
    os_mock.listdir.side_effect = IOError("Failed to read files")

    splittext_calls = []
    for backup_file in os_mock.listdir.return_value:
      splittext_calls.append(mock.call(backup_file))
      os_mock.path.splitext.return_value = ("file", ".abc")

    self.assertRaises(IOError, chunker.get_files_to_chunk, self.constants_mock)
    os_mock.listdir.assert_called_with(self.constants_mock.MYSQL_BACKUP_FOLDER)
    os_mock.path.splitext.assert_not_called()


if __name__ == "__main__":
  unittest.main()
