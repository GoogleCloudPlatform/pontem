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
"""Module that holds chunking functions used in import operations."""
from __future__ import absolute_import

import logging
import math
import os
import shutil

import file_writer


def chunk_by_line(data_file_name, constants):
  """Chunks provided files in a memory efficient manner.

  Chunks files based on constants.CHUNKS, the chunks are
  processed by reading data into a ~512MB buffer and appending the retrieved
  data into the chunk file. When the file reaches it's max chunk size the
  destination folder is rotated and a new file is written.

  Note: Small data_files may end up being chunked in less files than requested
  as readlines byte hint usually reads more bytes than requested. Nevertheless,
  these chunked files should contain all the data present in the original file.

  Args:
    data_file_name: The name of the file being chunked
    constants: The importer / exporter constants
  """
  data_file_path = os.path.join(constants.MYSQL_BACKUP_FOLDER, data_file_name)
  data_file_size = os.path.getsize(data_file_path)

  chunk_size = (data_file_size / constants.CHUNKS)
  chunk_size = int(math.ceil(chunk_size))

  max_buffer_size = 512 * 1024 * 1024 # 512MB
  buffer_size = min(max_buffer_size, chunk_size)
  total_written_bytes = 0
  written_chunk_bytes = 0
  load_group_index = 1

  with open(data_file_path, "r") as data_file:
    while total_written_bytes < data_file_size:
      if written_chunk_bytes >= chunk_size:
        load_group_index += 1
        written_chunk_bytes = 0

      remaining_bytes_in_chunk = chunk_size - written_chunk_bytes
      bytes_to_read = min(buffer_size, remaining_bytes_in_chunk)

      chunk, chunk_bytes = get_chunk(data_file, bytes_to_read)
      total_written_bytes += chunk_bytes
      written_chunk_bytes += chunk_bytes

      chunked_file_path = os.path.join(
          constants.LOAD_GROUP_FOLDER +
          str(load_group_index), data_file_name)

      file_writer.write(chunked_file_path, chunk, "a")


def get_chunk(data_file, bytes_to_read_hint):
  """Reads bytes from data file and returns a storable chunk.

  The function reads the hinted bytes from the provided mysqldump file if the
  read file contains mysqldump's line continuation we continue reading until a
  line termination is found. The resulting list is converted into an array and
  returned.

  Args:
    data_file: The file to read data from.
    bytes_to_read_hint: (int) The byte hint used by file_object.readlines()

  Returns:
    The read lines in string format.
  """
  chunk = data_file.readlines(bytes_to_read_hint)

  # mysqldump uses '\\' to denote that the following line is actually a
  # continuation but python appends '\n' to any line end therefore, we end up
  # with '\\\n' and keep looking till we find the real termination '\n'.
  mysqldump_line_continuation = "\\\n"
  while chunk and chunk[-1].endswith(mysqldump_line_continuation):
    line = data_file.readline()
    chunk.append(line)

  chunk = "".join(chunk)
  return (chunk, len(chunk))


def delete_preexisting_load_group_folders(constants):
  """Deletes any pre-existing content in the load_group folders.

  Chunked data is buffered so as not to excessively consume memory and chunks
  need to be appended when saving therefore, we clean-up the destination folder
  so as to ensure that the new chunks are not appended to old data.

  Args:
      constants: The importer / exporter constants
  """
  logging.debug("Cleaning load group folders")
  for i in range(1, constants.CHUNKS + 1):
    load_group_path = (
        constants.LOAD_GROUP_FOLDER + str(i))
    if os.path.exists(load_group_path):
      shutil.rmtree(load_group_path)


def get_files_to_chunk(constants):
  """Returns the files that need to be chunked from mysql's backup folder.

  Args:
      constants: The importer / exporter constants
  """
  files_to_chunk = []

  for backup_file in os.listdir(
      constants.MYSQL_BACKUP_FOLDER):
    logging.debug("Processing file: %s", backup_file)
    _, backup_file_extension = os.path.splitext(backup_file)

    if backup_file_extension == ".txt":
      files_to_chunk.append(backup_file)
    else:
      logging.debug(
          "Skipping file that does not have a txt extension. File: %s",
          backup_file_extension)

  return files_to_chunk
