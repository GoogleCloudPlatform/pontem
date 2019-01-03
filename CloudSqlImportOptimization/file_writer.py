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
"""Module to append or write files used in both import and export operations."""
from __future__ import absolute_import
from distutils import dir_util

import logging
import os


def write(filename, file_contents, mode="w"):
  """Writes the provided contents into a file.

  Args:
    filename: The name of the file to write
    file_contents: The contents to write into the file
    mode: The mode used by the context manager
  """
  file_path = os.path.dirname(filename)
  logging.debug("Writing file: %s", filename)
  dir_util.mkpath(file_path)
  with open(filename, mode) as output:
    output.write(file_contents)
