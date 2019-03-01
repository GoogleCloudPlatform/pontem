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
"""Decorator to handle the execution pipeline for CloudSQL imports."""
from __future__ import absolute_import

import functools
import logging

SUCCESSFUL_CHAIN_LINKS = {
    "force_drop_database":
        "fail_if_database_exists",
    "fail_if_database_exists":
        "create_database",
    "create_database":
        "load_schema",
    "load_schema":
        "create_temp_folder",
    "create_temp_folder":
        "generate_restore_secondary_indexes_commands",
    "generate_restore_secondary_indexes_commands":
        "drop_secondary_indexes",
    "drop_secondary_indexes":
        "chunk_data",
    "chunk_data":
        "load_data",
    "load_data":
        "restore_secondary_indexes",
    "restore_secondary_indexes":
        "verify_checksum",
    "verify_checksum":
        "drop_database",
    "drop_database":
        "cleanup",
    "cleanup":
        None
}

FAILING_CHAIN_LINKS = {
    "force_drop_database":
        None,
    "fail_if_database_exists":
        None,
    "create_database":
        "drop_database",
    "load_schema":
        "drop_database",
    "create_temp_folder":
        "drop_database",
    "generate_restore_secondary_indexes_commands":
        "drop_database",
    "drop_secondary_indexes":
        "drop_database",
    "chunk_data":
        "drop_database",
    "load_data":
        None,
    "restore_secondary_indexes":
        None,
    "verify_checksum":
        None,
    "drop_database":
        "cleanup",
    "cleanup":
        None
}


def manage_pipeline(func):
  """Decorates a function by executing it and returning the next one to call.

  If execution is successful then the next function in the pipeline is returned.
  If if fails the corresponding failure functions will be returned to ensure
  that intermediate state is cleaned  or saved in the case we've reached a
  checkpointable function.

  Args:
    func: The function to decorate.

  Returns:
    wrapper: The decorator wrapper.
  """

  @functools.wraps(func)
  def wrapper():
    try:
      func()
      return SUCCESSFUL_CHAIN_LINKS[func.__name__]
    except Exception as e:
      logging.exception("Caught exception while executing '%s'", func.__name__)

      if hasattr(e, "output"):
        logging.error("Exception output: %s", e.output)

      return FAILING_CHAIN_LINKS[func.__name__]

  return wrapper
