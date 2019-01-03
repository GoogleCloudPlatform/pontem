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
"""Decorator to handle checkpoint management for CloudSQL imports."""
from __future__ import absolute_import

import functools
import logging
import cloudsql_importer_constants

NO_CHECKPOINT_STAGE = {
    "checkpoint_stage": "NO_CHECKPOINT",
    "checkpoint_database_name": None,
    "checkpoint_temp_folder": None
}


def _build_checkpoint_stage(checkpoint_stage, database_name, tmp_folder):
  """Builds a custom checkpoint stage dict based on the provided data."""
  checkpoint = {
      "checkpoint_stage": checkpoint_stage,
      "checkpoint_database_name": database_name,
      "checkpoint_temp_folder": tmp_folder
  }

  return checkpoint


def get_checkpoint_stage_for_function(func_name):
  """Returns the Checkpoint stage for the provided Function name.

  Wraps the checkpoint_for_function dict so as to avoid early initialization
  when constants have not yet been configured.

  Args:
    func_name: Name of the function that we want to checkpoint

  Returns:
    The checkpoint stage i.e. the data needed to restore execution.
  """
  checkpoint_for_function = {
      "force_drop_database":
          NO_CHECKPOINT_STAGE,
      "fail_if_database_exists":
          NO_CHECKPOINT_STAGE,
      "create_database":
          NO_CHECKPOINT_STAGE,
      "load_schema":
          NO_CHECKPOINT_STAGE,
      "create_temp_folder":
          NO_CHECKPOINT_STAGE,
      "generate_restore_secondary_indexes_commands":
          NO_CHECKPOINT_STAGE,
      "drop_secondary_indexes":
          NO_CHECKPOINT_STAGE,
      "chunk_data":
          NO_CHECKPOINT_STAGE,
      "load_data":
          _build_checkpoint_stage("LOAD_DATA_CHECKPOINT",
                                  cloudsql_importer_constants.DATABASE_NAME,
                                  cloudsql_importer_constants.TEMP_FOLDER),
      "restore_secondary_indexes":
          _build_checkpoint_stage("RESTORE_INDEX_CHECKPOINT",
                                  cloudsql_importer_constants.DATABASE_NAME,
                                  cloudsql_importer_constants.TEMP_FOLDER),
      "verify_checksum":
          _build_checkpoint_stage("VERIFY_CHECKSUM_CHECKPOINT",
                                  cloudsql_importer_constants.DATABASE_NAME,
                                  cloudsql_importer_constants.TEMP_FOLDER),
      "drop_database":
          NO_CHECKPOINT_STAGE,
      "cleanup":
          NO_CHECKPOINT_STAGE
  }

  return checkpoint_for_function[func_name]


INITIAL_FUNCTION_FROM_CHECKPOINT_STAGE = {
    "NO_CHECKPOINT": "force_drop_database",
    "LOAD_DATA_CHECKPOINT": "load_data",
    "RESTORE_INDEX_CHECKPOINT": "restore_secondary_indexes",
    "VERIFY_CHECKSUM_CHECKPOINT": "verify_checksum"
}


def manage_checkpoint(func):
  """Decorates a function to store the execution's checkpoint state.

  This is a stateful decorator used to hold the state that needs to be saved
  into the Checkpoint configuration file in order for execution to
  restart/resume if there's a runtime failure.

  Args:
    func: The function to decorate.

  Returns:
    wrapper: The decorator wrapper.
  """
  manage_checkpoint.checkpoint = NO_CHECKPOINT_STAGE

  @functools.wraps(func)
  def wrapper():
    try:
      func()
    finally:
      stage_to_save = get_checkpoint_stage_for_function(func.__name__)
      logging.debug("Updated checkpoint stage to be saved '%s'", stage_to_save)
      manage_checkpoint.checkpoint = stage_to_save

  return wrapper
