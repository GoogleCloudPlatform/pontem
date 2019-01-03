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
"""Tests for the Checkpoint Manager decorator."""
from __future__ import absolute_import

import unittest
import unittest.mock as mock

import constants_mock

EXPECTED_EXCEPTION_MESSAGE = "Kablamo"


class CheckpointManangerTest(unittest.TestCase):

  def setUp(self):
    super(CheckpointManangerTest, self).setUp()

    self.constants_mock = constants_mock.configure()
    modules = {"cloudsql_importer_constants": self.constants_mock}
    self.patches = mock.patch.dict("sys.modules", modules)
    self.patches.start()

    import checkpoint_manager
    self.checkpoint_manager = checkpoint_manager

    self.expected_checkpoint_for_function = {
        "force_drop_database":
            checkpoint_manager.NO_CHECKPOINT_STAGE,
        "fail_if_database_exists":
            checkpoint_manager.NO_CHECKPOINT_STAGE,
        "create_database":
            checkpoint_manager.NO_CHECKPOINT_STAGE,
        "load_schema":
            checkpoint_manager.NO_CHECKPOINT_STAGE,
        "create_temp_folder":
            checkpoint_manager.NO_CHECKPOINT_STAGE,
        "generate_restore_secondary_indexes_commands":
            checkpoint_manager.NO_CHECKPOINT_STAGE,
        "drop_secondary_indexes":
            checkpoint_manager.NO_CHECKPOINT_STAGE,
        "chunk_data":
            checkpoint_manager.NO_CHECKPOINT_STAGE,
        "load_data":
            checkpoint_manager._build_checkpoint_stage(
                "LOAD_DATA_CHECKPOINT", self.constants_mock.DATABASE_NAME,
                self.constants_mock.TEMP_FOLDER),
        "restore_secondary_indexes":
            checkpoint_manager._build_checkpoint_stage(
                "RESTORE_INDEX_CHECKPOINT", self.constants_mock.DATABASE_NAME,
                self.constants_mock.TEMP_FOLDER),
        "verify_checksum":
            checkpoint_manager._build_checkpoint_stage(
                "VERIFY_CHECKSUM_CHECKPOINT", self.constants_mock.DATABASE_NAME,
                self.constants_mock.TEMP_FOLDER),
        "drop_database":
            checkpoint_manager.NO_CHECKPOINT_STAGE,
        "cleanup":
            checkpoint_manager.NO_CHECKPOINT_STAGE
    }

  def tearDown(self):
    super(CheckpointManangerTest, self).tearDown()
    self.patches.stop()

  def verify_checkpoint_is_saved(self, checkpoint_func_mock):
    for calling_fn, checkpoint in self.expected_checkpoint_for_function.items():
      checkpoint_func_mock.__name__ = calling_fn
      decorated_fn = self.checkpoint_manager.manage_checkpoint(
          checkpoint_func_mock)

      try:
        decorated_fn()
      except RuntimeError as e:
        self.assertEqual(EXPECTED_EXCEPTION_MESSAGE, str(e))
      finally:
        self.assertEqual(checkpoint,
                         self.checkpoint_manager.manage_checkpoint.checkpoint)

  def test_successful_func_saves_checkpoint(self):
    func = mock.Mock()
    self.verify_checkpoint_is_saved(func)

  def test_failing_func_saves_checkpoint(self):
    func = mock.Mock()
    func.side_effect = RuntimeError(EXPECTED_EXCEPTION_MESSAGE)
    self.verify_checkpoint_is_saved(func)

if __name__ == "__main__":
  unittest.main()
