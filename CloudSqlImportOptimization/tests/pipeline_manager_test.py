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
"""Tests for the Pipeline Manager decorator."""
from __future__ import absolute_import

import unittest
import unittest.mock as mock

import pipeline_manager


class PipelineManangerTest(unittest.TestCase):

  def test_successful_pipeline(self):
    func = mock.Mock()
    self.verify_pipeline_execution(pipeline_manager.SUCCESSFUL_CHAIN_LINKS,
                                   func)

  def test_failing_pipeline(self):
    func = mock.Mock()
    func.side_effect = RuntimeError("Kablamo")
    self.verify_pipeline_execution(pipeline_manager.FAILING_CHAIN_LINKS, func)

  def verify_pipeline_execution(self, pipeline_dict, pipeline_stage_mock):
    for calling_fn, next_fn in pipeline_dict.items():
      pipeline_stage_mock.__name__ = calling_fn
      decorated_fn = pipeline_manager.manage_pipeline(pipeline_stage_mock)
      next_pipeline_step = decorated_fn()

      self.assertEqual(next_fn, next_pipeline_step)

if __name__ == "__main__":
  unittest.main()
