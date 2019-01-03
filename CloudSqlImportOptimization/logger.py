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
"""Handles logging configuration."""
from __future__ import absolute_import

import logging
import sys


def configure(verbose):
  """Configures logger for script execution.

  The logger is configured to log into stdout with a user friendly format.
  The log level is adjusted depending on whether the --verbose argument is used
  or not.

  Args:
    verbose: Whether verbose logging should be enabled
  """
  logger = logging.getLogger()

  if logger.handlers:
    logger.handlers.pop()

  stdout_handler = logging.StreamHandler(sys.stdout)

  level = logging.INFO
  if verbose:
    level = logging.DEBUG
  logger.setLevel(level)
  stdout_handler.setLevel(level)

  formatter = logging.Formatter(
      "%(asctime)s - %(levelname)s -  %(filename)s - %(funcName)s:%(lineno)s"
      " - %(message)s")

  stdout_handler.setFormatter(formatter)
  logger.addHandler(stdout_handler)

  logger.debug("Finished configuring logger")
