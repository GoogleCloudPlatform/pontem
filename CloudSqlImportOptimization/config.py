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
"""Handler to load and save config data to a the provided file."""
from __future__ import absolute_import

import json
import logging


def load_config(config_file_name):
  """Loads a configuration entry from the script's config file.

  The config file is defined in CONFIG_FILE_NAME.
  Args:
    config_file_name: The config file's name.

  Returns:
    dict: The script's configuration.
  """
  logging.info("Loading config from file: %s", config_file_name)
  with open(config_file_name, "r") as config_file:
    config = json.load(config_file)
    logging.debug("Loaded the following config %s", config)

    return config


def save_config(config_file_name, config_data):
  """Saves the provided value to the script's config file.

  The config file is defined in CONFIG_FILE_NAME.
  Args:
    config_file_name: The config file's name.
    config_data: The configuration entry to save in the Config file.
  """
  logging.info("Saving config to file: %s", config_file_name)
  logging.debug("Config to save: %s", config_data)
  with open(config_file_name, "w") as config_file:
    json.dump(
        config_data,
        config_file,
        sort_keys=True,
        indent=4,
        separators=(",", ": "))
