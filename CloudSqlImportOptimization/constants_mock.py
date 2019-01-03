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
"""Builds and configures the Constants Mock."""
from __future__ import absolute_import

import os
import unittest.mock as mock


def configure(constants_mock=None):
  constants_mock = constants_mock or mock.Mock()
  constants_mock.DATABASE_NAME = "test_database"
  constants_mock.DATABASE_ENCODING = "utf-8"

  constants_mock.DATABASE_HOSTNAME = "test_hostname"
  constants_mock.DATABASE_PORT = "123"

  constants_mock.DATABASE_USE_SSL = False
  constants_mock.DATABASE_USE_SSL_MODE = False
  constants_mock.DATABASE_SSL_CA = "test_ca"
  constants_mock.DATABASE_SSL_CERT = "test_cert"
  constants_mock.DATABASE_SSL_KEY = "test_key"

  constants_mock.DATABASE_USER = "test_user"
  constants_mock.DATABASE_PASSWORD = "test_password"
  constants_mock.DO_NOT_USE_CREDENTIALS_FILE = True
  constants_mock.DATABASE_LOGIN_PATH = None

  constants_mock.MYSQL_BACKUP_FOLDER = "/test_backup_folder/"
  constants_mock.MYSQL_EXPORT_DATABASE_OUTPUT_FOLDER = os.path.join(
      constants_mock.MYSQL_BACKUP_FOLDER, constants_mock.DATABASE_NAME)

  constants_mock.SCHEMA_FILE_SUFFIX = "_schema.sql"
  constants_mock.TEMP_FOLDER = "/test/temp/path/root/"
  constants_mock.DATABASE_TEMP_FOLDER = os.path.join(
      constants_mock.TEMP_FOLDER, constants_mock.DATABASE_NAME)

  constants_mock.SECONDARY_INDEXES_SCRIPTS_FOLDER = "test_indexes_script_folder"
  constants_mock.DROP_INDEXES_TEMPLATE_PATH = "test_drop_setup.sql"
  constants_mock.DROP_INDEXES_SCRIPT_SUFFIX = "_test_drop_setup.sql"

  constants_mock.RESTORE_INDEXES_TEMPLATE_PATH = "test_restore_setup.sql"
  constants_mock.RESTORE_INDEXES_SCRIPT_SUFFIX = "_test_restore_setup.sql"
  constants_mock.RESTORE_INDEXES_COMMANDS_FOLDER = "test_restore_folder"
  constants_mock.RESTORE_INDEXES_COMMANDS_SUFFIX = "_test_restore_commands.sql"

  constants_mock.CHUNKS = 10
  constants_mock.CHUNKED_FILE_SUFFIX = "_test_chunked"
  constants_mock.CHUNKED_FILE_SUFFIX_LENGTH = 3

  constants_mock.DATABASE_IMPORT_THREADS = str(100)
  constants_mock.LOAD_GROUP_PREFIX = "test_load_group"
  constants_mock.LOAD_GROUP_FOLDER = (
      constants_mock.LOAD_GROUP_PREFIX + "_folder")

  constants_mock.SKIP_DATE = False
  constants_mock.USING_SECURE_FILE_PRIV = False

  return constants_mock
