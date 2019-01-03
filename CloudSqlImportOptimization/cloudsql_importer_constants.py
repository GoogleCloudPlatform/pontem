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
"""Constants used by the CloudSQL import pipeline.

  The following variables are considered constants please refrain of using them
  as mutable global state.
"""
from __future__ import absolute_import

import os
import config
import sql_args

parser = sql_args.create_importer_parser()
args = parser.parse_args()


def _get_execution_info():
  """Determines whether execution should resume from a checkpoint or not."""
  initial_execution_config = config.load_config(CHECKPOINT_CONFIG_FILE_NAME)

  if args.do_not_continue:
    initial_execution_stage = NO_CHECKPOINT
  else:
    initial_execution_stage = initial_execution_config["checkpoint_stage"]

  if initial_execution_stage == NO_CHECKPOINT:
    database_name = args.database_name
    temp_folder = args.temp_folder
  else:
    database_name = initial_execution_config["checkpoint_database_name"]
    temp_folder = initial_execution_config["checkpoint_temp_folder"]

  return (initial_execution_stage, database_name, temp_folder)


### Constants
DATABASE_IMPORT_THREADS = str(10)

CHECKPOINT_CONFIG_FILE_NAME = "checkpoint_state.json"
CHECKSUM_FILE_SUFFIX = "_checksum.chk"
CHUNKED_FILE_SUFFIX = "_chunked"
SCHEMA_FILE_SUFFIX = "_schema.sql"
NO_CHECKPOINT = "NO_CHECKPOINT"

SECONDARY_INDEXES_SCRIPT_TEMPLATES_FOLDER = "secondary_indexes_script_templates"

DROP_INDEXES_TEMPLATE_FILE = "drop_secondary_indexes_setup.sql"
DROP_INDEXES_SCRIPT_SUFFIX = "_" + DROP_INDEXES_TEMPLATE_FILE

RESTORE_INDEXES_TEMPLATE_FILE = "restore_secondary_indexes_setup.sql"
RESTORE_INDEXES_SCRIPT_SUFFIX = "_" + RESTORE_INDEXES_TEMPLATE_FILE
RESTORE_INDEXES_COMMANDS_SUFFIX = "_restore_secondary_indexes_commands.sql"

LOAD_GROUP_PREFIX = "load_group"

### Args / Config based constants
DATABASE_ENCODING =  args.database_encoding

DATABASE_HOSTNAME = args.hostname
DATABASE_PORT = str(args.port)

DATABASE_USE_SSL = args.ssl
DATABASE_USE_SSL_MODE = args.ssl_mode_required
DATABASE_SSL_CA = args.ssl_ca
DATABASE_SSL_CERT = args.ssl_cert
DATABASE_SSL_KEY = args.ssl_key

DATABASE_USER = args.user
DATABASE_LOGIN_PATH = args.login_path
DATABASE_PASSWORD = args.password
DO_NOT_USE_CREDENTIALS_FILE = args.do_not_use_credentials_file

CHUNKS = args.chunks

INITIAL_EXECUTION_STAGE, DATABASE_NAME, TEMP_FOLDER = _get_execution_info()
MYSQL_BACKUP_FOLDER = args.backup_path

### Optional features
DROP_DATABASE = args.drop_database
FORCE_DROP_DATABASE = args.force_drop_database
VERIFY_CHECKSUM = args.verify_checksum
VERBOSE = args.verbose

### Constructed path constants
SCHEMA_PATH = os.path.join(MYSQL_BACKUP_FOLDER,
                           DATABASE_NAME + SCHEMA_FILE_SUFFIX)
DATABASE_TEMP_FOLDER = os.path.join(TEMP_FOLDER, DATABASE_NAME)

LOAD_GROUP_FOLDER = os.path.join(DATABASE_TEMP_FOLDER, LOAD_GROUP_PREFIX)

SECONDARY_INDEXES_SCRIPTS_FOLDER = os.path.join(DATABASE_TEMP_FOLDER,
                                                "secondary_indexes_scripts")
DROP_INDEXES_TEMPLATE_PATH = os.path.join(
    SECONDARY_INDEXES_SCRIPT_TEMPLATES_FOLDER, DROP_INDEXES_TEMPLATE_FILE)
RESTORE_INDEXES_TEMPLATE_PATH = os.path.join(
    SECONDARY_INDEXES_SCRIPT_TEMPLATES_FOLDER, RESTORE_INDEXES_TEMPLATE_FILE)
RESTORE_INDEXES_COMMANDS_FOLDER = os.path.join(DATABASE_TEMP_FOLDER,
                                               "restore_indexes_commands")
