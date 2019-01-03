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
"""Constants used by the MySql export pipeline.

  The following variables are considered constants please refrain of using them
  as mutable global state.
"""
from __future__ import absolute_import

import os
import sql_args

parser = sql_args.create_exporter_parser()
args = parser.parse_args()

### Constants
CHECKSUM_FILE_SUFFIX = "_checksum.chk"
SCHEMA_FILE_SUFFIX = "_schema.sql"

### Args / Config based constants
DATABASE_NAME = args.database_name
DATABASE_ENCODING = args.database_encoding

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

### Optional features
CHECKSUM = args.checksum
VERBOSE = args.verbose
SKIP_DATE = args.skip_date
USING_SECURE_FILE_PRIV = args.using_secure_file_priv

### Path constants
if USING_SECURE_FILE_PRIV:
  TEMP_FOLDER = args.output_folder
  MYSQL_EXPORT_DATABASE_OUTPUT_FOLDER = args.output_folder
else:
  TEMP_FOLDER = args.temp_folder
  MYSQL_EXPORT_DATABASE_OUTPUT_FOLDER = os.path.join(args.output_folder,
                                                     DATABASE_NAME)
