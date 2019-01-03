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
"""Args used by MySqlExport and CloudSqlImport pipelines."""
from __future__ import absolute_import

import argparse


def _create_shared_args(parser):
  """Defines shared command line arguments for both importer and exporter."""

  parser.add_argument(
      "database_name", help="The name of the MySQL database being processed.")

  parser.add_argument(
      "-de",
      "--database-encoding",
      default="utf-8",
      help="Your database enconding. Please use a Python compatible name if you"
      " don't use defaults."
  )

  parser.add_argument(
      "-hn",
      "--hostname",
      default="localhost",
      help="Your database Hostname/ IP address.")

  parser.add_argument(
      "-po", "--port", default=3306, type=int, help="Your database port.")

  parser.add_argument(
      "-sm",
      "--ssl-mode-required",
      action="store_true",
      help="Whether to configure the SSL Mode to be required. Please note that"
      " this option takes precedence over --ssl and is not available in all"
      " MySQL client implementations.")

  parser.add_argument(
      "-s",
      "--ssl",
      action="store_true",
      help="Whether to use SSL option to require secure connections. Please "
      " note that this option is superseded by --ssl-mode-required and, that it"
      " has been removed from MySQL 8.0 and over clients.")

  parser.add_argument(
      "-ca",
      "--ssl-ca",
      default="server-ca.pem",
      help="The CA certificate please note that to enable CloudSQL connections"
      " it should be stored in a text file called 'server-ca.pem'. WARNING: "
      " This option will only be read when used alongside --ssl or"
      " --ssl-mode-required.")

  parser.add_argument(
      "-cert",
      "--ssl-cert",
      default="client-cert.pem",
      help="The client's Public Key certificate please note that to enable"
      " CloudSQL connections it should be stored in a text file called"
      " 'client-cert.pem'. WARNING: This option will only be read when used"
      " alongside --ssl or --ssl-mode-required.")

  parser.add_argument(
      "-key",
      "--ssl-key",
      default="client-key.pem",
      help="The client's Private Key please note that to enable CloudSQL"
      " connections it should be stored in a text file called 'client-key.pem'."
      " WARNING: This option will only be read when used alongside --ssl or"
      " --ssl-mode-required.")

  parser.add_argument(
      "-lp",
      "--login-path",
      default="",
      help="The login path to read from your MySQL Option File. WARNING: This"
      " argument takes precedence over --do-not-use-credentials-file.")

  parser.add_argument(
      "-no-conf",
      "--do-not-use-credentials-file",
      action="store_true",
      help="Whether credentials should be loaded from the credentials config"
      " file or args. Note: user / password will be ignored unless this arg is"
      " also used.")

  parser.add_argument("-u", "--user", default="", help="Your database user")

  parser.add_argument(
      "-p",
      "--password",
      default="",
      help="Your instance Database password."
      " WARNING: Using this argument will leak your password to the process"
      " viewer and our application logs. Please prefer using MySQL's"
      " credentials config file.")

  parser.add_argument(
      "-v",
      "--verbose",
      action="store_true",
      help="Whether verbose logging should be used.")


def _create_importer_args(parser):
  """Defines importer specific command line arguments."""
  parser.add_argument(
      "backup_path", help="The directory for your MySQL database backup files.")

  parser.add_argument(
      "-t",
      "--temp-folder",
      default="/tmp/import",
      help="The temporary directory for the Importer's intermediate files."
      " The temp path will be created if it doesn't already exist, any"
      " pre-existing files will be ignored unless the script is resuming from a"
      " checkpointed stage.")

  parser.add_argument(
      "-ch",
      "--chunks",
      type=int,
      default=5,
      help="The number of chunks your data files will be split into")

  parser.add_argument(
      "-d",
      "--drop-database",
      action="store_true",
      help="Use this option to delete the imported CloudSQL database if there"
      " is an import failure. Please note that the if the failure occurs during"
      " the data load phase or later the database won't be dropped as this is"
      " considered a checkpointable state and failures can be retried going"
      " forward.")

  parser.add_argument(
      "-fd",
      "--force-drop-database",
      action="store_true",
      help="Drops any preexisting database that matches the name provided in"
      " 'database-name' before beginning the import processing.")

  parser.add_argument(
      "-dnc",
      "--do-not-continue",
      action="store_true",
      help="Use this option if you do not wish to continue from a checkpointed"
      " stage but instead prefer a fresh run.")

  parser.add_argument(
      "-vc",
      "--verify-checksum",
      action="store_true",
      help="Verifies that the checksum files provided in the MySQL database"
      " backup match the checksum for the newly imported CloudSQL tables."
      " Please note that dumping checksum information can be a lengthy process."
  )


def _create_exporter_args(parser):
  """Defines exporter specific command line arguments."""
  parser.add_argument(
      "output_folder",
      help="The out folder path for the exported MySQL instance backup files")

  parser.add_argument(
      "-t",
      "--temp-folder",
      default="/tmp/exporter",
      help="The temporary directory for the Exporter's dumped files. The temp"
      " path will be created if it doesn't already exist, any pre-existing"
      " database files for the database will be deleted and, once all files are"
      " dumped the contents will be moved to the 'output_folder'. Warning: "
      " Prefer a folder inside /tmp/ as the MySQL user needs to have write"
      " access to the temp directories which can be programatically configured"
      "inside temp. If you use folders outside of temp you'll need to ensure"
      " the proper permissions to run SELECT INTO OUTFILE are present. Also"
      " note that some MySQL implementations, such as MariaDB have config"
      " defaults that prevent writing to home or system directories.")

  parser.add_argument(
      "-sd",
      "--skip-date",
      action="store_true",
      help="Skips printing date and time of dump in the exported sql files.")

  parser.add_argument(
      "-c",
      "--checksum",
      action="store_true",
      help="Whether to store checksum information for the exported MySQL"
      " tables. Please note that dumping checksum information can be a lengthy"
      "process.")

  parser.add_argument(
      "-sfp",
      "--using-secure-file-priv",
      action="store_true",
      help="Denotes that the MySQL instance is running using --secure-file-priv"
      " directories. MySQLDump will only be able to write to these directories"
      " and even locks down subdirectories. Given this, we override the"
      " Exporter's default behavior of creating a subdirectory for the exported"
      " database and we just dump the data inside the provided 'output_folder'"
      " ensuring that no access violations occur.")


def create_importer_parser():
  """Creates a parser with all the necessary arguments to perform imports."""
  parser = argparse.ArgumentParser()
  _create_shared_args(parser)
  _create_importer_args(parser)

  return parser


def create_exporter_parser():
  """Creates a parser with all the necessary arguments to perform exports."""
  parser = argparse.ArgumentParser()
  _create_shared_args(parser)
  _create_exporter_args(parser)

  return parser
