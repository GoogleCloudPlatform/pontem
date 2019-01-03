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
r"""Script that optimizes importing MySQL backups into a CloudSQL instance.

This  script leverages a set of techniques: dropping indexes before a data load,
chunking data to be loaded, using parallel load via threads to optimize import
times when using mysqlimport. This greatly improves import times by avoiding
the long tail effect wherein importing a group of tables of different sizes in
parallel with one table being considerably larger than the others can take as
long as it would take to load largest table alone

Sample usage:
    $ ./cloudsql_importer.py <database_name> <path_to_mysql_backups_folder> \
      --hostname=<cloudsql_hostname> --port=<cloudsql_port>
"""
from __future__ import absolute_import
from distutils import dir_util
from multiprocessing.dummy import Pool as thread_pool

import logging
import os
import re
import shutil
import subprocess

import checkpoint_manager
import checksum
import chunker
import cloudsql_importer_constants
import config
import file_writer
import logger
import pipeline_manager
import sql_wrapper


### Helper functions
def prepare_secondary_index_script(input_file, output_file, replacements):
  """Creates secondary index scripts to be run against CloudSQL.

  Replaces the <source_database_name> placeholder if it's in the provided file
  and stores the new version in the path defined by the output_file parameter.

  Args:
    input_file: The file that contains the pattern to replace
    output_file: The file to write
    replacements: A dict containing the patterns to replace as keys and the
      actual text as values.
  """
  replaced_text = replace_pattern_in_file(input_file, replacements)
  file_writer.write(output_file, replaced_text)


def replace_pattern_in_file(input_file, replacements):
  """Given a file, replace the provided patterns and return the result string.

  Args:
    input_file: The file that contains the pattern to replace
    replacements: A dict containing the patterns to replace as keys and the
      actual text as values.

  Returns:
    A string with all the regex replacements.
  """
  with open(input_file, "r") as original:
    logging.debug("Replacing patterns in file '%s'", input_file)
    replaced_text = original.read()

    for pattern in replacements:
      replacement = replacements[pattern]
      escaped_replacement = sql_wrapper.escape_string(
          replacement, cloudsql_importer_constants.DATABASE_ENCODING)

      logging.debug("Replacing pattern '%s' with '%s'", pattern,
                    escaped_replacement)

      replaced_text = re.sub(pattern, escaped_replacement, replaced_text)

    return replaced_text


### Top Level Functions
@pipeline_manager.manage_pipeline
@checkpoint_manager.manage_checkpoint
def force_drop_database():
  """Drops the CloudSQL copy of the user's database.

  Use this when you want to ensure there's no copy of the database present
  before starting the import process.
  """
  drop_database_if_condition(cloudsql_importer_constants.FORCE_DROP_DATABASE)


@pipeline_manager.manage_pipeline
@checkpoint_manager.manage_checkpoint
def fail_if_database_exists():
  """Verifies that the user provided database name is not in use.

  Raises:
    RuntimeError: If database already exists.
  """
  logging.info("Checking if database %s exists",
               cloudsql_importer_constants.DATABASE_NAME)

  databases_matching_name = sql_wrapper.call_show_databases_like(
      cloudsql_importer_constants)

  if databases_matching_name and (
      cloudsql_importer_constants.DATABASE_NAME in databases_matching_name):
    logging.debug("Matching database(s): %s", databases_matching_name)
    raise RuntimeError("Tried to start a new Import operation in an already"
                       " existing database")


@pipeline_manager.manage_pipeline
@checkpoint_manager.manage_checkpoint
def create_database():
  """Creates the provided database name in CloudSQL."""
  logging.info("Creating CloudSQL database")
  sql_wrapper.call_create_database(cloudsql_importer_constants)


@pipeline_manager.manage_pipeline
@checkpoint_manager.manage_checkpoint
def load_schema():
  """Ingests the backed up databases schema into the CloudSQL copy."""
  logging.info("Loading schema backup into database")
  logging.debug("Schema file located in '%s'",
                cloudsql_importer_constants.SCHEMA_PATH)

  with open(cloudsql_importer_constants.SCHEMA_PATH,
            "r") as database_schema_file:
    sql_wrapper.call_mysql_with_stdin_command(database_schema_file,
                                              cloudsql_importer_constants)


@pipeline_manager.manage_pipeline
@checkpoint_manager.manage_checkpoint
def create_temp_folder():
  logging.info("Checking if temp folder creation is necessary")
  temp_folder = cloudsql_importer_constants.TEMP_FOLDER

  if not os.path.exists(temp_folder):
    logging.debug("Creating temp folder in '%s'", temp_folder)
    dir_util.mkpath(temp_folder)
  else:
    logging.debug("Temp folder already exists in '%s'", temp_folder)


@pipeline_manager.manage_pipeline
@checkpoint_manager.manage_checkpoint
def generate_restore_secondary_indexes_commands():
  """Saves the SQL commands that will be run to restore secondary indexes.

  Before dropping the secondary indexes to make data ingestion faster we save a
  list of SQL files containing the commands that will need to be run to restore
  said indexes once the data loading stage is finished. We save the restore
  commands for each table separately so that we can later execute them in
  parallel.
  """
  logging.info("Saving commands needed to restore secondary indexes")
  raw_tables = sql_wrapper.call_show_tables(cloudsql_importer_constants)
  database_tables = filter(bool, raw_tables.split("\n"))

  for table_name in database_tables:
    replacements = {
        "<source_database_name>": cloudsql_importer_constants.DATABASE_NAME,
        "<source_database_table>": table_name
    }

    restore_indexes_script_path = os.path.join(
        cloudsql_importer_constants.SECONDARY_INDEXES_SCRIPTS_FOLDER,
        table_name + cloudsql_importer_constants.RESTORE_INDEXES_SCRIPT_SUFFIX)

    prepare_secondary_index_script(
        cloudsql_importer_constants.RESTORE_INDEXES_TEMPLATE_PATH,
        restore_indexes_script_path, replacements)

    with open(restore_indexes_script_path, "r") as restore_indexes_script:
      options = ["--skip_column_names"]
      restore_indexes_commands_to_run = (
          sql_wrapper.call_mysql_with_stdin_command(
              restore_indexes_script, cloudsql_importer_constants, options))

      restore_indexes_commands_path = os.path.join(
          cloudsql_importer_constants.RESTORE_INDEXES_COMMANDS_FOLDER,
          table_name +
          cloudsql_importer_constants.RESTORE_INDEXES_COMMANDS_SUFFIX)

      file_writer.write(restore_indexes_commands_path,
                        restore_indexes_commands_to_run)


@pipeline_manager.manage_pipeline
@checkpoint_manager.manage_checkpoint
def drop_secondary_indexes():
  """Drops secondary indexes from the imported schema.

  This is done to speed up the import process as having extra indexes slow
  down ingestion.
  """
  logging.info("Dropping secondary indexes")
  replacements = {
      "<source_database_name>": cloudsql_importer_constants.DATABASE_NAME
  }

  drop_indexes_script_path = os.path.join(
      cloudsql_importer_constants.SECONDARY_INDEXES_SCRIPTS_FOLDER,
      cloudsql_importer_constants.DATABASE_NAME +
      cloudsql_importer_constants.DROP_INDEXES_SCRIPT_SUFFIX)

  prepare_secondary_index_script(
      cloudsql_importer_constants.DROP_INDEXES_TEMPLATE_PATH,
      drop_indexes_script_path, replacements)

  with open(drop_indexes_script_path, "r") as drop_indexes_script:
    options = ["--skip_column_names"]
    drop_indexes_commands_to_run = sql_wrapper.call_mysql_with_stdin_command(
        drop_indexes_script, cloudsql_importer_constants, options)

    logging.debug(
        "Executing the following SQL commands to drop secondary"
        " indexes '%s'", drop_indexes_commands_to_run)

    command = [
        "--execute=" + drop_indexes_commands_to_run,
        cloudsql_importer_constants.DATABASE_NAME
    ]
    sql_wrapper.call_mysql(command, cloudsql_importer_constants)


@pipeline_manager.manage_pipeline
@checkpoint_manager.manage_checkpoint
def chunk_data():
  """Chunks the table data to import and groups them for ingestion.

  The chunks are organized into load group folders where load_group1 contains
  the first chunk for all tables to import and so on. This allows for all chunks
  in a load group to be ingested in parallel by mysqlimport.
  """
  logging.info("Chunking data to import")
  logging.debug("Using the temp folder located in: %s",
                cloudsql_importer_constants.DATABASE_TEMP_FOLDER)

  chunker.delete_preexisting_load_group_folders(cloudsql_importer_constants)
  files_to_chunk = chunker.get_files_to_chunk(cloudsql_importer_constants)

  for file_to_chunk in files_to_chunk:
    logging.info("Chunking file: %s", file_to_chunk)
    chunker.chunk_by_line(file_to_chunk, cloudsql_importer_constants)


@pipeline_manager.manage_pipeline
@checkpoint_manager.manage_checkpoint
def load_data():
  """Imports chunked data in parallel into CloudSql by using mysqlimport.

  Iterates over each numbered load group folder and runs mysqlimport for each
  chunked table data file in the folder. The mysqlimport command uses
  parallelism to speed-up file ingestion.
  """

  logging.info("Loading chunked data into database")

  load_groups = [
      tmp_file for tmp_file in os.listdir(
          cloudsql_importer_constants.DATABASE_TEMP_FOLDER)
      if tmp_file.startswith(cloudsql_importer_constants.LOAD_GROUP_PREFIX)
  ]

  load_group_folder_paths = [
      os.path.join(cloudsql_importer_constants.DATABASE_TEMP_FOLDER, load_group)
      for load_group in load_groups
  ]

  for load_group_folder_path in load_group_folder_paths:
    logging.info("Importing data for load group: %s", load_group_folder_path)

    for chunked_table_data in os.listdir(load_group_folder_path):
      chunked_table_data_path = os.path.join(load_group_folder_path,
                                             chunked_table_data)
      logging.info("Loading chunked data for file: %s", chunked_table_data_path)

      options = [
          "--local",
          "--use-threads=" + cloudsql_importer_constants.DATABASE_IMPORT_THREADS
      ]
      load_output = sql_wrapper.call_mysqlimport(
          chunked_table_data_path, cloudsql_importer_constants, options)

      logging.info(load_output)


@pipeline_manager.manage_pipeline
@checkpoint_manager.manage_checkpoint
def restore_secondary_indexes():
  """Restores dropped secondary indexes.

  This is done to replicate the original databases' index configuration. Indexes
  commands are stored in files by table name so that each table's indexes can be
  loaded in parallel.

  Raises:
    RuntimeError: If restoring any secondary index failed.
  """
  logging.info("Restoring secondary indexes")

  restore_files = []
  commands_and_files = {}
  for restore_table_indexes_file in os.listdir(
      cloudsql_importer_constants.RESTORE_INDEXES_COMMANDS_FOLDER):
    restore_path = os.path.join(
        cloudsql_importer_constants.RESTORE_INDEXES_COMMANDS_FOLDER,
        restore_table_indexes_file)
    logging.debug("Reading restore commands from file '%s'", restore_path)

    restore_file = open(restore_path, "r")
    commands = restore_file.read()
    restore_files += [restore_file]

    commands_and_files[commands] = restore_path

  pool = thread_pool(len(commands_and_files.keys()))
  failed_commands = pool.map(restore, commands_and_files.keys())
  failed_commands = [command for command in failed_commands if command]

  for restore_file in restore_files:
    restore_file.close()

  if failed_commands:
    handle_restore_secondary_indexes_failures(failed_commands,
                                              commands_and_files)
    raise RuntimeError("Not all secondary indexes were properly restored.")


def restore(command):
  """Restores  indexes for a specific table.

  Args:
    command: The command/s to run.

  Returns:
    If execution fails commands is returned.
  """
  logging.debug("Executing restore command(s):\n%s", command)

  try:
    sql_command = [
        "--execute=" + command, cloudsql_importer_constants.DATABASE_NAME
    ]
    sql_wrapper.call_mysql(sql_command, cloudsql_importer_constants)
  except subprocess.CalledProcessError as e:
    logging.error("Failed to restore secondary index: %s", e.output)
    return command


def handle_restore_secondary_indexes_failures(failed_commands,
                                              commands_and_files):
  """Handles Restore Secondary Indexes failures.

  Logs out the list of commands that failed to be executed; deletes and
  recreates restore secondary indexes folder and saves the newly failing
  commands in case the user attempts to retry the operation.

  Args:
    failed_commands: List of groups of failing commands.
    commands_and_files: Dict containing the commands were executed and their
      filesystem path.
  """

  logging.warning("Not all secondary indexes were restored.")
  dir_util.remove_tree(
      cloudsql_importer_constants.RESTORE_INDEXES_COMMANDS_FOLDER)
  dir_util.mkpath(cloudsql_importer_constants.RESTORE_INDEXES_COMMANDS_FOLDER)

  for failed_commands_group in failed_commands:
    failed_commands_path = os.path.join(
        cloudsql_importer_constants.RESTORE_INDEXES_COMMANDS_FOLDER,
        commands_and_files[failed_commands_group])

    logging.warning(
        "One or more of the following restore indexes commands"
        " failed:\n%s", failed_commands_group)
    logging.info(
        "The failed restore indexes commands will be saved in order to"
        " be retried in a follow-up execution. If these keep failing or"
        " you just wish to skip processing them delete the contents of file"
        " '%s'", failed_commands_path)

    file_writer.write(failed_commands_path, failed_commands_group)


@pipeline_manager.manage_pipeline
@checkpoint_manager.manage_checkpoint
def drop_database():
  """Drops the CloudSQL copy of the user's database.

  Use this when the import process fails before reaching a checkpointable
  stage.
  """
  drop_database_if_condition(cloudsql_importer_constants.DROP_DATABASE)


@pipeline_manager.manage_pipeline
@checkpoint_manager.manage_checkpoint
def verify_checksum():
  """Verfies that the backup checksum matches the imported data checksum.

  If verification fails we halt the temp file cleanup so the user can re-check
  the checksum files and decide how to proceed.
  """
  if cloudsql_importer_constants.VERIFY_CHECKSUM:
    checksumed_tables = checksum.get_checksum(cloudsql_importer_constants)

    checksum_file_name = (
        cloudsql_importer_constants.DATABASE_NAME +
        cloudsql_importer_constants.CHECKSUM_FILE_SUFFIX)

    import_checksum_path = os.path.join(
        cloudsql_importer_constants.DATABASE_TEMP_FOLDER, checksum_file_name)
    backup_checksum_path = os.path.join(
        cloudsql_importer_constants.MYSQL_BACKUP_FOLDER, checksum_file_name)

    file_writer.write(import_checksum_path, checksumed_tables)

    checksum.compare_checksum_files(import_checksum_path, backup_checksum_path)
  else:
    logging.info("Skipping checksum verification")


def drop_database_if_condition(condition):
  """Drops the CloudSQL copy of the user's database if the condition is met."""
  if condition:
    logging.info("Dropping CloudSQL database")

    sql_wrapper.call_drop_database(cloudsql_importer_constants)
  else:
    logging.info("Condition not met therefore, not dropping the database.")


@pipeline_manager.manage_pipeline
@checkpoint_manager.manage_checkpoint
def cleanup():
  """Removes the temp folder and all it's contents."""
  temp_root = cloudsql_importer_constants.TEMP_FOLDER
  logging.info("Removing contents from temp folder '%s'", temp_root)

  if os.path.exists(temp_root):
    shutil.rmtree(temp_root)


def main():
  logger.configure(cloudsql_importer_constants.VERBOSE)

  next_func_name = checkpoint_manager.INITIAL_FUNCTION_FROM_CHECKPOINT_STAGE[
      cloudsql_importer_constants.INITIAL_EXECUTION_STAGE]
  checkpoint_data = None

  if (cloudsql_importer_constants.INITIAL_EXECUTION_STAGE ==
      cloudsql_importer_constants.NO_CHECKPOINT):
    logging.info("Starting a new Cloud SQL Import run")
  else:
    logging.info("Resuming execution from '%s'", next_func_name)

  while next_func_name is not None:
    func = globals()[next_func_name]
    next_func_name = func()
    checkpoint_data = checkpoint_manager.manage_checkpoint.checkpoint

  logging.debug("Saving final checkpoint state to config file '%s'",
                checkpoint_data)
  config.save_config(cloudsql_importer_constants.CHECKPOINT_CONFIG_FILE_NAME,
                     checkpoint_data)


if __name__ == "__main__":
  main()
