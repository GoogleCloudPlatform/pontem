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
"""Module that holds checksum functions used in import and export operations."""
from __future__ import absolute_import

import filecmp
import logging
import sql_wrapper


def get_checksum(constants):
  """Returns the checksums for all tables in the database being processed.

  Uses the constants configuration to extract the necessary data to query
  the database and export the corresponding checksums.

  Args:
    constants: The importer / exporter constants
  """
  logging.info("Checksumming %s's tables.", constants.DATABASE_NAME)

  tables_in_database = sql_wrapper.call_show_tables(constants)
  tables_to_checksum = list(filter(bool, tables_in_database.split("\n")))

  checksumed_tables = sql_wrapper.call_checksum(tables_to_checksum, constants)

  logging.debug("Checksum results:\n%s", checksumed_tables)

  return checksumed_tables


def compare_checksum_files(this_checksum, that_checksum):
  """Checks whether two checksum files are equal raises error otherwise.

  Args:
    this_checksum: Checksum file to be compared
    that_checksum: Checksum file to be compared

  Raises:
    RuntimeError: if comparison fails
  """
  logging.info("Comparing checksum file '%s' against '%s'", this_checksum,
               that_checksum)

  are_equal = filecmp.cmp(this_checksum, that_checksum, shallow=False)

  if are_equal:
    logging.info("Checksums match!")
  else:
    raise RuntimeError(
        "Checksum comparison failed please check the files located in '%s' and"
        "'%s'" % (this_checksum, that_checksum))
