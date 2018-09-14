/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.pontem;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

/** Base class for performing a backup of a Cloud Spanner database. */
public abstract class BaseCloudSpannerDatabaseBackup {
  protected static final Logger LOG =
      Logger.getLogger(BaseCloudSpannerDatabaseBackup.class.getName());

  public static final String LIST_ALL_TABLES_SQL_QUERY =
      "SELECT table_name, parent_table_name FROM information_schema.tables AS t WHERE t"
          + ".table_catalog = '' and t.table_schema = '' ORDER BY"
          + " parent_table_name, table_name DESC";

  /**
   * Fetch all the table names in a specific cloud spanner database.
   *
   * @return all table names in a database.
   */
  public static ImmutableSet<String> queryListOfAllTablesInDatabase(
      String projectId,
      String instance,
      String databaseId,
      SpannerUtil spannerUtil,
      Timestamp timestampForDb) {
    ImmutableList<Struct> resultSet =
        spannerUtil.performSingleSpannerReadQueryAtTimestamp(
            projectId, instance, databaseId, LIST_ALL_TABLES_SQL_QUERY, timestampForDb);
    Set<String> tableNames = Sets.newHashSet();
    for (Struct row : resultSet) {
      tableNames.add(row.getString(0));
    }
    LOG.info(
        "Finished querying list of all table names in database "
            + databaseId
            + ". Returned ("
            + tableNames.size()
            + ")"
            + " tables.");
    return ImmutableSet.copyOf(tableNames);
  }

  /**
   * Get list of tables to backup.
   *
   * <p>If the user specified a list of tables to include in backup, return only these tables after
   * verifying their name is valid in the database. No other tables will be returned.
   *
   * <p>If the user specified a list of tables to exclude from backup, return all tables except for
   * these excluded tables.
   *
   * <p>{@param tableNamesToIncludeInBackup} and {@param tableNamesToExcludeFromBackup} cannot both
   * be set and populated.
   *
   * @return List of tables to perform a backup on.
   */
  public static ImmutableList<String> getListOfTablesToBackup(
      ImmutableSet<String> allTableNames,
      String[] tableNamesToIncludeInBackup,
      String[] tableNamesToExcludeFromBackup)
      throws Exception {
    if (allTableNames.size() == 0) {
      throw new Exception("Database has no tables.");
    }

    boolean isTablesToIncludeSet =
        (tableNamesToIncludeInBackup != null && tableNamesToIncludeInBackup.length > 0);
    boolean isTablesToExcludeSet =
        (tableNamesToExcludeFromBackup != null && tableNamesToExcludeFromBackup.length > 0);
    if (isTablesToIncludeSet && isTablesToExcludeSet) {
      throw new Exception("Cannot set a table inclusion list AND a table exclusion list");
    }

    if (isTablesToIncludeSet) {
      LOG.info("Tables to include set with " + tableNamesToIncludeInBackup.length + " values");
      // User has specified a list of tables to include, so only include these tables.
      // Check each table name to ensure it is valid.
      for (String tableNameToIncludeInBackup : tableNamesToIncludeInBackup) {
        if (!allTableNames.contains(tableNameToIncludeInBackup)) {
          throw new Exception(
              "Table "
                  + tableNameToIncludeInBackup
                  + " required in backup but not found in database.");
        }
      }
      return ImmutableList.copyOf(tableNamesToIncludeInBackup);
    }

    if (isTablesToExcludeSet) {
      // Since we're going to need to exclude some tables, this is going to involve
      // mutating the set
      Set<String> modifiedTableNamesWithExclusions = new HashSet<String>(allTableNames);

      LOG.info("Tables to exclude set with " + tableNamesToExcludeFromBackup.length + " values");
      // User has specified a list of tables to exclude, so remove those tables.
      for (String tableToExcludeFromBackup : tableNamesToExcludeFromBackup) {
        if (allTableNames.contains(tableToExcludeFromBackup)) {
          modifiedTableNamesWithExclusions.remove(tableToExcludeFromBackup);
        } else {
          throw new Exception(
              "Table "
                  + tableToExcludeFromBackup
                  + " listed to exclude from backup yet table was not found in database");
        }
      }
      return ImmutableList.copyOf(modifiedTableNamesWithExclusions);
    }

    return ImmutableList.copyOf(allTableNames);
  }

  /**
   * Form a SQL query for Spanner getting table information about the tables to backup.
   *
   * @param tablesToBackup List of tables to backup.
   * @return SQL query.
   */
  public static String getSqlQueryForTablesToBackup(ImmutableList<String> tablesToBackup) {
    String query =
        "SELECT table_name, parent_table_name FROM information_schema.tables AS t "
            + "WHERE t.table_catalog = '' and t.table_schema = '' and "
            + "table_name IN (";
    for (String tableToBackup : tablesToBackup) {
      query += "\"" + tableToBackup + "\",";
    }
    query = query.substring(0, query.length() - 1);
    query += ") ORDER BY parent_table_name DESC";
    return query;
  }
}
