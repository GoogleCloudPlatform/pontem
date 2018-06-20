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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Set;
import java.util.logging.Logger;

/** Base class for performing a restore of a Cloud Spanner database. */
public abstract class BaseCloudSpannerDatabaseRestore {
  protected static final Logger LOG =
      Logger.getLogger(BaseCloudSpannerDatabaseRestore.class.getName());

  /**
   * Since Cloud Spanner supports parent-child relationships
   * https://cloud.google.com/spanner/docs/schema-and-data-model#parent-child_table_relationships
   * and since the parent table must be populated before the child table can be populated, we need
   * to be very careful to perform the restore of the table contents in a very specific order.
   *
   * <p>This method returns a list of root tables. For each root table, there is a linked list of
   * children tables in descending order (i.e., the first child will be listed in the linked list
   * before the first grandchild).
   *
   * <p>For example: rootTable1 => [rootTable1] // Table has no children dependencies rootTable2 =>
   * [rootTable2 -> child2 -> grandchild2]
   *
   * <p>If the user specified a list of tables to include in backup, return only these tables after
   * verifying their name is valid in the database. No other tables will be returned.
   *
   * <p>If the user specified a list of tables to exclude from backup, return all tables except for
   * these excluded tables.
   *
   * <p>{@param tableNamesToIncludeInRestore} and {@param tableNamesToExcludeFromRestore} cannot
   * both be set and populated.
   */
  public static LinkedHashMap<String, LinkedList<String>> queryListOfTablesToRestore(
      String projectId,
      String inputGcsPath,
      String[] tableNamesToIncludeInRestore,
      String[] tableNamesToExcludeFromRestore,
      Util util)
      throws Exception {

    boolean isTablesToIncludeSet =
        (tableNamesToIncludeInRestore != null && tableNamesToIncludeInRestore.length > 0);
    boolean isTablesToExcludeSet =
        (tableNamesToExcludeFromRestore != null && tableNamesToExcludeFromRestore.length > 0);
    if (isTablesToIncludeSet && isTablesToExcludeSet) {
      throw new Exception("Cannot set a table inclusion list AND a table exclusion list");
    }

    // Step 1: Fetch and parse file.
    String rawFileContents =
        util.getContentsOfFileFromGcs(
            projectId,
            Util.getGcsBucketNameFromDatabaseBackupLocation(inputGcsPath),
            Util.getGcsFolderPathFromDatabaseBackupLocation(inputGcsPath),
            Util.FILE_PATH_FOR_DATABASE_TABLE_NAMES);
    String lines[] = rawFileContents.split("\\r?\\n");

    // Step 2: Store all table names without a parent as well as
    // a multi-map mapping parent -> children.
    Set<String> rootTables = Sets.newHashSet();
    HashMultimap<String, String> miltiMapOfParentTableToChildrenTables = HashMultimap.create();
    for (String line : lines) {
      line = line.trim();
      String tableName = line.substring(0, line.indexOf(','));
      String parentTableName = line.substring(line.indexOf(',') + 1);
      boolean isParentTableNameSet = (line.length() - 1) > tableName.length();

      // If a list of tables to include or exclude in the restore job is set, check the
      // current table against that now.
      // If an inclusion list is set, do not restore tables outside that list.
      // If an exclusion list is set, do not restore tables inside that list.
      if ((isTablesToIncludeSet && !Arrays.asList(tableNamesToIncludeInRestore).contains(tableName))
          || (isTablesToExcludeSet
              && Arrays.asList(tableNamesToExcludeFromRestore).contains(tableName))) {
        continue;
      }

      // If parentTableName exists AND tableName is contained in either an inclusion or exclusion
      // table list, then both the parent table name and child table name must be contained
      // within the inclusion or exclusion list. I.e., we cannot have a situation where an inclusion
      // list contains the child table but not its parent because this would orphan the child table
      // and the restore job would fail.
      if (isParentTableNameSet) {
        if (isTablesToIncludeSet
            && !Arrays.asList(tableNamesToIncludeInRestore).contains(parentTableName)) {
          throw new Exception(
              "Parent-child table relationship exists for table "
                  + tableName
                  + ", yet tables to"
                  + " include in restore list does not contain parent table "
                  + parentTableName);
        }
        if (isTablesToExcludeSet
            && Arrays.asList(tableNamesToExcludeFromRestore).contains(parentTableName)) {
          throw new Exception(
              "Parent-child table relationship exists for table "
                  + tableName
                  + ", yet tables to"
                  + " exclude from restore list contains parent table "
                  + parentTableName);
        }
      }

      // Check whether a parent table exists
      // if a parent table exists, line is formatted: "childTableName,parentTableName"
      // if no parent table exists, line is formatted: "tableName,"
      if (isParentTableNameSet) {
        // table has a parent table
        miltiMapOfParentTableToChildrenTables.put(parentTableName, tableName);
      } else {
        // Line only has one table name, so table has no parent. So, table is a root.
        rootTables.add(tableName);
      }
    }

    // Step 3: Starting with the tables without a parent, walk down the tree adding each table
    // as you go.
    LinkedHashMap<String, LinkedList<String>> mapOfParentToAllChildrenTablesInOrderToFetch =
        new LinkedHashMap();

    for (String rootTable : rootTables) {
      // Create a LinkedList for each root note and populate that list with the root node
      mapOfParentToAllChildrenTablesInOrderToFetch.put(rootTable, new LinkedList<String>());
      mapOfParentToAllChildrenTablesInOrderToFetch.get(rootTable).add(rootTable);

      // Check whether the root table has any children. If it does, walk down that tree.
      if (miltiMapOfParentTableToChildrenTables.get(rootTable).size() > 0) {
        LinkedList<String> tablesToExamine = new LinkedList<String>();
        Set<String> firstChildrenTables = miltiMapOfParentTableToChildrenTables.get(rootTable);
        tablesToExamine.addAll(firstChildrenTables);

        while (!tablesToExamine.isEmpty()) {
          // For each table, walk down a level in the tree.
          String tableToExamine = tablesToExamine.remove();
          mapOfParentToAllChildrenTablesInOrderToFetch.get(rootTable).add(tableToExamine);
          Set<String> grandChildrenTables =
              miltiMapOfParentTableToChildrenTables.get(tableToExamine);
          tablesToExamine.addAll(grandChildrenTables);
        }
      }
    }

    return mapOfParentToAllChildrenTablesInOrderToFetch;
  }

  public static void createDatabaseAndTables(
      String projectId,
      String instanceId,
      String databaseId,
      String inputFolderPath,
      Util util,
      SpannerUtil spannerUtil)
      throws Exception {
    // STEP 1: Check whether DDL is backed-up in GCS. If not, fail out.
    String backedUpDdlFromGcs =
        util.getContentsOfFileFromGcs(
            projectId,
            Util.getGcsBucketNameFromDatabaseBackupLocation(inputFolderPath),
            Util.getGcsFolderPathFromDatabaseBackupLocation(inputFolderPath),
            SpannerUtil.FILE_PATH_FOR_DATABASE_DDL);
    if (backedUpDdlFromGcs.length() < 1) {
      throw new Exception("Serious error. Unable to fetch backed-up DDL in: " + inputFolderPath);
    }
    ImmutableList<String> backedupDdl = SpannerUtil.convertRawDdlIntoDdlList(backedUpDdlFromGcs);

    // STEP 2: Re-create database and apply DDL.
    spannerUtil.createDatabaseAndTables(projectId, instanceId, databaseId, backedupDdl);
  }
}
