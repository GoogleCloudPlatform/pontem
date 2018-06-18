/*
 * Copyright 2018 Google LLC
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.pontem;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Struct;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BaseCloudSpannerDatabaseBackup}. */
@RunWith(JUnit4.class)
public class BaseCloudSpannerDatabaseBackupTest {

  @Test
  public void testGetSqlQueryForTablesToBackup() {
    String expected =
        "SELECT table_name, parent_table_name FROM information_schema.tables AS t "
            + "WHERE t.table_catalog = '' and t.table_schema = '' and "
            + "table_name IN (\"tableName1\",\"tableName2\") ORDER BY parent_table_name DESC";
    String actual =
        BaseCloudSpannerDatabaseBackup.getSqlQueryForTablesToBackup(
            ImmutableList.of("tableName1", "tableName2"));
    assertEquals(expected, actual);
  }

  @Test
  public void testQueryListOfAllTablesInDatabase() {
    String projectId = "cloud-project-id";
    String instance = "gs://my-bucket/myPath";
    String databaseId = "";

    Util mockUtil = mock(Util.class);
    when(mockUtil.performSingleSpannerQuery(
            eq(projectId),
            eq(instance),
            eq(databaseId),
            eq(BaseCloudSpannerDatabaseBackup.LIST_ALL_TABLES_SQL_QUERY)))
        .thenReturn(
            ImmutableList.of(
                Struct.newBuilder().set("table_name").to("tableName1").build(),
                Struct.newBuilder().set("table_name").to("tableName2").build()));

    ImmutableSet<String> expected = ImmutableSet.of("tableName1", "tableName2");
    ImmutableSet<String> actual =
        BaseCloudSpannerDatabaseBackup.queryListOfAllTablesInDatabase(
            projectId, instance, databaseId, mockUtil);
    assertEquals(expected, actual);
  }

  @Test
  public void testGetListOfTablesToBackup() throws Exception {
    String[] emptyTableNamesToIncludeInBackup = new String[0];
    String[] emptyTableNamesToExcludeFromBackup = new String[0];

    ImmutableList<String> tablesToBackup =
        BaseCloudSpannerDatabaseBackup.getListOfTablesToBackup(
            ImmutableSet.of("Table0", "Table1"),
            emptyTableNamesToIncludeInBackup,
            emptyTableNamesToExcludeFromBackup);
    assertEquals("number of tables to back is wrong", 2, tablesToBackup.size());
    assertEquals("Table0", tablesToBackup.get(0));
    assertEquals("Table1", tablesToBackup.get(1));
  }

  @Test(expected = Exception.class)
  public void testGetListOfTablesToBackup_exclusionAndInclusionSet() throws Exception {
    String[] tableNamesToIncludeInBackup = {"Table0"};
    String[] tableNamesToExcludeFromBackup = {"Table0"};

    ImmutableList<String> tablesToBackup =
        BaseCloudSpannerDatabaseBackup.getListOfTablesToBackup(
            ImmutableSet.of("Table0", "Table1"),
            tableNamesToIncludeInBackup,
            tableNamesToExcludeFromBackup);
  }

  @Test(expected = Exception.class)
  public void testGetListOfTablesToBackup_emptyTables() throws Exception {
    String[] tableNamesToIncludeInBackup = {"Table0"};
    String[] tableNamesToExcludeFromBackup = {"Table0"};

    ImmutableList<String> tablesToBackup =
        BaseCloudSpannerDatabaseBackup.getListOfTablesToBackup(
            ImmutableSet.of(), tableNamesToIncludeInBackup, tableNamesToExcludeFromBackup);
  }

  @Test(expected = Exception.class)
  public void testGetListOfTablesToBackup_excludeTableMissing() throws Exception {
    String[] tableNamesToIncludeInBackup = {"Table1"};
    String[] tableNamesToExcludeFromBackup = {"Table0"};

    ImmutableList<String> tablesToBackup =
        BaseCloudSpannerDatabaseBackup.getListOfTablesToBackup(
            ImmutableSet.of("Table1"), tableNamesToIncludeInBackup, tableNamesToExcludeFromBackup);
  }

  @Test(expected = Exception.class)
  public void testGetListOfTablesToBackup_includeTableMissing() throws Exception {
    String[] tableNamesToIncludeInBackup = {"Table1"};
    String[] tableNamesToExcludeFromBackup = new String[0];

    ImmutableList<String> tablesToBackup =
        BaseCloudSpannerDatabaseBackup.getListOfTablesToBackup(
            ImmutableSet.of("Table0"), tableNamesToIncludeInBackup, tableNamesToExcludeFromBackup);
  }

  @Test
  public void testGetListOfTablesToBackup_inclusionListSet() throws Exception {
    String[] tableNamesToIncludeInBackup = {"Table1"};
    String[] emptyTableNamesToExcludeFromBackup = new String[0];

    ImmutableList<String> tablesToBackup =
        BaseCloudSpannerDatabaseBackup.getListOfTablesToBackup(
            ImmutableSet.of("Table0", "Table1", "Table2"),
            tableNamesToIncludeInBackup,
            emptyTableNamesToExcludeFromBackup);
    assertEquals("number of tables to backup is wrong", 1, tablesToBackup.size());
    assertEquals("Table1", tablesToBackup.get(0));
  }

  @Test
  public void testGetListOfTablesToBackup_exclusionListSet() throws Exception {
    String[] emptyTableNamesToIncludeInBackup = new String[0];
    String[] tableNamesToExcludeFromBackup = {"Table1"};

    ImmutableList<String> tablesToBackup =
        BaseCloudSpannerDatabaseBackup.getListOfTablesToBackup(
            ImmutableSet.of("Table0", "Table1", "Table2"),
            emptyTableNamesToIncludeInBackup,
            tableNamesToExcludeFromBackup);
    assertEquals("number of tables to backup is wrong", 2, tablesToBackup.size());
    assertFalse("Table1 not excluded", tablesToBackup.contains("Table1"));
  }
}
