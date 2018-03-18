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

import com.google.api.services.dataflow.model.JobMetrics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Util}. */
@RunWith(JUnit4.class)
public class UtilTest {
  private String basicDdl1 =
      "CREATE TABLE Singers (\n"
          + "  SingerId INT64 NOT NULL,\n"
          + "  FirstName STRING(1024),\n"
          + "  LastName STRING(1024),\n"
          + ") PRIMARY KEY(SingerId)";
  private String basicDdl2 =
      "CREATE TABLE Albums (\n"
          + "  SingerId INT64 NOT NULL,\n"
          + "  AlbumId INT64 NOT NULL,\n"
          + "  AlbumTitle STRING(MAX),\n"
          + ") PRIMARY KEY(SingerId, AlbumId),\n"
          + "  INTERLEAVE IN PARENT Singers ON DELETE CASCADE";
  private String basicDdl3 =
      "CREATE TABLE AlbumPromitions (\n"
          + "  AlbumId INT64 NOT NULL,\n"
          + "  SingerId INT64 NOT NULL,\n"
          + "  PromotionDescription STRING(MAX) NOT NULL,\n"
          + ") PRIMARY KEY(SingerId, AlbumId),\n"
          + "  INTERLEAVE IN PARENT Albums ON DELETE CASCADE";
  private String basicDdl4 =
      "CREATE INDEX wordWordsIndex ON two_hundred_million_words(word, words)";

  @Test
  public void testGetGcsBucketNameFromDatabaseBackupLocation() throws Exception {
    assertEquals(
        "Bucket name parsing failed",
        "cloud-spanner-backup-test",
        Util.getGcsBucketNameFromDatabaseBackupLocation(
            "gs://cloud-spanner-backup-test/multi-backup"));

    assertEquals(
        "Bucket name parsing failed",
        "bucketName",
        Util.getGcsBucketNameFromDatabaseBackupLocation("gs://bucketName/multi-backup/djskd"));
  }

  @Test
  public void testGetGcsFolderPathFromDatabaseBackupLocation() throws Exception {
    assertEquals(
        "Folder path parsing failed",
        "multi-backup/djskd/",
        Util.getGcsFolderPathFromDatabaseBackupLocation("gs://bucketName/multi-backup/djskd"));
    assertEquals(
        "Folder path parsing failed",
        "multi-backup/",
        Util.getGcsFolderPathFromDatabaseBackupLocation("gs://bucketName/multi-backup/"));
  }

  @Test
  public void testConvertTablenamesIntoSet() throws Exception {
    assertEquals(
        "Parsing table names into Set failed",
        ImmutableSet.of("tableName1", "TableName2", "table_name_3"),
        Util.convertTablenamesIntoSet("tableName1\nTableName2\ntable_name_3"));

    assertEquals(
        "Parsing table names into Set failed",
        ImmutableSet.of("MyName1", "my-name-2B", "table_name_3"),
        Util.convertTablenamesIntoSet("MyName1\r\nmy-name-2B\r\ntable_name_3"));
  }

  @Test
  public void testConvertTableMetadataContentsToMap() throws Exception {
    assertEquals(
        "Parsing table names and num rows into Map failed",
        ImmutableMap.of(
            "AlbumPromotions", 1L, "seven_words", 7L, "two_hundred_million_words", 200000000L),
        Util.convertTableMetadataContentsToMap(
            "AlbumPromotions,1\nseven_words,7\ntwo_hundred_million_words,200000000"));

    assertEquals(
        "Parsing table names and num rows into Map failed",
        ImmutableMap.of(
            "MyTable100", 100L, "THEseven_words", 79L, "two_hundred_million_words", 200000000L),
        Util.convertTableMetadataContentsToMap(
            "MyTable100,100\nTHEseven_words,79\ntwo_hundred_million_words,200000000"));
  }

  @Test
  public void testGetTableRowCountsFromJobMetrics() throws Exception {
    Map<String, Long> expectedParsedJobMetricsMap =
        ImmutableMap.of(
            "THEseven_words", 7L, "MyTable100", 100L, "two_hundred_million_words", 200000000L);
    JobMetrics jobMetrics = TestHelper.getJobMetrics(expectedParsedJobMetricsMap);

    Map<String, Long> actualParsedJobMetricsMap = Util.getTableRowCountsFromJobMetrics(jobMetrics);
    assertEquals(
        "Parsing table names and num rows into Map failed",
        expectedParsedJobMetricsMap,
        actualParsedJobMetricsMap);
  }

  @Test
  public void testConvertDdlListIntoRawText() {
    String actualText =
        Util.convertDdlListIntoRawText(
            ImmutableList.of(basicDdl1, basicDdl2, basicDdl3, basicDdl4));
    String expectedText =
        basicDdl1
            + Util.DDL_DELIMITER
            + basicDdl2
            + Util.DDL_DELIMITER
            + basicDdl3
            + Util.DDL_DELIMITER
            + basicDdl4;
    assertEquals(expectedText, actualText);
  }

  @Test
  public void testConvertRawDdlIntoDdlList() {
    String rawText =
        basicDdl1
            + Util.DDL_DELIMITER
            + basicDdl2
            + Util.DDL_DELIMITER
            + basicDdl3
            + Util.DDL_DELIMITER
            + basicDdl4;
    ImmutableList<String> expected = ImmutableList.of(basicDdl1, basicDdl2, basicDdl3, basicDdl4);
    ImmutableList<String> actual = Util.convertRawDdlIntoDdlList(rawText);
    assertEquals(expected, actual);
  }

  @Test
  public void testConvertDdlBetweenStringAndList() {
    ImmutableList<String> ddlStatementsAsList =
        ImmutableList.of(basicDdl1, basicDdl2, basicDdl3, basicDdl4);
    String ddlStatementsAsString = Util.convertDdlListIntoRawText(ddlStatementsAsList);
    assertEquals(ddlStatementsAsList, Util.convertRawDdlIntoDdlList(ddlStatementsAsString));
    assertEquals(
        ddlStatementsAsString,
        Util.convertDdlListIntoRawText(Util.convertRawDdlIntoDdlList(ddlStatementsAsString)));
  }
}
