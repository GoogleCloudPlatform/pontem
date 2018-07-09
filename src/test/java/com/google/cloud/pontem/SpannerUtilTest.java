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

import com.google.cloud.spanner.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SpannerUtil}. */
@RunWith(JUnit4.class)
public class SpannerUtilTest {
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
  private String basicDdl5 =
      "CREATE TABLE complex (\n"
          + "\tboolArray ARRAY<BOOL> NOT NULL,\n"
          + "\tdate DATE NOT NULL,\n"
          + "\tintArray ARRAY<INT64>,\n"
          + "\tstringArray ARRAY<STRING(50)>,\n"
          + "\ttimestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),\n"
          + ") PRIMARY KEY (int64 DESC, bool DESC)";
  private String basicDdl6 =
      "CREATE UNIQUE NULL_FILTERED INDEX index1 \n"
          + "ON complex (\n"
          + "    bool,\n"
          + "    string DESC\n"
          + ") STORING (\n"
          + "    date\n"
          + ")";
  private String basicDdl7 =
      "CREATE TABLE wordsInterleaved (\n"
          + "\tword STRING(MAX) NOT NULL,\n"
          + "\tnum INT64 NOT NULL,\n"
          + ") PRIMARY KEY (word, num),\n"
          + "INTERLEAVE IN PARENT word";
  private String basicDdl8 =
      "CREATE TABLE wordsInterleavedTwo (word STRING(MAX) NOT NULL,num INT64 NOT NULL,\n"
          + ") PRIMARY KEY (word, num),\n"
          + "INTERLEAVE IN PARENT word";
  private String advancedDdl =
      "CREATE TABLE complex_2 (\n"
          + "\tdate DATE NOT NULL,\n"
          + "\tbool BOOL NOT NULL,\n"
          + "\tint64_ INT64 NOT NULL,\n"
          + "\tboolArray ARRAY<BOOL> NOT NULL,\n"
          + "\tbytesArray ARRAY<BYTES(1024)>,\n"
          + "\tbytesArrayMax ARRAY<BYTES(MAX)>,\n"
          + "\tbytesNotNull BYTES(1024) NOT NULL,\n"
          + "\tbytesVal BYTES(MAX),\n"
          + "\tfloatArray ARRAY<FLOAT64>,\n"
          + "\tint_Array ARRAY<INT64>,\n"
          + "\tstringArray ARRAY<STRING(50)>,\n"
          + "\tstringArrayMax ARRAY<STRING(MAX)>,\n"
          + "\ttimestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),\n"
          + "\ttimestampArray ARRAY<TIMESTAMP>,\n"
          + "\tdateArray ARRAY<DATE> NOT NULL,\n"
          + ") PRIMARY KEY (int64_ DESC, bool DESC)";

  @Test
  public void testConvertDdlListIntoRawText() {
    String actualText =
        SpannerUtil.convertDdlListIntoRawText(
            ImmutableList.of(basicDdl1, basicDdl2, basicDdl3, basicDdl4));
    String expectedText =
        basicDdl1
            + SpannerUtil.DDL_DELIMITER
            + basicDdl2
            + SpannerUtil.DDL_DELIMITER
            + basicDdl3
            + SpannerUtil.DDL_DELIMITER
            + basicDdl4;
    assertEquals(expectedText, actualText);
  }

  @Test
  public void testConvertDdlListIntoRawText_advanced() {
    String actualText =
        SpannerUtil.convertDdlListIntoRawText(
            ImmutableList.of(
                basicDdl1,
                basicDdl2,
                basicDdl3,
                basicDdl4,
                basicDdl5,
                basicDdl6,
                basicDdl7,
                basicDdl8,
                advancedDdl));
    String expectedText =
        basicDdl1
            + SpannerUtil.DDL_DELIMITER
            + basicDdl2
            + SpannerUtil.DDL_DELIMITER
            + basicDdl3
            + SpannerUtil.DDL_DELIMITER
            + basicDdl4
            + SpannerUtil.DDL_DELIMITER
            + basicDdl5
            + SpannerUtil.DDL_DELIMITER
            + basicDdl6
            + SpannerUtil.DDL_DELIMITER
            + basicDdl7
            + SpannerUtil.DDL_DELIMITER
            + basicDdl8
            + SpannerUtil.DDL_DELIMITER
            + advancedDdl;
    assertEquals(expectedText, actualText);
  }

  @Test
  public void testConvertRawDdlIntoDdlList() {
    String rawText =
        basicDdl1
            + SpannerUtil.DDL_DELIMITER
            + basicDdl2
            + SpannerUtil.DDL_DELIMITER
            + basicDdl3
            + SpannerUtil.DDL_DELIMITER
            + basicDdl4;
    ImmutableList<String> expected = ImmutableList.of(basicDdl1, basicDdl2, basicDdl3, basicDdl4);
    ImmutableList<String> actual = SpannerUtil.convertRawDdlIntoDdlList(rawText);
    assertEquals(expected, actual);
  }

  @Test
  public void testConvertRawDdlIntoDdlList_advannced() {
    String rawText =
        basicDdl1
            + SpannerUtil.DDL_DELIMITER
            + basicDdl2
            + SpannerUtil.DDL_DELIMITER
            + basicDdl3
            + SpannerUtil.DDL_DELIMITER
            + basicDdl4
            + SpannerUtil.DDL_DELIMITER
            + basicDdl5
            + SpannerUtil.DDL_DELIMITER
            + basicDdl6
            + SpannerUtil.DDL_DELIMITER
            + basicDdl7
            + SpannerUtil.DDL_DELIMITER
            + basicDdl8
            + SpannerUtil.DDL_DELIMITER
            + advancedDdl;
    ImmutableList<String> expected =
        ImmutableList.of(
            basicDdl1,
            basicDdl2,
            basicDdl3,
            basicDdl4,
            basicDdl5,
            basicDdl6,
            basicDdl7,
            basicDdl8,
            advancedDdl);
    ImmutableList<String> actual = SpannerUtil.convertRawDdlIntoDdlList(rawText);
    assertEquals(expected, actual);
  }

  @Test
  public void testConvertDdlBetweenStringAndList() {
    ImmutableList<String> ddlStatementsAsList =
        ImmutableList.of(
            basicDdl1,
            basicDdl2,
            basicDdl3,
            basicDdl4,
            basicDdl5,
            basicDdl6,
            basicDdl7,
            basicDdl8,
            advancedDdl);
    String ddlStatementsAsString = SpannerUtil.convertDdlListIntoRawText(ddlStatementsAsList);
    assertEquals(ddlStatementsAsList, SpannerUtil.convertRawDdlIntoDdlList(ddlStatementsAsString));
    assertEquals(
        ddlStatementsAsString,
        SpannerUtil.convertDdlListIntoRawText(
            SpannerUtil.convertRawDdlIntoDdlList(ddlStatementsAsString)));
  }

  @Test
  public void testConvertEntireDtaabaseDdlIntoTableSpecificDdl_basic() {
    ImmutableList<String> map =
        ImmutableList.of(
            "CREATE TABLE word (\n" + "  word STRING(MAX) NOT NULL,\n" + ") PRIMARY KEY(word)",
            "CREATE UNIQUE NULL_FILTERED INDEX testIndex \n" + "ON word (\n" + "\tword\n" + ")");
    ImmutableMap<String, String> expectedResult =
        ImmutableMap.of(
            "word",
            "CREATE TABLE word (\n" + "  word STRING(MAX) NOT NULL,\n" + ") PRIMARY KEY(word)");
    ImmutableMap<String, String> actualResult =
        SpannerUtil.convertEntireDtaabaseDdlIntoTableSpecificDdl(map);
    assertEquals(expectedResult, actualResult);
  }

  @Test
  public void testConvertEntireDtaabaseDdlIntoTableSpecificDdl_intermediate() {
    ImmutableList<String> map = ImmutableList.of(basicDdl1, basicDdl2, basicDdl3, basicDdl4);
    ImmutableMap<String, String> expectedResult =
        ImmutableMap.of("Singers", basicDdl1, "Albums", basicDdl2, "AlbumPromitions", basicDdl3);
    ImmutableMap<String, String> actualResult =
        SpannerUtil.convertEntireDtaabaseDdlIntoTableSpecificDdl(map);
    assertEquals(expectedResult, actualResult);
  }

  @Test
  public void testConvertEntireDtaabaseDdlIntoTableSpecificDdl_advanced() {
    ImmutableList<String> map =
        ImmutableList.of(
            basicDdl2, basicDdl3, basicDdl4, basicDdl5, basicDdl6, basicDdl7, basicDdl8);
    ImmutableMap<String, String> expectedResult =
        ImmutableMap.of(
            "Albums",
            basicDdl2,
            "AlbumPromitions",
            basicDdl3,
            "complex",
            basicDdl5,
            "wordsInterleaved",
            basicDdl7,
            "wordsInterleavedTwo",
            basicDdl8);
    ImmutableMap<String, String> actualResult =
        SpannerUtil.convertEntireDtaabaseDdlIntoTableSpecificDdl(map);
    assertEquals(expectedResult, actualResult);
  }

  @Test
  public void testConvertEntireDtaabaseDdlIntoTableSpecificDdl_advanced2() {
    ImmutableList<String> map =
        ImmutableList.of(
            advancedDdl, basicDdl3, basicDdl4, basicDdl5, basicDdl6, basicDdl7, basicDdl8);
    ImmutableMap<String, String> expectedResult =
        ImmutableMap.of(
            "complex_2",
            advancedDdl,
            "AlbumPromitions",
            basicDdl3,
            "complex",
            basicDdl5,
            "wordsInterleaved",
            basicDdl7,
            "wordsInterleavedTwo",
            basicDdl8);
    ImmutableMap<String, String> actualResult =
        SpannerUtil.convertEntireDtaabaseDdlIntoTableSpecificDdl(map);
    assertEquals(expectedResult, actualResult);
  }

  @Test(expected = RuntimeException.class)
  public void testConvertEntireDtaabaseDdlIntoTableSpecificDdl_errorBasic1() {
    ImmutableList<String> map = ImmutableList.of(basicDdl1, "CREATE TABLEskl");
    ImmutableMap<String, String> actualResult =
        SpannerUtil.convertEntireDtaabaseDdlIntoTableSpecificDdl(map);
  }

  @Test(expected = RuntimeException.class)
  public void testConvertEntireDtaabaseDdlIntoTableSpecificDdl_errorBasic2() {
    ImmutableList<String> map = ImmutableList.of(basicDdl1, "CREATE TABLE skl");
    ImmutableMap<String, String> actualResult =
        SpannerUtil.convertEntireDtaabaseDdlIntoTableSpecificDdl(map);
  }

  @Test(expected = RuntimeException.class)
  public void testConvertEntireDtaabaseDdlIntoTableSpecificDdl_errorBasic3() {
    ImmutableList<String> map = ImmutableList.of(basicDdl1, "CREATE TABLE sk78l_@_ ( sdjs");
    ImmutableMap<String, String> actualResult =
        SpannerUtil.convertEntireDtaabaseDdlIntoTableSpecificDdl(map);
  }

  @Test
  public void testGetSpannerType() {
    assertEquals(Type.string(), SpannerUtil.getSpannerType("STRING(100)"));
    assertEquals(Type.int64(), SpannerUtil.getSpannerType("INT64"));
    assertEquals(Type.string(), SpannerUtil.getSpannerType("STRING(MAX)"));
    assertEquals(Type.timestamp(), SpannerUtil.getSpannerType("TIMESTAMP"));
    assertEquals(Type.bool(), SpannerUtil.getSpannerType("BOOL"));
    assertEquals(Type.date(), SpannerUtil.getSpannerType("DATE"));
    assertEquals(Type.bytes(), SpannerUtil.getSpannerType("BYTES(1024)"));
    assertEquals(Type.bytes(), SpannerUtil.getSpannerType("BYTES(MAX)"));
    assertEquals(Type.float64(), SpannerUtil.getSpannerType("FLOAT64"));
    assertEquals(Type.array(Type.string()), SpannerUtil.getSpannerType("ARRAY<STRING(50)>"));
    assertEquals(Type.array(Type.string()), SpannerUtil.getSpannerType("ARRAY<STRING(MAX)>"));
    assertEquals(Type.array(Type.int64()), SpannerUtil.getSpannerType("ARRAY<INT64>"));
    assertEquals(Type.array(Type.date()), SpannerUtil.getSpannerType("ARRAY<DATE>"));
    assertEquals(Type.array(Type.bool()), SpannerUtil.getSpannerType("ARRAY<BOOL>"));
    assertEquals(Type.array(Type.bytes()), SpannerUtil.getSpannerType("ARRAY<BYTES(1024)>"));
  }

  @Test(expected = RuntimeException.class)
  public void testGetSpannerType_error1() {
    SpannerUtil.getSpannerType("ARRAY<BTES(1024)>");
  }

  @Test(expected = RuntimeException.class)
  public void testGetSpannerType_error2() {
    SpannerUtil.getSpannerType("STR(1024)");
  }
}
