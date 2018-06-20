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

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Util}. */
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
  public void testConvertDdlBetweenStringAndList() {
    ImmutableList<String> ddlStatementsAsList =
        ImmutableList.of(basicDdl1, basicDdl2, basicDdl3, basicDdl4);
    String ddlStatementsAsString = SpannerUtil.convertDdlListIntoRawText(ddlStatementsAsList);
    assertEquals(ddlStatementsAsList, SpannerUtil.convertRawDdlIntoDdlList(ddlStatementsAsString));
    assertEquals(
        ddlStatementsAsString,
        SpannerUtil.convertDdlListIntoRawText(
            SpannerUtil.convertRawDdlIntoDdlList(ddlStatementsAsString)));
  }
}
