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
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link TableInformation}. */
@RunWith(JUnit4.class)
public class TableInformationTest {
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
  public void testTableInformation_basic() {
    TableInformation tableInformation = new TableInformation(basicDdl1);
    ImmutableMap<String, Type> expectedSpannerTypeMap =
        ImmutableMap.of(
            "SingerId", Type.int64(), "FirstName", Type.string(), "LastName", Type.string());
    assertEquals(expectedSpannerTypeMap, tableInformation.getMapOfColumnNamesToSpannerTypes());
    ImmutableMap<String, Boolean> expectedNullableMap =
        ImmutableMap.of("SingerId", false, "FirstName", true, "LastName", true);
    assertEquals(expectedNullableMap, tableInformation.getMapOfColumnNamesToNullable());
    ImmutableMap<String, Schema> expectedAvroMap =
        ImmutableMap.of(
            "SingerId", SchemaBuilder.builder().longType(),
            "FirstName", SchemaBuilder.builder().nullable().stringType(),
            "LastName", SchemaBuilder.builder().nullable().stringType());
    assertEquals(expectedAvroMap, tableInformation.getMapOfColumnNamesToAvroTypes());
  }

  @Test
  public void testTableInformation_basic2() {
    TableInformation tableInformation = new TableInformation(basicDdl2);
    ImmutableMap<String, Type> expectedSpannerTypeMap =
        ImmutableMap.of(
            "AlbumId", Type.int64(), "SingerId", Type.int64(), "AlbumTitle", Type.string());
    assertEquals(expectedSpannerTypeMap, tableInformation.getMapOfColumnNamesToSpannerTypes());
    ImmutableMap<String, Boolean> expectedNullableMap =
        ImmutableMap.of("AlbumId", false, "SingerId", false, "AlbumTitle", true);
    assertEquals(expectedNullableMap, tableInformation.getMapOfColumnNamesToNullable());
    ImmutableMap<String, Schema> expectedAvroMap =
        ImmutableMap.of(
            "AlbumId", SchemaBuilder.builder().longType(),
            "SingerId", SchemaBuilder.builder().longType(),
            "AlbumTitle", SchemaBuilder.builder().nullable().stringType());
    assertEquals(expectedAvroMap, tableInformation.getMapOfColumnNamesToAvroTypes());
  }

  @Test
  public void testTableInformation_basic3() {
    TableInformation tableInformation = new TableInformation(basicDdl8);
    ImmutableMap<String, Type> expectedSpannerTypeMap =
        ImmutableMap.of("word", Type.string(), "num", Type.int64());
    assertEquals(expectedSpannerTypeMap, tableInformation.getMapOfColumnNamesToSpannerTypes());
    ImmutableMap<String, Boolean> expectedNullableMap =
        ImmutableMap.of("word", false, "num", false);
    assertEquals(expectedNullableMap, tableInformation.getMapOfColumnNamesToNullable());
    ImmutableMap<String, Schema> expectedAvroMap =
        ImmutableMap.of(
            "word", SchemaBuilder.builder().stringType(),
            "num", SchemaBuilder.builder().longType());
    assertEquals(expectedAvroMap, tableInformation.getMapOfColumnNamesToAvroTypes());
  }

  @Test
  public void testTableInformation_advanced1() {
    TableInformation tableInformation = new TableInformation(basicDdl5);
    ImmutableMap<String, Type> expectedSpannerTypeMap =
        ImmutableMap.of(
            "boolArray",
            Type.array(Type.bool()),
            "date",
            Type.date(),
            "intArray",
            Type.array(Type.int64()),
            "stringArray",
            Type.array(Type.string()),
            "timestamp",
            Type.timestamp());
    assertEquals(expectedSpannerTypeMap, tableInformation.getMapOfColumnNamesToSpannerTypes());
    ImmutableMap<String, Boolean> expectedNullableMap =
        ImmutableMap.of(
            "boolArray",
            false,
            "date",
            false,
            "intArray",
            true,
            "stringArray",
            true,
            "timestamp",
            false);
    assertEquals(expectedNullableMap, tableInformation.getMapOfColumnNamesToNullable());
    ImmutableMap<String, Schema> expectedAvroMap =
        ImmutableMap.of(
            "boolArray",
                SchemaBuilder.builder().array().items().type(SchemaBuilder.builder().booleanType()),
            "date", SchemaBuilder.builder().stringType(),
            "intArray",
                SchemaBuilder.builder()
                    .nullable()
                    .array()
                    .items()
                    .type(SchemaBuilder.builder().longType()),
            "stringArray",
                SchemaBuilder.builder()
                    .nullable()
                    .array()
                    .items()
                    .type(SchemaBuilder.builder().stringType()),
            "timestamp", SchemaBuilder.builder().stringType());
    assertEquals(expectedAvroMap, tableInformation.getMapOfColumnNamesToAvroTypes());
  }

  @Test
  public void testTableInformation_advanced2() {
    TableInformation tableInformation = new TableInformation(advancedDdl);
    ImmutableMap<String, Type> expectedSpannerTypeMap =
        new ImmutableMap.Builder<String, Type>()
            .put("date", Type.date())
            .put("bool", Type.bool())
            .put("int64_", Type.int64())
            .put("boolArray", Type.array(Type.bool()))
            .put("bytesArray", Type.array(Type.bytes()))
            .put("bytesArrayMax", Type.array(Type.bytes()))
            .put("bytesNotNull", Type.bytes())
            .put("bytesVal", Type.bytes())
            .put("floatArray", Type.array(Type.float64()))
            .put("int_Array", Type.array(Type.int64()))
            .put("stringArray", Type.array(Type.string()))
            .put("stringArrayMax", Type.array(Type.string()))
            .put("timestamp", Type.timestamp())
            .put("timestampArray", Type.array(Type.timestamp()))
            .put("dateArray", Type.array(Type.date()))
            .build();
    assertEquals(expectedSpannerTypeMap, tableInformation.getMapOfColumnNamesToSpannerTypes());

    ImmutableMap<String, Boolean> expectedNullableMap =
        new ImmutableMap.Builder<String, Boolean>()
            .put("date", false)
            .put("bool", false)
            .put("int64_", false)
            .put("boolArray", false)
            .put("bytesArray", true)
            .put("bytesArrayMax", true)
            .put("bytesNotNull", false)
            .put("bytesVal", true)
            .put("floatArray", true)
            .put("int_Array", true)
            .put("stringArray", true)
            .put("stringArrayMax", true)
            .put("timestamp", false)
            .put("timestampArray", true)
            .put("dateArray", false)
            .build();
    assertEquals(expectedNullableMap, tableInformation.getMapOfColumnNamesToNullable());

    ImmutableMap<String, Schema> expectedAvroMap =
        new ImmutableMap.Builder<String, Schema>()
            .put("date", SchemaBuilder.builder().stringType())
            .put("bool", SchemaBuilder.builder().booleanType())
            .put("int64_", SchemaBuilder.builder().longType())
            .put(
                "boolArray",
                SchemaBuilder.builder().array().items().type(SchemaBuilder.builder().booleanType()))
            .put(
                "bytesArray",
                SchemaBuilder.builder()
                    .nullable()
                    .array()
                    .items()
                    .type(SchemaBuilder.builder().bytesType()))
            .put(
                "bytesArrayMax",
                SchemaBuilder.builder()
                    .nullable()
                    .array()
                    .items()
                    .type(SchemaBuilder.builder().bytesType()))
            .put("bytesNotNull", SchemaBuilder.builder().bytesType())
            .put("bytesVal", SchemaBuilder.builder().nullable().bytesType())
            .put(
                "floatArray",
                SchemaBuilder.builder()
                    .nullable()
                    .array()
                    .items()
                    .type(SchemaBuilder.builder().doubleType()))
            .put(
                "int_Array",
                SchemaBuilder.builder()
                    .nullable()
                    .array()
                    .items()
                    .type(SchemaBuilder.builder().longType()))
            .put(
                "stringArray",
                SchemaBuilder.builder()
                    .nullable()
                    .array()
                    .items()
                    .type(SchemaBuilder.builder().stringType()))
            .put(
                "stringArrayMax",
                SchemaBuilder.builder()
                    .nullable()
                    .array()
                    .items()
                    .type(SchemaBuilder.builder().stringType()))
            .put("timestamp", SchemaBuilder.builder().stringType())
            .put(
                "timestampArray",
                SchemaBuilder.builder()
                    .nullable()
                    .array()
                    .items()
                    .type(SchemaBuilder.builder().stringType()))
            .put(
                "dateArray",
                SchemaBuilder.builder().array().items().type(SchemaBuilder.builder().stringType()))
            .build();
    assertEquals(expectedAvroMap, tableInformation.getMapOfColumnNamesToAvroTypes());
  }

  @Test(expected = RuntimeException.class)
  public void testTableInformation_error() {
    String brokenDdl =
        "CREATE TABLE word (\n"
            + "    word STRING(MAX) NOT NULL,\n"
            + "    arrBytes ARRAY<BYTES(1024)>,\n"
            + "    arrString ARRAY<STRING(100)>,\n"
            + "    bytes BYTES(MAX),\n"
            + "    bytes BYTES(1024),\n"
            + "    bytes2 BYTES(3923902),\n"
            + ") PRIMARY KEY (word)";

    TableInformation tableInfo = new TableInformation(brokenDdl);
  }

  @Test(expected = RuntimeException.class)
  public void testTableInformation_error2() {
    String brokenDdl =
        "CREATETABLE word (\n"
            + "    word STRING(MAX) NOT NULL,\n"
            + "    arrBytes ARRAY<BYTES(1024)>,\n"
            + "    arrString ARRAY<STRING(100)>,\n"
            + "    bytes BYTES(MAX)\n"
            + "    bytes BYTES(1024),\n"
            + "    bytes2 BYTES(3923902),\n"
            + ") PRIMARY KEY (word)";

    TableInformation tableInfo = new TableInformation(brokenDdl);
  }

  @Test(expected = RuntimeException.class)
  public void testTableInformation_error3() {
    String brokenDdl =
        "CREATE TABLE word (\n"
            + "    word STRING(MAX) NOT NULL,\n"
            + "    arrBytes ARRAY<BYTES(1024)>,\n"
            + "    arrString ARRAY<STRING(100)>,\n"
            + "    bytesFoo2392@@# BYTES(MAX)\n"
            + "    bytes BYTES(1024),\n"
            + "    bytes2 BYTES(3923902),\n"
            + ") PRIMARY KEY (word)";

    TableInformation tableInfo = new TableInformation(brokenDdl);
  }

  @Test(expected = RuntimeException.class)
  public void testGetMapOfColumnNamesToSpannerTypesFromTableDdl_error2() {
    String brokenDdl =
        "CREATE TABLE word (\n"
            + "    wor239d STRING(MAX) NOT NULL,\n"
            + "    arrBytes ARRAY<BYTES(1024)>,\n"
            + "    arrString ARRAY<STRING(100)>,\n"
            + "    bytes BYTES(MAX),\n"
            + "    bytes BYTES(1024),\n"
            + "    bytes2 BYTES(3923902),\n"
            + ") PRIMARY KEY (word)";

    TableInformation tableInformation = new TableInformation(brokenDdl);
  }

  @Test
  public void testGetAvroTypeOfColumn() {
    TableInformation tableInformation = new TableInformation(basicDdl1);
    assertEquals(SchemaBuilder.builder().longType(), tableInformation.getAvroTypeOfColumn("SingerId"));
  }

  @Test(expected = RuntimeException.class)
  public void testGetAvroTypeOfColumn_exception() {
    TableInformation tableInformation = new TableInformation(basicDdl1);
    tableInformation.getAvroTypeOfColumn("NoColumn");
  }

  @Test
  public void testGetSpannerTypeOfColumn() {
    TableInformation tableInformation = new TableInformation(basicDdl1);
    assertEquals(Type.int64(), tableInformation.getSpannerTypeOfColumn("SingerId"));
  }

  @Test(expected = RuntimeException.class)
  public void testGetSpannerTypeOfColumn_exception() {
    TableInformation tableInformation = new TableInformation(basicDdl1);
    tableInformation.getSpannerTypeOfColumn("NoColumn");
  }

  @Test
  public void testIsColumnNullable() {
    TableInformation tableInformation = new TableInformation(basicDdl1);
    assertEquals(true, tableInformation.isColumnNullable("FirstName"));
    assertEquals(false, tableInformation.isColumnNullable("SingerId"));
  }

  @Test(expected = RuntimeException.class)
  public void testIsColumnNullable_error() {
    TableInformation tableInformation = new TableInformation(basicDdl1);
    tableInformation.isColumnNullable("NoSuchColumn");
  }
}
