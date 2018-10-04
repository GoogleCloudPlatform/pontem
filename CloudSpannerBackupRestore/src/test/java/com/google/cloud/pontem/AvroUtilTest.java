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
import java.io.File;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link AvroUtil}. */
@RunWith(JUnit4.class)
public class AvroUtilTest {
  @Test
  public void testGetSingleAvroTypeFromNullableUnion() {
    assertEquals(
        SchemaBuilder.builder().stringType(),
        AvroUtil.getSingleAvroTypeFromNullableUnion(
            SchemaBuilder.builder().nullable().stringType()));
  }

  @Test
  public void testGetSingleAvroTypeFromNullableUnion2() {
    assertEquals(
        SchemaBuilder.builder().stringType(),
        AvroUtil.getSingleAvroTypeFromNullableUnion(
            SchemaBuilder.builder()
                .unionOf()
                .stringType()
                .and()
                .type(Schema.create(Schema.Type.NULL))
                .endUnion()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSingleAvroTypeFromNullableUnion_exception() {
    AvroUtil.getSingleAvroTypeFromNullableUnion(SchemaBuilder.builder().stringType());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSingleAvroTypeFromNullableUnion_exception_noNullType() {
    AvroUtil.getSingleAvroTypeFromNullableUnion(
        SchemaBuilder.builder().unionOf().stringType().and().type(TestHelper.SCHEMA_1).endUnion());
  }

  @Test
  public void testBuildSchemaForTable_basic() {
    Schema actualSchema1 =
        AvroUtil.buildSchemaForTable(
            TestHelper.TABLE_NAME_1, new TableInformation(TestHelper.TABLE_DDL_1));
    assertEquals(TestHelper.SCHEMA_1, actualSchema1);

    Schema actualSchema2 =
        AvroUtil.buildSchemaForTable(
            TestHelper.TABLE_NAME_2, new TableInformation(TestHelper.TABLE_DDL_2));
    assertEquals(TestHelper.SCHEMA_2, actualSchema2);

    Schema actualSchema3 =
        AvroUtil.buildSchemaForTable(
            TestHelper.TABLE_NAME_3, new TableInformation(TestHelper.TABLE_DDL_3));
    assertEquals(TestHelper.SCHEMA_3, actualSchema3);

    Schema actualSchema4 =
        AvroUtil.buildSchemaForTable(
            TestHelper.TABLE_NAME_4, new TableInformation(TestHelper.TABLE_DDL_4));
    assertEquals(TestHelper.SCHEMA_4, actualSchema4);
  }

  @Test
  public void testBuildSchemaForTable_advanced() {
    Schema actualSchema5 =
        AvroUtil.buildSchemaForTable(
            TestHelper.TABLE_NAME_5, new TableInformation(TestHelper.TABLE_DDL_5));
    assertEquals(TestHelper.SCHEMA_5, actualSchema5);

    Schema actualSchema6 =
        AvroUtil.buildSchemaForTable(
            TestHelper.TABLE_NAME_6, new TableInformation(TestHelper.TABLE_DDL_6));
    assertEquals(TestHelper.SCHEMA_6, actualSchema6);

    Schema actualSchema7 =
        AvroUtil.buildSchemaForTable(
            TestHelper.TABLE_NAME_7, new TableInformation(TestHelper.TABLE_DDL_7));
    assertEquals(TestHelper.SCHEMA_7, actualSchema7);
  }

  @Test
  public void testGetAvroSchemaFileLocation() {
    String tableName = "fooBar";
    assertEquals(
        "metadata"
            + File.separator
            + AvroUtil.AVRO_SCHEMA_FOLDER_LOCATION
            + File.separator
            + tableName
            + ".json",
        AvroUtil.getAvroSchemaFileLocation(tableName));
  }

  @Test
  public void testGetAvroTypeFromSpannerType_basic() {
    assertEquals(
        SchemaBuilder.builder().stringType(),
        AvroUtil.getAvroTypeFromSpannerType(Type.string(), false));

    assertEquals(
        SchemaBuilder.builder().nullable().stringType(),
        AvroUtil.getAvroTypeFromSpannerType(Type.string(), true));

    assertEquals(
        SchemaBuilder.builder().booleanType(),
        AvroUtil.getAvroTypeFromSpannerType(Type.bool(), false));

    assertEquals(
        SchemaBuilder.builder().nullable().booleanType(),
        AvroUtil.getAvroTypeFromSpannerType(Type.bool(), true));

    assertEquals(
        SchemaBuilder.builder().longType(),
        AvroUtil.getAvroTypeFromSpannerType(Type.int64(), false));

    assertEquals(
        SchemaBuilder.builder().nullable().longType(),
        AvroUtil.getAvroTypeFromSpannerType(Type.int64(), true));

    assertEquals(
        SchemaBuilder.builder().doubleType(),
        AvroUtil.getAvroTypeFromSpannerType(Type.float64(), false));

    assertEquals(
        SchemaBuilder.builder().nullable().doubleType(),
        AvroUtil.getAvroTypeFromSpannerType(Type.float64(), true));

    assertEquals(
        SchemaBuilder.builder().bytesType(),
        AvroUtil.getAvroTypeFromSpannerType(Type.bytes(), false));

    assertEquals(
        SchemaBuilder.builder().nullable().bytesType(),
        AvroUtil.getAvroTypeFromSpannerType(Type.bytes(), true));

    assertEquals(
        SchemaBuilder.builder().stringType(),
        AvroUtil.getAvroTypeFromSpannerType(Type.date(), false));

    assertEquals(
        SchemaBuilder.builder().nullable().stringType(),
        AvroUtil.getAvroTypeFromSpannerType(Type.date(), true));

    assertEquals(
        SchemaBuilder.builder().stringType(),
        AvroUtil.getAvroTypeFromSpannerType(Type.timestamp(), false));

    assertEquals(
        SchemaBuilder.builder().nullable().stringType(),
        AvroUtil.getAvroTypeFromSpannerType(Type.timestamp(), true));

    assertEquals(
        SchemaBuilder.builder().array().items().type(SchemaBuilder.builder().stringType()),
        AvroUtil.getAvroTypeFromSpannerType(Type.array(Type.timestamp()), false));

    assertEquals(
        SchemaBuilder.builder()
            .nullable()
            .array()
            .items()
            .type(SchemaBuilder.builder().stringType()),
        AvroUtil.getAvroTypeFromSpannerType(Type.array(Type.timestamp()), true));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetAvroTypeFromSpannerType_illegalSpannerType() {
    AvroUtil.getAvroTypeFromSpannerType(Type.struct(), true);
  }
}
