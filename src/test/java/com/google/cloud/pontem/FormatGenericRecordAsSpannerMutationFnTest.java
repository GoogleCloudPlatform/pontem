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

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Type;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FormatGenericRecordAsSpannerMutationFnTest}. */
@RunWith(JUnit4.class)
public final class FormatGenericRecordAsSpannerMutationFnTest {

  @Test
  public void testFormatGenericRecordAsSpannerMutationFn_basic() {
    String tableName1 = "basicTable1";
    Schema schema1 =
        SchemaBuilder.record(tableName1)
            .namespace("com.google.cloud.pontem")
            .fields()
            .name("userName")
            .type(SchemaBuilder.builder().stringType())
            .noDefault()
            .name("userId")
            .type(SchemaBuilder.builder().longType())
            .noDefault()
            .endRecord();
    GenericRecord genericRecord1 =
        new GenericRecordBuilder(schema1).set("userName", "John Doe").set("userId", 10L).build();
    ImmutableMap<String, Type> tableMap1 =
        ImmutableMap.of("userName", Type.string(), "userId", Type.int64());

    Mutation expectedMutation =
        Mutation.newInsertOrUpdateBuilder(tableName1)
            .set("userName")
            .to((String) "John Doe")
            .set("userId")
            .to((long) 10L)
            .build();

    FormatGenericRecordAsSpannerMutationFn fn =
        new FormatGenericRecordAsSpannerMutationFn(tableName1, tableMap1);
    Mutation actualMutation = fn.apply(genericRecord1);
    assertEquals(expectedMutation, actualMutation);
  }

  @Test
  public void testFormatGenericRecordAsSpannerMutationFn_basic1() {
    FormatGenericRecordAsSpannerMutationFn fn =
        new FormatGenericRecordAsSpannerMutationFn(
            TestHelper.TABLE_NAME_1, TestHelper.MAP_OF_COLUMN_NAMES_TO_SPANNER_TYPES_1);
    Mutation actualMutation = fn.apply(TestHelper.GENERIC_RECORD_1);
    assertEquals(TestHelper.MUTATION_1, actualMutation);
  }

  @Test
  public void testFormatGenericRecordAsSpannerMutationFn_basic2() {
    FormatGenericRecordAsSpannerMutationFn fn =
        new FormatGenericRecordAsSpannerMutationFn(
            TestHelper.TABLE_NAME_2, TestHelper.MAP_OF_COLUMN_NAMES_TO_SPANNER_TYPES_2);
    Mutation actualMutation = fn.apply(TestHelper.GENERIC_RECORD_2);
    assertEquals(TestHelper.MUTATION_2, actualMutation);
  }

  @Test
  public void testFormatGenericRecordAsSpannerMutationFn_basic3() {
    FormatGenericRecordAsSpannerMutationFn fn =
        new FormatGenericRecordAsSpannerMutationFn(
            TestHelper.TABLE_NAME_3, TestHelper.MAP_OF_COLUMN_NAMES_TO_SPANNER_TYPES_3);
    Mutation actualMutation = fn.apply(TestHelper.GENERIC_RECORD_3);
    assertEquals(TestHelper.MUTATION_3, actualMutation);
  }

  @Test
  public void testFormatGenericRecordAsSpannerMutationFn_basic4() {
    FormatGenericRecordAsSpannerMutationFn fn =
        new FormatGenericRecordAsSpannerMutationFn(
            TestHelper.TABLE_NAME_4, TestHelper.MAP_OF_COLUMN_NAMES_TO_SPANNER_TYPES_4);
    Mutation actualMutation = fn.apply(TestHelper.GENERIC_RECORD_4);
    assertEquals(TestHelper.MUTATION_4, actualMutation);
  }

  @Test
  public void testFormatGenericRecordAsSpannerMutationFn_advanced5() {
    FormatGenericRecordAsSpannerMutationFn fn =
        new FormatGenericRecordAsSpannerMutationFn(
            TestHelper.TABLE_NAME_5, TestHelper.MAP_OF_COLUMN_NAMES_TO_SPANNER_TYPES_5);
    Mutation actualMutation = fn.apply(TestHelper.GENERIC_RECORD_5);
    assertEquals(TestHelper.MUTATION_5, actualMutation);
  }

  @Test
  public void testFormatGenericRecordAsSpannerMutationFn_advanced6() {
    FormatGenericRecordAsSpannerMutationFn fn =
        new FormatGenericRecordAsSpannerMutationFn(
            TestHelper.TABLE_NAME_6, TestHelper.MAP_OF_COLUMN_NAMES_TO_SPANNER_TYPES_6);
    Mutation actualMutation = fn.apply(TestHelper.GENERIC_RECORD_6);
    assertEquals(TestHelper.MUTATION_6, actualMutation);
  }

  @Test
  public void testFormatGenericRecordAsSpannerMutationFn_advanced7() {
    FormatGenericRecordAsSpannerMutationFn fn =
        new FormatGenericRecordAsSpannerMutationFn(
            TestHelper.TABLE_NAME_7, TestHelper.MAP_OF_COLUMN_NAMES_TO_SPANNER_TYPES_7);
    Mutation actualMutation = fn.apply(TestHelper.GENERIC_RECORD_7);
    assertEquals(TestHelper.MUTATION_7, actualMutation);
  }

  @Test
  public void testFormatGenericRecordAsSpannerMutationFn_advanced8() {
    FormatGenericRecordAsSpannerMutationFn fn =
        new FormatGenericRecordAsSpannerMutationFn(
            TestHelper.TABLE_NAME_8, TestHelper.MAP_OF_COLUMN_NAMES_TO_SPANNER_TYPES_8);
    Mutation actualMutation = fn.apply(TestHelper.GENERIC_RECORD_8);
    assertEquals(TestHelper.MUTATION_8, actualMutation);
  }
}
