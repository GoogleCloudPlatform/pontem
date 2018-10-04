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

import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricStructuredName;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

/** Class to provide functionality to help with tests. */
public class TestHelper {

  public static final String TABLE_NAME_1 = "tableName";
  public static final Schema SCHEMA_1 =
      SchemaBuilder.record(TABLE_NAME_1)
          .namespace(AvroUtil.AVRO_SCHEMA_NAMESPACE)
          .prop("pontemAvroSchemaFormattingVersion", AvroUtil.AVRO_RECORD_VERSION)
          .fields()
          .name("property_a")
          .type(SchemaBuilder.builder().nullable().stringType())
          .noDefault()
          .name("property_b")
          .type(SchemaBuilder.builder().nullable().stringType())
          .noDefault()
          .name("property_c")
          .type(SchemaBuilder.builder().nullable().longType())
          .noDefault()
          .endRecord();
  public static final ImmutableMap<String, Type> MAP_OF_COLUMN_NAMES_TO_SPANNER_TYPES_1 =
      ImmutableMap.of(
          "property_a", Type.string(), "property_b", Type.string(), "property_c", Type.int64());
  public static final String TABLE_DDL_1 =
      "CREATE TABLE "
          + TABLE_NAME_1
          + " (\n"
          + "\tproperty_a STRING(MAX),\n"
          + "\tproperty_b STRING(MAX),\n"
          + "\tproperty_c INT64,\n"
          + ") PRIMARY KEY (property_a)";

  public static final String TABLE_NAME_2 = "tableName";
  public static final Schema SCHEMA_2 =
      SchemaBuilder.record(TABLE_NAME_2)
          .namespace(AvroUtil.AVRO_SCHEMA_NAMESPACE)
          .prop("pontemAvroSchemaFormattingVersion", AvroUtil.AVRO_RECORD_VERSION)
          .fields()
          .name("property_a")
          .type(SchemaBuilder.builder().stringType())
          .noDefault()
          .name("property_b")
          .type(SchemaBuilder.builder().nullable().stringType())
          .noDefault()
          .name("property_c")
          .type(SchemaBuilder.builder().nullable().longType())
          .noDefault()
          .endRecord();
  public static final ImmutableMap<String, Type> MAP_OF_COLUMN_NAMES_TO_SPANNER_TYPES_2 =
      ImmutableMap.of(
          "property_a", Type.string(), "property_b", Type.string(), "property_c", Type.int64());
  public static final String TABLE_DDL_2 =
      "CREATE TABLE "
          + TABLE_NAME_2
          + " (\n"
          + "\tproperty_a STRING(MAX) NOT NULL,\n"
          + "\tproperty_b STRING(MAX),\n"
          + "\tproperty_c INT64,\n"
          + ") PRIMARY KEY (property_a)";

  public static final String TABLE_NAME_3 = "tableName";
  public static final Schema SCHEMA_3 =
      SchemaBuilder.record(TABLE_NAME_3)
          .namespace(AvroUtil.AVRO_SCHEMA_NAMESPACE)
          .prop("pontemAvroSchemaFormattingVersion", AvroUtil.AVRO_RECORD_VERSION)
          .fields()
          .name("property_a")
          .type(SchemaBuilder.builder().longType())
          .noDefault()
          .name("property_b")
          .type(SchemaBuilder.builder().nullable().stringType())
          .noDefault()
          .endRecord();
  public static final ImmutableMap<String, Type> MAP_OF_COLUMN_NAMES_TO_SPANNER_TYPES_3 =
      ImmutableMap.of("property_a", Type.int64(), "property_b", Type.string());
  public static final String TABLE_DDL_3 =
      "CREATE TABLE "
          + TABLE_NAME_3
          + " (\n"
          + "\tproperty_a INT64 NOT NULL,\n"
          + "\tproperty_b STRING(MAX),\n"
          + ") PRIMARY KEY (property_a)";

  public static final String TABLE_NAME_4 = "tableName";
  public static final Schema SCHEMA_4 =
      SchemaBuilder.record(TABLE_NAME_4)
          .namespace(AvroUtil.AVRO_SCHEMA_NAMESPACE)
          .prop("pontemAvroSchemaFormattingVersion", AvroUtil.AVRO_RECORD_VERSION)
          .fields()
          .name("property_a")
          .type(SchemaBuilder.builder().longType())
          .noDefault()
          .name("property_b")
          .type(SchemaBuilder.builder().nullable().array().items().stringType())
          .noDefault()
          .endRecord();
  public static final ImmutableMap<String, Type> MAP_OF_COLUMN_NAMES_TO_SPANNER_TYPES_4 =
      ImmutableMap.of("property_a", Type.int64(), "property_b", Type.array(Type.string()));
  public static final String TABLE_DDL_4 =
      "CREATE TABLE "
          + TABLE_NAME_4
          + " (\n"
          + "\tproperty_a INT64 NOT NULL,\n"
          + "\tproperty_b ARRAY<STRING(MAX)>,\n"
          + ") PRIMARY KEY (property_a)";

  public static final String TABLE_NAME_5 = "tableName";
  public static final Schema SCHEMA_5 =
      SchemaBuilder.record(TABLE_NAME_5)
          .namespace(AvroUtil.AVRO_SCHEMA_NAMESPACE)
          .prop("pontemAvroSchemaFormattingVersion", AvroUtil.AVRO_RECORD_VERSION)
          .fields()
          .name("property_a")
          .type(SchemaBuilder.builder().array().items().booleanType())
          .noDefault()
          .name("property_b")
          .type(SchemaBuilder.builder().nullable().array().items().stringType())
          .noDefault()
          .name("property_c")
          .type(SchemaBuilder.builder().nullable().array().items().doubleType())
          .noDefault()
          .name("property_d")
          .type(SchemaBuilder.builder().array().items().longType())
          .noDefault()
          .name("property_e")
          .type(SchemaBuilder.builder().array().items().stringType())
          .noDefault()
          .name("property_f")
          .type(SchemaBuilder.builder().array().items().stringType())
          .noDefault()
          .endRecord();
  public static final ImmutableMap<String, Type> MAP_OF_COLUMN_NAMES_TO_SPANNER_TYPES_5 =
      new ImmutableMap.Builder<String, Type>()
          .put("property_a", Type.array(Type.bool()))
          .put("property_b", Type.array(Type.date()))
          .put("property_c", Type.array(Type.float64()))
          .put("property_d", Type.array(Type.int64()))
          .put("property_e", Type.array(Type.string()))
          .put("property_f", Type.array(Type.timestamp()))
          .build();
  public static final String TABLE_DDL_5 =
      "CREATE TABLE "
          + TABLE_NAME_5
          + " (\n"
          + "\tproperty_a ARRAY<BOOL> NOT NULL,\n"
          + "\tproperty_b ARRAY<DATE>,\n"
          + "\tproperty_c ARRAY<FLOAT64>,\n"
          + "\tproperty_d ARRAY<INT64> NOT NULL,\n"
          + "\tproperty_e ARRAY<STRING> NOT NULL,\n"
          + "\tproperty_f ARRAY<TIMESTAMP> NOT NULL,\n"
          + ") PRIMARY KEY (property_a)";

  public static final String TABLE_NAME_6 = "tableName";
  public static final Schema SCHEMA_6 =
      SchemaBuilder.record(TABLE_NAME_6)
          .namespace(AvroUtil.AVRO_SCHEMA_NAMESPACE)
          .prop("pontemAvroSchemaFormattingVersion", AvroUtil.AVRO_RECORD_VERSION)
          .fields()
          .name("property_a")
          .type(SchemaBuilder.builder().array().items().booleanType())
          .noDefault()
          .name("property_b")
          .type(SchemaBuilder.builder().nullable().array().items().stringType())
          .noDefault()
          .name("property_c")
          .type(SchemaBuilder.builder().nullable().array().items().doubleType())
          .noDefault()
          .name("property_d")
          .type(SchemaBuilder.builder().array().items().longType())
          .noDefault()
          .name("property_e")
          .type(SchemaBuilder.builder().array().items().stringType())
          .noDefault()
          .name("property_f")
          .type(SchemaBuilder.builder().array().items().stringType())
          .noDefault()
          .name("property_g")
          .type(SchemaBuilder.builder().booleanType())
          .noDefault()
          .name("property_h")
          .type(SchemaBuilder.builder().stringType())
          .noDefault()
          .name("property_i")
          .type(SchemaBuilder.builder().nullable().stringType())
          .noDefault()
          .name("property_j")
          .type(SchemaBuilder.builder().longType())
          .noDefault()
          .endRecord();
  public static final ImmutableMap<String, Type> MAP_OF_COLUMN_NAMES_TO_SPANNER_TYPES_6 =
      new ImmutableMap.Builder<String, Type>()
          .put("property_a", Type.array(Type.bool()))
          .put("property_b", Type.array(Type.date()))
          .put("property_c", Type.array(Type.float64()))
          .put("property_d", Type.array(Type.int64()))
          .put("property_e", Type.array(Type.string()))
          .put("property_f", Type.array(Type.timestamp()))
          .put("property_g", Type.bool())
          .put("property_h", Type.date())
          .put("property_i", Type.timestamp())
          .put("property_j", Type.int64())
          .build();
  public static final String TABLE_DDL_6 =
      "CREATE TABLE "
          + TABLE_NAME_6
          + " (\n"
          + "\tproperty_a ARRAY<BOOL> NOT NULL,\n"
          + "\tproperty_b ARRAY<DATE>,\n"
          + "\tproperty_c ARRAY<FLOAT64>,\n"
          + "\tproperty_d ARRAY<INT64> NOT NULL,\n"
          + "\tproperty_e ARRAY<STRING> NOT NULL,\n"
          + "\tproperty_f ARRAY<TIMESTAMP> NOT NULL,\n"
          + "\tproperty_g BOOL NOT NULL,\n"
          + "\tproperty_h DATE NOT NULL,\n"
          + "\tproperty_i TIMESTAMP,\n"
          + "\tproperty_j INT64 NOT NULL,\n"
          + ") PRIMARY KEY (property_a)";

  public static final String TABLE_NAME_7 = "tableName7";
  public static final Schema SCHEMA_7 =
      SchemaBuilder.record(TABLE_NAME_7)
          .namespace(AvroUtil.AVRO_SCHEMA_NAMESPACE)
          .prop("pontemAvroSchemaFormattingVersion", AvroUtil.AVRO_RECORD_VERSION)
          .fields()
          .name("property_a")
          .type(SchemaBuilder.builder().array().items().booleanType())
          .noDefault()
          .name("property_b")
          .type(SchemaBuilder.builder().array().items().stringType())
          .noDefault()
          .name("property_c")
          .type(SchemaBuilder.builder().array().items().doubleType())
          .noDefault()
          .name("property_d")
          .type(SchemaBuilder.builder().array().items().longType())
          .noDefault()
          .name("property_e")
          .type(SchemaBuilder.builder().array().items().stringType())
          .noDefault()
          .name("property_f")
          .type(SchemaBuilder.builder().array().items().stringType())
          .noDefault()
          .name("property_g")
          .type(SchemaBuilder.builder().array().items().bytesType())
          .noDefault()
          .name("property_h")
          .type(SchemaBuilder.builder().nullable().array().items().booleanType())
          .noDefault()
          .name("property_i")
          .type(SchemaBuilder.builder().nullable().array().items().stringType())
          .noDefault()
          .name("property_j")
          .type(SchemaBuilder.builder().nullable().array().items().doubleType())
          .noDefault()
          .name("property_k")
          .type(SchemaBuilder.builder().nullable().array().items().longType())
          .noDefault()
          .name("property_l")
          .type(SchemaBuilder.builder().nullable().array().items().stringType())
          .noDefault()
          .name("property_m")
          .type(SchemaBuilder.builder().nullable().array().items().stringType())
          .noDefault()
          .name("property_n")
          .type(SchemaBuilder.builder().nullable().array().items().bytesType())
          .noDefault()
          .name("property_o")
          .type(SchemaBuilder.builder().booleanType())
          .noDefault()
          .name("property_p")
          .type(SchemaBuilder.builder().stringType())
          .noDefault()
          .name("property_q")
          .type(SchemaBuilder.builder().doubleType())
          .noDefault()
          .name("property_r")
          .type(SchemaBuilder.builder().longType())
          .noDefault()
          .name("property_s")
          .type(SchemaBuilder.builder().stringType())
          .noDefault()
          .name("property_t")
          .type(SchemaBuilder.builder().stringType())
          .noDefault()
          .name("property_u")
          .type(SchemaBuilder.builder().bytesType())
          .noDefault()
          .name("property_v")
          .type(SchemaBuilder.builder().nullable().booleanType())
          .noDefault()
          .name("property_w")
          .type(SchemaBuilder.builder().nullable().stringType())
          .noDefault()
          .name("property_x")
          .type(SchemaBuilder.builder().nullable().doubleType())
          .noDefault()
          .name("property_y")
          .type(SchemaBuilder.builder().nullable().longType())
          .noDefault()
          .name("property_z")
          .type(SchemaBuilder.builder().nullable().stringType())
          .noDefault()
          .name("property_aa")
          .type(SchemaBuilder.builder().nullable().stringType())
          .noDefault()
          .name("property_bb")
          .type(SchemaBuilder.builder().nullable().bytesType())
          .noDefault()
          .endRecord();
  public static final ImmutableMap<String, Type> MAP_OF_COLUMN_NAMES_TO_SPANNER_TYPES_7 =
      new ImmutableMap.Builder<String, Type>()
          .put("property_a", Type.array(Type.bool()))
          .put("property_b", Type.array(Type.date()))
          .put("property_c", Type.array(Type.float64()))
          .put("property_d", Type.array(Type.int64()))
          .put("property_e", Type.array(Type.string()))
          .put("property_f", Type.array(Type.timestamp()))
          .put("property_g", Type.array(Type.bytes()))
          .put("property_h", Type.array(Type.bool()))
          .put("property_i", Type.array(Type.date()))
          .put("property_j", Type.array(Type.float64()))
          .put("property_k", Type.array(Type.int64()))
          .put("property_l", Type.array(Type.string()))
          .put("property_m", Type.array(Type.timestamp()))
          .put("property_n", Type.array(Type.bytes()))
          .put("property_o", Type.bool())
          .put("property_p", Type.date())
          .put("property_q", Type.float64())
          .put("property_r", Type.int64())
          .put("property_s", Type.string())
          .put("property_t", Type.timestamp())
          .put("property_u", Type.bytes())
          .put("property_v", Type.bool())
          .put("property_w", Type.date())
          .put("property_x", Type.float64())
          .put("property_y", Type.int64())
          .put("property_z", Type.string())
          .put("property_aa", Type.timestamp())
          .put("property_bb", Type.bytes())
          .build();
  public static final String TABLE_DDL_7 =
      "CREATE TABLE "
          + TABLE_NAME_7
          + " (\n"
          + "\tproperty_a ARRAY<BOOL> NOT NULL,\n"
          + "\tproperty_b ARRAY<DATE> NOT NULL,\n"
          + "\tproperty_c ARRAY<FLOAT64> NOT NULL,\n"
          + "\tproperty_d ARRAY<INT64> NOT NULL,\n"
          + "\tproperty_e ARRAY<STRING> NOT NULL,\n"
          + "\tproperty_f ARRAY<TIMESTAMP> NOT NULL,\n"
          + "\tproperty_g ARRAY<BYTES> NOT NULL,\n"
          + "\tproperty_h ARRAY<BOOL>,\n"
          + "\tproperty_i ARRAY<DATE>,\n"
          + "\tproperty_j ARRAY<FLOAT64>,\n"
          + "\tproperty_k ARRAY<INT64>,\n"
          + "\tproperty_l ARRAY<STRING>,\n"
          + "\tproperty_m ARRAY<TIMESTAMP>,\n"
          + "\tproperty_n ARRAY<BYTES>,\n"
          + "\tproperty_o BOOL NOT NULL,\n"
          + "\tproperty_p DATE NOT NULL,\n"
          + "\tproperty_q FLOAT64 NOT NULL,\n"
          + "\tproperty_r INT64 NOT NULL,\n"
          + "\tproperty_s STRING NOT NULL,\n"
          + "\tproperty_t TIMESTAMP NOT NULL,\n"
          + "\tproperty_u BYTES NOT NULL,\n"
          + "\tproperty_v BOOL,\n"
          + "\tproperty_w DATE,\n"
          + "\tproperty_x FLOAT64,\n"
          + "\tproperty_y INT64,\n"
          + "\tproperty_z STRING,\n"
          + "\tproperty_aa TIMESTAMP,\n"
          + "\tproperty_bb BYTES,\n"
          + ") PRIMARY KEY (property_a)";
  public static final String TABLE_NAME_8 = "table8";
  public static final Schema SCHEMA_8 =
      SchemaBuilder.record(TABLE_NAME_8)
          .namespace(AvroUtil.AVRO_SCHEMA_NAMESPACE)
          .prop("pontemAvroSchemaFormattingVersion", AvroUtil.AVRO_RECORD_VERSION)
          .fields()
          .name("property_a")
          .type(SchemaBuilder.builder().bytesType())
          .noDefault()
          .name("property_b")
          .type(SchemaBuilder.builder().array().items().bytesType())
          .noDefault()
          .name("property_c")
          .type(SchemaBuilder.builder().stringType())
          .noDefault()
          .endRecord();
  public static final ImmutableMap<String, Type> MAP_OF_COLUMN_NAMES_TO_SPANNER_TYPES_8 =
      ImmutableMap.of(
          "property_a",
          Type.bytes(),
          "property_b",
          Type.array(Type.bytes()),
          "property_c",
          Type.string());
  public static final String TABLE_DDL_8 =
      "CREATE TABLE "
          + TABLE_NAME_8
          + " (\n"
          + "\tproperty_a BYTES NOT NULL,\n"
          + "\tproperty_b ARRAY<BYTES> NOT NULL,\n"
          + "\tproperty_c STRING(MAX) NOT NULL,\n"
          + ") PRIMARY KEY (property_a)";

  private static final String PROPERTY_A1 = "propertyA1";
  private static final String PROPERTY_B1 = "propertyB1";
  private static final long PROPERTY_C1 = 111L;
  public static final Struct STRUCT_1 =
      Struct.newBuilder()
          .set("property_a")
          .to(PROPERTY_A1)
          .set("property_b")
          .to(PROPERTY_B1)
          .set("property_c")
          .to(PROPERTY_C1)
          .build();
  public static final Mutation MUTATION_1 =
      Mutation.newInsertOrUpdateBuilder(TestHelper.TABLE_NAME_1)
          .set("property_a")
          .to(PROPERTY_A1)
          .set("property_b")
          .to(PROPERTY_B1)
          .set("property_c")
          .to(PROPERTY_C1)
          .build();
  public static final GenericRecord GENERIC_RECORD_1 =
      new GenericRecordBuilder(SCHEMA_1)
          .set("property_a", PROPERTY_A1)
          .set("property_b", PROPERTY_B1)
          .set("property_c", PROPERTY_C1)
          .build();
  private static final String PROPERTY_A2 = "propertyA2";
  private static final String PROPERTY_B2 = "propertyB2";
  private static final long PROPERTY_C2 = 222L;
  public static final Struct STRUCT_2 =
      Struct.newBuilder()
          .set("property_a")
          .to(PROPERTY_A2)
          .set("property_b")
          .to(PROPERTY_B2)
          .set("property_c")
          .to(PROPERTY_C2)
          .build();
  public static final Mutation MUTATION_2 =
      Mutation.newInsertOrUpdateBuilder(TestHelper.TABLE_NAME_2)
          .set("property_a")
          .to(PROPERTY_A2)
          .set("property_b")
          .to(PROPERTY_B2)
          .set("property_c")
          .to(PROPERTY_C2)
          .build();
  public static final GenericRecord GENERIC_RECORD_2 =
      new GenericRecordBuilder(SCHEMA_2)
          .set("property_a", PROPERTY_A2)
          .set("property_b", PROPERTY_B2)
          .set("property_c", PROPERTY_C2)
          .build();
  private static final int PROPERTY_A3 = 10;
  public static final Mutation MUTATION_3 =
      Mutation.newInsertOrUpdateBuilder(TestHelper.TABLE_NAME_3)
          .set("property_a")
          .to(PROPERTY_A3)
          .build();
  private static final String PROPERTY_B3 = null;
  public static final Struct STRUCT_3 =
      Struct.newBuilder()
          .set("property_a")
          .to(PROPERTY_A3)
          .set("property_b")
          .to(PROPERTY_B3)
          .build();
  public static final GenericRecord GENERIC_RECORD_3 =
      new GenericRecordBuilder(SCHEMA_3)
          .set("property_a", new Long(PROPERTY_A3))
          .set("property_b", PROPERTY_B3)
          .build();
  private static final int PROPERTY_A4 = 1009;
  private static final List<String> PROPERTY_B4 = ImmutableList.of("hello", "d", "232a!");
  public static final Struct STRUCT_4 =
      Struct.newBuilder()
          .set("property_a")
          .to(PROPERTY_A4)
          .set("property_b")
          .toStringArray(PROPERTY_B4)
          .build();
  public static final Mutation MUTATION_4 =
      Mutation.newInsertOrUpdateBuilder(TestHelper.TABLE_NAME_4)
          .set("property_a")
          .to(PROPERTY_A4)
          .set("property_b")
          .toStringArray(PROPERTY_B4)
          .build();
  public static final GenericRecord GENERIC_RECORD_4 =
      new GenericRecordBuilder(SCHEMA_4)
          .set("property_a", new Long(PROPERTY_A4))
          .set("property_b", PROPERTY_B4)
          .build();
  private static final List<Boolean> PROPERTY_A5 = ImmutableList.of(true, false, true);
  private static final List<Date> PROPERTY_B5 =
      ImmutableList.of(Date.fromYearMonthDay(2018, 2, 1), Date.fromYearMonthDay(2017, 4, 8));
  private static final List<Double> PROPERTY_C5 = ImmutableList.of(23902d, 392d);
  private static final List<Long> PROPERTY_D5 = ImmutableList.of(329L, 2329302L);
  private static final List<String> PROPERTY_E5 = ImmutableList.of("hello", "d", "232a!");
  private static final List<Timestamp> PROPERTY_F5 =
      ImmutableList.of(
          Timestamp.ofTimeMicroseconds(23892983L), Timestamp.ofTimeMicroseconds(23923892L));
  public static final Struct STRUCT_5 =
      Struct.newBuilder()
          .set("property_a")
          .toBoolArray(PROPERTY_A5)
          .set("property_b")
          .toDateArray(PROPERTY_B5)
          .set("property_c")
          .toFloat64Array(PROPERTY_C5)
          .set("property_d")
          .toInt64Array(PROPERTY_D5)
          .set("property_e")
          .toStringArray(PROPERTY_E5)
          .set("property_f")
          .toTimestampArray(PROPERTY_F5)
          .build();
  public static final Mutation MUTATION_5 =
      Mutation.newInsertOrUpdateBuilder(TestHelper.TABLE_NAME_5)
          .set("property_a")
          .toBoolArray(PROPERTY_A5)
          .set("property_b")
          .toDateArray(PROPERTY_B5)
          .set("property_c")
          .toFloat64Array(PROPERTY_C5)
          .set("property_d")
          .toInt64Array(PROPERTY_D5)
          .set("property_e")
          .toStringArray(PROPERTY_E5)
          .set("property_f")
          .toTimestampArray(PROPERTY_F5)
          .build();
  public static final GenericRecord GENERIC_RECORD_5 =
      new GenericRecordBuilder(SCHEMA_5)
          .set("property_a", PROPERTY_A5)
          .set(
              "property_b",
              PROPERTY_B5.stream().map(val -> val.toString()).collect(Collectors.toList()))
          .set("property_c", PROPERTY_C5)
          .set("property_d", PROPERTY_D5)
          .set("property_e", PROPERTY_E5)
          .set(
              "property_f",
              PROPERTY_F5.stream().map(val -> val.toString()).collect(Collectors.toList()))
          .build();
  private static final List<Boolean> PROPERTY_A6 = ImmutableList.of(true, false, true);
  private static final List<Date> PROPERTY_B6 =
      ImmutableList.of(Date.fromYearMonthDay(2018, 2, 1), Date.fromYearMonthDay(2017, 4, 8));
  private static final List<Double> PROPERTY_C6 = ImmutableList.of(23902d, 392d);
  private static final List<Long> PROPERTY_D6 = ImmutableList.of(329L, 2329302L);
  private static final List<String> PROPERTY_E6 = ImmutableList.of("hello", "d", "232a!");
  private static final List<Timestamp> PROPERTY_F6 =
      ImmutableList.of(
          Timestamp.ofTimeMicroseconds(23892983L), Timestamp.ofTimeMicroseconds(23923892L));
  private static final List<Boolean> PROPERTY_A7 = ImmutableList.of(true, false, true);
  private static final List<Date> PROPERTY_B7 =
      ImmutableList.of(Date.fromYearMonthDay(2018, 2, 1), Date.fromYearMonthDay(2017, 4, 8));
  private static final List<Double> PROPERTY_C7 = ImmutableList.of(23902d, 392d);
  private static final List<Long> PROPERTY_D7 = ImmutableList.of(329L, 2329302L);
  private static final List<String> PROPERTY_E7 = ImmutableList.of("hello", "你好", "Halló");
  private static final List<Timestamp> PROPERTY_F7 =
      ImmutableList.of(
          Timestamp.ofTimeMicroseconds(23892983L), Timestamp.ofTimeMicroseconds(23923892L));
  private static final List<ByteArray> PROPERTY_G7 =
      ImmutableList.of(
          ByteArray.copyFrom("dsjks"),
          ByteArray.copyFrom("2002j9w"),
          ByteArray.copyFrom("*%$@(%*(Js"));
  private static final List<Boolean> PROPERTY_H7 = null;
  private static final List<Date> PROPERTY_I7 = null;
  private static final List<Double> PROPERTY_J7 = null;
  private static final List<Long> PROPERTY_K7 = null;
  private static final List<String> PROPERTY_L7 = null;
  private static final List<Timestamp> PROPERTY_M7 = null;
  private static final List<ByteArray> PROPERTY_N7 = null;
  private static final boolean PROPERTY_O7 = false;
  private static final Date PROPERTY_P7 = Date.fromYearMonthDay(2017, 2, 9);
  private static final double PROPERTY_Q7 = 31.29d;
  private static final long PROPERTY_R7 = 930493L;
  private static final String PROPERTY_S7 = "测试";
  private static final Timestamp PROPERTY_T7 = Timestamp.ofTimeMicroseconds(23921932L);
  private static final ByteArray PROPERTY_U7 = ByteArray.copyFrom("再见");
  public static final Mutation MUTATION_7 =
      Mutation.newInsertOrUpdateBuilder(TestHelper.TABLE_NAME_7)
          .set("property_a")
          .toBoolArray(PROPERTY_A7)
          .set("property_b")
          .toDateArray(PROPERTY_B7)
          .set("property_c")
          .toFloat64Array(PROPERTY_C7)
          .set("property_d")
          .toInt64Array(PROPERTY_D7)
          .set("property_e")
          .toStringArray(PROPERTY_E7)
          .set("property_f")
          .toTimestampArray(PROPERTY_F7)
          .set("property_g")
          .toBytesArray(PROPERTY_G7)
          // Properties H -> N are NULL
          .set("property_o")
          .to(PROPERTY_O7)
          .set("property_p")
          .to(PROPERTY_P7)
          .set("property_q")
          .to(PROPERTY_Q7)
          .set("property_r")
          .to(PROPERTY_R7)
          .set("property_s")
          .to(PROPERTY_S7)
          .set("property_t")
          .to(PROPERTY_T7)
          .set("property_u")
          .to(PROPERTY_U7)
          // Properties V -> BB are NULL
          .build();
  private static final Boolean PROPERTY_V7 = null;
  private static final Date PROPERTY_W7 = null;
  private static final Double PROPERTY_X7 = null;
  private static final Long PROPERTY_Y7 = null;
  private static final String PROPERTY_Z7 = null;
  private static final Timestamp PROPERTY_AA7 = null;
  private static final ByteArray PROPERTY_BB7 = null;
  public static final Struct STRUCT_7 =
      Struct.newBuilder()
          .set("property_a")
          .toBoolArray(PROPERTY_A7)
          .set("property_b")
          .toDateArray(PROPERTY_B7)
          .set("property_c")
          .toFloat64Array(PROPERTY_C7)
          .set("property_d")
          .toInt64Array(PROPERTY_D7)
          .set("property_e")
          .toStringArray(PROPERTY_E7)
          .set("property_f")
          .toTimestampArray(PROPERTY_F7)
          .set("property_g")
          .toBytesArray(PROPERTY_G7)
          .set("property_h")
          .toBoolArray(PROPERTY_H7)
          .set("property_i")
          .toDateArray(PROPERTY_I7)
          .set("property_j")
          .toFloat64Array(PROPERTY_J7)
          .set("property_k")
          .toInt64Array(PROPERTY_K7)
          .set("property_l")
          .toStringArray(PROPERTY_L7)
          .set("property_m")
          .toTimestampArray(PROPERTY_M7)
          .set("property_n")
          .toBytesArray(PROPERTY_N7)
          .set("property_o")
          .to(PROPERTY_O7)
          .set("property_p")
          .to(PROPERTY_P7)
          .set("property_q")
          .to(PROPERTY_Q7)
          .set("property_r")
          .to(PROPERTY_R7)
          .set("property_s")
          .to(PROPERTY_S7)
          .set("property_t")
          .to(PROPERTY_T7)
          .set("property_u")
          .to(PROPERTY_U7)
          .set("property_v")
          .to(PROPERTY_V7)
          .set("property_w")
          .to(PROPERTY_W7)
          .set("property_x")
          .to(PROPERTY_X7)
          .set("property_y")
          .to(PROPERTY_Y7)
          .set("property_z")
          .to(PROPERTY_Z7)
          .set("property_aa")
          .to(PROPERTY_AA7)
          .set("property_bb")
          .to(PROPERTY_BB7)
          .build();
  public static final GenericRecord GENERIC_RECORD_7 =
      new GenericRecordBuilder(SCHEMA_7)
          .set("property_a", PROPERTY_A7)
          .set(
              "property_b",
              PROPERTY_B7.stream().map(val -> val.toString()).collect(Collectors.toList()))
          .set("property_c", PROPERTY_C7)
          .set("property_d", PROPERTY_D7)
          .set("property_e", PROPERTY_E7)
          .set(
              "property_f",
              PROPERTY_F7.stream().map(val -> val.toString()).collect(Collectors.toList()))
          .set(
              "property_g",
              PROPERTY_G7
                  .stream()
                  .map(val -> ByteBuffer.wrap(val.toByteArray()))
                  .collect(Collectors.toList()))
          .set("property_h", PROPERTY_H7)
          .set("property_i", PROPERTY_I7)
          .set("property_j", PROPERTY_J7)
          .set("property_k", PROPERTY_K7)
          .set("property_l", PROPERTY_L7)
          .set("property_m", PROPERTY_M7)
          .set("property_n", PROPERTY_N7)
          .set("property_o", PROPERTY_O7)
          .set("property_p", PROPERTY_P7.toString())
          .set("property_q", PROPERTY_Q7)
          .set("property_r", PROPERTY_R7)
          .set("property_s", PROPERTY_S7)
          .set("property_t", PROPERTY_T7.toString())
          .set("property_u", ByteBuffer.wrap(PROPERTY_U7.toByteArray()))
          .set("property_v", PROPERTY_V7)
          .set("property_w", PROPERTY_W7)
          .set("property_x", PROPERTY_X7)
          .set("property_y", PROPERTY_Y7)
          .set("property_z", PROPERTY_Z7)
          .set("property_aa", PROPERTY_AA7)
          .set("property_bb", PROPERTY_BB7)
          .build();
  private static final ByteArray PROPERTY_A8 = ByteArray.copyFrom("dsjksjd");
  private static final List<ByteArray> PROPERTY_B8 =
      ImmutableList.of(ByteArray.copyFrom("dss"), ByteArray.copyFrom("as%$@(%*(Js"));
  private static final String PROPERTY_C8 = "foo";
  public static final Struct STRUCT_8 =
      Struct.newBuilder()
          .set("property_a")
          .to(PROPERTY_A8)
          .set("property_b")
          .toBytesArray(PROPERTY_B8)
          .set("property_c")
          .to(PROPERTY_C8)
          .build();
  public static final Mutation MUTATION_8 =
      Mutation.newInsertOrUpdateBuilder(TestHelper.TABLE_NAME_8)
          .set("property_a")
          .to(PROPERTY_A8)
          .set("property_b")
          .toBytesArray(PROPERTY_B8)
          .set("property_c")
          .to(PROPERTY_C8)
          .build();
  public static final GenericRecord GENERIC_RECORD_8 =
      new GenericRecordBuilder(SCHEMA_8)
          .set("property_a", ByteBuffer.wrap(PROPERTY_A8.toByteArray()))
          .set("property_c", PROPERTY_C8)
          .set(
              "property_b",
              PROPERTY_B8
                  .stream()
                  .map(val -> ByteBuffer.wrap(val.toByteArray()))
                  .collect(Collectors.toList()))
          .build();
  private static Boolean PROPERTY_G6 = true;
  private static Date PROPERTY_H6 = Date.fromYearMonthDay(2017, 4, 8);
  private static Timestamp PROPERTY_I6 = Timestamp.ofTimeMicroseconds(23923892L);
  private static Long PROPERTY_J6 = 2930L;
  public static final Struct STRUCT_6 =
      Struct.newBuilder()
          .set("property_a")
          .toBoolArray(PROPERTY_A6)
          .set("property_b")
          .toDateArray(PROPERTY_B6)
          .set("property_c")
          .toFloat64Array(PROPERTY_C6)
          .set("property_d")
          .toInt64Array(PROPERTY_D6)
          .set("property_e")
          .toStringArray(PROPERTY_E6)
          .set("property_f")
          .toTimestampArray(PROPERTY_F6)
          .set("property_g")
          .to((boolean) PROPERTY_G6)
          .set("property_h")
          .to((Date) PROPERTY_H6)
          .set("property_i")
          .to((Timestamp) PROPERTY_I6)
          .set("property_j")
          .to((long) PROPERTY_J6)
          .build();
  public static final Mutation MUTATION_6 =
      Mutation.newInsertOrUpdateBuilder(TestHelper.TABLE_NAME_6)
          .set("property_a")
          .toBoolArray(PROPERTY_A6)
          .set("property_b")
          .toDateArray(PROPERTY_B6)
          .set("property_c")
          .toFloat64Array(PROPERTY_C6)
          .set("property_d")
          .toInt64Array(PROPERTY_D6)
          .set("property_e")
          .toStringArray(PROPERTY_E6)
          .set("property_f")
          .toTimestampArray(PROPERTY_F6)
          .set("property_g")
          .to((boolean) PROPERTY_G6)
          .set("property_h")
          .to((Date) PROPERTY_H6)
          .set("property_i")
          .to((Timestamp) PROPERTY_I6)
          .set("property_j")
          .to((long) PROPERTY_J6)
          .build();
  public static final GenericRecord GENERIC_RECORD_6 =
      new GenericRecordBuilder(SCHEMA_6)
          .set("property_a", PROPERTY_A6)
          .set(
              "property_b",
              PROPERTY_B6.stream().map(val -> val.toString()).collect(Collectors.toList()))
          .set("property_c", PROPERTY_C6)
          .set("property_d", PROPERTY_D6)
          .set("property_e", PROPERTY_E6)
          .set(
              "property_f",
              PROPERTY_F6.stream().map(val -> val.toString()).collect(Collectors.toList()))
          .set("property_g", PROPERTY_G6)
          .set("property_h", PROPERTY_H6.toString())
          .set("property_i", PROPERTY_I6.toString())
          .set("property_j", PROPERTY_J6)
          .build();

  /** Returns fake JobMetrics based on the provided Table and Row counts plus a filler Metric. */
  public static JobMetrics getJobMetrics(Map<String, Long> tableRowCounts) {
    // Add a filler/noise metric
    MetricUpdate metricUpdate1 = new MetricUpdate();
    metricUpdate1.setScalar(new BigDecimal(267L));
    metricUpdate1.setUpdateTime("2017-10-23T19:10:48.566Z");
    MetricStructuredName structuredName1 = new MetricStructuredName();
    structuredName1.setOrigin("dataflow/v1b3");
    structuredName1.setName("MeanByteCount");
    structuredName1.setContext(
        ImmutableMap.of(
            "output_user_name",
            "Read Table List/SpannerIO.CreateTransaction/Createtransaction-out0",
            "original_name",
            "Read Table List/SpannerIO.CreateTransaction/Createtransaction-out0-"
                + "MeanByteCount"));
    metricUpdate1.setName(structuredName1);

    List<MetricUpdate> metricUpdates = Lists.newArrayList();
    metricUpdates.add(metricUpdate1);

    for (Map.Entry<String, Long> entry : tableRowCounts.entrySet()) {
      Long numRows = entry.getValue();

      MetricUpdate metricUpdate2 = new MetricUpdate();
      metricUpdate2.setScalar(new BigDecimal(numRows));
      metricUpdate2.setUpdateTime("2017-10-23T19:10:48.566Z");

      String tableName = entry.getKey();

      MetricStructuredName structuredName2 = new MetricStructuredName();
      structuredName2.setOrigin("dataflow/v1b3");
      structuredName2.setName("ElementCount");
      structuredName2.setContext(
          ImmutableMap.of(
              "output_user_name",
              "Read_Data"
                  + Util.TRANSFORM_NODE_NAME_DELIMITER
                  + tableName
                  + "/Execute query-out0"));
      metricUpdate2.setName(structuredName2);
      metricUpdates.add(metricUpdate2);
    }

    JobMetrics jobMetrics = new JobMetrics();
    jobMetrics.setMetrics(ImmutableList.copyOf(metricUpdates));

    return jobMetrics;
  }
}
