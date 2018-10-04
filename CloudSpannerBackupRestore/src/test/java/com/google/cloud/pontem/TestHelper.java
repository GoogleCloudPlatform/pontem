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
  // CHECKSTYLE.OFF: LineLength
  public static final String STRUCT_1_BASE64_SERIALIZED =
      "rO0ABXNyAC9jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuU3RydWN0JFZhbHVlTGlzdFN0cnVjdCJPEfWmuB9hAgACTAAEdHlwZXQAH0xjb20vZ29vZ2xlL2Nsb3VkL3NwYW5uZXIvVHlwZTtMAAZ2YWx1ZXN0ABBMamF2YS91dGlsL0xpc3Q7eHIAH2NvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5TdHJ1Y3TreRgevRZR1wIAAHhwc3IAHWNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5UeXBl1U9P8MIRFWoCAARMABBhcnJheUVsZW1lbnRUeXBlcQB+AAFMAARjb2RldAAkTGNvbS9nb29nbGUvY2xvdWQvc3Bhbm5lci9UeXBlJENvZGU7TAAMZmllbGRzQnlOYW1ldAAPTGphdmEvdXRpbC9NYXA7TAAMc3RydWN0RmllbGRzdAApTGNvbS9nb29nbGUvY29tbW9uL2NvbGxlY3QvSW1tdXRhYmxlTGlzdDt4cHB+cgAiY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkQ29kZQAAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAASAAB4cHQABlNUUlVDVHBzcgA2Y29tLmdvb2dsZS5jb21tb24uY29sbGVjdC5JbW11dGFibGVMaXN0JFNlcmlhbGl6ZWRGb3JtAAAAAAAAAAACAAFbAAhlbGVtZW50c3QAE1tMamF2YS9sYW5nL09iamVjdDt4cHVyABNbTGphdmEubGFuZy5PYmplY3Q7kM5YnxBzKWwCAAB4cAAAAANzcgApY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkU3RydWN0RmllbGR36UM9x2XCkgIAAkwABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZztMAAR0eXBlcQB+AAF4cHQACnByb3BlcnR5X2FzcQB+AAVwfnEAfgAKdAAGU1RSSU5HcHBzcQB+ABN0AApwcm9wZXJ0eV9icQB+ABdzcQB+ABN0AApwcm9wZXJ0eV9jc3EAfgAFcH5xAH4ACnQABUlOVDY0cHBzcQB+AA51cQB+ABEAAAADc3IAKWNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRTdHJpbmdJbXBsjNNcmhMipikCAAB4cgAyY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJEFic3RyYWN0T2JqZWN0VmFsdWX/laoSsDtinQIAAUwABXZhbHVldAASTGphdmEvbGFuZy9PYmplY3Q7eHIALGNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRBYnN0cmFjdFZhbHVlWGHIp61Am88CAAJaAAZpc051bGxMAAR0eXBlcQB+AAF4cgAeY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVltpagbFkjPDYCAAB4cABxAH4AF3QACnByb3BlcnR5QTFzcQB+ACMAcQB+ABd0AApwcm9wZXJ0eUIxc3IAKGNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRJbnQ2NEltcGwasOEnWHMLzgIAAUoABXZhbHVleHEAfgAmAHEAfgAeAAAAAAAAAG8=";
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
  // CHECKSTYLE.OFF: LineLength
  public static final String STRUCT_2_BASE64_SERIALIZED =
      "rO0ABXNyAC9jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuU3RydWN0JFZhbHVlTGlzdFN0cnVjdCJPEfWmuB9hAgACTAAEdHlwZXQAH0xjb20vZ29vZ2xlL2Nsb3VkL3NwYW5uZXIvVHlwZTtMAAZ2YWx1ZXN0ABBMamF2YS91dGlsL0xpc3Q7eHIAH2NvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5TdHJ1Y3TreRgevRZR1wIAAHhwc3IAHWNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5UeXBl1U9P8MIRFWoCAARMABBhcnJheUVsZW1lbnRUeXBlcQB+AAFMAARjb2RldAAkTGNvbS9nb29nbGUvY2xvdWQvc3Bhbm5lci9UeXBlJENvZGU7TAAMZmllbGRzQnlOYW1ldAAPTGphdmEvdXRpbC9NYXA7TAAMc3RydWN0RmllbGRzdAApTGNvbS9nb29nbGUvY29tbW9uL2NvbGxlY3QvSW1tdXRhYmxlTGlzdDt4cHB+cgAiY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkQ29kZQAAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAASAAB4cHQABlNUUlVDVHBzcgA2Y29tLmdvb2dsZS5jb21tb24uY29sbGVjdC5JbW11dGFibGVMaXN0JFNlcmlhbGl6ZWRGb3JtAAAAAAAAAAACAAFbAAhlbGVtZW50c3QAE1tMamF2YS9sYW5nL09iamVjdDt4cHVyABNbTGphdmEubGFuZy5PYmplY3Q7kM5YnxBzKWwCAAB4cAAAAANzcgApY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkU3RydWN0RmllbGR36UM9x2XCkgIAAkwABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZztMAAR0eXBlcQB+AAF4cHQACnByb3BlcnR5X2FzcQB+AAVwfnEAfgAKdAAGU1RSSU5HcHBzcQB+ABN0AApwcm9wZXJ0eV9icQB+ABdzcQB+ABN0AApwcm9wZXJ0eV9jc3EAfgAFcH5xAH4ACnQABUlOVDY0cHBzcQB+AA51cQB+ABEAAAADc3IAKWNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRTdHJpbmdJbXBsjNNcmhMipikCAAB4cgAyY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJEFic3RyYWN0T2JqZWN0VmFsdWX/laoSsDtinQIAAUwABXZhbHVldAASTGphdmEvbGFuZy9PYmplY3Q7eHIALGNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRBYnN0cmFjdFZhbHVlWGHIp61Am88CAAJaAAZpc051bGxMAAR0eXBlcQB+AAF4cgAeY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVltpagbFkjPDYCAAB4cABxAH4AF3QACnByb3BlcnR5QTJzcQB+ACMAcQB+ABd0AApwcm9wZXJ0eUIyc3IAKGNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRJbnQ2NEltcGwasOEnWHMLzgIAAUoABXZhbHVleHEAfgAmAHEAfgAeAAAAAAAAAN4=";
  public static final String TABLE_NAME_3 = "tableName";
  // CHECKSTYLE.ON: LineLength
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
  // CHECKSTYLE.OFF: LineLength
  public static final String STRUCT_3_BASE64_SERIALIZED =
      "rO0ABXNyAC9jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuU3RydWN0JFZhbHVlTGlzdFN0cnVjdCJPEfWmuB9hAgACTAAEdHlwZXQAH0xjb20vZ29vZ2xlL2Nsb3VkL3NwYW5uZXIvVHlwZTtMAAZ2YWx1ZXN0ABBMamF2YS91dGlsL0xpc3Q7eHIAH2NvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5TdHJ1Y3TreRgevRZR1wIAAHhwc3IAHWNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5UeXBl1U9P8MIRFWoCAARMABBhcnJheUVsZW1lbnRUeXBlcQB+AAFMAARjb2RldAAkTGNvbS9nb29nbGUvY2xvdWQvc3Bhbm5lci9UeXBlJENvZGU7TAAMZmllbGRzQnlOYW1ldAAPTGphdmEvdXRpbC9NYXA7TAAMc3RydWN0RmllbGRzdAApTGNvbS9nb29nbGUvY29tbW9uL2NvbGxlY3QvSW1tdXRhYmxlTGlzdDt4cHB+cgAiY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkQ29kZQAAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAASAAB4cHQABlNUUlVDVHBzcgA2Y29tLmdvb2dsZS5jb21tb24uY29sbGVjdC5JbW11dGFibGVMaXN0JFNlcmlhbGl6ZWRGb3JtAAAAAAAAAAACAAFbAAhlbGVtZW50c3QAE1tMamF2YS9sYW5nL09iamVjdDt4cHVyABNbTGphdmEubGFuZy5PYmplY3Q7kM5YnxBzKWwCAAB4cAAAAAJzcgApY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkU3RydWN0RmllbGR36UM9x2XCkgIAAkwABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZztMAAR0eXBlcQB+AAF4cHQACnByb3BlcnR5X2FzcQB+AAVwfnEAfgAKdAAFSU5UNjRwcHNxAH4AE3QACnByb3BlcnR5X2JzcQB+AAVwfnEAfgAKdAAGU1RSSU5HcHBzcQB+AA51cQB+ABEAAAACc3IAKGNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRJbnQ2NEltcGwasOEnWHMLzgIAAUoABXZhbHVleHIALGNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRBYnN0cmFjdFZhbHVlWGHIp61Am88CAAJaAAZpc051bGxMAAR0eXBlcQB+AAF4cgAeY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVltpagbFkjPDYCAAB4cABxAH4AFwAAAAAAAAAKc3IAKWNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRTdHJpbmdJbXBsjNNcmhMipikCAAB4cgAyY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJEFic3RyYWN0T2JqZWN0VmFsdWX/laoSsDtinQIAAUwABXZhbHVldAASTGphdmEvbGFuZy9PYmplY3Q7eHEAfgAiAXEAfgAccA==";
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
  // CHECKSTYLE.OFF: LineLength
  public static final String STRUCT_4_BASE64_SERIALIZED =
      "rO0ABXNyAC9jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuU3RydWN0JFZhbHVlTGlzdFN0cnVjdCJPEfWmuB9hAgACTAAEdHlwZXQAH0xjb20vZ29vZ2xlL2Nsb3VkL3NwYW5uZXIvVHlwZTtMAAZ2YWx1ZXN0ABBMamF2YS91dGlsL0xpc3Q7eHIAH2NvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5TdHJ1Y3TreRgevRZR1wIAAHhwc3IAHWNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5UeXBl1U9P8MIRFWoCAARMABBhcnJheUVsZW1lbnRUeXBlcQB+AAFMAARjb2RldAAkTGNvbS9nb29nbGUvY2xvdWQvc3Bhbm5lci9UeXBlJENvZGU7TAAMZmllbGRzQnlOYW1ldAAPTGphdmEvdXRpbC9NYXA7TAAMc3RydWN0RmllbGRzdAApTGNvbS9nb29nbGUvY29tbW9uL2NvbGxlY3QvSW1tdXRhYmxlTGlzdDt4cHB+cgAiY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkQ29kZQAAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAASAAB4cHQABlNUUlVDVHBzcgA2Y29tLmdvb2dsZS5jb21tb24uY29sbGVjdC5JbW11dGFibGVMaXN0JFNlcmlhbGl6ZWRGb3JtAAAAAAAAAAACAAFbAAhlbGVtZW50c3QAE1tMamF2YS9sYW5nL09iamVjdDt4cHVyABNbTGphdmEubGFuZy5PYmplY3Q7kM5YnxBzKWwCAAB4cAAAAAJzcgApY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkU3RydWN0RmllbGR36UM9x2XCkgIAAkwABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZztMAAR0eXBlcQB+AAF4cHQACnByb3BlcnR5X2FzcQB+AAVwfnEAfgAKdAAFSU5UNjRwcHNxAH4AE3QACnByb3BlcnR5X2JzcQB+AAVzcQB+AAVwfnEAfgAKdAAGU1RSSU5HcHB+cQB+AAp0AAVBUlJBWXBwc3EAfgAOdXEAfgARAAAAAnNyAChjb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWUkSW50NjRJbXBsGrDhJ1hzC84CAAFKAAV2YWx1ZXhyACxjb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWUkQWJzdHJhY3RWYWx1ZVhhyKetQJvPAgACWgAGaXNOdWxsTAAEdHlwZXEAfgABeHIAHmNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZbaWoGxZIzw2AgAAeHAAcQB+ABcAAAAAAAAD8XNyAC5jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWUkU3RyaW5nQXJyYXlJbXBsQ8wI6EOlg/gCAAB4cgAxY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJEFic3RyYWN0QXJyYXlWYWx1ZXMVg4XP7ZZbAgAAeHIAMmNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRBYnN0cmFjdE9iamVjdFZhbHVl/5WqErA7Yp0CAAFMAAV2YWx1ZXQAEkxqYXZhL2xhbmcvT2JqZWN0O3hxAH4AJQBxAH4AHHNyACZqYXZhLnV0aWwuQ29sbGVjdGlvbnMkVW5tb2RpZmlhYmxlTGlzdPwPJTG17I4QAgABTAAEbGlzdHEAfgACeHIALGphdmEudXRpbC5Db2xsZWN0aW9ucyRVbm1vZGlmaWFibGVDb2xsZWN0aW9uGUIAgMte9x4CAAFMAAFjdAAWTGphdmEvdXRpbC9Db2xsZWN0aW9uO3hwc3IAE2phdmEudXRpbC5BcnJheUxpc3R4gdIdmcdhnQMAAUkABHNpemV4cAAAAAN3BAAAAAN0AAVoZWxsb3QAAWR0AAUyMzJhIXhxAH4AMg==";
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
  // CHECKSTYLE.ON: LineLength
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
  // CHECKSTYLE.OFF: LineLength
  public static final String STRUCT_5_BASE64_SERIALIZED =
      "rO0ABXNyAC9jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuU3RydWN0JFZhbHVlTGlzdFN0cnVjdCJPEfWmuB9hAgACTAAEdHlwZXQAH0xjb20vZ29vZ2xlL2Nsb3VkL3NwYW5uZXIvVHlwZTtMAAZ2YWx1ZXN0ABBMamF2YS91dGlsL0xpc3Q7eHIAH2NvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5TdHJ1Y3TreRgevRZR1wIAAHhwc3IAHWNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5UeXBl1U9P8MIRFWoCAARMABBhcnJheUVsZW1lbnRUeXBlcQB+AAFMAARjb2RldAAkTGNvbS9nb29nbGUvY2xvdWQvc3Bhbm5lci9UeXBlJENvZGU7TAAMZmllbGRzQnlOYW1ldAAPTGphdmEvdXRpbC9NYXA7TAAMc3RydWN0RmllbGRzdAApTGNvbS9nb29nbGUvY29tbW9uL2NvbGxlY3QvSW1tdXRhYmxlTGlzdDt4cHB+cgAiY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkQ29kZQAAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAASAAB4cHQABlNUUlVDVHBzcgA2Y29tLmdvb2dsZS5jb21tb24uY29sbGVjdC5JbW11dGFibGVMaXN0JFNlcmlhbGl6ZWRGb3JtAAAAAAAAAAACAAFbAAhlbGVtZW50c3QAE1tMamF2YS9sYW5nL09iamVjdDt4cHVyABNbTGphdmEubGFuZy5PYmplY3Q7kM5YnxBzKWwCAAB4cAAAAAZzcgApY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkU3RydWN0RmllbGR36UM9x2XCkgIAAkwABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZztMAAR0eXBlcQB+AAF4cHQACnByb3BlcnR5X2FzcQB+AAVzcQB+AAVwfnEAfgAKdAAEQk9PTHBwfnEAfgAKdAAFQVJSQVlwcHNxAH4AE3QACnByb3BlcnR5X2JzcQB+AAVzcQB+AAVwfnEAfgAKdAAEREFURXBwcQB+ABtwcHNxAH4AE3QACnByb3BlcnR5X2NzcQB+AAVzcQB+AAVwfnEAfgAKdAAHRkxPQVQ2NHBwcQB+ABtwcHNxAH4AE3QACnByb3BlcnR5X2RzcQB+AAVzcQB+AAVwfnEAfgAKdAAFSU5UNjRwcHEAfgAbcHBzcQB+ABN0AApwcm9wZXJ0eV9lc3EAfgAFc3EAfgAFcH5xAH4ACnQABlNUUklOR3BwcQB+ABtwcHNxAH4AE3QACnByb3BlcnR5X2ZzcQB+AAVzcQB+AAVwfnEAfgAKdAAJVElNRVNUQU1QcHBxAH4AG3Bwc3EAfgAOdXEAfgARAAAABnNyACxjb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWUkQm9vbEFycmF5SW1wbHm3HmFquCx/AgABWwAGdmFsdWVzdAACW1p4cgAxY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJFByaW1pdGl2ZUFycmF5SW1wbAFDR4q2w6JNAgABTAAFbnVsbHN0ABJMamF2YS91dGlsL0JpdFNldDt4cgAsY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJEFic3RyYWN0VmFsdWVYYcinrUCbzwIAAloABmlzTnVsbEwABHR5cGVxAH4AAXhyAB5jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWW2lqBsWSM8NgIAAHhwAHEAfgAXcHVyAAJbWlePIDkUuF3iAgAAeHAAAAADAQABc3IALGNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSREYXRlQXJyYXlJbXBsLNTHG0WUK6MCAAB4cgAxY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJEFic3RyYWN0QXJyYXlWYWx1ZXMVg4XP7ZZbAgAAeHIAMmNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRBYnN0cmFjdE9iamVjdFZhbHVl/5WqErA7Yp0CAAFMAAV2YWx1ZXQAEkxqYXZhL2xhbmcvT2JqZWN0O3hxAH4AQQBxAH4AH3NyACZqYXZhLnV0aWwuQ29sbGVjdGlvbnMkVW5tb2RpZmlhYmxlTGlzdPwPJTG17I4QAgABTAAEbGlzdHEAfgACeHIALGphdmEudXRpbC5Db2xsZWN0aW9ucyRVbm1vZGlmaWFibGVDb2xsZWN0aW9uGUIAgMte9x4CAAFMAAFjdAAWTGphdmEvdXRpbC9Db2xsZWN0aW9uO3hwc3IAE2phdmEudXRpbC5BcnJheUxpc3R4gdIdmcdhnQMAAUkABHNpemV4cAAAAAJ3BAAAAAJzcgAVY29tLmdvb2dsZS5jbG91ZC5EYXRlb/QX6UAMjDkCAANJAApkYXlPZk1vbnRoSQAFbW9udGhJAAR5ZWFyeHAAAAABAAAAAgAAB+JzcQB+AFEAAAAIAAAABAAAB+F4cQB+AFBzcgAvY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJEZsb2F0NjRBcnJheUltcGyirBWaVt2I8AIAAVsABnZhbHVlc3QAAltEeHEAfgA/AHEAfgAlcHVyAAJbRD6mjBSrY1oeAgAAeHAAAAACQNdXgAAAAABAeIAAAAAAAHNyAC1jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWUkSW50NjRBcnJheUltcGx6vqNMGyz/xAIAAVsABnZhbHVlc3QAAltKeHEAfgA/AHEAfgArcHVyAAJbSnggBLUSsXWTAgAAeHAAAAACAAAAAAAAAUkAAAAAACOK1nNyAC5jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWUkU3RyaW5nQXJyYXlJbXBsQ8wI6EOlg/gCAAB4cQB+AEcAcQB+ADFzcQB+AEtzcQB+AE8AAAADdwQAAAADdAAFaGVsbG90AAFkdAAFMjMyYSF4cQB+AGFzcgAxY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJFRpbWVzdGFtcEFycmF5SW1wbL17ij0EzZBKAgAAeHEAfgBHAHEAfgA3c3EAfgBLc3EAfgBPAAAAAncEAAAAAnNyABpjb20uZ29vZ2xlLmNsb3VkLlRpbWVzdGFtcEeAF0633q+kAgACSQAFbmFub3NKAAdzZWNvbmRzeHA1OdbYAAAAAAAAABdzcQB+AGk3EXkgAAAAAAAAABd4cQB+AGg=";
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
  // CHECKSTYLE.OFF: LineLength
  public static final String STRUCT_6_BASE64_SERIALIZED =
      "rO0ABXNyAC9jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuU3RydWN0JFZhbHVlTGlzdFN0cnVjdCJPEfWmuB9hAgACTAAEdHlwZXQAH0xjb20vZ29vZ2xlL2Nsb3VkL3NwYW5uZXIvVHlwZTtMAAZ2YWx1ZXN0ABBMamF2YS91dGlsL0xpc3Q7eHIAH2NvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5TdHJ1Y3TreRgevRZR1wIAAHhwc3IAHWNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5UeXBl1U9P8MIRFWoCAARMABBhcnJheUVsZW1lbnRUeXBlcQB+AAFMAARjb2RldAAkTGNvbS9nb29nbGUvY2xvdWQvc3Bhbm5lci9UeXBlJENvZGU7TAAMZmllbGRzQnlOYW1ldAAPTGphdmEvdXRpbC9NYXA7TAAMc3RydWN0RmllbGRzdAApTGNvbS9nb29nbGUvY29tbW9uL2NvbGxlY3QvSW1tdXRhYmxlTGlzdDt4cHB+cgAiY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkQ29kZQAAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAASAAB4cHQABlNUUlVDVHBzcgA2Y29tLmdvb2dsZS5jb21tb24uY29sbGVjdC5JbW11dGFibGVMaXN0JFNlcmlhbGl6ZWRGb3JtAAAAAAAAAAACAAFbAAhlbGVtZW50c3QAE1tMamF2YS9sYW5nL09iamVjdDt4cHVyABNbTGphdmEubGFuZy5PYmplY3Q7kM5YnxBzKWwCAAB4cAAAAApzcgApY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkU3RydWN0RmllbGR36UM9x2XCkgIAAkwABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZztMAAR0eXBlcQB+AAF4cHQACnByb3BlcnR5X2FzcQB+AAVzcQB+AAVwfnEAfgAKdAAEQk9PTHBwfnEAfgAKdAAFQVJSQVlwcHNxAH4AE3QACnByb3BlcnR5X2JzcQB+AAVzcQB+AAVwfnEAfgAKdAAEREFURXBwcQB+ABtwcHNxAH4AE3QACnByb3BlcnR5X2NzcQB+AAVzcQB+AAVwfnEAfgAKdAAHRkxPQVQ2NHBwcQB+ABtwcHNxAH4AE3QACnByb3BlcnR5X2RzcQB+AAVzcQB+AAVwfnEAfgAKdAAFSU5UNjRwcHEAfgAbcHBzcQB+ABN0AApwcm9wZXJ0eV9lc3EAfgAFc3EAfgAFcH5xAH4ACnQABlNUUklOR3BwcQB+ABtwcHNxAH4AE3QACnByb3BlcnR5X2ZzcQB+AAVzcQB+AAVwfnEAfgAKdAAJVElNRVNUQU1QcHBxAH4AG3Bwc3EAfgATdAAKcHJvcGVydHlfZ3EAfgAYc3EAfgATdAAKcHJvcGVydHlfaHEAfgAgc3EAfgATdAAKcHJvcGVydHlfaXEAfgA4c3EAfgATdAAKcHJvcGVydHlfanEAfgAsc3EAfgAOdXEAfgARAAAACnNyACxjb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWUkQm9vbEFycmF5SW1wbHm3HmFquCx/AgABWwAGdmFsdWVzdAACW1p4cgAxY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJFByaW1pdGl2ZUFycmF5SW1wbAFDR4q2w6JNAgABTAAFbnVsbHN0ABJMamF2YS91dGlsL0JpdFNldDt4cgAsY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJEFic3RyYWN0VmFsdWVYYcinrUCbzwIAAloABmlzTnVsbEwABHR5cGVxAH4AAXhyAB5jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWW2lqBsWSM8NgIAAHhwAHEAfgAXcHVyAAJbWlePIDkUuF3iAgAAeHAAAAADAQABc3IALGNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSREYXRlQXJyYXlJbXBsLNTHG0WUK6MCAAB4cgAxY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJEFic3RyYWN0QXJyYXlWYWx1ZXMVg4XP7ZZbAgAAeHIAMmNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRBYnN0cmFjdE9iamVjdFZhbHVl/5WqErA7Yp0CAAFMAAV2YWx1ZXQAEkxqYXZhL2xhbmcvT2JqZWN0O3hxAH4ASQBxAH4AH3NyACZqYXZhLnV0aWwuQ29sbGVjdGlvbnMkVW5tb2RpZmlhYmxlTGlzdPwPJTG17I4QAgABTAAEbGlzdHEAfgACeHIALGphdmEudXRpbC5Db2xsZWN0aW9ucyRVbm1vZGlmaWFibGVDb2xsZWN0aW9uGUIAgMte9x4CAAFMAAFjdAAWTGphdmEvdXRpbC9Db2xsZWN0aW9uO3hwc3IAE2phdmEudXRpbC5BcnJheUxpc3R4gdIdmcdhnQMAAUkABHNpemV4cAAAAAJ3BAAAAAJzcgAVY29tLmdvb2dsZS5jbG91ZC5EYXRlb/QX6UAMjDkCAANJAApkYXlPZk1vbnRoSQAFbW9udGhJAAR5ZWFyeHAAAAABAAAAAgAAB+JzcQB+AFkAAAAIAAAABAAAB+F4cQB+AFhzcgAvY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJEZsb2F0NjRBcnJheUltcGyirBWaVt2I8AIAAVsABnZhbHVlc3QAAltEeHEAfgBHAHEAfgAlcHVyAAJbRD6mjBSrY1oeAgAAeHAAAAACQNdXgAAAAABAeIAAAAAAAHNyAC1jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWUkSW50NjRBcnJheUltcGx6vqNMGyz/xAIAAVsABnZhbHVlc3QAAltKeHEAfgBHAHEAfgArcHVyAAJbSnggBLUSsXWTAgAAeHAAAAACAAAAAAAAAUkAAAAAACOK1nNyAC5jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWUkU3RyaW5nQXJyYXlJbXBsQ8wI6EOlg/gCAAB4cQB+AE8AcQB+ADFzcQB+AFNzcQB+AFcAAAADdwQAAAADdAAFaGVsbG90AAFkdAAFMjMyYSF4cQB+AGlzcgAxY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJFRpbWVzdGFtcEFycmF5SW1wbL17ij0EzZBKAgAAeHEAfgBPAHEAfgA3c3EAfgBTc3EAfgBXAAAAAncEAAAAAnNyABpjb20uZ29vZ2xlLmNsb3VkLlRpbWVzdGFtcEeAF0633q+kAgACSQAFbmFub3NKAAdzZWNvbmRzeHA1OdbYAAAAAAAAABdzcQB+AHE3EXkgAAAAAAAAABd4cQB+AHBzcgAnY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJEJvb2xJbXBskj+6k2UFD5ECAAFaAAV2YWx1ZXhxAH4ASQBxAH4AGAFzcgAnY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJERhdGVJbXBseCET0hKhYS8CAAB4cQB+AFAAcQB+ACBzcQB+AFkAAAAIAAAABAAAB+FzcgAsY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJFRpbWVzdGFtcEltcGzCOi3CF9SrYwIAAHhxAH4AUABxAH4AOHNxAH4AcTcReSAAAAAAAAAAF3NyAChjb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWUkSW50NjRJbXBsGrDhJ1hzC84CAAFKAAV2YWx1ZXhxAH4ASQBxAH4ALAAAAAAAAAty";
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
  // CHECKSTYLE.ON: LineLength
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
  // CHECKSTYLE.OFF: LineLength
  public static final String STRUCT_8_BASE64_SERIALIZED =
      "rO0ABXNyAC9jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuU3RydWN0JFZhbHVlTGlzdFN0cnVjdCJPEfWmuB9hAgACTAAEdHlwZXQAH0xjb20vZ29vZ2xlL2Nsb3VkL3NwYW5uZXIvVHlwZTtMAAZ2YWx1ZXN0ABBMamF2YS91dGlsL0xpc3Q7eHIAH2NvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5TdHJ1Y3TreRgevRZR1wIAAHhwc3IAHWNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5UeXBl1U9P8MIRFWoCAARMABBhcnJheUVsZW1lbnRUeXBlcQB+AAFMAARjb2RldAAkTGNvbS9nb29nbGUvY2xvdWQvc3Bhbm5lci9UeXBlJENvZGU7TAAMZmllbGRzQnlOYW1ldAAPTGphdmEvdXRpbC9NYXA7TAAMc3RydWN0RmllbGRzdAApTGNvbS9nb29nbGUvY29tbW9uL2NvbGxlY3QvSW1tdXRhYmxlTGlzdDt4cHB+cgAiY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkQ29kZQAAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAASAAB4cHQABlNUUlVDVHBzcgA2Y29tLmdvb2dsZS5jb21tb24uY29sbGVjdC5JbW11dGFibGVMaXN0JFNlcmlhbGl6ZWRGb3JtAAAAAAAAAAACAAFbAAhlbGVtZW50c3QAE1tMamF2YS9sYW5nL09iamVjdDt4cHVyABNbTGphdmEubGFuZy5PYmplY3Q7kM5YnxBzKWwCAAB4cAAAAANzcgApY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkU3RydWN0RmllbGR36UM9x2XCkgIAAkwABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZztMAAR0eXBlcQB+AAF4cHQACnByb3BlcnR5X2FzcQB+AAVwfnEAfgAKdAAFQllURVNwcHNxAH4AE3QACnByb3BlcnR5X2JzcQB+AAVxAH4AF35xAH4ACnQABUFSUkFZcHBzcQB+ABN0AApwcm9wZXJ0eV9jc3EAfgAFcH5xAH4ACnQABlNUUklOR3Bwc3EAfgAOdXEAfgARAAAAA3NyAChjb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWUkQnl0ZXNJbXBsRs+l5NMK1AsCAAB4cgAyY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJEFic3RyYWN0T2JqZWN0VmFsdWX/laoSsDtinQIAAUwABXZhbHVldAASTGphdmEvbGFuZy9PYmplY3Q7eHIALGNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRBYnN0cmFjdFZhbHVlWGHIp61Am88CAAJaAAZpc051bGxMAAR0eXBlcQB+AAF4cgAeY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVltpagbFkjPDYCAAB4cABxAH4AF3NyABpjb20uZ29vZ2xlLmNsb3VkLkJ5dGVBcnJheeWCjDS+PXLIAgABTAAKYnl0ZVN0cmluZ3QAIExjb20vZ29vZ2xlL3Byb3RvYnVmL0J5dGVTdHJpbmc7eHBzcgAwY29tLmdvb2dsZS5wcm90b2J1Zi5CeXRlU3RyaW5nJExpdGVyYWxCeXRlU3RyaW5nAAAAAAAAAAECAAFbAAVieXRlc3QAAltCeHIALWNvbS5nb29nbGUucHJvdG9idWYuQnl0ZVN0cmluZyRMZWFmQnl0ZVN0cmluZ7TgNIKoj3N4AgAAeHIAHmNvbS5nb29nbGUucHJvdG9idWYuQnl0ZVN0cmluZ/0UNwb1jrwNAgABSQAEaGFzaHhwAAAAAHVyAAJbQqzzF/gGCFTgAgAAeHAAAAAHZHNqa3NqZHNyAC1jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWUkQnl0ZXNBcnJheUltcGyw5dkZnnk28wIAAHhyADFjb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWUkQWJzdHJhY3RBcnJheVZhbHVlcxWDhc/tllsCAAB4cQB+ACcAcQB+ABxzcgAmamF2YS51dGlsLkNvbGxlY3Rpb25zJFVubW9kaWZpYWJsZUxpc3T8DyUxteyOEAIAAUwABGxpc3RxAH4AAnhyACxqYXZhLnV0aWwuQ29sbGVjdGlvbnMkVW5tb2RpZmlhYmxlQ29sbGVjdGlvbhlCAIDLXvceAgABTAABY3QAFkxqYXZhL3V0aWwvQ29sbGVjdGlvbjt4cHNyABNqYXZhLnV0aWwuQXJyYXlMaXN0eIHSHZnHYZ0DAAFJAARzaXpleHAAAAACdwQAAAACc3EAfgAsc3EAfgAvAAAAAHVxAH4ANAAAAANkc3NzcQB+ACxzcQB+AC8AAAAAdXEAfgA0AAAAC2FzJSRAKCUqKEpzeHEAfgA+c3IAKWNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRTdHJpbmdJbXBsjNNcmhMipikCAAB4cQB+ACcAcQB+ACF0AANmb28=";
  private static final String PROPERTY_A1 = "propertyA1";
  private static final String PROPERTY_B1 = "propertyB1";
  private static final long PROPERTY_C1 = 111L;
  // CHECKSTYLE.ON: LineLength
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
  // CHECKSTYLE.ON: LineLength
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
  // CHECKSTYLE.ON: LineLength
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
  // CHECKSTYLE.ON: LineLength

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
