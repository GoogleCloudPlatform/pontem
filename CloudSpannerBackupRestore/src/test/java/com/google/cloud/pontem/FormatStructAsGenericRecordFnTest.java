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

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.SerializationUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FormatStructAsGenericRecordFn}. */
@RunWith(JUnit4.class)
public final class FormatStructAsGenericRecordFnTest {

  @Test
  public void testFormatStructAsGenericRecordFn_basic() throws Exception {
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
    GenericRecord expectedGenericRecord =
        new GenericRecordBuilder(schema1).set("userName", "John Doe").set("userId", 10L).build();
    ImmutableMap<String, Type> tableMap1 =
        ImmutableMap.of("userName", Type.string(), "userId", Type.int64());

    Struct struct =
        Struct.newBuilder()
            .set("userName")
            .to((String) "John Doe")
            .set("userId")
            .to((long) 10L)
            .build();

    FormatStructAsGenericRecordFn fn = new FormatStructAsGenericRecordFn(schema1.toString());
    GenericRecord actualGenericRecord = fn.apply(struct);
    assertEquals(expectedGenericRecord, actualGenericRecord);
  }

  // NOTE: The {@type Struct} needs to be cloned here because the method under test
  // changes the serialized value (though not the underlying data) of the {@type Struct}
  // object. Consquently, if we do not clone the object, other tests that rely on
  // a particular serialized value which are run after this test will fail.

  @Test
  public void testFormatStructAsGenericRecordFn_basic1() throws Exception {
    FormatStructAsGenericRecordFn fn =
        new FormatStructAsGenericRecordFn(TestHelper.SCHEMA_1.toString());
    GenericRecord actualGenericRecord = fn.apply(SerializationUtils.clone(TestHelper.STRUCT_1));
    assertEquals(TestHelper.GENERIC_RECORD_1, actualGenericRecord);
  }

  @Test
  public void testFormatStructAsGenericRecordFn_basic2() throws Exception {
    FormatStructAsGenericRecordFn fn =
        new FormatStructAsGenericRecordFn(TestHelper.SCHEMA_2.toString());
    GenericRecord actualGenericRecord = fn.apply(SerializationUtils.clone(TestHelper.STRUCT_2));
    assertEquals(TestHelper.GENERIC_RECORD_2, actualGenericRecord);
  }

  @Test
  public void testFormatStructAsGenericRecordFn_basic3() throws Exception {
    FormatStructAsGenericRecordFn fn =
        new FormatStructAsGenericRecordFn(TestHelper.SCHEMA_3.toString());
    GenericRecord actualGenericRecord = fn.apply(SerializationUtils.clone(TestHelper.STRUCT_3));
    assertEquals(TestHelper.GENERIC_RECORD_3, actualGenericRecord);
  }

  @Test
  public void testFormatStructAsGenericRecordFn_basic4() throws Exception {
    FormatStructAsGenericRecordFn fn =
        new FormatStructAsGenericRecordFn(TestHelper.SCHEMA_4.toString());
    GenericRecord actualGenericRecord = fn.apply(SerializationUtils.clone(TestHelper.STRUCT_4));
    assertEquals(TestHelper.GENERIC_RECORD_4, actualGenericRecord);
  }

  @Test
  public void testFormatStructAsGenericRecordFn_basic5() throws Exception {
    FormatStructAsGenericRecordFn fn =
        new FormatStructAsGenericRecordFn(TestHelper.SCHEMA_5.toString());
    GenericRecord actualGenericRecord = fn.apply(SerializationUtils.clone(TestHelper.STRUCT_5));
    assertEquals(TestHelper.GENERIC_RECORD_5, actualGenericRecord);
  }

  @Test
  public void testFormatStructAsGenericRecordFn_advanced6() throws Exception {
    FormatStructAsGenericRecordFn fn =
        new FormatStructAsGenericRecordFn(TestHelper.SCHEMA_6.toString());
    GenericRecord actualGenericRecord = fn.apply(SerializationUtils.clone(TestHelper.STRUCT_6));
    assertEquals(TestHelper.GENERIC_RECORD_6, actualGenericRecord);
  }

  @Test
  public void testFormatStructAsGenericRecordFn_advanced7() throws Exception {
    FormatStructAsGenericRecordFn fn =
        new FormatStructAsGenericRecordFn(TestHelper.SCHEMA_7.toString());
    GenericRecord actualGenericRecord = fn.apply(SerializationUtils.clone(TestHelper.STRUCT_7));
    assertEquals(TestHelper.GENERIC_RECORD_7, actualGenericRecord);
  }

  @Test
  public void testFormatStructAsGenericRecordFn_advanced8() throws Exception {
    FormatStructAsGenericRecordFn fn =
        new FormatStructAsGenericRecordFn(TestHelper.SCHEMA_8.toString());
    GenericRecord actualGenericRecord = fn.apply(SerializationUtils.clone(TestHelper.STRUCT_8));
    assertEquals(TestHelper.GENERIC_RECORD_8, actualGenericRecord);
  }
}
