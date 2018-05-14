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
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/** Class to provide functionality to help with tests. */
public class TestHelper {

  public static final String TABLE_NAME = "tableName";
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

  // CHECKSTYLE.OFF: LineLength
  public static final String STRUCT_1_BASE64_SERIALIZED =
      "rO0ABXNyAC9jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuU3RydWN0JFZhbHVlTGlzdFN0cnVjdCJPEfWmuB9hAgACTAAEdHlwZXQAH0xjb20vZ29vZ2xlL2Nsb3VkL3NwYW5uZXIvVHlwZTtMAAZ2YWx1ZXN0ABBMamF2YS91dGlsL0xpc3Q7eHIAH2NvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5TdHJ1Y3TreRgevRZR1wIAAHhwc3IAHWNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5UeXBl1U9P8MIRFWoCAARMABBhcnJheUVsZW1lbnRUeXBlcQB+AAFMAARjb2RldAAkTGNvbS9nb29nbGUvY2xvdWQvc3Bhbm5lci9UeXBlJENvZGU7TAAMZmllbGRzQnlOYW1ldAAPTGphdmEvdXRpbC9NYXA7TAAMc3RydWN0RmllbGRzdAApTGNvbS9nb29nbGUvY29tbW9uL2NvbGxlY3QvSW1tdXRhYmxlTGlzdDt4cHB+cgAiY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkQ29kZQAAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAASAAB4cHQABlNUUlVDVHBzcgA2Y29tLmdvb2dsZS5jb21tb24uY29sbGVjdC5JbW11dGFibGVMaXN0JFNlcmlhbGl6ZWRGb3JtAAAAAAAAAAACAAFbAAhlbGVtZW50c3QAE1tMamF2YS9sYW5nL09iamVjdDt4cHVyABNbTGphdmEubGFuZy5PYmplY3Q7kM5YnxBzKWwCAAB4cAAAAANzcgApY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkU3RydWN0RmllbGR36UM9x2XCkgIAAkwABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZztMAAR0eXBlcQB+AAF4cHQACnByb3BlcnR5X2FzcQB+AAVwfnEAfgAKdAAGU1RSSU5HcHBzcQB+ABN0AApwcm9wZXJ0eV9icQB+ABdzcQB+ABN0AApwcm9wZXJ0eV9jc3EAfgAFcH5xAH4ACnQABUlOVDY0cHBzcQB+AA51cQB+ABEAAAADc3IAKWNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRTdHJpbmdJbXBsjNNcmhMipikCAAB4cgAyY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJEFic3RyYWN0T2JqZWN0VmFsdWX/laoSsDtinQIAAUwABXZhbHVldAASTGphdmEvbGFuZy9PYmplY3Q7eHIALGNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRBYnN0cmFjdFZhbHVlWGHIp61Am88CAAJaAAZpc051bGxMAAR0eXBlcQB+AAF4cgAeY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVltpagbFkjPDYCAAB4cABxAH4AF3QACnByb3BlcnR5QTFzcQB+ACMAcQB+ABd0AApwcm9wZXJ0eUIxc3IAKGNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRJbnQ2NEltcGwasOEnWHMLzgIAAUoABXZhbHVleHEAfgAmAHEAfgAeAAAAAAAAAG8=";
  // CHECKSTYLE.ON: LineLength

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

  // CHECKSTYLE.OFF: LineLength
  public static final String STRUCT_2_BASE64_SERIALIZED =
      "rO0ABXNyAC9jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuU3RydWN0JFZhbHVlTGlzdFN0cnVjdCJPEfWmuB9hAgACTAAEdHlwZXQAH0xjb20vZ29vZ2xlL2Nsb3VkL3NwYW5uZXIvVHlwZTtMAAZ2YWx1ZXN0ABBMamF2YS91dGlsL0xpc3Q7eHIAH2NvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5TdHJ1Y3TreRgevRZR1wIAAHhwc3IAHWNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5UeXBl1U9P8MIRFWoCAARMABBhcnJheUVsZW1lbnRUeXBlcQB+AAFMAARjb2RldAAkTGNvbS9nb29nbGUvY2xvdWQvc3Bhbm5lci9UeXBlJENvZGU7TAAMZmllbGRzQnlOYW1ldAAPTGphdmEvdXRpbC9NYXA7TAAMc3RydWN0RmllbGRzdAApTGNvbS9nb29nbGUvY29tbW9uL2NvbGxlY3QvSW1tdXRhYmxlTGlzdDt4cHB+cgAiY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkQ29kZQAAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAASAAB4cHQABlNUUlVDVHBzcgA2Y29tLmdvb2dsZS5jb21tb24uY29sbGVjdC5JbW11dGFibGVMaXN0JFNlcmlhbGl6ZWRGb3JtAAAAAAAAAAACAAFbAAhlbGVtZW50c3QAE1tMamF2YS9sYW5nL09iamVjdDt4cHVyABNbTGphdmEubGFuZy5PYmplY3Q7kM5YnxBzKWwCAAB4cAAAAANzcgApY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkU3RydWN0RmllbGR36UM9x2XCkgIAAkwABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZztMAAR0eXBlcQB+AAF4cHQACnByb3BlcnR5X2FzcQB+AAVwfnEAfgAKdAAGU1RSSU5HcHBzcQB+ABN0AApwcm9wZXJ0eV9icQB+ABdzcQB+ABN0AApwcm9wZXJ0eV9jc3EAfgAFcH5xAH4ACnQABUlOVDY0cHBzcQB+AA51cQB+ABEAAAADc3IAKWNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRTdHJpbmdJbXBsjNNcmhMipikCAAB4cgAyY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJEFic3RyYWN0T2JqZWN0VmFsdWX/laoSsDtinQIAAUwABXZhbHVldAASTGphdmEvbGFuZy9PYmplY3Q7eHIALGNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRBYnN0cmFjdFZhbHVlWGHIp61Am88CAAJaAAZpc051bGxMAAR0eXBlcQB+AAF4cgAeY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVltpagbFkjPDYCAAB4cABxAH4AF3QACnByb3BlcnR5QTJzcQB+ACMAcQB+ABd0AApwcm9wZXJ0eUIyc3IAKGNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRJbnQ2NEltcGwasOEnWHMLzgIAAUoABXZhbHVleHEAfgAmAHEAfgAeAAAAAAAAAN4=";
  // CHECKSTYLE.ON: LineLength

  private static final int PROPERTY_A3 = 10;
  private static final String PROPERTY_B3 = null;
  public static final Struct STRUCT_3 =
      Struct.newBuilder()
          .set("property_a")
          .to(PROPERTY_A3)
          .set("property_b")
          .to(PROPERTY_B3)
          .build();

  // CHECKSTYLE.OFF: LineLength
  public static final String STRUCT_3_BASE64_SERIALIZED =
      "rO0ABXNyAC9jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuU3RydWN0JFZhbHVlTGlzdFN0cnVjdCJPEfWmuB9hAgACTAAEdHlwZXQAH0xjb20vZ29vZ2xlL2Nsb3VkL3NwYW5uZXIvVHlwZTtMAAZ2YWx1ZXN0ABBMamF2YS91dGlsL0xpc3Q7eHIAH2NvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5TdHJ1Y3TreRgevRZR1wIAAHhwc3IAHWNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5UeXBl1U9P8MIRFWoCAARMABBhcnJheUVsZW1lbnRUeXBlcQB+AAFMAARjb2RldAAkTGNvbS9nb29nbGUvY2xvdWQvc3Bhbm5lci9UeXBlJENvZGU7TAAMZmllbGRzQnlOYW1ldAAPTGphdmEvdXRpbC9NYXA7TAAMc3RydWN0RmllbGRzdAApTGNvbS9nb29nbGUvY29tbW9uL2NvbGxlY3QvSW1tdXRhYmxlTGlzdDt4cHB+cgAiY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkQ29kZQAAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAASAAB4cHQABlNUUlVDVHBzcgA2Y29tLmdvb2dsZS5jb21tb24uY29sbGVjdC5JbW11dGFibGVMaXN0JFNlcmlhbGl6ZWRGb3JtAAAAAAAAAAACAAFbAAhlbGVtZW50c3QAE1tMamF2YS9sYW5nL09iamVjdDt4cHVyABNbTGphdmEubGFuZy5PYmplY3Q7kM5YnxBzKWwCAAB4cAAAAAJzcgApY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkU3RydWN0RmllbGR36UM9x2XCkgIAAkwABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZztMAAR0eXBlcQB+AAF4cHQACnByb3BlcnR5X2FzcQB+AAVwfnEAfgAKdAAFSU5UNjRwcHNxAH4AE3QACnByb3BlcnR5X2JzcQB+AAVwfnEAfgAKdAAGU1RSSU5HcHBzcQB+AA51cQB+ABEAAAACc3IAKGNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRJbnQ2NEltcGwasOEnWHMLzgIAAUoABXZhbHVleHIALGNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRBYnN0cmFjdFZhbHVlWGHIp61Am88CAAJaAAZpc051bGxMAAR0eXBlcQB+AAF4cgAeY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVltpagbFkjPDYCAAB4cABxAH4AFwAAAAAAAAAKc3IAKWNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRTdHJpbmdJbXBsjNNcmhMipikCAAB4cgAyY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJEFic3RyYWN0T2JqZWN0VmFsdWX/laoSsDtinQIAAUwABXZhbHVldAASTGphdmEvbGFuZy9PYmplY3Q7eHEAfgAiAXEAfgAccA==";
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

  // CHECKSTYLE.OFF: LineLength
  public static final String STRUCT_4_BASE64_SERIALIZED =
      "rO0ABXNyAC9jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuU3RydWN0JFZhbHVlTGlzdFN0cnVjdCJPEfWmuB9hAgACTAAEdHlwZXQAH0xjb20vZ29vZ2xlL2Nsb3VkL3NwYW5uZXIvVHlwZTtMAAZ2YWx1ZXN0ABBMamF2YS91dGlsL0xpc3Q7eHIAH2NvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5TdHJ1Y3TreRgevRZR1wIAAHhwc3IAHWNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5UeXBl1U9P8MIRFWoCAARMABBhcnJheUVsZW1lbnRUeXBlcQB+AAFMAARjb2RldAAkTGNvbS9nb29nbGUvY2xvdWQvc3Bhbm5lci9UeXBlJENvZGU7TAAMZmllbGRzQnlOYW1ldAAPTGphdmEvdXRpbC9NYXA7TAAMc3RydWN0RmllbGRzdAApTGNvbS9nb29nbGUvY29tbW9uL2NvbGxlY3QvSW1tdXRhYmxlTGlzdDt4cHB+cgAiY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkQ29kZQAAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAASAAB4cHQABlNUUlVDVHBzcgA2Y29tLmdvb2dsZS5jb21tb24uY29sbGVjdC5JbW11dGFibGVMaXN0JFNlcmlhbGl6ZWRGb3JtAAAAAAAAAAACAAFbAAhlbGVtZW50c3QAE1tMamF2YS9sYW5nL09iamVjdDt4cHVyABNbTGphdmEubGFuZy5PYmplY3Q7kM5YnxBzKWwCAAB4cAAAAAJzcgApY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkU3RydWN0RmllbGR36UM9x2XCkgIAAkwABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZztMAAR0eXBlcQB+AAF4cHQACnByb3BlcnR5X2FzcQB+AAVwfnEAfgAKdAAFSU5UNjRwcHNxAH4AE3QACnByb3BlcnR5X2JzcQB+AAVzcQB+AAVwfnEAfgAKdAAGU1RSSU5HcHB+cQB+AAp0AAVBUlJBWXBwc3EAfgAOdXEAfgARAAAAAnNyAChjb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWUkSW50NjRJbXBsGrDhJ1hzC84CAAFKAAV2YWx1ZXhyACxjb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWUkQWJzdHJhY3RWYWx1ZVhhyKetQJvPAgACWgAGaXNOdWxsTAAEdHlwZXEAfgABeHIAHmNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZbaWoGxZIzw2AgAAeHAAcQB+ABcAAAAAAAAD8XNyAC5jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWUkU3RyaW5nQXJyYXlJbXBsQ8wI6EOlg/gCAAB4cgAxY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJEFic3RyYWN0QXJyYXlWYWx1ZXMVg4XP7ZZbAgAAeHIAMmNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRBYnN0cmFjdE9iamVjdFZhbHVl/5WqErA7Yp0CAAFMAAV2YWx1ZXQAEkxqYXZhL2xhbmcvT2JqZWN0O3hxAH4AJQBxAH4AHHNyACZqYXZhLnV0aWwuQ29sbGVjdGlvbnMkVW5tb2RpZmlhYmxlTGlzdPwPJTG17I4QAgABTAAEbGlzdHEAfgACeHIALGphdmEudXRpbC5Db2xsZWN0aW9ucyRVbm1vZGlmaWFibGVDb2xsZWN0aW9uGUIAgMte9x4CAAFMAAFjdAAWTGphdmEvdXRpbC9Db2xsZWN0aW9uO3hwc3IAE2phdmEudXRpbC5BcnJheUxpc3R4gdIdmcdhnQMAAUkABHNpemV4cAAAAAN3BAAAAAN0AAVoZWxsb3QAAWR0AAUyMzJhIXhxAH4AMg==";
  // CHECKSTYLE.ON: LineLength

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

  // CHECKSTYLE.OFF: LineLength
  public static final String STRUCT_5_BASE64_SERIALIZED = "rO0ABXNyAC9jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuU3RydWN0JFZhbHVlTGlzdFN0cnVjdCJPEfWmuB9hAgACTAAEdHlwZXQAH0xjb20vZ29vZ2xlL2Nsb3VkL3NwYW5uZXIvVHlwZTtMAAZ2YWx1ZXN0ABBMamF2YS91dGlsL0xpc3Q7eHIAH2NvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5TdHJ1Y3TreRgevRZR1wIAAHhwc3IAHWNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5UeXBl1U9P8MIRFWoCAARMABBhcnJheUVsZW1lbnRUeXBlcQB+AAFMAARjb2RldAAkTGNvbS9nb29nbGUvY2xvdWQvc3Bhbm5lci9UeXBlJENvZGU7TAAMZmllbGRzQnlOYW1ldAAPTGphdmEvdXRpbC9NYXA7TAAMc3RydWN0RmllbGRzdAApTGNvbS9nb29nbGUvY29tbW9uL2NvbGxlY3QvSW1tdXRhYmxlTGlzdDt4cHB+cgAiY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkQ29kZQAAAAAAAAAAEgAAeHIADmphdmEubGFuZy5FbnVtAAAAAAAAAAASAAB4cHQABlNUUlVDVHBzcgA2Y29tLmdvb2dsZS5jb21tb24uY29sbGVjdC5JbW11dGFibGVMaXN0JFNlcmlhbGl6ZWRGb3JtAAAAAAAAAAACAAFbAAhlbGVtZW50c3QAE1tMamF2YS9sYW5nL09iamVjdDt4cHVyABNbTGphdmEubGFuZy5PYmplY3Q7kM5YnxBzKWwCAAB4cAAAAAZzcgApY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlR5cGUkU3RydWN0RmllbGR36UM9x2XCkgIAAkwABG5hbWV0ABJMamF2YS9sYW5nL1N0cmluZztMAAR0eXBlcQB+AAF4cHQACnByb3BlcnR5X2FzcQB+AAVzcQB+AAVwfnEAfgAKdAAEQk9PTHBwfnEAfgAKdAAFQVJSQVlwcHNxAH4AE3QACnByb3BlcnR5X2JzcQB+AAVzcQB+AAVwfnEAfgAKdAAEREFURXBwcQB+ABtwcHNxAH4AE3QACnByb3BlcnR5X2NzcQB+AAVzcQB+AAVwfnEAfgAKdAAHRkxPQVQ2NHBwcQB+ABtwcHNxAH4AE3QACnByb3BlcnR5X2RzcQB+AAVzcQB+AAVwfnEAfgAKdAAFSU5UNjRwcHEAfgAbcHBzcQB+ABN0AApwcm9wZXJ0eV9lc3EAfgAFc3EAfgAFcH5xAH4ACnQABlNUUklOR3BwcQB+ABtwcHNxAH4AE3QACnByb3BlcnR5X2ZzcQB+AAVzcQB+AAVwfnEAfgAKdAAJVElNRVNUQU1QcHBxAH4AG3Bwc3EAfgAOdXEAfgARAAAABnNyACxjb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWUkQm9vbEFycmF5SW1wbHm3HmFquCx/AgABWwAGdmFsdWVzdAACW1p4cgAxY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJFByaW1pdGl2ZUFycmF5SW1wbAFDR4q2w6JNAgABTAAFbnVsbHN0ABJMamF2YS91dGlsL0JpdFNldDt4cgAsY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJEFic3RyYWN0VmFsdWVYYcinrUCbzwIAAloABmlzTnVsbEwABHR5cGVxAH4AAXhyAB5jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWW2lqBsWSM8NgIAAHhwAHEAfgAXcHVyAAJbWlePIDkUuF3iAgAAeHAAAAADAQABc3IALGNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSREYXRlQXJyYXlJbXBsLNTHG0WUK6MCAAB4cgAxY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJEFic3RyYWN0QXJyYXlWYWx1ZXMVg4XP7ZZbAgAAeHIAMmNvbS5nb29nbGUuY2xvdWQuc3Bhbm5lci5WYWx1ZSRBYnN0cmFjdE9iamVjdFZhbHVl/5WqErA7Yp0CAAFMAAV2YWx1ZXQAEkxqYXZhL2xhbmcvT2JqZWN0O3hxAH4AQQBxAH4AH3NyACZqYXZhLnV0aWwuQ29sbGVjdGlvbnMkVW5tb2RpZmlhYmxlTGlzdPwPJTG17I4QAgABTAAEbGlzdHEAfgACeHIALGphdmEudXRpbC5Db2xsZWN0aW9ucyRVbm1vZGlmaWFibGVDb2xsZWN0aW9uGUIAgMte9x4CAAFMAAFjdAAWTGphdmEvdXRpbC9Db2xsZWN0aW9uO3hwc3IAE2phdmEudXRpbC5BcnJheUxpc3R4gdIdmcdhnQMAAUkABHNpemV4cAAAAAJ3BAAAAAJzcgAVY29tLmdvb2dsZS5jbG91ZC5EYXRlb/QX6UAMjDkCAANJAApkYXlPZk1vbnRoSQAFbW9udGhJAAR5ZWFyeHAAAAABAAAAAgAAB+JzcQB+AFEAAAAIAAAABAAAB+F4cQB+AFBzcgAvY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJEZsb2F0NjRBcnJheUltcGyirBWaVt2I8AIAAVsABnZhbHVlc3QAAltEeHEAfgA/AHEAfgAlcHVyAAJbRD6mjBSrY1oeAgAAeHAAAAACQNdXgAAAAABAeIAAAAAAAHNyAC1jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWUkSW50NjRBcnJheUltcGx6vqNMGyz/xAIAAVsABnZhbHVlc3QAAltKeHEAfgA/AHEAfgArcHVyAAJbSnggBLUSsXWTAgAAeHAAAAACAAAAAAAAAUkAAAAAACOK1nNyAC5jb20uZ29vZ2xlLmNsb3VkLnNwYW5uZXIuVmFsdWUkU3RyaW5nQXJyYXlJbXBsQ8wI6EOlg/gCAAB4cQB+AEcAcQB+ADFzcQB+AEtzcQB+AE8AAAADdwQAAAADdAAFaGVsbG90AAFkdAAFMjMyYSF4cQB+AGFzcgAxY29tLmdvb2dsZS5jbG91ZC5zcGFubmVyLlZhbHVlJFRpbWVzdGFtcEFycmF5SW1wbL17ij0EzZBKAgAAeHEAfgBHAHEAfgA3c3EAfgBLc3EAfgBPAAAAAncEAAAAAnNyABpjb20uZ29vZ2xlLmNsb3VkLlRpbWVzdGFtcEeAF0633q+kAgACSQAFbmFub3NKAAdzZWNvbmRzeHA1OdbYAAAAAAAAABdzcQB+AGk3EXkgAAAAAAAAABd4cQB+AGg=";
  // CHECKSTYLE.ON: LineLength

  public static final Mutation MUTATION_1 =
      Mutation.newInsertOrUpdateBuilder(TestHelper.TABLE_NAME)
          .set("property_a")
          .to(PROPERTY_A1)
          .set("property_b")
          .to(PROPERTY_B1)
          .set("property_c")
          .to(PROPERTY_C1)
          .build();

  public static final Mutation MUTATION_2 =
      Mutation.newInsertOrUpdateBuilder(TestHelper.TABLE_NAME)
          .set("property_a")
          .to(PROPERTY_A2)
          .set("property_b")
          .to(PROPERTY_B2)
          .set("property_c")
          .to(PROPERTY_C2)
          .build();

  public static final Mutation MUTATION_3 =
      Mutation.newInsertOrUpdateBuilder(TestHelper.TABLE_NAME)
          .set("property_a")
          .to(PROPERTY_A3)
          .build();

  public static final Mutation MUTATION_4 =
      Mutation.newInsertOrUpdateBuilder(TestHelper.TABLE_NAME)
          .set("property_a")
          .to(PROPERTY_A4)
          .set("property_b")
          .toStringArray(PROPERTY_B4)
          .build();

  public static final Mutation MUTATION_5 =
      Mutation.newInsertOrUpdateBuilder(TestHelper.TABLE_NAME)
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

  public static JobMetrics getJobMetrics(Map<String, Long> tableRowCounts) {
    JobMetrics jobMetrics = new JobMetrics();
    List<MetricUpdate> metricUpdates = Lists.newArrayList();

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
    metricUpdates.add(metricUpdate1);

    for (Map.Entry<String, Long> entry : tableRowCounts.entrySet()) {
      String tableName = entry.getKey();
      Long numRows = entry.getValue();

      MetricUpdate metricUpdate2 = new MetricUpdate();
      metricUpdate2.setScalar(new BigDecimal(numRows));
      metricUpdate2.setUpdateTime("2017-10-23T19:10:48.566Z");
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

    jobMetrics.setMetrics(ImmutableList.copyOf(metricUpdates));
    return jobMetrics;
  }
}
