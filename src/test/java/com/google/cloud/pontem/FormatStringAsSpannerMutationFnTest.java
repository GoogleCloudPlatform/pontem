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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FormatStringAsSpannerMutationFnTest}. */
@RunWith(JUnit4.class)
public final class FormatStringAsSpannerMutationFnTest {
  @Test
  public void methodUnderTest_nullValue() throws Exception {
    FormatStringAsSpannerMutationFn simpleFn =
        new FormatStringAsSpannerMutationFn(TestHelper.TABLE_NAME_3);
    assertEquals(TestHelper.MUTATION_3, simpleFn.apply(TestHelper.STRUCT_3_BASE64_SERIALIZED));
  }

  @Test
  public void methodUnderTest_expectedResult() throws Exception {
    FormatStringAsSpannerMutationFn simpleFn =
        new FormatStringAsSpannerMutationFn(TestHelper.TABLE_NAME_1);
    assertEquals(TestHelper.MUTATION_1, simpleFn.apply(TestHelper.STRUCT_1_BASE64_SERIALIZED));
  }

  @Test
  public void methodUnderTest_expectedResult2() throws Exception {
    FormatStringAsSpannerMutationFn simpleFn =
        new FormatStringAsSpannerMutationFn(TestHelper.TABLE_NAME_2);
    assertEquals(TestHelper.MUTATION_2, simpleFn.apply(TestHelper.STRUCT_2_BASE64_SERIALIZED));
  }

  @Test
  public void methodUnderTest_expectedResult3() throws Exception {
    FormatStringAsSpannerMutationFn simpleFn =
        new FormatStringAsSpannerMutationFn(TestHelper.TABLE_NAME_3);
    assertEquals(TestHelper.MUTATION_3, simpleFn.apply(TestHelper.STRUCT_3_BASE64_SERIALIZED));
  }

  @Test
  public void methodUnderTest_expectedResult4() throws Exception {
    FormatStringAsSpannerMutationFn simpleFn =
        new FormatStringAsSpannerMutationFn(TestHelper.TABLE_NAME_4);
    assertEquals(TestHelper.MUTATION_4, simpleFn.apply(TestHelper.STRUCT_4_BASE64_SERIALIZED));
  }

  @Test
  public void methodUnderTest_expectedResult5() throws Exception {
    FormatStringAsSpannerMutationFn simpleFn =
        new FormatStringAsSpannerMutationFn(TestHelper.TABLE_NAME_5);
    assertEquals(TestHelper.MUTATION_5, simpleFn.apply(TestHelper.STRUCT_5_BASE64_SERIALIZED));
  }

  @Test
  public void methodUnderTest_expectedResult6() throws Exception {
    FormatStringAsSpannerMutationFn simpleFn =
        new FormatStringAsSpannerMutationFn(TestHelper.TABLE_NAME_6);
    assertEquals(TestHelper.MUTATION_6, simpleFn.apply(TestHelper.STRUCT_6_BASE64_SERIALIZED));
  }

  @Test(expected = Exception.class)
  public void methodUnderTest_emptyAndException() throws Exception {
    FormatStringAsSpannerMutationFn simpleFn =
        new FormatStringAsSpannerMutationFn(TestHelper.TABLE_NAME_1);
    assertEquals(TestHelper.MUTATION_1, simpleFn.apply("sfkfjsk"));
  }
}
