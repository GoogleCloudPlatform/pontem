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

/** Tests for {@link FormatTextAsGenericSpannerMutationFnTest}. */
@RunWith(JUnit4.class)
public final class FormatTextAsGenericSpannerMutationFnTest {
  @Test
  public void methodUnderTest_nullValue() throws Exception {
    FormatTextAsGenericSpannerMutationFn simpleFn =
        new FormatTextAsGenericSpannerMutationFn(TestHelper.TABLE_NAME);
    assertEquals(TestHelper.MUTATION_3, simpleFn.apply(TestHelper.STRUCT_3_BASE64_SERIALIZED));
  }

  @Test
  public void methodUnderTest_expectedResult() throws Exception {
    FormatTextAsGenericSpannerMutationFn simpleFn =
        new FormatTextAsGenericSpannerMutationFn(TestHelper.TABLE_NAME);
    assertEquals(TestHelper.MUTATION_1, simpleFn.apply(TestHelper.STRUCT_1_BASE64_SERIALIZED));
    assertEquals(TestHelper.MUTATION_2, simpleFn.apply(TestHelper.STRUCT_2_BASE64_SERIALIZED));
    assertEquals(TestHelper.MUTATION_3, simpleFn.apply(TestHelper.STRUCT_3_BASE64_SERIALIZED));
    assertEquals(TestHelper.MUTATION_4, simpleFn.apply(TestHelper.STRUCT_4_BASE64_SERIALIZED));
    assertEquals(TestHelper.MUTATION_5, simpleFn.apply(TestHelper.STRUCT_5_BASE64_SERIALIZED));
  }
}
