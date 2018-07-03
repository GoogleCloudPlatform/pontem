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

import com.google.cloud.spanner.Mutation;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SerializedCloudSpannerDatabaseRestore}. */
@RunWith(JUnit4.class)
public class SerializedCloudSpannerDatabaseRestoreTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testPipelineCanRunSuccessfully() throws Exception {
    PCollection<String> rows =
        pipeline.apply(
            Create.of(
                TestHelper.STRUCT_1_BASE64_SERIALIZED, TestHelper.STRUCT_2_BASE64_SERIALIZED));

    PCollection<Mutation> structDataAsMutation =
        rows.apply(MapElements.via(new FormatStringAsSpannerMutationFn(TestHelper.TABLE_NAME_1)));

    PAssert.that(structDataAsMutation)
        .containsInAnyOrder(TestHelper.MUTATION_1, TestHelper.MUTATION_2);

    pipeline.run().waitUntilFinish();
  }
}
