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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CloudSpannerDatabaseBackup}. */
@RunWith(JUnit4.class)
public class CloudSpannerDatabaseBackupTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testGetListOfTablesToBackup() throws Exception {
    String[] emptyTableNamesToIncludeInBackup = new String[0];
    String[] emptyTableNamesToExcludeFromBackup = new String[0];

    ImmutableList<String> tablesToBackup =
        CloudSpannerDatabaseBackup.getListOfTablesToBackup(
            ImmutableSet.of("Table0", "Table1"),
            emptyTableNamesToIncludeInBackup,
            emptyTableNamesToExcludeFromBackup);
    assertEquals("number of tables to back is wrong", 2, tablesToBackup.size());
    assertEquals("Table0", tablesToBackup.get(0));
    assertEquals("Table1", tablesToBackup.get(1));
  }

  @Test(expected = Exception.class)
  public void testGetListOfTablesToBackup_exclusionAndInclusionSet() throws Exception {
    String[] emptyTableNamesToIncludeInBackup = {"Table0"};
    String[] emptyTableNamesToExcludeFromBackup = {"Table0"};

    ImmutableList<String> tablesToBackup =
        CloudSpannerDatabaseBackup.getListOfTablesToBackup(
            ImmutableSet.of("Table0", "Table1"),
            emptyTableNamesToIncludeInBackup,
            emptyTableNamesToExcludeFromBackup);
  }

  @Test
  public void testGetListOfTablesToBackup_inclusionListSet() throws Exception {
    String[] emptyTableNamesToIncludeInBackup = {"Table1"};
    String[] emptyTableNamesToExcludeFromBackup = new String[0];

    ImmutableList<String> tablesToBackup =
        CloudSpannerDatabaseBackup.getListOfTablesToBackup(
            ImmutableSet.of("Table0", "Table1", "Table2"),
            emptyTableNamesToIncludeInBackup,
            emptyTableNamesToExcludeFromBackup);
    assertEquals("number of tables to back is wrong", 1, tablesToBackup.size());
    assertEquals("Table1", tablesToBackup.get(0));
  }

  @Test
  public void testPipelineCanRunSuccessfully() throws Exception {
    // Create an input PCollection.
    PCollection<Struct> input = pipeline.apply(Create.of(TestHelper.STRUCT_1, TestHelper.STRUCT_2));

    // Apply the Count transform under test.
    PCollection<String> structDataAsString =
        input.apply(MapElements.via(new FormatGenericSpannerStructAsTextFn(TestHelper.TABLE_NAME)));

    // Assert on the results.
    PAssert.that(structDataAsString)
        .containsInAnyOrder(
            TestHelper.STRUCT_1_BASE64_SERIALIZED, TestHelper.STRUCT_2_BASE64_SERIALIZED);

    // Run the pipeline.
    pipeline.run().waitUntilFinish();
  }
}
