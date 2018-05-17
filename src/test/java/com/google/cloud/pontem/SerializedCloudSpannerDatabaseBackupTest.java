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

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Struct;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SerializedCloudSpannerDatabaseBackup}. */
@RunWith(JUnit4.class)
public class SerializedCloudSpannerDatabaseBackupTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

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

  @Test
  public void testConstructPipeline() throws Exception {
    String projectId = "fooP";
    String instanceId = "fooI";
    String databaseId = "fooD";
    String outputFolder = "gs://foo/myPath";

    BaseCloudSpannerBackupOptions options =
        PipelineOptionsFactory.fromArgs(
                "--projectId=" + projectId,
                "--inputSpannerInstanceId=" + instanceId,
                "--inputSpannerDatabaseId=" + databaseId,
                "--outputFolder=" + outputFolder,
                "--shouldBackupDatabaseDdl=false")
            .withValidation()
            .as(BaseCloudSpannerBackupOptions.class);

    Util mockUtil = mock(Util.class);
    when(mockUtil.performSingleSpannerQuery(
            eq(projectId),
            eq(instanceId),
            eq(databaseId),
            eq(BaseCloudSpannerDatabaseBackup.LIST_ALL_TABLES_SQL_QUERY)))
        .thenReturn(
            ImmutableList.of(
                Struct.newBuilder()
                    .set("table_name")
                    .to("tableName1")
                    .set("parent_table_name")
                    .to("")
                    .build(),
                Struct.newBuilder()
                    .set("table_name")
                    .to("tableName2")
                    .set("parent_table_name")
                    .to("")
                    .build()));

    String tableNamesBeingBackedUp =
        "SELECT table_name, parent_table_name FROM information_schema.tables AS t WHERE t.table_catalog = '' and t.table_schema = '' and table_name IN (\"tableName2\",\"tableName1\") ORDER BY parent_table_name DESC";
    when(mockUtil.performSingleSpannerQuery(
            eq(projectId), eq(instanceId), eq(databaseId), eq(tableNamesBeingBackedUp)))
        .thenReturn(
            ImmutableList.of(
                Struct.newBuilder()
                    .set("table_name")
                    .to("tableName1")
                    .set("parent_table_name")
                    .to("")
                    .build(),
                Struct.newBuilder()
                    .set("table_name")
                    .to("tableName2")
                    .set("parent_table_name")
                    .to("")
                    .build()));

    SpannerConfig mockSpannerConfig = mock(SpannerConfig.class);

    TestPipeline testPipeline = TestPipeline.create();
    SerializedCloudSpannerDatabaseBackup.constructPipeline(
        testPipeline, options, mockSpannerConfig, mockUtil);

    verify(mockUtil, times(2))
        .performSingleSpannerQuery(anyString(), anyString(), anyString(), anyString());
  }
}
