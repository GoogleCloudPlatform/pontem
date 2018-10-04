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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import org.apache.commons.cli.Option;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link EndToEndHelper}. */
@RunWith(JUnit4.class)
public final class EndToEndHelperTest {

  @Test
  public void testVerifyGcsBackupMetaData_valid() throws Exception {
    String projectId = "cloud-project-id";
    String inputGcsPath = "gs://my-bucket/myPath";

    GcsUtil mockGcsUtil = mock(GcsUtil.class);
    when(mockGcsUtil.getContentsOfFileFromGcs(
            eq(projectId),
            eq(GcsUtil.getGcsBucketNameFromDatabaseBackupLocation(inputGcsPath)),
            eq(GcsUtil.getGcsFolderPathFromDatabaseBackupLocation(inputGcsPath)),
            eq(Util.FILE_PATH_FOR_DATABASE_TABLE_NAMES)))
        .thenReturn(
            EndToEndHelper.CHILD_TABLE_NAME
                + ","
                + EndToEndHelper.PARENT_TABLE_NAME
                + "\n"
                + EndToEndHelper.PARENT_TABLE_NAME
                + ",\n"
                + EndToEndHelper.FOO_TABLE_NAME
                + ",");

    EndToEndHelper.verifyGcsBackupMetaData(projectId, inputGcsPath, mockGcsUtil);
    assertTrue("Verification of GCS backup succeeded", true);
  }

  @Test(expected = Exception.class)
  public void testVerifyGcsBackupMetaData_invalid() throws Exception {
    String projectId = "cloud-project-id";
    String inputGcsPath = "gs://my-bucket/myPath";

    GcsUtil mockGcsUtil = mock(GcsUtil.class);
    when(mockGcsUtil.getContentsOfFileFromGcs(
            eq(projectId),
            eq(GcsUtil.getGcsBucketNameFromDatabaseBackupLocation(inputGcsPath)),
            eq(GcsUtil.getGcsFolderPathFromDatabaseBackupLocation(inputGcsPath)),
            eq(Util.FILE_PATH_FOR_DATABASE_TABLE_NAMES)))
        .thenReturn(
            "Foo"
                + EndToEndHelper.CHILD_TABLE_NAME
                + ","
                + EndToEndHelper.PARENT_TABLE_NAME
                + "\n"
                + EndToEndHelper.PARENT_TABLE_NAME
                + ",");

    EndToEndHelper.verifyGcsBackupMetaData(projectId, inputGcsPath, mockGcsUtil);
  }

  @Test
  public void testVerifyDatabaseStructureAndContent_valid() throws Exception {
    String projectId = "cloud-project-id";
    String instanceId = "instance-id";
    String databaseId = "database-id";

    SpannerUtil mockSpannerUtil = mock(SpannerUtil.class);
    when(mockSpannerUtil.queryDatabaseDdl(eq(projectId), eq(instanceId), eq(databaseId)))
        .thenReturn(EndToEndHelper.GOOGLE_CLOUD_SPANNER_DDL);

    when(mockSpannerUtil.performSingleSpannerReadQuery(
            eq(projectId),
            eq(instanceId),
            eq(databaseId),
            eq("SELECT * FROM " + EndToEndHelper.FOO_TABLE_NAME + ";")))
        .thenReturn(EndToEndHelper.FOO_TABLE_STRUCTS);

    when(mockSpannerUtil.performSingleSpannerReadQuery(
            eq(projectId),
            eq(instanceId),
            eq(databaseId),
            eq("SELECT * FROM " + EndToEndHelper.PARENT_TABLE_NAME + ";")))
        .thenReturn(EndToEndHelper.PARENT_TABLE_STRUCTS);

    when(mockSpannerUtil.performSingleSpannerReadQuery(
            eq(projectId),
            eq(instanceId),
            eq(databaseId),
            eq("SELECT * FROM " + EndToEndHelper.CHILD_TABLE_NAME + ";")))
        .thenReturn(EndToEndHelper.CHILD_TABLE_STRUCTS);

    EndToEndHelper.verifyDatabaseStructureAndContent(
        projectId, instanceId, databaseId, mockSpannerUtil);
    assertTrue("Verification of database structure succeeded", true);
  }

  @Test(expected = Exception.class)
  public void testVerifyDatabaseStructureAndContent_DdlNotMatching() throws Exception {
    String projectId = "cloud-project-id";
    String instanceId = "instance-id";
    String databaseId = "database-id";

    SpannerUtil mockSpannerUtil = mock(SpannerUtil.class);
    when(mockSpannerUtil.queryDatabaseDdl(eq(projectId), eq(instanceId), eq(databaseId)))
        .thenReturn(ImmutableList.of("CREATE TABLE FooTable {}"));

    EndToEndHelper.verifyDatabaseStructureAndContent(
        projectId, instanceId, databaseId, mockSpannerUtil);
  }

  @Test
  public void testConfigureCommandlineOptions() throws Exception {
    Collection<Option> options = EndToEndHelper.configureCommandlineOptions().getOptions();
    assertEquals("All options present", 6, options.size());
  }
}
