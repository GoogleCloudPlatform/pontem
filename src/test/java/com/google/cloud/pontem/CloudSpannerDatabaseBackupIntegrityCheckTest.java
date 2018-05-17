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

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.JobMetrics;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CloudSpannerDatabaseBackupIntegrityCheck}. */
@RunWith(JUnit4.class)
public class CloudSpannerDatabaseBackupIntegrityCheckTest {

  @Test
  public void testPerformDatabaseBackupIntegrityCheck_valid() throws Exception {
    String projectId = "cloud-spanner-successful-backup";
    String jobId = "dataflow-backup-job-id";
    String gcsBucketName = "cloud-spanner-backup-bucket-success";
    String gcsFolderPath = "subFolder/";
    boolean shouldCheckRowCountsAgainstGcsMetadataFile = false;
    boolean shouldSkipWriteRowCountsOfVerifiedBackupToGcs = true;

    Map<String, Long> tableRowCounts = ImmutableMap.of("MyTable100", 100L, "tableName2", 2L);

    JobMetrics jobMetrics = TestHelper.getJobMetrics(tableRowCounts);
    Util mockUtil = mock(Util.class);
    when(mockUtil.getContentsOfFileFromGcs(eq(projectId), anyString(), anyString(), anyString()))
        .thenReturn("MyTable100\ntableName2");
    when(mockUtil.fetchMetricsForDataflowJob(eq(projectId), eq(jobId))).thenReturn(jobMetrics);

    assertTrue(
        CloudSpannerDatabaseBackupIntegrityCheck.performDatabaseBackupIntegrityCheck(
            projectId,
            jobId,
            gcsBucketName,
            gcsFolderPath,
            shouldCheckRowCountsAgainstGcsMetadataFile,
            shouldSkipWriteRowCountsOfVerifiedBackupToGcs,
            mockUtil));
  }

  @Test
  public void testPerformDatabaseBackupIntegrityCheck_gcsMetaDataValid() throws Exception {
    String projectId = "cloud-spanner-successful-backup";
    String jobId = "dataflow-backup-job-id";
    String gcsBucketName = "cloud-spanner-backup-bucket-success";
    String gcsFolderPath = "subFolder/";
    boolean shouldCheckRowCountsAgainstGcsMetadataFile = true;
    boolean shouldSkipWriteRowCountsOfVerifiedBackupToGcs = true;

    Map<String, Long> tableRowCounts = ImmutableMap.of("MyTable100", 100L, "tableName2", 2L);

    JobMetrics jobMetrics = TestHelper.getJobMetrics(tableRowCounts);
    String rawTableData = "MyTable100,100\ntableName2,2";
    Util mockUtil = mock(Util.class);
    when(mockUtil.getContentsOfFileFromGcs(eq(projectId), anyString(), anyString(), anyString()))
        .thenReturn(rawTableData);
    when(mockUtil.fetchMetricsForDataflowJob(eq(projectId), eq(jobId))).thenReturn(jobMetrics);

    assertTrue(
        CloudSpannerDatabaseBackupIntegrityCheck.performDatabaseBackupIntegrityCheck(
            projectId,
            jobId,
            gcsBucketName,
            gcsFolderPath,
            shouldCheckRowCountsAgainstGcsMetadataFile,
            shouldSkipWriteRowCountsOfVerifiedBackupToGcs,
            mockUtil));
  }

  @Test(expected = DataIntegrityErrorException.class)
  public void testPerformDatabaseBackupIntegrityCheck_invalid() throws Exception {
    String projectId = "cloud-spanner-successful-backup";
    String jobId = "dataflow-backup-job-id";
    String gcsBucketName = "cloud-spanner-backup-bucket-success";
    String gcsFolderPath = "subFolder/";
    boolean shouldCheckRowCountsAgainstGcsMetadataFile = false;
    boolean shouldSkipWriteRowCountsOfVerifiedBackupToGcs = true;

    Map<String, Long> tableRowCounts = ImmutableMap.of("MyTable100", 100L, "tableName2", 2L);

    JobMetrics jobMetrics = TestHelper.getJobMetrics(tableRowCounts);
    Util mockUtil = mock(Util.class);
    when(mockUtil.getContentsOfFileFromGcs(eq(projectId), anyString(), anyString(), anyString()))
        .thenReturn("MyTable100\ntableName2\nExtraTable");
    when(mockUtil.fetchMetricsForDataflowJob(eq(projectId), eq(jobId))).thenReturn(jobMetrics);

    CloudSpannerDatabaseBackupIntegrityCheck.performDatabaseBackupIntegrityCheck(
        projectId,
        jobId,
        gcsBucketName,
        gcsFolderPath,
        shouldCheckRowCountsAgainstGcsMetadataFile,
        shouldSkipWriteRowCountsOfVerifiedBackupToGcs,
        mockUtil);
  }
}
