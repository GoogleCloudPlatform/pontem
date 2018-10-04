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
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.JobMetrics;
import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Map;
import org.apache.commons.cli.Option;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CloudSpannerDatabaseRestoreIntegrityCheck}. */
@RunWith(JUnit4.class)
public class CloudSpannerDatabaseRestoreIntegrityCheckTest {
  private static final String PROJECT_ID = "cloud-spanner-successful-restore";
  private static final String[] RESTORE_JOB_IDS = {
    "dataflow-restore-job-id-1", "dataflow-restore-job-id-2"
  };
  private static final String BACKUP_JOB_ID = "dataflow-backup-job-id";
  private static final String GCS_BUCKET_NAME = "cloud-spanner-backup-bucket-success";
  private static final String GCS_FOLDER_PATH = "subFolder/";
  boolean requireAllTablesRestored = true;

  @Test
  public void testPerformDatabaseBackupIntegrityCheck_valid() throws Exception {
    Map<String, Long> tableRowCountsBackup = ImmutableMap.of("MyTable100", 100L, "tableName2", 2L);
    JobMetrics jobMetricsBackup = TestHelper.getJobMetrics(tableRowCountsBackup);

    Map<String, Long> tableRowCountsRestore0 = ImmutableMap.of("MyTable100", 100L);
    JobMetrics jobMetricsRestore0 = TestHelper.getJobMetrics(tableRowCountsRestore0);

    Map<String, Long> tableRowCountsRestore1 = ImmutableMap.of("tableName2", 2L);
    JobMetrics jobMetricsRestore1 = TestHelper.getJobMetrics(tableRowCountsRestore1);

    GcsUtil mockGcsUtil = mock(GcsUtil.class);
    Util mockUtil = mock(Util.class);
    when(mockGcsUtil.getContentsOfFileFromGcs(
            eq(PROJECT_ID), anyString(), anyString(), anyString()))
        .thenReturn("MyTable100\ntableName2");
    when(mockUtil.fetchMetricsForDataflowJob(eq(PROJECT_ID), eq(BACKUP_JOB_ID)))
        .thenReturn(jobMetricsBackup);
    when(mockUtil.fetchMetricsForDataflowJob(eq(PROJECT_ID), eq(RESTORE_JOB_IDS[0])))
        .thenReturn(jobMetricsRestore0);
    when(mockUtil.fetchMetricsForDataflowJob(eq(PROJECT_ID), eq(RESTORE_JOB_IDS[1])))
        .thenReturn(jobMetricsRestore1);

    assertTrue(
        CloudSpannerDatabaseRestoreIntegrityCheck.performDatabaseRestoreIntegrityCheck(
            PROJECT_ID,
            RESTORE_JOB_IDS,
            BACKUP_JOB_ID,
            GCS_BUCKET_NAME,
            GCS_FOLDER_PATH,
            requireAllTablesRestored,
            mockGcsUtil,
            mockUtil));
  }

  @Test(expected = Exception.class)
  public void testPerformDatabaseBackupIntegrityCheck_invalid() throws Exception {
    Map<String, Long> tableRowCountsBackup =
        ImmutableMap.of("MyTable100", 100L, "tableName2", 2L, "third_table", 300L);
    JobMetrics jobMetricsBackup = TestHelper.getJobMetrics(tableRowCountsBackup);

    Map<String, Long> tableRowCountsRestore0 = ImmutableMap.of("MyTable100", 100L);
    JobMetrics jobMetricsRestore0 = TestHelper.getJobMetrics(tableRowCountsRestore0);

    Map<String, Long> tableRowCountsRestore1 = ImmutableMap.of("tableName2", 2L);
    JobMetrics jobMetricsRestore1 = TestHelper.getJobMetrics(tableRowCountsRestore1);

    GcsUtil mockGcsUtil = mock(GcsUtil.class);
    Util mockUtil = mock(Util.class);
    when(mockGcsUtil.getContentsOfFileFromGcs(
            eq(PROJECT_ID), anyString(), anyString(), anyString()))
        .thenReturn("MyTable100\ntableName2");
    when(mockUtil.fetchMetricsForDataflowJob(eq(PROJECT_ID), eq(BACKUP_JOB_ID)))
        .thenReturn(jobMetricsBackup);
    when(mockUtil.fetchMetricsForDataflowJob(eq(PROJECT_ID), eq(RESTORE_JOB_IDS[0])))
        .thenReturn(jobMetricsRestore0);
    when(mockUtil.fetchMetricsForDataflowJob(eq(PROJECT_ID), eq(RESTORE_JOB_IDS[1])))
        .thenReturn(jobMetricsRestore1);

    CloudSpannerDatabaseRestoreIntegrityCheck.performDatabaseRestoreIntegrityCheck(
        PROJECT_ID,
        RESTORE_JOB_IDS,
        BACKUP_JOB_ID,
        GCS_BUCKET_NAME,
        GCS_FOLDER_PATH,
        requireAllTablesRestored,
        mockGcsUtil,
        mockUtil);
  }

  @Test
  public void testConfigureCommandlineOptions() throws Exception {
    Collection<Option> options =
        CloudSpannerDatabaseRestoreIntegrityCheck.configureCommandlineOptions().getOptions();
    assertEquals("All options present", 5, options.size());
  }
}
