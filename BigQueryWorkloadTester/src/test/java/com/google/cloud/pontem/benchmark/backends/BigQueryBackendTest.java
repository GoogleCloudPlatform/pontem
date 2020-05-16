/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.pontem.benchmark.backends;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration.Priority;
import com.google.cloud.pontem.model.BigQueryResult;
import com.google.cloud.pontem.testing.BigQueryResultHelper;
import com.google.cloud.pontem.testing.TestConstants;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

/** Tests for {@link BigQueryBackend}. */
@RunWith(JUnit4.class)
public class BigQueryBackendTest {

  private BigQuery bigQueryMock;
  private BigQueryBackend bigQueryBackend;
  private Job jobMock;
  private JobStatus jobStatusMock;
  private JobStatistics jobStatisticsMock;

  @Before
  public void setUp() {
    this.bigQueryMock = mock(BigQuery.class);
    this.bigQueryBackend = new BigQueryBackend(bigQueryMock);
    this.jobMock = mock(Job.class);
    this.jobStatusMock = mock(JobStatus.class);
    this.jobStatisticsMock = mock(JobStatistics.class);
  }

  @Test
  public void executeSuccessfulQuery() throws Exception {
    when(jobStatusMock.getError()).thenReturn(null);
    when(jobStatisticsMock.getStartTime()).thenReturn(TestConstants.SUCCESSFUL_START_TIME);
    when(jobStatisticsMock.getEndTime()).thenReturn(TestConstants.SUCCESSFUL_END_TIME);

    executeQuery(BigQueryResultHelper.getSuccessfulBigQueryResult());
  }

  @Test
  public void executeFailingQuery() throws Exception {
    when(jobStatusMock.getError()).thenReturn(TestConstants.ERRORS.get(0));
    when(jobStatusMock.getExecutionErrors()).thenReturn(TestConstants.ERRORS);
    when(jobStatisticsMock.getStartTime()).thenReturn(TestConstants.FAILING_START_TIME);
    when(jobStatisticsMock.getEndTime()).thenReturn(TestConstants.FAILING_END_TIME);

    executeQuery(BigQueryResultHelper.getBigQueryResultWithMultipleQueryErrors());
  }

  private void executeQuery(BigQueryResult expectedResult) throws Exception {
    when(bigQueryMock.create(any(JobInfo.class))).thenReturn(jobMock);
    when(jobMock.waitFor()).thenReturn(jobMock);
    when(jobMock.getJobId()).thenReturn(TestConstants.JOB_ID);
    when(jobMock.getStatus()).thenReturn(jobStatusMock);
    when(jobMock.getStatistics()).thenReturn(jobStatisticsMock);

    when(jobStatisticsMock.getCreationTime()).thenReturn(TestConstants.CREATION_TIME);

    ArgumentCaptor<JobInfo> captor = ArgumentCaptor.forClass(JobInfo.class);
    BigQueryResult bigQueryResult = bigQueryBackend.executeQuery(TestConstants.TEST_QUERY);

    assertEquals(expectedResult, bigQueryResult);

    verify(bigQueryMock).create(captor.capture());
    assertEquals(getQueryJobConfig(TestConstants.TEST_QUERY), captor.getValue().getConfiguration());
  }

  @Test(expected = BackendException.class)
  public void executeQueryFailsToCreateJob() throws Exception {
    when(bigQueryMock.create(any(JobInfo.class))).thenThrow(BigQueryException.class);

    bigQueryBackend.executeQuery(TestConstants.TEST_QUERY);
  }

  @Test(expected = BackendException.class)
  public void executeQueryFailsWithNullJob() throws Exception {
    when(bigQueryMock.create(any(JobInfo.class))).thenReturn(jobMock);
    when(jobMock.waitFor()).thenReturn(null);

    bigQueryBackend.executeQuery(TestConstants.TEST_QUERY);
  }

  private QueryJobConfiguration getQueryJobConfig(final String query) {
    return QueryJobConfiguration.newBuilder(query)
        .setUseLegacySql(false)
        .setUseQueryCache(false)
        .setPriority(Priority.INTERACTIVE)
        .build();
  }
}
