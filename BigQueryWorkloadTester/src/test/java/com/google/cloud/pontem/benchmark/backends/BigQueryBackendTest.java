/*
 * Copyright 2018 Google LLC
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
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration.Priority;
import com.google.cloud.pontem.model.BigQueryResult;
import com.google.cloud.pontem.model.Status;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

/** Tests for {@link BigQueryBackend}. */
@RunWith(JUnit4.class)
public class BigQueryBackendTest {

  private static final String TEST_QUERY = "THIS IS MY TEST QUERY";
  private static final String REASON = "It just blew-up";
  private static final String LOCATION = "Somewhere";

  private static final long CREATION_TIME = 1;
  private static final long START_TIME = 10;
  private static final long END_TIME = 100;

  private static final JobId JOB_ID = JobId.of("123");

  private static final List<BigQueryError> BIGQUERY_ERRORS =
      Arrays.asList(
          new BigQueryError(REASON, LOCATION, "Boom"),
          new BigQueryError(REASON, LOCATION, "Kaboom"),
          new BigQueryError(REASON, LOCATION, "Bakoom"));

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

    executeQuery(getSuccessfulResult());
  }

  @Test
  public void executeFailingQuery() throws Exception {
    when(jobStatusMock.getError()).thenReturn(BIGQUERY_ERRORS.get(0));
    when(jobStatusMock.getExecutionErrors()).thenReturn(BIGQUERY_ERRORS);
    executeQuery(getFailingResult());
  }

  private void executeQuery(BigQueryResult expectedResult) throws Exception {
    when(bigQueryMock.create(any(JobInfo.class))).thenReturn(jobMock);
    when(jobMock.waitFor()).thenReturn(jobMock);
    when(jobMock.getJobId()).thenReturn(JOB_ID);
    when(jobMock.getStatus()).thenReturn(jobStatusMock);
    when(jobMock.getStatistics()).thenReturn(jobStatisticsMock);

    when(jobStatisticsMock.getStartTime()).thenReturn(START_TIME);
    when(jobStatisticsMock.getEndTime()).thenReturn(END_TIME);
    when(jobStatisticsMock.getCreationTime()).thenReturn(CREATION_TIME);

    ArgumentCaptor<JobInfo> captor = ArgumentCaptor.forClass(JobInfo.class);
    BigQueryResult bigQueryResult = bigQueryBackend.executeQuery(TEST_QUERY);

    assertEquals(expectedResult, bigQueryResult);

    verify(bigQueryMock).create(captor.capture());
    assertEquals(getQueryJobConfig(TEST_QUERY), captor.getValue().getConfiguration());
  }

  @Test(expected = BackendException.class)
  public void executeQueryFailsToCreateJob() throws Exception {
    when(bigQueryMock.create(any(JobInfo.class))).thenThrow(BackendException.class);

    bigQueryBackend.executeQuery(TEST_QUERY);
  }

  @Test(expected = BackendException.class)
  public void executeQueryFailsWithNullJob() throws Exception {
    when(bigQueryMock.create(any(JobInfo.class))).thenReturn(jobMock);
    when(jobMock.waitFor()).thenReturn(null);

    bigQueryBackend.executeQuery(TEST_QUERY);
  }

  private QueryJobConfiguration getQueryJobConfig(final String query) {
    return QueryJobConfiguration.newBuilder(query)
        .setUseLegacySql(false)
        .setUseQueryCache(false)
        .setPriority(Priority.INTERACTIVE)
        .build();
  }

  private BigQueryResult getSuccessfulResult() {
    return getSharedValues().setStatus(Status.SUCCESS).setErrors(new ArrayList<>()).build();
  }

  private BigQueryResult getFailingResult() {
    List<String> errors = new ArrayList<>();
    BIGQUERY_ERRORS.forEach(e -> errors.add(e.getMessage()));

    return getSharedValues().setStatus(Status.ERROR).setErrors(errors).build();
  }

  private BigQueryResult.Builder getSharedValues() {
    return BigQueryResult.newBuilder()
        .setId(JOB_ID.toString())
        .setStartTime(START_TIME)
        .setEndTime(END_TIME)
        .setCreationTime(CREATION_TIME);
  }
}
