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
package com.google.cloud.pontem.benchmark.runners;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.AdditionalAnswers.returnsElementsOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.pontem.benchmark.backends.BackendException;
import com.google.cloud.pontem.benchmark.backends.BigQueryBackend;
import com.google.cloud.pontem.model.BigQueryResult;
import com.google.cloud.pontem.model.QueryResult;
import com.google.cloud.pontem.testing.BigQueryResultHelper;
import com.google.cloud.pontem.testing.QueryResultHelper;
import com.google.cloud.pontem.testing.TestConstants;
import com.google.common.base.Stopwatch;
import com.google.common.testing.FakeTicker;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SerialQueryRunner}. */
@RunWith(JUnit4.class)
public class SerialQueryRunnerTest {

  private BigQueryBackend bigQueryBackendMock;
  private FakeTicker fakeTicker;
  private Stopwatch stopwatch;
  private SerialQueryRunner serialQueryRunner;

  @Before
  public void setUp() {
    this.bigQueryBackendMock = mock(BigQueryBackend.class);
    this.fakeTicker = new FakeTicker();
    this.stopwatch = Stopwatch.createUnstarted(fakeTicker);

    this.serialQueryRunner = new SerialQueryRunner(bigQueryBackendMock, stopwatch);
  }

  @Test
  public void successfulQuery() throws Exception {
    fakeTicker.setAutoIncrementStep(TestConstants.SUCCESSFUL_QUERY_WALLTIME, TimeUnit.MILLISECONDS);

    when(bigQueryBackendMock.executeQuery(TestConstants.TEST_QUERY))
        .thenReturn(BigQueryResultHelper.getSuccessfulBigQueryResult());
    List<QueryResult> queryResults = serialQueryRunner.run(Arrays.asList(TestConstants.TEST_QUERY));

    assertThat(queryResults.size(), is(1));
    assertThat(queryResults.get(0), is(QueryResultHelper.getSuccessfulQueryResult()));
  }

  @Test
  public void successfulQueries() throws Exception {
    fakeTicker.setAutoIncrementStep(TestConstants.SUCCESSFUL_QUERY_WALLTIME, TimeUnit.MILLISECONDS);

    Map<String, QueryResult> expectedQueryResults = new LinkedHashMap<>();
    List<BigQueryResult> bigQueryResults = new ArrayList<>();
    for (Entry<String, JobId> entry : TestConstants.QUERIES_AND_IDS.entrySet()) {
      String jobId = entry.getValue().toString();
      bigQueryResults.add(BigQueryResultHelper.getSuccessfulBigQueryResult(jobId));

      QueryResult expectedResult =
          QueryResultHelper.getSuccessfulQueryResult(jobId, entry.getKey());
      expectedQueryResults.put(jobId, expectedResult);
    }

    when(bigQueryBackendMock.executeQuery(anyString()))
        .thenAnswer(returnsElementsOf(bigQueryResults));

    List<QueryResult> queryResults = serialQueryRunner.run(TestConstants.TEST_QUERIES);
    assertThat(queryResults.size(), is(TestConstants.TEST_QUERIES.size()));

    for (QueryResult queryResult : queryResults) {
      assertThat(queryResult, is(expectedQueryResults.get(queryResult.getId())));
      verify(bigQueryBackendMock, times(1)).executeQuery(queryResult.getQuery());
    }
  }

  @Test
  public void successfulQueryWithExecutionErrors() throws Exception {
    fakeTicker.setAutoIncrementStep(TestConstants.FAILING_QUERY_WALLTIME, TimeUnit.MILLISECONDS);
    when(bigQueryBackendMock.executeQuery(TestConstants.TEST_QUERY))
        .thenReturn(BigQueryResultHelper.getBigQueryResultWithQueryErrors());

    List<QueryResult> queryResults = serialQueryRunner.run(Arrays.asList(TestConstants.TEST_QUERY));

    assertThat(queryResults.size(), is(1));
    assertThat(queryResults.get(0), is(QueryResultHelper.getQueryResultWithSingleError()));
  }

  @Test
  public void successfulQueriesWithExecutionErrors() throws Exception {
    fakeTicker.setAutoIncrementStep(TestConstants.FAILING_QUERY_WALLTIME, TimeUnit.MILLISECONDS);

    Map<String, QueryResult> expectedQueryResults = new LinkedHashMap<>();
    for (String query : TestConstants.QUERIES_AND_IDS.keySet()) {
      String jobId = TestConstants.QUERIES_AND_IDS.get(query).toString();
      String errorMessage = TestConstants.QUERIES_AND_ERRORS.get(query).getMessage();

      when(bigQueryBackendMock.executeQuery(query))
          .thenReturn(BigQueryResultHelper.getBigQueryResultWithQueryErrors(jobId, errorMessage));

      QueryResult expectedQueryResult =
          QueryResultHelper.getQueryResultWithSingleError(jobId, query, errorMessage);
      expectedQueryResults.put(jobId, expectedQueryResult);
    }

    List<QueryResult> queryResults = serialQueryRunner.run(TestConstants.TEST_QUERIES);

    assertThat(queryResults.size(), is(TestConstants.TEST_QUERIES.size()));

    for (QueryResult queryResult : queryResults) {
      assertThat(queryResult, is(expectedQueryResults.get(queryResult.getId())));
      verify(bigQueryBackendMock, times(1)).executeQuery(queryResult.getQuery());
    }
  }

  @Test
  public void successfulQueryWithMultipleExecutionErrors() throws Exception {
    fakeTicker.setAutoIncrementStep(TestConstants.FAILING_QUERY_WALLTIME, TimeUnit.MILLISECONDS);
    when(bigQueryBackendMock.executeQuery(TestConstants.TEST_QUERY))
        .thenReturn(BigQueryResultHelper.getBigQueryResultWithMultipleQueryErrors());

    List<QueryResult> queryResults = serialQueryRunner.run(Arrays.asList(TestConstants.TEST_QUERY));

    assertThat(queryResults.size(), is(1));
    assertThat(queryResults.get(0), is(QueryResultHelper.getQueryResultWithMultipleErrors()));
  }

  @Test
  public void successfulQueriesWithMultipleExecutionErrors() throws Exception {
    fakeTicker.setAutoIncrementStep(TestConstants.FAILING_QUERY_WALLTIME, TimeUnit.MILLISECONDS);

    Map<String, QueryResult> expectedQueryResults = new LinkedHashMap<>();
    for (String query : TestConstants.QUERIES_AND_IDS.keySet()) {
      String jobId = TestConstants.QUERIES_AND_IDS.get(query).toString();
      List<String> errorMessages = TestConstants.QUERIES_AND_ERROR_MESSAGE_LISTS.get(query);

      when(bigQueryBackendMock.executeQuery(query))
          .thenReturn(
              BigQueryResultHelper.getBigQueryResultWithMultipleQueryErrors(jobId, errorMessages));

      QueryResult expectedQueryResult =
          QueryResultHelper.getQueryResultWithMultipleErrors(jobId, query, errorMessages);
      expectedQueryResults.put(jobId, expectedQueryResult);
    }

    List<QueryResult> queryResults = serialQueryRunner.run(TestConstants.TEST_QUERIES);
    assertThat(queryResults.size(), is(TestConstants.TEST_QUERIES.size()));

    for (QueryResult queryResult : queryResults) {
      assertThat(queryResult, is(expectedQueryResults.get(queryResult.getId())));
      verify(bigQueryBackendMock, times(1)).executeQuery(queryResult.getQuery());
    }
  }

  @Test
  public void failingQuery() throws Exception {
    fakeTicker.setAutoIncrementStep(TestConstants.EXCEPTION_QUERY_WALLTIME, TimeUnit.MILLISECONDS);
    when(bigQueryBackendMock.executeQuery(TestConstants.TEST_QUERY))
        .thenThrow(new BackendException(TestConstants.ERROR_MESSAGE_1));
    List<QueryResult> queryResults = serialQueryRunner.run(Arrays.asList(TestConstants.TEST_QUERY));

    assertThat(queryResults.size(), is(1));
    assertThat(queryResults.get(0), is(QueryResultHelper.getExceptionQueryResult()));
  }

  @Test
  public void failingQueries() throws Exception {
    fakeTicker.setAutoIncrementStep(TestConstants.EXCEPTION_QUERY_WALLTIME, TimeUnit.MILLISECONDS);

    Map<String, QueryResult> expectedQueryResults = new LinkedHashMap<>();
    for (Entry<String, BigQueryError> entry : TestConstants.QUERIES_AND_ERRORS.entrySet()) {
      String query = entry.getKey();
      String errorMessage = entry.getValue().getMessage();
      when(bigQueryBackendMock.executeQuery(query)).thenThrow(new BackendException(errorMessage));

      QueryResult expectedQueryResult =
          QueryResultHelper.getExceptionQueryResult(query, errorMessage);
      expectedQueryResults.put(query, expectedQueryResult);
    }

    List<QueryResult> queryResults = serialQueryRunner.run(TestConstants.TEST_QUERIES);
    assertThat(queryResults.size(), is(TestConstants.TEST_QUERIES.size()));

    for (QueryResult queryResult : queryResults) {
      assertThat(queryResult, is(expectedQueryResults.get(queryResult.getQuery())));
      verify(bigQueryBackendMock, times(1)).executeQuery(queryResult.getQuery());
    }
  }
}
