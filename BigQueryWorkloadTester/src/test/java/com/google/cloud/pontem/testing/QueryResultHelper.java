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
package com.google.cloud.pontem.testing;

import com.google.cloud.bigquery.JobId;
import com.google.cloud.pontem.model.QueryResult;
import com.google.cloud.pontem.model.Status;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

/** Helper that builds QueryResult objects for testing. */
public class QueryResultHelper {

  public static QueryResult getSuccessfulQueryResult() {
    return getSuccessfulQueryResult(TestConstants.JOB_ID.toString(), TestConstants.TEST_QUERY);
  }

  public static QueryResult getSuccessfulQueryResult(final String jobId, final String query) {
    return getSharedQueryResultValues(jobId, query)
        .setStatus(Status.SUCCESS)
        .setErrors(new ArrayList<>())
        .setRuntime(TestConstants.SUCCESSFUL_QUERY_RUNTIME)
        .setWallTime(TestConstants.SUCCESSFUL_QUERY_WALLTIME)
        .build();
  }

  public static List<QueryResult> getSuccessfulQueryResults() {
    List<QueryResult> queryResults = new ArrayList<>();
    for (Entry<String, JobId> entry : TestConstants.QUERIES_AND_IDS.entrySet()) {
      queryResults.add(getSuccessfulQueryResult(entry.getValue().toString(), entry.getKey()));
    }

    return queryResults;
  }

  public static QueryResult getQueryResultWithSingleError() {
    return getQueryResultWithSingleError(
        TestConstants.JOB_ID.toString(), TestConstants.TEST_QUERY, TestConstants.ERROR_MESSAGE_1);
  }

  public static QueryResult getQueryResultWithSingleError(
      final String jobId, final String query, final String errorMessage) {
    return getQueryResultWithSingleError(jobId, query, Arrays.asList(errorMessage));
  }

  public static QueryResult getQueryResultWithSingleError(
      final String jobId, final String query, final List<String> errorMessages) {
    return getSharedQueryResultValues(jobId, query)
        .setStatus(Status.ERROR)
        .setErrors(errorMessages)
        .setRuntime(TestConstants.FAILING_QUERY_RUNTIME)
        .setWallTime(TestConstants.FAILING_QUERY_WALLTIME)
        .build();
  }

  public static List<QueryResult> getQueryResultsWithSingleError() {
    List<QueryResult> queryResults = new ArrayList<>();
    for (Entry<String, JobId> entry : TestConstants.QUERIES_AND_IDS.entrySet()) {
      List<String> errorMessages =
          TestConstants.QUERIES_AND_ERROR_MESSAGE_LISTS.get(entry.getKey());
      queryResults.add(
          getQueryResultWithSingleError(
              entry.getValue().toString(), entry.getKey(), errorMessages));
    }

    return queryResults;
  }

  public static QueryResult getQueryResultWithMultipleErrors() {
    return getQueryResultWithMultipleErrors(
        TestConstants.JOB_ID.toString(), TestConstants.TEST_QUERY, TestConstants.ERROR_MESSAGES);
  }

  public static QueryResult getQueryResultWithMultipleErrors(
      final String jobId, final String query, final List<String> errorMessages) {
    return getSharedQueryResultValues(jobId, query)
        .setStatus(Status.ERROR)
        .setErrors(errorMessages)
        .setRuntime(TestConstants.FAILING_QUERY_RUNTIME)
        .setWallTime(TestConstants.FAILING_QUERY_WALLTIME)
        .build();
  }

  public static QueryResult getExceptionQueryResult() {
    return getExceptionQueryResult(TestConstants.TEST_QUERY, TestConstants.ERROR_MESSAGE_1);
  }

  public static QueryResult getExceptionQueryResult(final String query, final String error) {
    return QueryResult.newBuilder()
        .setId("")
        .setQuery(query)
        .setStatus(Status.EXCEPTION)
        .addError(error)
        .setCreationTime(TestConstants.NO_CREATION_TIME)
        .setRuntime(TestConstants.NO_RUNTIME)
        .setWallTime(TestConstants.EXCEPTION_QUERY_WALLTIME)
        .build();
  }

  private static QueryResult.Builder getSharedQueryResultValues(
      final String jobId, final String query) {
    return QueryResult.newBuilder()
        .setId(jobId)
        .setQuery(query)
        .setCreationTime(TestConstants.CREATION_TIME);
  }
}
