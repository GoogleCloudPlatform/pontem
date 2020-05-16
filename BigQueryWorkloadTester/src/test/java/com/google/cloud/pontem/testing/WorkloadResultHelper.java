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
import com.google.cloud.pontem.model.WorkloadResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/** Helper that builds WorkloadResult objects and lists for testing. */
public class WorkloadResultHelper {

  public static WorkloadResult getWorkloadResultMultipleSuccessfulQueries() {
    return getWorkloadResultMultipleSuccessfulQueries(
        TestConstants.QUERIES_AND_IDS, TestConstants.CONCURRENCY_LEVEL_BELOW_LIMIT);
  }

  /**
   * Returns a WorkloadResult for multiple successful queries.
   * @param queriesAndIds a Map of query String to JobId
   * @param concurrencyLevel an int representing the concurrency level
   * @return WorkloadResult
   */
  public static WorkloadResult getWorkloadResultMultipleSuccessfulQueries(
      final Map<String, JobId> queriesAndIds, final int concurrencyLevel) {
    List<QueryResult> queryResults = new ArrayList<>();
    for (Entry<String, JobId> entry : queriesAndIds.entrySet()) {
      queryResults.add(
          QueryResultHelper.getSuccessfulQueryResult(entry.getValue().toString(), entry.getKey()));
    }

    return WorkloadResult.newBuilder()
        .setConcurrencyLevel(concurrencyLevel)
        .setWorkloadName(TestConstants.WORKLOAD_NAME)
        .setProjectId(TestConstants.PROJECT_ID)
        .setStatus(Status.SUCCESS)
        .setError("")
        .setQueryResults(queryResults)
        .setRunTime(TestConstants.SUCCESSFUL_WORKLOAD_RUNTIME_MULTIPLE_QUERIES)
        .setWallTime(TestConstants.SUCCESSFUL_WORKLOAD_WALLTIME)
        .build();
  }

  /**
   * Returns a WorkloadResult with multiple query errors.
   * @return WorkloadResult
   */
  public static WorkloadResult getWorkloadResultMultipleQueryErrors() {
    List<QueryResult> queryResults = new ArrayList<>();
    for (Entry<String, JobId> entry : TestConstants.QUERIES_AND_IDS.entrySet()) {
      List<String> errorMessages =
          TestConstants.QUERIES_AND_ERROR_MESSAGE_LISTS.get(entry.getKey());
      queryResults.add(
          QueryResultHelper.getQueryResultWithSingleError(
              entry.getValue().toString(), entry.getKey(), errorMessages));
    }

    return WorkloadResult.newBuilder()
        .setConcurrencyLevel(TestConstants.CONCURRENCY_LEVEL_BELOW_LIMIT)
        .setWorkloadName(TestConstants.WORKLOAD_NAME)
        .setProjectId(TestConstants.PROJECT_ID)
        .setStatus(Status.SUCCESS)
        .setError("")
        .setRunTime(TestConstants.FAILING_WORKLOAD_RUNTIME_MULTIPLE_QUERIES)
        .setWallTime(TestConstants.FAILING_WORKLOAD_WALLTIME)
        .setQueryResults(queryResults)
        .build();
  }

  public static List<WorkloadResult> getWorkloadResultsForConcurrencyLevel() {
    return getWorkloadResultsForConcurrencyLevel(TestConstants.CONCURRENCY_LEVEL_BELOW_LIMIT);
  }

  /**
   * Returns a list of WorkloadResult for a given concurrency level.
   * @param concurrencyLevel an int representing the concurrency level
   * @return WorkloadResult
   */
  public static List<WorkloadResult> getWorkloadResultsForConcurrencyLevel(
      final int concurrencyLevel) {
    WorkloadResult workloadResult =
        getWorkloadResultMultipleSuccessfulQueries(TestConstants.QUERIES_AND_IDS, concurrencyLevel);
    return Arrays.asList(workloadResult);
  }

  public static List<WorkloadResult> getConcurrentWorkloadResults() {
    return getConcurrentWorkloadResults(
        TestConstants.CONCURRENCY_LEVEL_BELOW_LIMIT, getWorkloadResultMultipleSuccessfulQueries());
  }

  private static List<WorkloadResult> getConcurrentWorkloadResults(
      final int concurrencyLevel, final WorkloadResult workloadResult) {
    List<WorkloadResult> workloadResults = new ArrayList<>(concurrencyLevel);
    for (int i = 0; i < concurrencyLevel; i++) {
      workloadResults.add(workloadResult);
    }

    return workloadResults;
  }

  public static List<WorkloadResult> getConcurrentWorkloadResultsWithQueryErrors() {
    return getConcurrentWorkloadResults(
        TestConstants.CONCURRENCY_LEVEL_BELOW_LIMIT, getWorkloadResultMultipleQueryErrors());
  }

  public static List<WorkloadResult> getConcurrentWorkloadResultsOverLimit() {
    return getConcurrentWorkloadResults(
        TestConstants.CONCURRENCY_LEVEL_LIMIT, getWorkloadResultMultipleSuccessfulQueries());
  }

  public static List<WorkloadResult> getConcurrentWorkloadExceptionResults() {
    return Arrays.asList(getConcurrentWorkloadExceptionResult());
  }

  /**
   * Returns a WorkloadResult with exceptions.
   * @return WorkloadResult
   */
  public static WorkloadResult getConcurrentWorkloadExceptionResult() {
    return WorkloadResult.newBuilder()
        .setWorkloadName(TestConstants.WORKLOAD_NAME)
        .setProjectId(TestConstants.PROJECT_ID)
        .setConcurrencyLevel(TestConstants.CONCURRENCY_LEVEL_BELOW_LIMIT)
        .setStatus(Status.EXCEPTION)
        .setError(TestConstants.ERROR_MESSAGE_1)
        .setQueryResults(new ArrayList<>())
        .setRunTime(TestConstants.NO_RUNTIME)
        .setWallTime(TestConstants.NO_WALLTIME)
        .build();
  }
}
