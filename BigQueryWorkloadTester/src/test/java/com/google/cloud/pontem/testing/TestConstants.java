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

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.JobId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Constants for Tests. */
public class TestConstants {
  public static final String TEST_QUERY = "TEST_QUERY_1";
  public static final List<String> TEST_QUERIES =
      Arrays.asList(TEST_QUERY, "TEST_QUERY_2", "TEST_QUERY_3");

  public static final JobId JOB_ID = JobId.of("123");
  public static final List<JobId> JOB_IDS = Arrays.asList(JOB_ID, JobId.of("456"), JobId.of("789"));

  private static final String ERROR_REASON = "ERROR REASON ";
  private static final String ERROR_LOCATION = "ERROR LOCATION ";
  private static final String ERROR_MESSAGE = "ERROR MESSAGE ";

  public static final String ERROR_MESSAGE_1 = ERROR_MESSAGE + 1;

  public static final List<BigQueryError> ERRORS =
      Arrays.asList(
          new BigQueryError(ERROR_REASON + 1, ERROR_LOCATION + 1, ERROR_MESSAGE + 1),
          new BigQueryError(ERROR_REASON + 2, ERROR_LOCATION + 2, ERROR_MESSAGE + 2),
          new BigQueryError(ERROR_REASON + 3, ERROR_LOCATION + 3, ERROR_MESSAGE + 3));

  public static final List<List<BigQueryError>> ERROR_LIST =
      Arrays.asList(
          ERRORS,
          Arrays.asList(
              new BigQueryError(ERROR_REASON + 4, ERROR_LOCATION + 4, ERROR_MESSAGE + 4),
              new BigQueryError(ERROR_REASON + 5, ERROR_LOCATION + 5, ERROR_MESSAGE + 5),
              new BigQueryError(ERROR_REASON + 6, ERROR_LOCATION + 6, ERROR_MESSAGE + 6)),
          Arrays.asList(
              new BigQueryError(ERROR_REASON + 7, ERROR_LOCATION + 7, ERROR_MESSAGE + 7),
              new BigQueryError(ERROR_REASON + 8, ERROR_LOCATION + 8, ERROR_MESSAGE + 8),
              new BigQueryError(ERROR_REASON + 9, ERROR_LOCATION + 9, ERROR_MESSAGE + 9)));

  public static final List<String> ERROR_MESSAGES =
      ERRORS.stream().map(e -> e.getMessage()).collect(Collectors.toList());
  public static final List<List<String>> ERROR_MESSAGE_LISTS =
      ERROR_LIST
          .stream()
          .map(l -> l.stream().map(e -> e.getMessage()).collect(Collectors.toList()))
          .collect(Collectors.toList());

  public static final Map<String, JobId> QUERIES_AND_IDS =
      MapBuilder.getImmutableMapFromLists(TEST_QUERIES, JOB_IDS);

  public static final Map<String, BigQueryError> QUERIES_AND_ERRORS =
      MapBuilder.getImmutableMapFromLists(TEST_QUERIES, ERRORS);

  public static Map<String, List<String>> QUERIES_AND_ERROR_MESSAGE_LISTS =
      MapBuilder.getImmutableMapFromLists(TEST_QUERIES, ERROR_MESSAGE_LISTS);

  public static final String WORKLOAD_NAME = "WORKLOAD_NAME";
  public static final String PROJECT_ID = "PROJECT_ID";

  public static final int CONCURRENCY_LEVEL_BELOW_LIMIT = 25;
  public static final int CONCURRENCY_LEVEL_ABOVE_LIMIT = 100;
  public static final int CONCURRENCY_LEVEL_LIMIT = 50;

  public static final long SUCCESSFUL_QUERY_RUNTIME = 9000L;
  public static final long FAILING_QUERY_RUNTIME = 900L;

  public static final long SUCCESSFUL_QUERY_WALLTIME = 10 * 1000L;
  public static final long FAILING_QUERY_WALLTIME = 1000L;
  public static final long EXCEPTION_QUERY_WALLTIME = 100L;

  public static final long SUCCESSFUL_WORKLOAD_RUNTIME_MULTIPLE_QUERIES =
      SUCCESSFUL_QUERY_RUNTIME * TEST_QUERIES.size();
  public static final long FAILING_WORKLOAD_RUNTIME_MULTIPLE_QUERIES =
      FAILING_QUERY_RUNTIME * TEST_QUERIES.size();

  public static final long SUCCESSFUL_WORKLOAD_WALLTIME = 30 * 1000L;
  public static final long FAILING_WORKLOAD_WALLTIME = 3000L;

  public static final long NO_RUNTIME = 0L;
  public static final long NO_WALLTIME = 0L;

  public static final long NO_CREATION_TIME = 0L;
  public static final long CREATION_TIME = 1000L;

  public static final long SUCCESSFUL_START_TIME = 1000L;
  public static final long SUCCESSFUL_END_TIME = 10 * 1000L;

  public static final long FAILING_START_TIME = 100L;
  public static final long FAILING_END_TIME = 1000L;
}
