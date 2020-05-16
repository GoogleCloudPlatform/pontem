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

import com.google.cloud.pontem.benchmark.backends.BackendException;
import com.google.cloud.pontem.benchmark.backends.BigQueryBackend;
import com.google.cloud.pontem.model.BigQueryResult;
import com.google.cloud.pontem.model.QueryResult;
import com.google.cloud.pontem.model.Status;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Runs the provided query list against BigQuery in a serial manner. */
public class SerialQueryRunner {
  private static final Logger logger = Logger.getLogger(SerialQueryRunner.class.getName());

  private final BigQueryBackend bigQueryBackend;
  private final Stopwatch stopwatch;

  public SerialQueryRunner(BigQueryBackend bigQueryBackend, Stopwatch stopwatch) {
    this.bigQueryBackend = bigQueryBackend;
    this.stopwatch = stopwatch;
  }

  /**
   * Runs queries serially against the backend returns a list of POJOs with execution metrics.
   *
   * @param queries the queries to run in the backend
   * @return The query result along with their execution metrics.
   */
  public List<QueryResult> run(final List<String> queries) {
    logger.fine("Executing queries serially");
    logger.finest("Queries to execute: " + queries);
    List<QueryResult> queryResults = new ArrayList<>(queries.size());
    for (String query : queries) {
      queryResults.add(executeQuery(query));
    }

    return queryResults;
  }

  private QueryResult executeQuery(final String query) {
    QueryResult queryResult;
    try {
      stopwatch.start();
      BigQueryResult bigQueryResult = bigQueryBackend.executeQuery(query);
      stopwatch.stop();

      long wallTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);

      queryResult = getQueryResult(query, bigQueryResult, wallTime);
    } catch (BackendException e) {
      logger.log(Level.SEVERE, "Caught exception while executing query: ", e);

      stopwatch.stop();
      long wallTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);

      queryResult = getExceptionQueryResult(query, wallTime, e.getMessage());
    } finally {
      stopwatch.reset();
    }

    return queryResult;
  }

  private QueryResult getQueryResult(
      final String query, final BigQueryResult bigQueryResult, final long wallTime) {
    return QueryResult.newBuilder()
        .setId(bigQueryResult.getId())
        .setQuery(query)
        .setStatus(bigQueryResult.getStatus())
        .setErrors(bigQueryResult.getErrors())
        .setCreationTime(bigQueryResult.getCreationTime())
        .setRuntime(bigQueryResult.getRuntime())
        .setWallTime(wallTime)
        .build();
  }

  private QueryResult getExceptionQueryResult(
      final String query, final long wallTime, final String exceptionMessage) {
    String message = "";
    if (!Strings.isNullOrEmpty(exceptionMessage)) {
      message = exceptionMessage;
    }

    return QueryResult.newBuilder()
        .setId("")
        .setQuery(query)
        .setStatus(Status.EXCEPTION)
        .addError(message)
        .setCreationTime(0L)
        .setRuntime(0L)
        .setWallTime(wallTime)
        .build();
  }
}
