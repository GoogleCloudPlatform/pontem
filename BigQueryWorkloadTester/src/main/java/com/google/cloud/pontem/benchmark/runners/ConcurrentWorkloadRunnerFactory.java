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

import com.google.cloud.pontem.benchmark.backends.BigQueryBackend;
import com.google.cloud.pontem.benchmark.runners.callables.WorkloadRunnerCallable;
import com.google.cloud.pontem.config.WorkloadSettings;
import com.google.cloud.pontem.model.WorkloadResult;
import com.google.common.base.Stopwatch;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/** A factory for ConcurrentWorkloadRunner objects. */
public class ConcurrentWorkloadRunnerFactory {
  private static final Logger logger =
      Logger.getLogger(ConcurrentWorkloadRunnerFactory.class.getName());

  // The max number of Queries that can be run against the backend at once.
  //
  // See: https://cloud.google.com/bigquery/quotas
  private static final int QUERY_CONCURRENCY_LIMIT = 50;

  private final BigQueryBackend bigQueryBackend;

  public ConcurrentWorkloadRunnerFactory(BigQueryBackend bigQueryBackend) {
    this.bigQueryBackend = bigQueryBackend;
  }

  /**
   * Ensures that the ConcurrentWorkloadRunner and it's dependencies are properly built and
   * configured.
   *
   * @param workload the workload to execute
   * @param concurrencyLevel the number of times a Workload will be executed in parallel
   * @return a properly configuredConcurrentWorkloadRunner
   */
  public ConcurrentWorkloadRunner getConcurrentWorkloadRunner(
      final WorkloadSettings workload, final int concurrencyLevel) {
    logger.fine("Building ConcurrentWorkloadRunner.");

    int concurrencyLimit = getConcurrencyLimit(concurrencyLevel);

    ExecutorService executorService = Executors.newFixedThreadPool(concurrencyLimit);
    List<Callable<WorkloadResult>> callables = new ArrayList<>(concurrencyLimit);

    for (int i = 0; i < concurrencyLimit; i++) {
      SerialQueryRunner serialQueryRunner =
          new SerialQueryRunner(bigQueryBackend, Stopwatch.createUnstarted());
      WorkloadRunnerCallable callable =
          new WorkloadRunnerCallable(
              serialQueryRunner, Stopwatch.createUnstarted(), workload, concurrencyLimit);
      callables.add(callable);
    }

    return new ConcurrentWorkloadRunner(executorService, callables, Stopwatch.createUnstarted());
  }

  /**
   * Verifies that the provieded Concurrency Level does not exceed our Concurrency Limit.
   *
   * @param concurrencyLevel the number of times a Workload will be executed in parallel
   * @return the same Concurrency Level or, the Concurrency Limit if the provided Level exceeds it.
   */
  public int getConcurrencyLimit(final int concurrencyLevel) {
    int concurrencyLimit = concurrencyLevel;

    if (concurrencyLevel > QUERY_CONCURRENCY_LIMIT) {
      logger.warning(
          "Requested concurrency level:"
              + concurrencyLevel
              + ", exceeds the maximum concurrency limit: "
              + QUERY_CONCURRENCY_LIMIT
              + ". Using the concurrency limit instead of the user provided one.");
      concurrencyLimit = QUERY_CONCURRENCY_LIMIT;
    }

    return concurrencyLimit;
  }
}
