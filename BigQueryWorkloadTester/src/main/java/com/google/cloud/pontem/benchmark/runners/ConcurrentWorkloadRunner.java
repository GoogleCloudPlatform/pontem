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

import com.google.cloud.pontem.config.WorkloadSettings;
import com.google.cloud.pontem.model.Status;
import com.google.cloud.pontem.model.WorkloadResult;
import com.google.common.base.Stopwatch;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Runs the provided Workload in a parallel manner based on the concurrency level provided. */
public class ConcurrentWorkloadRunner<T> {
  private static final Logger logger = Logger.getLogger(ConcurrentWorkloadRunner.class.getName());

  private final ExecutorService executorService;
  private final List<Callable<WorkloadResult>> callables;
  private final Stopwatch stopwatch;

  /**
   * Constructs an instance of ConcurrentWorkloadRunner.
   * @param executorService an ExecutorService
   * @param callables a List of Callables returning WorkloadResult
   * @param stopwatch a Stopwatch
   */
  public ConcurrentWorkloadRunner(
      ExecutorService executorService,
      List<Callable<WorkloadResult>> callables,
      Stopwatch stopwatch) {
    this.executorService = executorService;
    this.callables = callables;
    this.stopwatch = stopwatch;
  }

  /**
   * Runs the provided Workload in a parallel manner based on the concurrency level provided while
   * measuring performance results.
   *
   * @param workload the workload to execute
   * @param concurrencyLevel the number of times a Workload will be executed in parallel
   * @return a WorkloadResult list containing execution and performance metrics.
   */
  public List<WorkloadResult> run(final WorkloadSettings workload, final int concurrencyLevel) {
    logger.info("Executing Workload with Concurrency Level: " + concurrencyLevel);

    List<Future<WorkloadResult>> futures;
    try {
      futures = executorService.invokeAll(callables);
    } catch (InterruptedException e) {
      logger.log(Level.SEVERE, "Caught exception while invoking executor all result failed: ", e);

      // Short-circuit
      return Arrays.asList(getExecutorExceptionResult(workload, concurrencyLevel, e));
    }

    waitUntilDone(futures);

    List<WorkloadResult> results = new ArrayList<>(futures.size());
    for (Future<WorkloadResult> future : futures) {
      try {
        results.add(future.get());
      } catch (ExecutionException | InterruptedException e) {
        logger.log(Level.SEVERE, "Caught exception while executing workflow: ", e);

        results.add(getExecutorExceptionResult(workload, concurrencyLevel, e));
      } finally {
        executorService.shutdown();
      }
    }

    return results;
  }

  private void waitUntilDone(final List<Future<WorkloadResult>> futures) {
    logger.fine("Waiting for all results");

    List<Future<WorkloadResult>> remaining = futures;
    while (remaining.size() > 0) {
      List<Future<WorkloadResult>> remainingAfterLoop = new ArrayList<>();
      for (Future<WorkloadResult> future : remaining) {
        if (!future.isDone()) {
          remainingAfterLoop.add(future);
        }
      }

      remaining = remainingAfterLoop;
    }
  }

  private WorkloadResult getExecutorExceptionResult(
      final WorkloadSettings workload, final int concurrencyLevel, final Exception e) {
    return WorkloadResult.newBuilder()
        .setWorkloadName(workload.getName())
        .setProjectId(workload.getProjectId())
        .setConcurrencyLevel(concurrencyLevel)
        .setStatus(Status.EXCEPTION)
        .setError(e.getMessage())
        .setRunTime(0L)
        .setWallTime(0L)
        .setQueryResults(new ArrayList<>())
        .build();
  }
}
