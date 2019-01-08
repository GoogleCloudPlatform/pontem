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
package com.google.cloud.pontem.benchmark.runners.callables;

import com.google.cloud.pontem.benchmark.runners.SerialQueryRunner;
import com.google.cloud.pontem.config.WorkloadSettings;
import com.google.cloud.pontem.model.QueryResult;
import com.google.cloud.pontem.model.Status;
import com.google.cloud.pontem.model.WorkloadResult;
import com.google.common.base.Stopwatch;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/** A {@link Callable} that executes a Workload */
public class WorkloadRunnerCallable implements Callable<WorkloadResult> {

  private final SerialQueryRunner serialQueryRunner;
  private final Stopwatch stopwatch;
  private final int concurrencyLevel;
  private final WorkloadSettings workload;

  public WorkloadRunnerCallable(
      SerialQueryRunner serialQueryRunner,
      Stopwatch stopwatch,
      final WorkloadSettings workload,
      final int concurrencyLevel) {
    this.serialQueryRunner = serialQueryRunner;
    this.stopwatch = stopwatch;
    this.workload = workload;
    this.concurrencyLevel = concurrencyLevel;
  }

  @Override
  public WorkloadResult call() {
    stopwatch.start();
    List<QueryResult> queryResults = serialQueryRunner.run(workload.getQueries());
    stopwatch.stop();

    long runtime = 0L;
    for (QueryResult queryResult : queryResults) {
      runtime += queryResult.getRuntime();
    }

    long wallTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    stopwatch.reset();

    return WorkloadResult.newBuilder()
        .setWorkloadName(workload.getName())
        .setProjectId(workload.getProjectId())
        .setConcurrencyLevel(concurrencyLevel)
        .setStatus(Status.SUCCESS)
        .setError("")
        .setRunTime(runtime)
        .setWallTime(wallTime)
        .setQueryResults(queryResults)
        .build();
  }
}
