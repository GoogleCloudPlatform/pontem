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
package com.google.cloud.pontem.benchmark;

import com.google.cloud.pontem.benchmark.runners.ConcurrentWorkloadRunner;
import com.google.cloud.pontem.benchmark.runners.ConcurrentWorkloadRunnerFactory;
import com.google.cloud.pontem.config.WorkloadSettings;
import com.google.cloud.pontem.model.WorkloadResult;
import java.util.List;
import java.util.logging.Logger;

/** @see Benchmark */
public class WorkloadBenchmark implements Benchmark {

  private static final Logger logger =
      Logger.getLogger(RatioBasedWorkloadBenchmark.class.getName());

  private final ConcurrentWorkloadRunnerFactory runnerFactory;

  public WorkloadBenchmark(ConcurrentWorkloadRunnerFactory runnerFactory) {
    this.runnerFactory = runnerFactory;
  }

  /**
   * Executes Workload Benchmarks based on the provided settings and concurrency level.
   *
   * @param workload the workload to execute
   * @param concurrencyLevel the number of times a Workload will be executed in parallell
   */
  @Override
  public List<WorkloadResult> run(final WorkloadSettings workload, final int concurrencyLevel) {
    logger.info("Executing Standard Benchmark for Workload: " + workload.getName());

    ConcurrentWorkloadRunner concurrentWorkloadRunner =
        runnerFactory.getConcurrentWorkloadRunner(workload, concurrencyLevel);
    return concurrentWorkloadRunner.run(workload, concurrencyLevel);
  }
}
