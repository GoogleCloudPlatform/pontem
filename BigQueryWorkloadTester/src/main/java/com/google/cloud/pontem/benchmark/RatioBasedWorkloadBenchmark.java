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
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@SuppressWarnings("checkstyle:SummaryJavadoc")
/** @see Benchmark */
public final class RatioBasedWorkloadBenchmark implements Benchmark {

  private static final Logger logger =
      Logger.getLogger(RatioBasedWorkloadBenchmark.class.getName());

  private final ConcurrentWorkloadRunnerFactory runnerFactory;
  private final List<Double> benchmarkRatios;

  public RatioBasedWorkloadBenchmark(
      ConcurrentWorkloadRunnerFactory runnerFactory, List<Double> benchmarkRatios) {
    this.runnerFactory = runnerFactory;
    this.benchmarkRatios = benchmarkRatios;
  }

  /**
   * Executes the same Workload Benckmark at varying percentages of the provided Concurrency Level
   * so as to compare performance deltas between different Concurrency Levels.
   *
   * @param workload the workload to execute
   * @param concurrencyLevel the number of times a Workload will be executed in parallel
   */
  @Override
  public List<WorkloadResult> run(final WorkloadSettings workload, final int concurrencyLevel) {
    logger.info("Executing Ratio Based Benchmark for Workload: " + workload.getName());

    Preconditions.checkArgument(benchmarkRatios != null, "No concurrency ratios provided!");
    Preconditions.checkArgument(benchmarkRatios.size() > 0, "No concurrency ratios provided!");

    List<WorkloadResult> workloadResults = new ArrayList<>();
    Map<Integer, Double> concurrencyLevelAndPercentages = new LinkedHashMap<>();

    for (double percentage : benchmarkRatios) {
      int concurrencyLevelForPercentage = calculateConcurrencyLevel(concurrencyLevel, percentage);
      concurrencyLevelForPercentage =
          runnerFactory.getConcurrencyLimit(concurrencyLevelForPercentage);

      ConcurrentWorkloadRunner concurrentWorkloadRunner =
          runnerFactory.getConcurrentWorkloadRunner(workload, concurrencyLevelForPercentage);

      if (concurrencyLevelAndPercentages.containsKey(concurrencyLevelForPercentage)) {
        // Given that we are working with integer math when the requested concurrency level is too
        // small we can have repeated concurrency levels so, we ignore these repeated levels.
        // For example: 1% of 4 = 1, 10% of 4 = 1, 25% of 4 = 1
        //
        // On the other side of the spectrum we ignore further computations once the concurrency
        // limit has been reached.
        continue;
      }

      List<WorkloadResult> resultsForPercentage =
          concurrentWorkloadRunner.run(workload, concurrencyLevelForPercentage);

      workloadResults.addAll(resultsForPercentage);
      concurrencyLevelAndPercentages.put(concurrencyLevelForPercentage, percentage);
    }

    return workloadResults;
  }

  private int calculateConcurrencyLevel(final int number, final double percentage) {
    return (int) Math.ceil(number * percentage);
  }
}
