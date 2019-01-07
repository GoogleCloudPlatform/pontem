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
package com.google.cloud.pontem;

import com.google.cloud.pontem.auth.BigQueryCredentialManager;
import com.google.cloud.pontem.benchmark.Benchmark;
import com.google.cloud.pontem.benchmark.RatioBasedWorkloadBenchmark;
import com.google.cloud.pontem.benchmark.WorkloadBenchmark;
import com.google.cloud.pontem.benchmark.backends.BigQueryBackend;
import com.google.cloud.pontem.benchmark.backends.BigQueryBackendFactory;
import com.google.cloud.pontem.benchmark.runners.ConcurrentWorkloadRunnerFactory;
import com.google.cloud.pontem.config.Configuration;
import com.google.cloud.pontem.config.WorkloadSettings;
import com.google.cloud.pontem.model.WorkloadResult;
import com.google.cloud.pontem.result.JsonResultProcessor;
import com.google.cloud.pontem.result.JsonResultProcessorFactory;
import com.google.common.base.Preconditions;
import java.io.File;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Tool to Benchmark BigQuery Workloads */
public final class BigQueryWorkloadTester {

  private static final Logger logger = Logger.getLogger(BigQueryWorkloadTester.class.getName());

  /** Main entry point for benchmark. Sets up the object graph and kicks-off execution. */
  public static void main(String[] args) {
    Configuration.loadConfig("config.yaml");
    Configuration config = Configuration.getInstance();
    BigQueryCredentialManager bigQueryCredentialManager = new BigQueryCredentialManager();

    logger.info("Starting execution");
    try {
      for (WorkloadSettings workload : config.getWorkloads()) {
        BigQueryBackend bigQueryBackend =
            BigQueryBackendFactory.get(bigQueryCredentialManager, workload);
        ConcurrentWorkloadRunnerFactory runnerFactory =
            new ConcurrentWorkloadRunnerFactory(bigQueryBackend);
        Benchmark benchmark = getBenchmark(config, runnerFactory);

        int concurrencyLevel = config.getConcurrencyLevel();
        Preconditions.checkArgument(
            concurrencyLevel > 0, "Concurrency Level must be higher than 0!");
        List<WorkloadResult> workloadResults = benchmark.run(workload, concurrencyLevel);

        logger.info("Finished bechmarking phase, processing results");
        String outputPath =
            config.getOutputFileFolder() + File.separator + workload.getOutputFileName();
        JsonResultProcessor jsonResultProcessor = JsonResultProcessorFactory.get();
        jsonResultProcessor.run(outputPath, workloadResults);
      }
    } catch (Throwable t) {
      logger.log(Level.SEVERE, "Caught Exception while executing the Workload Benchmark: ", t);
    }

    logger.info("Finished Workload Tester execution");
  }

  private static Benchmark getBenchmark(
      final Configuration config, final ConcurrentWorkloadRunnerFactory runnerFactory) {
    Benchmark benchmark;
    if (config.isRatioBasedBenchmark()) {
      benchmark = new RatioBasedWorkloadBenchmark(runnerFactory);
    } else {
      benchmark = new WorkloadBenchmark(runnerFactory);
    }

    return benchmark;
  }
}
