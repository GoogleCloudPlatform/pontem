# Pontem - BigQuery Workload Tester

## Overview

**BigQuery Workload Tester** is a simple tool to help BigQuery users
**benchmark** their **Concurrent BigQuery Workloads**.

We define a **BigQuery Workload** as a **list of queries that will be run
serially** against BigQuery. And a **Concurrent BigQuery Workload** as a
BigQuery Workload that is **run many times in parallel** but, all the queries
contained are run in a serial manner.

The **benchmark results** will be written to a JSON file and will track the
**execution status, run time and wall time** of each **Query** as well as the
**execution status, run time and wall time** of each **Workload**.

## Building and Testing the BigQuery Workload Tester

BigQuery Workload Tester uses Gradle so building is as easy as:

        $ gradle clean :BigQueryWorkloadTester:build

And testing: $ gradle clean :BigQueryWorkloadTester:build

## Config

Before executing BigQuery Workload Tester you will need to configure your own
Workflows inside:

        src/main/resources/config.yaml

A sample config file has been provided to demonstrate how the configuration
might be setup:

        src/main/resources.sample_config.yaml

## Running the BigQuery Workload Tester

Once you've configured BigQuery Workload Tester you can run it by invoking:

        $ gradle clean :BigQueryWorkloadTester:run

### Concurrency Level

The **Concurrency Level** is the number of times a Workload will be run in
parallel during the benchmark.

### Benchmark modes

BigQuery Workload Tester supports two Benchmarking modes:

*   Normal Benchmark
*   Ratio Based Benchmark

A **Ratio Based Benchmark** will run the Workload Benchmark at your requested
Concurrency Level but also at different percentage of the requested Concurrency
Levels. This way, you can easily determine whether there are performance
variations in your Workload at different Concurrency Levels.

The current percentages applied are: **1%, 10%, 25%, 50%, 100% ,150% and 200%**

### Output

The Workload Results are written to a JSON file that you define in the config
file.

## Support

While this is not an officially supported Google product, we encourage you to
[file issues](https://github.com/GoogleCloudPlatform/pontem/issues/new) for
feature requests and bugs.
