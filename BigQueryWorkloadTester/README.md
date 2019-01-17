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

We have set a **hard limit of 50** for the Maximum Concurreny Level in order to
respect [BigQuery Quotas](https://cloud.google.com/bigquery/quotas) and ensure
all your queries will successfully run against BigQuery.

WARNING: If you set a Concurreny Level over the limit it will be truncated to
50.

Please note that **BigQuery's Maximum Concurrency Level is not a hard limit** if
you wish for it to be increased please reach out to Cloud Support or your
Account manager. Once your limit is increased, you can override BigQuery
Workload Tester's hard limit by updating
[ConcurrentWorkloadRunnerFactory.QUERY_CONCURRENCY_LIMIT](https://github.com/GoogleCloudPlatform/pontem/blob/9e7e27a3c03e6da9a0dc77c41e182a6b25693516/BigQueryWorkloadTester/src/main/java/com/google/cloud/pontem/benchmark/runners/ConcurrentWorkloadRunnerFactory.java#L38).

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

The JSON file contains a list of Workloads and their individual Query results.

#### Query Results

Below are the noteworthy fields for a Query Result:

*   **ID:** BigQuery's JobId used for the Query.
*   **Status:** Possible values are SUCCESS, EXCEPTION or ERROR.
    *   EXCEPTION: is used for runtime exceptions thrown by Java
    *   ERROR: Denotes that BigQuery failed to process the provided query, be on
        the lookout for syntax errors in your queries and the like.
*   **Error:** The error or exception message if any.
*   **Walltime:** The time, in milliseconds, that it took for the query to be
    executed since we isued the BigQuery request until we received the response.
    *   Do note that Walltime will include the time for the network roundtrip
        and any scheduling / processing BigQuery may perform before running your
        query.
*   **Runtime:** The processing time, in milliseconds, that BigQuery took to
    execute your query.

#### Workload Results

Below are the noteworthy fields for a Workload Result:

*   **Concurrency Level:** The concurrency level used when executing the
    workflow
*   **Status:** Possible values are SUCCESS or EXCEPTION
*   **Error:** The exception message if any
*   **Walltime:** The time, in milliseconds, that it took for all queries in a
    worload to be executed since we began the Workload execution, until we
    received the Query Results.
    *   Do note that Walltime will include the time for the network roundtrip
        and any scheduling / processing BigQuery may perform before running your
        query.
*   **Runtime:** The sum of Query Result's runtimes.

## Support

While this is not an officially supported Google product, we encourage you to
[file issues](https://github.com/GoogleCloudPlatform/pontem/issues/new) for
feature requests and bugs.

