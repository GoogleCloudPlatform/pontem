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

BigQuery Workload Tester uses Gradle. In the Cloud Shell, you can run the following
to build the Workload Tester:

```bash
sudo apt-get -y install gradle
git clone https://github.com/GoogleCloudPlatform/pontem.git
cd pontem/BigQueryWorkloadTester
gradle clean :BigQueryWorkloadTester:build
```

## Configuration

Before executing BigQuery Workload Tester you will need to configure your own
Workflows inside `src/main/resources/config.yaml`.

A complete sample config file has been provided to demonstrate how the configuration
might be setup `src/main/resources/sample_config.yaml`.

But you can create a simple workload configuration as follows (be sure to set your project ID):

```bash
export PROJECT_ID=<my-project-id>

mkdir src/main/resources/queries

cat <<EOF>src/main/resources/queries/query1.sql
SELECT 42
EOF

cat <<EOF>src/main/resources/queries/query2.sql
SELECT count(*) FROM UNNEST(["apple", "pear", "orange"]) as x
EOF

cat <<EOF>src/main/resources/config.yaml
concurrencyLevel: 1
isRatioBasedBenchmark: true
benchmarkRatios: [0.01, 0.1, 0.25, 0.5, 1.0, 1.5, 2.0]
outputFileFolder: workload_results/
workloads:
- name: "Simple and Text Queries"
  projectId: $PROJECT_ID
  queryFiles:
    - queries/query1.sql
    - queries/query2.sql
  outputFileName: simple_and_text_queries.json
EOF
```

This will create a couple of simple queries to execute.

## Running the BigQuery Workload Tester

```bash
mkdir -p workload_results
gradle clean :BigQueryWorkloadTester:run
```

If your output looks something like this, with no errors, it worked:

```bash
$ gradle clean :BigQueryWorkloadTester:run
:BigQueryWorkloadTester:clean
:BigQueryWorkloadTester:compileJava
Note: Some input files use unchecked or unsafe operations.
Note: Recompile with -Xlint:unchecked for details.
:BigQueryWorkloadTester:processResources
:BigQueryWorkloadTester:classes
:BigQueryWorkloadTester:run
Feb 11, 2019 5:31:11 PM com.google.cloud.pontem.BigQueryWorkloadTester main
INFO: Welcome to BigQuery Workload Tester!
Feb 11, 2019 5:31:11 PM com.google.cloud.pontem.BigQueryWorkloadTester main
INFO: Loading config
Feb 11, 2019 5:31:11 PM com.google.cloud.pontem.BigQueryWorkloadTester main
INFO: Starting execution
Feb 11, 2019 5:31:11 PM com.google.cloud.pontem.benchmark.RatioBasedWorkloadBenchmark run
INFO: Executing Ratio Based Benchmark for Workload: Simple and Text Queries
Feb 11, 2019 5:31:11 PM com.google.cloud.pontem.benchmark.runners.ConcurrentWorkloadRunner run
INFO: Executing Workload with Concurrency Level: 1
Feb 11, 2019 5:31:14 PM com.google.cloud.pontem.benchmark.runners.ConcurrentWorkloadRunner run
INFO: Executing Workload with Concurrency Level: 2
Feb 11, 2019 5:31:16 PM com.google.cloud.pontem.BigQueryWorkloadTester main
INFO: Finished bechmarking phase, processing results
Feb 11, 2019 5:31:16 PM com.google.cloud.pontem.BigQueryWorkloadTester main
INFO: Finished Workload Tester execution

BUILD SUCCESSFUL

Total time: 19.024 secs

This build could be faster, please consider using the Gradle Daemon: https://docs.gradle.org/2.12/userguide/gradle_daemon.html
```

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
Concurrency Level but also at different ratios of the requested Concurrency
Levels. This way, you can easily determine whether there are performance
variations in your Workload at different Concurrency Levels.

You can define which ratios of the Concurrency Level to test by changing the
**benchmarkRatios** array in the config. We have defined a set of default ratios
that correspond to the following percentages: **1%, 10%, 25%, 50%, 100% ,150%
and 200%**

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

