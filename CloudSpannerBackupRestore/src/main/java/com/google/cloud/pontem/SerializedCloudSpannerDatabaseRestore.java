/*
 * Copyright 2018 Google LLC
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

import com.google.cloud.spanner.Mutation;
import java.io.File;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;

/**
 * Perform a restore from backup of an entire Cloud Spanner database.
 *
 * <p>A sample run:
 *
 * <pre>
 * mvn compile exec:java \
 *   -Dexec.mainClass=com.google.cloud.pontem.SerializedCloudSpannerDatabaseRestore \
 *   -Dexec.args="--runner=DataflowRunner \
 *                --project=my-cloud-spanner-project \
 *                --gcpTempLocation=gs://my-cloud-spanner-project/tmp \
 *                --outputSpannerInstanceId=my-cloud-spanner-project-db \
 *                --outputSpannerDatabaseId=words2 \
 *                --inputFolder=gs://my-cloud-spanner-project/backups/latest \
 *                --projectId=my-cloud-spanner-project" \
 *   -Pdataflow-runner
 * </pre>
 *
 * <p>Another sample run with a smaller Spanner write batch size:
 *
 * <pre>
 * mvn compile exec:java \
 *   -Dexec.mainClass=com.google.cloud.pontem.SerializedCloudSpannerDatabaseRestore \
 *   -Dexec.args="--runner=DataflowRunner \
 *                --project=my-cloud-spanner-project \
 *                --gcpTempLocation=gs://my-cloud-spanner-project/tmp \
 *                --outputSpannerInstanceId=my-cloud-spanner-project-db \
 *                --outputSpannerDatabaseId=words2 \
 *                --inputFolder=gs://my-cloud-spanner-project/today_backup \
 *                --projectId=my-cloud-spanner-project \
 *                --spannerWriteBatchSizeBytes=524288" \
 *   -Pdataflow-runner
 * </pre>
 *
 * <p>Another sample run which only restores a single table:
 *
 * <pre>
 *  mvn compile exec:java \
 *    -Dexec.mainClass=com.google.cloud.pontem.SerializedCloudSpannerDatabaseRestore \
 *    -Dexec.args="--runner=DataflowRunner \
 *    --project=my-cloud-spanner-project \
 *    --gcpTempLocation=gs://my-cloud-spanner-project/tmp \
 *    --outputSpannerInstanceId=my-cloud-spanner-project-db \
 *    --outputSpannerDatabaseId=words2 \
 *    --inputFolder=gs://my-cloud-spanner-project/today_backup \
 *    --projectId=my-cloud-spanner-project \
 *    --tablesToIncludeInRestore=seven_words" \
 *  -Pdataflow-runner
 * </pre>
 *
 * <p>To minimize costs and backup time, you should seek to avoid cross-region network traffic. See
 * https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
 */
public class SerializedCloudSpannerDatabaseRestore extends BaseCloudSpannerDatabaseRestore {

  public static void main(String[] args) throws Exception {
    // STEP 1a: Parse Pipeline options
    final BaseCloudSpannerRestoreOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(BaseCloudSpannerRestoreOptions.class);
    final GcsUtil gcsUtil = new GcsUtil();
    final SpannerUtil spannerUtil = new SpannerUtil();

    // STEP 1b: Setup Spanner configuration
    final SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withHost(StaticValueProvider.of(options.getSpannerHost()))
            .withInstanceId(StaticValueProvider.of(options.getOutputSpannerInstanceId()))
            .withDatabaseId(StaticValueProvider.of(options.getOutputSpannerDatabaseId()));

    // STEP 2: Check whether to re-create database and tables
    if (options.getShouldCreateDatabaseAndTables()) {
      createDatabaseAndTablesFromStoredDdl(
          options.getProjectId(),
          options.getOutputSpannerInstanceId(),
          options.getOutputSpannerDatabaseId(),
          options.getInputFolder(),
          gcsUtil,
          spannerUtil);
    }

    // STEP 3: Query list of tables to restore
    // Since Cloud Spanner has parent-child table relationships
    // https://cloud.google.com/spanner/docs/schema-and-data-model#parent-child_table_relationships
    // and since in these instances, the parent table must be populated with data before
    // the child table can be populated, it is essential that we retrieve the tables
    // in a very specific order.
    final LinkedHashMap<String, LinkedList<String>> mapOfParentToAllChildrenTablesInOrderToFetch =
        queryListOfTablesToRestore(
            options.getInputFolder(),
            options.getInputFolder(),
            options.getTablesToIncludeInRestore(),
            options.getTablesToExcludeFromRestore(),
            gcsUtil);

    if (mapOfParentToAllChildrenTablesInOrderToFetch.size() == 0) {
      LOG.info(
          "Attempting to restore backup for 0 tables."
              + "Input Folder: "
              + options.getInputFolder());
    }

    // STEP 4: Create Pipeline, Read data from disk, Convert to Mutation, and Write to Cloud
    // Spanner.
    // For each root table, create a thread that handles that table and all its children.
    // This allows parallelization of as much of the recovery process as possible, while ensuring
    // that children tables are not restored before their parents.
    ExecutorService threadPool =
        Executors.newFixedThreadPool(mapOfParentToAllChildrenTablesInOrderToFetch.size());
    for (String rootTableName : mapOfParentToAllChildrenTablesInOrderToFetch.keySet()) {
      Runnable insertionTask =
          new PerformRestoreOfRootTableAndItsChildren(
              rootTableName,
              mapOfParentToAllChildrenTablesInOrderToFetch.get(rootTableName),
              args,
              spannerConfig);
      threadPool.execute(insertionTask);
    }
    threadPool.shutdown();
  }

  /**
   * Class to enclose a thread that will restore a root table and all its children in sequential
   * order.
   */
  private static class PerformRestoreOfRootTableAndItsChildren implements Runnable {

    final String rootTableName;
    final LinkedList<String> rootAndChildrenTables;
    final BaseCloudSpannerRestoreOptions options;
    final SpannerConfig spannerConfig;

    PerformRestoreOfRootTableAndItsChildren(
        String rootTableName,
        LinkedList<String> rootAndChildrenTables,
        String[] args,
        SpannerConfig spannerConfig) {
      this.rootTableName = rootTableName;
      this.rootAndChildrenTables = rootAndChildrenTables;

      // STEP 4a: Setup Pipeline options
      this.options =
          PipelineOptionsFactory.fromArgs(args)
              .withValidation()
              .as(BaseCloudSpannerRestoreOptions.class);
      if (!this.options.getInputFolder().endsWith("/")) {
        this.options.setInputFolder(options.getInputFolder() + "/");
      }

      String newUserAgent = Util.USER_AGENT_PREFIX + options.getUserAgent();
      this.options.setUserAgent(newUserAgent);

      this.spannerConfig = spannerConfig;
    }

    @Override
    public void run() {
      final String baseJobName = this.options.getJobName();
      // Due to parent-child table relationship dependency, each root table (i.e., table with no
      // parents) will run its own pipeline. From this root, all its children will run sequentially
      // in the same pipeline.
      for (String childTableName : this.rootAndChildrenTables) {
        Pipeline p = Pipeline.create(this.options);
        // Must make the name for each job unique in order to run the jobs in parallel
        // _ is not allowed in job name, but is allowed in Cloud Spanner table name
        // see https://cloud.google.com/spanner/docs/data-definition-language#create_table
        String jobName = "";
        if (rootTableName.equals(childTableName)) {
          jobName = this.rootTableName.replace('_', '-') + "--" + baseJobName;
        } else {
          jobName =
              this.rootTableName.replace('_', '-')
                  + "--"
                  + childTableName.replace('_', '-')
                  + "--"
                  + baseJobName;
        }
        this.options.setJobName(jobName);

        PCollection<String> rows =
            p.apply(
                Util.READ_DATA_TRANSFORM_NODE_NAME
                    + Util.TRANSFORM_NODE_NAME_DELIMITER
                    + childTableName,
                TextIO.read()
                    .from(
                        this.options.getInputFolder()
                            + "tables"
                            + File.separator
                            + childTableName
                            + File.separator
                            + "*")
                    .withCompressionType(TextIO.CompressionType.GZIP));
        PCollection<Mutation> mutations =
            rows.apply(
                Util.MAP_DATA_TRANSFORM_NODE_NAME
                    + Util.TRANSFORM_NODE_NAME_DELIMITER
                    + childTableName,
                MapElements.via(new FormatStringAsSpannerMutationFn(childTableName)));
        mutations.apply(
            Util.WRITE_DATA_TRANSFORM_NODE_NAME
                + Util.TRANSFORM_NODE_NAME_DELIMITER
                + childTableName,
            SpannerIO.write()
                .withSpannerConfig(this.spannerConfig)
                .withBatchSizeBytes(options.getSpannerWriteBatchSizeBytes()));

        // STEP 4b: Trigger pipeline
        // In order to perform Restore operation as fast as possible, ideally we would not
        // wait until the pipeline finishes (.waitUntilFinish()) before starting another one.
        // However, since there is no way to chain these oeprations, each one must finish before
        // starting the next pipeline. If we do not do this, then it is possible a child pipeline
        // will start before its parent is finished, which will result in an error when data
        // is written to the child that is not present in the parent.
        p.run().waitUntilFinish();
      }
    }
  }
}
