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
import com.google.cloud.spanner.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;

/**
 * Restore a database from Avro.
 *
 * <p>A sample run:
 *
 * <pre>
 * mvn compile exec:java \
 *   -Dexec.mainClass=com.google.cloud.pontem.AvroCloudSpannerDatabaseRestore \
 *   -Dexec.args="--runner=DataflowRunner \
 *                --project=my-cloud-spanner-project \
 *                --gcpTempLocation=gs://my-cloud-spanner-project/tmp \
 *                --outputSpannerInstanceId=my-cloud-spanner-project-db \
 *                --outputSpannerDatabaseId=words2 \
 *                --inputFolder=gs://my-cloud-spanner-project/backups/latest \
 *                --projectId=my-cloud-spanner-project" \
 *   -Pdataflow-runner
 * </pre>
 */
public class AvroCloudSpannerDatabaseRestore extends BaseCloudSpannerDatabaseRestore {
  public static void main(String[] args) throws Exception {
    // STEP 1a: Parse Pipeline options
    final BaseCloudSpannerRestoreOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(BaseCloudSpannerRestoreOptions.class);
    final GcsUtil gcsUtil = new GcsUtil();
    final SpannerUtil spannerUtil = new SpannerUtil();
    final Pipeline p = Pipeline.create(options);

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
    final ImmutableList<String> databaseDdl =
        spannerUtil.queryDatabaseDdl(
            options.getProjectId(),
            options.getOutputSpannerInstanceId(),
            options.getOutputSpannerDatabaseId());
    final ImmutableMap<String, String> mapOfTableNameToTableDdl =
        SpannerUtil.convertEntireDtaabaseDdlIntoTableSpecificDdl(databaseDdl);

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
              spannerConfig,
              mapOfTableNameToTableDdl);
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
    final ImmutableMap<String, String> mapOfTableNameToTableDdl;
    final GcsUtil gcsUtil;

    PerformRestoreOfRootTableAndItsChildren(
        String rootTableName,
        LinkedList<String> rootAndChildrenTables,
        String[] args,
        SpannerConfig spannerConfig,
        ImmutableMap<String, String> mapOfTableNameToTableDdl) {
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
      this.mapOfTableNameToTableDdl = mapOfTableNameToTableDdl;
      this.gcsUtil = new GcsUtil();
    }

    @Override
    public void run() {
      final String baseJobName = this.options.getJobName();
      // Due to parent-child table relationship dependency, each root table (i.e., table with no
      // parents) will run its own pipeline. From this root, all its children will run sequentially
      // in the same pipeline.
      for (String childTableName : this.rootAndChildrenTables) {

        String schemaAsString = "";
        try {
          schemaAsString =
              gcsUtil.getContentsOfFileFromGcs(
                  this.options.getProjectId(),
                  GcsUtil.getGcsBucketNameFromDatabaseBackupLocation(this.options.getInputFolder()),
                  GcsUtil.getGcsFolderPathFromDatabaseBackupLocation(this.options.getInputFolder()),
                  AvroUtil.getAvroSchemaFileLocation(childTableName));
        } catch (Exception e) {
          LOG.warning("Serious error\n" + e.toString());
          throw new RuntimeException(e);
        }
        Schema schema = new Schema.Parser().parse(schemaAsString);

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

        TableInformation tableInformation =
            new TableInformation(this.mapOfTableNameToTableDdl.get(childTableName));
        ImmutableMap<String, Type> mapOfColumnNamesToSpannerTypes =
            tableInformation.getMapOfColumnNamesToSpannerTypes();

        PCollection<GenericRecord> rows =
            p.apply(
                Util.READ_DATA_TRANSFORM_NODE_NAME
                    + Util.TRANSFORM_NODE_NAME_DELIMITER
                    + childTableName,
                AvroIO.readGenericRecords(schema.toString())
                    .from(options.getInputFolder() + "tables/" + childTableName + "/*.avro"));
        PCollection<Mutation> mutations =
            rows.apply(
                Util.MAP_DATA_TRANSFORM_NODE_NAME
                    + Util.TRANSFORM_NODE_NAME_DELIMITER
                    + childTableName,
                MapElements.via(
                    new FormatGenericRecordAsSpannerMutationFn(
                        childTableName, mapOfColumnNamesToSpannerTypes)));
        mutations.apply(
            Util.WRITE_DATA_TRANSFORM_NODE_NAME
                + Util.TRANSFORM_NODE_NAME_DELIMITER
                + childTableName,
            SpannerIO.write()
                .withSpannerConfig(spannerConfig)
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
