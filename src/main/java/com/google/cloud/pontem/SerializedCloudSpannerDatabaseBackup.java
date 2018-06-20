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

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.Transaction;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Perform a backup of an entire Cloud Spanner database using serialization.
 *
 * <p>A sample backup:
 *
 * <pre>
 * mvn compile exec:java \
 *   -Dexec.mainClass=com.google.cloud.pontem.SerializedCloudSpannerDatabaseBackup \
 *   -Dexec.args="--runner=DataflowRunner \
 *                --project=my-cloud-spanner-project \
 *                --gcpTempLocation=gs://my-cloud-spanner-project/tmp \
 *                --inputSpannerInstanceId=my-cloud-spanner-project-db \
 *                --inputSpannerDatabaseId=words2 \
 *                --outputFolder=gs://my-cloud-spanner-project/backups/latest \
 *                --projectId=my-cloud-spanner-project" \
 *   -Pdataflow-runner
 * </pre>
 *
 * <p>A sample backup that only includes "Sales" -- meaning only the "Sales" table will be
 * backed-up.
 *
 * <pre>
 * mvn compile exec:java \
 *   -Dexec.mainClass=com.google.cloud.pontem.SerializedCloudSpannerDatabaseBackup \
 *   -Dexec.args="--runner=DataflowRunner \
 *                --project=my-cloud-spanner-project \
 *                --gcpTempLocation=gs://my-cloud-spanner-project/tmp \
 *                --inputSpannerInstanceId=my-cloud-spanner-project-db \
 *                --inputSpannerDatabaseId=words2 \
 *                --outputFolder=gs://my-cloud-spanner-project/backup_today \
 *                --projectId=my-cloud-spanner-project \
 *                --tablesToIncludeInBackup=Sales" \
 *   -Pdataflow-runner
 * </pre>
 *
 * <p>To minimize costs and backup time, you should seek to avoid cross-region network traffic. See
 * https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
 */
public class SerializedCloudSpannerDatabaseBackup extends BaseCloudSpannerDatabaseBackup {

  public static void main(String[] args) throws Exception {
    // STEP 1: Setup pipeline and Spanner configuration
    final BaseCloudSpannerBackupOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(BaseCloudSpannerBackupOptions.class);

    final String newUserAgent = Util.USER_AGENT_PREFIX + options.getUserAgent();
    options.setUserAgent(newUserAgent);

    final Pipeline p = Pipeline.create(options);

    final SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withHost(StaticValueProvider.of(options.getSpannerHost()))
            .withInstanceId(StaticValueProvider.of(options.getInputSpannerInstanceId()))
            .withDatabaseId(StaticValueProvider.of(options.getInputSpannerDatabaseId()));

    final GcsUtil gcsUtil = new GcsUtil();
    final SpannerUtil spannerUtil = new SpannerUtil();

    // STEP 2a: Check proposed backup location
    if (!options.getShouldOverwriteGcsFileBackup()) {
      int numBlobs =
          gcsUtil.getNumGcsBlobsInGcsFilePath(
              options.getProjectId(),
              GcsUtil.getGcsBucketNameFromDatabaseBackupLocation(options.getOutputFolder()),
              GcsUtil.getGcsFolderPathFromDatabaseBackupLocation(options.getOutputFolder()));
      if (numBlobs > 0) {
        throw new Exception(
            "Attempts to backup to location "
                + options.getOutputFolder()
                + " failed as it appears there is already content there. You can either adjust "
                + " the --shouldOverwriteGcsFileBackup flag or chose an empty location.");
      }
    }

    final Timestamp timestampForDb = Timestamp.now();
    // STEP 2b: Save DDL to disk
    if (options.getShouldBackupDatabaseDdl()) {
      final ImmutableList<String> databaseDdl =
          spannerUtil.queryDatabaseDdl(
              options.getProjectId(),
              options.getInputSpannerInstanceId(),
              options.getInputSpannerDatabaseId());
      String databaseDdlAsString = SpannerUtil.convertDdlListIntoRawText(databaseDdl);
      gcsUtil.writeContentsToGcs(
          databaseDdlAsString,
          options.getProjectId(),
          GcsUtil.getGcsBucketNameFromDatabaseBackupLocation(options.getOutputFolder()),
          GcsUtil.getGcsFolderPathFromDatabaseBackupLocation(options.getOutputFolder()),
          SpannerUtil.FILE_PATH_FOR_DATABASE_DDL);
    }

    // STEP 2c: Query list of tables in database
    // Since Beam does not have the ability to materialize query results before the rest of
    // the pipeline runs, we must get the table list from a query using the Cloud Spanner API.
    final ImmutableSet<String> allTableNames =
        queryListOfAllTablesInDatabase(
            options.getProjectId(),
            options.getInputSpannerInstanceId(),
            options.getInputSpannerDatabaseId(),
            spannerUtil,
            timestampForDb);
    final ImmutableList<String> tableNamesToBackup =
        getListOfTablesToBackup(
            allTableNames,
            options.getTablesToIncludeInBackup(),
            options.getTablesToExcludeFromBackup());
    LOG.info("Final list of tables to backup includes (" + tableNamesToBackup.size() + ") tables.");

    constructPipeline(p, options, spannerConfig, gcsUtil, tableNamesToBackup, timestampForDb);

    // STEP 7: Trigger pipeline
    PipelineResult pipelineResult = p.run();
    pipelineResult.waitUntilFinish();
  }

  public static void constructPipeline(
      Pipeline p,
      BaseCloudSpannerBackupOptions options,
      SpannerConfig spannerConfig,
      GcsUtil gcsUtil,
      ImmutableList<String> tableNamesToBackup,
      Timestamp timestampForDb)
      throws Exception {

    // STEP 3: Setup single transaction for all queries of Spanner.
    // This ensures a consistent view of the data.
    final PCollectionView<Transaction> transaction =
        p.apply(
            SpannerIO.createTransaction()
                .withSpannerConfig(spannerConfig)
                .withTimestampBound(TimestampBound.ofReadTimestamp(timestampForDb)));
    LOG.info("Created consistent transaction for querying Cloud Spanner instance");

    // STEP 4: Query list of tables at read-time. This is done to ensure that the table
    // list we backup is the list present at the snapshot we created above. Otherwise,
    // it is possible a table could be missed or added to the backup OR parent-child table
    // relationships could change during run-time.

    final PCollection<Struct> collectionOfTableNamesAsStruct =
        p.apply(
            "Read Table List To Backup",
            SpannerIO.read()
                .withSpannerConfig(spannerConfig)
                .withQuery(getSqlQueryForTablesToBackup(tableNamesToBackup))
                .withTransaction(transaction)
                .withBatching(false));

    final PCollection<String> collectionOfTableNames =
        collectionOfTableNamesAsStruct.apply(
            "Map Table Names", MapElements.via(new FormatSpannerTablesListStructAsTextFn()));

    // Contents can be safely written to one file as contents
    // would not be more than a hundred rows in normal cases
    // and a few thousand rows in extreme cases
    collectionOfTableNames.apply(
        "Write Table Names",
        TextIO.write()
            .to(
                GcsUtil.getFormattedOutputPath(options.getOutputFolder())
                    + Util.FILE_PATH_FOR_DATABASE_TABLE_NAMES)
            .withoutSharding());
    LOG.info("Read list of table names and wrote them to disk");

    // STEP 5: Read underlying Spanner data from database
    // Using a for-loop since query results containing table names cannot be materialized
    // halfway through the pipeline.
    final Map<String, PCollection<Struct>> mapOfTableNamesToPCollections = Maps.newHashMap();
    for (String tableName : tableNamesToBackup) {
      mapOfTableNamesToPCollections.put(
          tableName,
          p.apply(
              Util.READ_DATA_TRANSFORM_NODE_NAME + Util.TRANSFORM_NODE_NAME_DELIMITER + tableName,
              SpannerIO.read()
                  .withSpannerConfig(spannerConfig)
                  .withQuery("SELECT * FROM " + tableName)
                  .withTransaction(transaction)));
    }

    // STEP 6: Write underlying Spanner data to disk
    for (Map.Entry<String, PCollection<Struct>> entry : mapOfTableNamesToPCollections.entrySet()) {
      entry
          .getValue()
          .apply(
              Util.MAP_DATA_TRANSFORM_NODE_NAME
                  + Util.TRANSFORM_NODE_NAME_DELIMITER
                  + entry.getKey(),
              MapElements.via(new FormatGenericSpannerStructAsTextFn(entry.getKey())))
          .apply(
              Util.WRITE_DATA_TRANSFORM_NODE_NAME
                  + Util.TRANSFORM_NODE_NAME_DELIMITER
                  + entry.getKey(),
              TextIO.write()
                  .to(
                      GcsUtil.getFormattedOutputPath(options.getOutputFolder())
                          + "tables/"
                          + entry.getKey()
                          + "/")
                  .withWritableByteChannelFactory(FileBasedSink.CompressionType.GZIP));
    }
  }
}
