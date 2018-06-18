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
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
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
 * <p>A sample backup that queries and saves the table row counts while not saving table schemas:
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
 *                --shouldQueryTableRowCounts=true \
 *                --shouldQueryTableSchema=false" \
 *   -Pdataflow-runner
 * </pre>
 *
 * <p>A sample backup that queries and saves the table row counts while not saving table schemas and
 * also exclude table name "Sales" -- meaning all tables will be backed-up except "Sales":
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
 *                --shouldQueryTableRowCounts=true \
 *                --shouldQueryTableSchema=false \
 *                --tablesToExcludeFromBackup=Sales" \
 *   -Pdataflow-runner
 * </pre>
 *
 * <p>A sample backup that queries and saves the table row counts while not saving table schemas and
 * only includes "Sales" -- meaning only the "Sales" table will be backed-up.
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
 *                --shouldQueryTableRowCounts=true \
 *                --shouldQueryTableSchema=false \
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

    final Util util = new Util();
    constructPipeline(p, options, spannerConfig, util);

    // STEP 9: Trigger pipeline
    PipelineResult pipelineResult = p.run();
    pipelineResult.waitUntilFinish();
  }

  public static void constructPipeline(
      Pipeline p, BaseCloudSpannerBackupOptions options, SpannerConfig spannerConfig, Util util)
      throws Exception {

    // STEP 2a: Check proposed backup location
    if (!options.getShouldOverwriteGcsFileBackup()) {
      int numBlobs =
          util.getNumGcsBlobsInGcsFilePath(
              options.getProjectId(),
              Util.getGcsBucketNameFromDatabaseBackupLocation(options.getOutputFolder()),
              Util.getGcsFolderPathFromDatabaseBackupLocation(options.getOutputFolder()));
      if (numBlobs > 0) {
        throw new Exception(
            "Attempts to backup to location "
                + options.getOutputFolder()
                + " failed as it appears there is already content there. You can either adjust "
                + " the --shouldOverwriteGcsFileBackup flag or chose an empty location.");
      }
    }

    // STEP 2b: Save DDL to disk
    if (options.getShouldBackupDatabaseDdl()) {
      final ImmutableList<String> databaseDdl =
          util.queryDatabaseDdl(
              options.getProjectId(),
              options.getInputSpannerInstanceId(),
              options.getInputSpannerDatabaseId());
      String databaseDdlAsString = Util.convertDdlListIntoRawText(databaseDdl);
      util.writeContentsToGcs(
          databaseDdlAsString,
          options.getProjectId(),
          Util.getGcsBucketNameFromDatabaseBackupLocation(options.getOutputFolder()),
          Util.getGcsFolderPathFromDatabaseBackupLocation(options.getOutputFolder()),
          Util.FILE_PATH_FOR_DATABASE_DDL);
    }

    // STEP 2c: Query list of tables in database
    // Since Beam does not have the ability to materialize query results before the rest of
    // the pipeline runs, we must get the table list from a query using the Cloud Spanner API.
    final ImmutableSet<String> allTableNames =
        queryListOfAllTablesInDatabase(
            options.getProjectId(),
            options.getInputSpannerInstanceId(),
            options.getInputSpannerDatabaseId(),
            util);
    final ImmutableList<String> tableNamesToBackup =
        getListOfTablesToBackup(
            allTableNames,
            options.getTablesToIncludeInBackup(),
            options.getTablesToExcludeFromBackup());
    LOG.info("Final list of tables to backup includes (" + tableNamesToBackup.size() + ") tables.");

    // STEP 3: Setup single transaction for all queries of Spanner.
    // This ensures a consistent view of the data.
    final PCollectionView<Transaction> transaction =
        p.apply(
            SpannerIO.createTransaction()
                .withSpannerConfig(spannerConfig)
                .withTimestampBound(TimestampBound.ofReadTimestamp(Timestamp.now())));
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
                Util.getFormattedOutputPath(options.getOutputFolder())
                    + Util.FILE_PATH_FOR_DATABASE_TABLE_NAMES)
            .withoutSharding());
    LOG.info("Read list of table names and wrote them to disk");

    // STEP 5: Get schema for tables to backup, and save to disk.
    // Table info https://cloud.google.com/spanner/docs/information-schema#table_columns
    if (options.getShouldQueryTableSchema()) {
      for (String tableName : tableNamesToBackup) {
        p.apply(
                "Read Table Schema " + tableName,
                SpannerIO.read()
                    .withSpannerConfig(spannerConfig)
                    .withQuery(
                        "SELECT t.table_catalog, t.table_schema, t.table_name, t.column_name, "
                            + "t.ordinal_position, t.column_default, t.data_type, t.is_nullable, "
                            + "t.spanner_type FROM information_schema.columns AS t "
                            + "WHERE t.table_name = '"
                            + tableName
                            + "' "
                            + "ORDER BY t.table_catalog, t.table_schema, t.table_name, "
                            + "t.ordinal_position")
                    .withTransaction(transaction))
            .apply(
                "Map Table Schema " + tableName,
                MapElements.via(new FormatSpannerTableSchemaStructAsTextFn()))
            // Contents can be safely written to one file as contents
            // would not be more than a hundred rows in normal cases
            // and a few thousand rows in extreme cases
            .apply(
                "Write Table Schema " + tableName,
                TextIO.write()
                    .to(
                        Util.getFormattedOutputPath(options.getOutputFolder())
                            + Util.FILE_PATH_FOR_DATABASE_TABLE_SCHEMAS_FOLDER
                            + tableName
                            + "--schema.txt")
                    .withoutSharding());
      }
    }

    // STEP 6: Retrieve and store the number of rows per table at transaction time.
    if (options.getShouldQueryTableRowCounts()) {
      // STEP 6a: Get number of rows per table at transaction time.
      final Map<String, PCollection<Struct>> mapOfTableNamesToNumRows = Maps.newHashMap();
      PCollectionList<Struct> collections = PCollectionList.empty(p);
      for (String tableName : tableNamesToBackup) {
        mapOfTableNamesToNumRows.put(
            tableName,
            p.apply(
                "Read Row Count " + tableName,
                SpannerIO.read()
                    .withSpannerConfig(spannerConfig)
                    .withQuery(
                        "SELECT COUNT(*) as num_rows, '"
                            + tableName
                            + "' as table_name FROM "
                            + tableName
                            + ";")
                    .withTransaction(transaction)));
        collections = collections.and(mapOfTableNamesToNumRows.get(tableName));
      }

      PCollection<Struct> tableNamesAndNumRows = collections.apply(Flatten.<Struct>pCollections());

      // STEP 6b: Write table name and number of rows to disk.
      // This is used for verification purposes later in another file.
      tableNamesAndNumRows
          .apply(
              "Map table names and row counts",
              MapElements.via(new FormatSpannerTableRowCountsStructAsTextFn()))
          // Contents can be safely written to one file as contents
          // would not be more than a hundred rows in normal cases
          // and a few thousand rows in extreme cases
          .apply(
              "Write Table Names and Num Rows",
              TextIO.write()
                  .to(
                      Util.getFormattedOutputPath(options.getOutputFolder())
                          + Util.FILE_PATH_FOR_DATABASE_TABLE_NAMES_ROW_COUNTS)
                  .withoutSharding());
      LOG.info("Read list of table names and rows and wrote them to disk");
    }

    // STEP 7: Read underlying Spanner data from database
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

    // STEP 8: Write underlying Spanner data to disk
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
                      Util.getFormattedOutputPath(options.getOutputFolder())
                          + "tables/"
                          + entry.getKey()
                          + "/")
                  .withWritableByteChannelFactory(FileBasedSink.CompressionType.GZIP));
    }
  }
}
