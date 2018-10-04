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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.io.File;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.Transaction;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Backup a Cloud Spanner database into Avro format.
 *
 * <p>A sample backup:
 *
 * <pre>
 * mvn compile exec:java \
 *   -Dexec.mainClass=com.google.cloud.pontem.AvroCloudSpannerDatabaseBackup \
 *   -Dexec.args="--runner=DataflowRunner \
 *                --project=my-cloud-spanner-project \
 *                --gcpTempLocation=gs://my-cloud-spanner-project/tmp \
 *                --inputSpannerInstanceId=my-cloud-spanner-project-db \
 *                --inputSpannerDatabaseId=words2 \
 *                --outputFolder=gs://my-cloud-spanner-project/backups/latest \
 *                --projectId=my-cloud-spanner-project" \
 *   -Pdataflow-runner
 * </pre>
 */
public class AvroCloudSpannerDatabaseBackup extends BaseCloudSpannerDatabaseBackup {

  /** Entry point for backing up a Cloud Spanner database into Avro format. */
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
    final ImmutableList<String> databaseDdl =
        spannerUtil.queryDatabaseDdl(
            options.getProjectId(),
            options.getInputSpannerInstanceId(),
            options.getInputSpannerDatabaseId());
    final ImmutableMap<String, String> mapOfTableNameToTableDdl =
        SpannerUtil.convertEntireDtaabaseDdlIntoTableSpecificDdl(databaseDdl);
    if (options.getShouldBackupDatabaseDdl()) {
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

    constructPipeline(
        p,
        options,
        spannerConfig,
        gcsUtil,
        tableNamesToBackup,
        timestampForDb,
        mapOfTableNameToTableDdl);

    // STEP 7: Trigger pipeline
    PipelineResult pipelineResult = p.run();
    pipelineResult.waitUntilFinish();
  }

  protected static void constructPipeline(
      Pipeline p,
      BaseCloudSpannerBackupOptions options,
      SpannerConfig spannerConfig,
      GcsUtil gcsUtil,
      ImmutableList<String> tableNamesToBackup,
      Timestamp timestampForDb,
      ImmutableMap<String, String> mapOfTableNameToTableDdl)
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
    p.apply(
            "Read Table List To Backup",
            SpannerIO.read()
                .withSpannerConfig(spannerConfig)
                .withQuery(getSqlQueryForTablesToBackup(tableNamesToBackup))
                .withTransaction(transaction)
                .withBatching(false))
        .apply("Map Table Names", MapElements.via(new FormatSpannerTablesListStructAsTextFn()))
        .apply(
            "Write Table Names",
            TextIO.write()
                .to(
                    GcsUtil.getFormattedOutputPath(options.getOutputFolder())
                        + Util.FILE_PATH_FOR_DATABASE_TABLE_NAMES)
                // Contents can be safely written to one file as contents
                // would not be more than a hundred rows in normal cases
                // and a few thousand rows in extreme cases
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
      if (!mapOfTableNameToTableDdl.containsKey(entry.getKey())) {
        throw new Exception("Unable to find DDL for table " + entry.getKey());
      }
      TableInformation tableInformation =
          new TableInformation(mapOfTableNameToTableDdl.get(entry.getKey()));
      // Create and Save Avro Schema
      Schema tableSchema = AvroUtil.buildSchemaForTable(entry.getKey(), tableInformation);
      PCollection<String> schemaCollection =
          p.apply("Read Schema", Create.of(tableSchema.toString())).setCoder(StringUtf8Coder.of());
      schemaCollection.apply(
          "Write Avro Schema to Disk",
          TextIO.write()
              .to(
                  GcsUtil.getFormattedOutputPath(options.getOutputFolder())
                      + AvroUtil.getAvroSchemaFileLocation(entry.getKey()))
              .withoutSharding());

      // Write Spanner Structs to disk in Avro format
      entry
          .getValue()
          .apply(
              Util.MAP_DATA_TRANSFORM_NODE_NAME
                  + Util.TRANSFORM_NODE_NAME_DELIMITER
                  + entry.getKey(),
              MapElements.via(new FormatStructAsGenericRecordFn(tableSchema.toString())))
          .setCoder(AvroCoder.of(tableSchema))
          .apply(
              Util.WRITE_DATA_TRANSFORM_NODE_NAME
                  + Util.TRANSFORM_NODE_NAME_DELIMITER
                  + entry.getKey(),
              AvroIO.writeGenericRecords(tableSchema)
                  .to(
                      GcsUtil.getFormattedOutputPath(options.getOutputFolder())
                          + "tables"
                          + File.separator
                          + entry.getKey()
                          + File.separator)
                  .withSuffix(".avro"));
    }
  }
}
