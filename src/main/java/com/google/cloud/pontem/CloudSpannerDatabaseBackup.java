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
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.Transaction;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Perform a backup of an entire Cloud Spanner database.
 *
 * <p>A sample backup:
 *
 * <pre>
 * mvn compile exec:java \
 *   -Dexec.mainClass=com.google.cloud.pontem.CloudSpannerDatabaseBackup \
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
 *   -Dexec.mainClass=com.google.cloud.pontem.CloudSpannerDatabaseBackup \
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
 *   -Dexec.mainClass=com.google.cloud.pontem.CloudSpannerDatabaseBackup \
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
 *   -Dexec.mainClass=com.google.cloud.pontem.CloudSpannerDatabaseBackup \
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
 */
public class CloudSpannerDatabaseBackup {
  private static final Logger LOG = LoggerFactory.getLogger(CloudSpannerDatabaseBackup.class);

  /**
   * Dataflow job configuration options. Inherits standard configuration options from {@code
   * PipelineOptions}.
   */
  public interface SpannerBackupOptions extends PipelineOptions {
    /** Get the Spanner instance id to read data from. */
    @Description("The Spanner instance id to write into")
    @Required
    String getInputSpannerInstanceId();

    void setInputSpannerInstanceId(String value);

    /** Get the Spanner database name to read from. */
    @Description("Name of the Spanner database to read from")
    @Required
    String getInputSpannerDatabaseId();

    void setInputSpannerDatabaseId(String value);

    /**
     * Where to write the backup output to. This should be a GCS bucket and it could also include a
     * subpath folder such as "gs://my-cloud-spanner-project/backup-location"
     */
    @Description("Full path of the GCS folder to write backup to")
    @Required
    String getOutputFolder();

    void setOutputFolder(String value);

    /** Get the Google Cloud project id. */
    @Description("Google Cloud project id")
    @Required
    String getProjectId();

    void setProjectId(String value);

    /**
     * Whether to query table row counts.
     *
     * <p>If the table row counts are queried, an additional level of data-integrity verification
     * can be provided (i.e., we can ensure that the number of rows in each table equals the number
     * of rows backed up for each table). However, querying the table row counts requires triggering
     * a table-scan which can be very time and resource consuming.
     */
    @Description("Whether to query table row counts")
    @Default.Boolean(false)
    Boolean getShouldQueryTableRowCounts();

    void setShouldQueryTableRowCounts(Boolean value);

    /**
     * Whether to backup DDL.
     *
     * <p>Cloud Spanner creates tables and indexes within a database using DDL (Data Definition
     * Language). This is the series of CREATE TABLE and CREATE INDEX commands that can be run. We
     * can backup these statements so the tables can be re-created.
     *
     * <p>WARNING: The backup of DDL relies on a Cloud Spanner API method and not a SQL query run in
     * Dataflow's SpannerIO.read() connector. Consequently, the Cloud Spanner API query to fetch the
     * DDL will not run at the exact same time as the backup of the underlying contents.
     * Consequently, it is possible that a database admin could alter the schema of the table
     * between the time the DDL is backed-up using the Cloud Spanner API and the time Dataflow takes
     * a snapshot of the database for use by SpannerIO.read(). This is highly unlikely but it is
     * theoritically possible.
     *
     * <p>WARNING: Since the SpannerAPI provides only for fetching the entire database's DDL, the
     * entire database DDL will be saved even if the flag to only backup a specific table is set.
     *
     * <p>@see
     * https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases/getDdl
     *
     * <p>@see https://cloud.google.com/spanner/docs/data-definition-language
     */
    @Description("Whether to backup the Data Definition Language (DDL)")
    @Default.Boolean(true)
    Boolean getShouldBackupDatabaseDdl();

    void setShouldBackupDatabaseDdl(Boolean value);

    /**
     * Get whether to query table schema. If the table schema is queried, it will be written to disk
     * in GCS. This preserves a snapshot of what the database schema looked like at backup time.
     */
    @Description("Whether to query table schema")
    @Default.Boolean(false)
    Boolean getShouldQueryTableSchema();

    void setShouldQueryTableSchema(Boolean value);

    /**
     * List of tables to include in the backup. If this is set, only these tables will be included
     * in the database backup. This value cannot be set if tablesToExcludeFromBackup is also set.
     */
    @Description("List of tables to include in backup. If set, only these tables included.")
    String[] getTablesToIncludeInBackup();

    void setTablesToIncludeInBackup(String[] value);

    /**
     * List of tables to exclude from the backup. If this is set, all tables in the database except
     * these tables will be included in the database backup. This value cannot be set if
     * tablesToIncludeInBackup is also set.
     */
    @Description("List of tables to exclude from backup. If set, all tables but these included.")
    String[] getTablesToExcludeFromBackup();

    void setTablesToExcludeFromBackup(String[] value);
  }

  /** Get path to write output of backup to. */
  public static String getOutputPath(String baseFolderPath) {
    if (baseFolderPath.endsWith("/")) {
      return baseFolderPath;
    } else {
      return baseFolderPath + "/";
    }
  }

  private static final String LIST_ALL_TABLES_SQL_QUERY =
      "SELECT table_name, parent_table_name FROM information_schema.tables AS t WHERE t"
          + ".table_catalog = '' and t.table_schema = '' ORDER BY parent_table_name DESC";

  /**
   * Fetch all the table names in a specific cloud spanner database.
   *
   * @return all table names in a database.
   */
  public static ImmutableSet<String> queryListOfAllTablesInDatabase(
      String projectId, String instance, String databaseId, Util util) {
    ImmutableList<Struct> resultSet =
        util.performSingleSpannerQuery(projectId, instance, databaseId, LIST_ALL_TABLES_SQL_QUERY);
    Set<String> tableNames = Sets.newHashSet();
    for (Struct row : resultSet) {
      tableNames.add(row.getString(0));
    }
    LOG.info(
        "Finished querying list of all table names in database "
            + databaseId
            + ". Returned ("
            + tableNames.size()
            + ")"
            + " tables.");
    return ImmutableSet.copyOf(tableNames);
  }

  /**
   * Get list of tables to backup.
   *
   * <p>If the user specified a list of tables to include in backup, return only these tables after
   * verifying their name is valid in the database. No other tables will be returned.
   *
   * <p>If the user specified a list of tables to exclude from backup, return all tables except for
   * these excluded tables.
   *
   * <p>{@param tableNamesToIncludeInBackup} and {@param tableNamesToExcludeFromBackup} cannot both
   * be set and populated.
   *
   * @return List of tables to perform a backup on.
   */
  public static ImmutableList<String> getListOfTablesToBackup(
      ImmutableSet<String> allTableNames,
      String[] tableNamesToIncludeInBackup,
      String[] tableNamesToExcludeFromBackup)
      throws Exception {
    if (allTableNames.size() == 0) {
      throw new Exception("Database has no tables.");
    }

    boolean isTablesToIncludeSet =
        (tableNamesToIncludeInBackup != null && tableNamesToIncludeInBackup.length > 0);
    boolean isTablesToExcludeSet =
        (tableNamesToExcludeFromBackup != null && tableNamesToExcludeFromBackup.length > 0);
    if (isTablesToIncludeSet && isTablesToExcludeSet) {
      throw new Exception("Cannot set a table inclusion list AND a table exclusion list");
    }

    if (isTablesToIncludeSet) {
      LOG.info("Tables to include set with " + tableNamesToIncludeInBackup.length + " values");
      // User has specified a list of tables to include, so only include these tables.
      // Check each table name to ensure it is valid.
      for (String tableNameToIncludeInBackup : tableNamesToIncludeInBackup) {
        if (!allTableNames.contains(tableNameToIncludeInBackup)) {
          throw new Exception(
              "Table "
                  + tableNameToIncludeInBackup
                  + " required in backup but not found in database.");
        }
      }
      return ImmutableList.copyOf(tableNamesToIncludeInBackup);
    }

    if (isTablesToExcludeSet) {
      LOG.info("Tables to exclude set with " + tableNamesToExcludeFromBackup.length + " values");
      // User has specified a list of tables to exclude, so remove those tables.
      for (String tableToExcludeFromBackup : tableNamesToExcludeFromBackup) {
        if (allTableNames.contains(tableToExcludeFromBackup)) {
          allTableNames.remove(tableToExcludeFromBackup);
        } else {
          throw new Exception(
              "Table "
                  + tableToExcludeFromBackup
                  + " listed to exclude from backup yet table was not found in database");
        }
      }
    }

    return ImmutableList.copyOf(allTableNames);
  }

  /**
   * Form a SQL query for Spanner getting table information about the tables to backup.
   *
   * @param tablesToBackup List of tables to backup.
   * @return SQL query.
   */
  public static String getSqlQueryForTablesToBackup(ImmutableList<String> tablesToBackup) {
    String query =
        "SELECT table_name, parent_table_name FROM information_schema.tables AS t "
            + "WHERE t.table_catalog = '' and t.table_schema = '' and "
            + "table_name IN (";
    for (String tableToBackup : tablesToBackup) {
      query += "\"" + tableToBackup + "\",";
    }
    query = query.substring(0, query.length() - 1);
    query += ") ORDER BY parent_table_name DESC";
    return query;
  }

  /**
   * Temporary workaround put in until https://github.com/apache/beam/pull/4946 is live.
   * Gets the list of tables to backup.
   */
  public static ImmutableList<String> getTableNamesBeingBackedUp(
      String projectId, String instance, String databaseId, String tableNamesQuery, Util util) {
    ImmutableList<Struct> tableNames =
        util.performSingleSpannerQuery(projectId, instance, databaseId, tableNamesQuery);
    ArrayList<String> tableNamesAsStrings = new ArrayList<String>();
    String parentTableName = "";
    for (Struct inputRow : tableNames) {
      if (!inputRow.isNull("parent_table_name")) {
        parentTableName = inputRow.getString("parent_table_name");
      }
      tableNamesAsStrings.add(inputRow.getString("table_name") + "," + parentTableName);
    }

    return ImmutableList.copyOf(tableNamesAsStrings);
  }

  public static void main(String[] args) throws Exception {
    // STEP 1: Setup pipeline and Spanner configuration
    final SpannerBackupOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(SpannerBackupOptions.class);

    final String newUserAgent = Util.USER_AGENT_PREFIX + options.getUserAgent();
    options.setUserAgent(newUserAgent);

    final Pipeline p = Pipeline.create(options);

    final SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withInstanceId(options.getInputSpannerInstanceId())
            .withDatabaseId(options.getInputSpannerDatabaseId());

    // STEP 2a: Save DDL to disk
    final Util util = new Util();
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

    // STEP 2b: Query list of tables in database
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

    // Remove when https://github.com/apache/beam/pull/4946 is live
    //    final PCollection<Struct> collectionOfTableNamesAsStruct =
    //        p.apply(
    //            "Read Table List To Backup",
    //            SpannerIO.read()
    //                .withSpannerConfig(spannerConfig)
    //                .withQuery(getSqlQueryForTablesToBackup(tableNamesToBackup))
    //                .withTransaction(transaction));

    // This is done as meta-data about the backup is stored for both verification and for
    // restoration.
    final PCollection<String> collectionOfTableNames =
        p.apply(
            Create.of(
                getTableNamesBeingBackedUp(
                    options.getProjectId(),
                    options.getInputSpannerInstanceId(),
                    options.getInputSpannerDatabaseId(),
                    getSqlQueryForTablesToBackup(tableNamesToBackup),
                    util)));

    // Remove when https://github.com/apache/beam/pull/4946 is live
    //        collectionOfTableNamesAsStruct.apply(
    //            "Map Table Names", MapElements.via(new FormatSpannerTablesListStructAsTextFn()));

    // Contents can be safely written to one file as contents
    // would not be more than a hundred rows in normal cases
    // and a few thousand rows in extreme cases
    collectionOfTableNames.apply(
        "Write Table Names",
        TextIO.write()
            .to(getOutputPath(options.getOutputFolder()) + Util.FILE_PATH_FOR_DATABASE_TABLE_NAMES)
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
                        getOutputPath(options.getOutputFolder())
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
                      getOutputPath(options.getOutputFolder())
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
                  .to(getOutputPath(options.getOutputFolder()) + "tables/" + entry.getKey() + "/")
                  .withWritableByteChannelFactory(FileBasedSink.CompressionType.GZIP));
    }

    // STEP 9: Trigger pipeline
    PipelineResult pipelineResult = p.run();
    pipelineResult.waitUntilFinish();
  }
}
