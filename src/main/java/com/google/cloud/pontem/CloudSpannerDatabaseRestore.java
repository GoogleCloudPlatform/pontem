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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;

/**
 * Perform a restore from backup of an entire Cloud Spanner database.
 *
 * <p>A sample run:
 *
 * <pre>
 * mvn compile exec:java \
 *   -Dexec.mainClass=com.google.cloud.pontem.CloudSpannerDatabaseRestore \
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
 *   -Dexec.mainClass=com.google.cloud.pontem.CloudSpannerDatabaseRestore \
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
 *    -Dexec.mainClass=com.google.cloud.pontem.CloudSpannerDatabaseRestore \
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
 */
public class CloudSpannerDatabaseRestore {
  private static final Logger LOG = Logger.getLogger(CloudSpannerDatabaseRestore.class.getName());

  /**
   * Dataflow job configuration options. Inherits standard {@code PipelineOptions} configuration
   * options.
   */
  public interface SpannerRestoreOptions extends PipelineOptions {
    /** Get the Spanner instance id to write data into. */
    @Description("The Spanner instance id to write into")
    @Required
    String getOutputSpannerInstanceId();

    void setOutputSpannerInstanceId(String value);

    /** Get the Spanner database name to write data into. */
    @Description("Name of the Spanner database to write into")
    @Required
    String getOutputSpannerDatabaseId();

    void setOutputSpannerDatabaseId(String value);

    /**
     * Where to read the backup from. This should be a GCS bucket and it could also include a
     * subpath folder such as "gs://cloud-spanner-backup-test/backup-location"
     */
    @Description("Full path of the GCS folder to read backup from")
    @Required
    String getInputFolder();

    void setInputFolder(String value);

    /** Get the Google Cloud project id. */
    @Description("Google Cloud project id")
    @Required
    String getProjectId();

    void setProjectId(String value);

    /**
     * The Spanner batch size in bytes. This corresponds to SpannerIO.Write().withBatchSizeBytes()
     *
     * <p>If running this Dataflow job produces the following error: "INVALID_ARGUMENT: The
     * transaction contains too many mutations." The solution is to decrease the write batch size by
     * explicitly setting this flag.
     *
     * <p>See org/apache/beam/sdk/io/gcp/spanner/SpannerIO.Write.html#withBatchSizeBytes-long-
     */
    @Description("Bytes write size. Corresponds to SpannerIO.Write withBatchSizeBytes")
    @Default.Long(1048576) // 1MB
    Long getSpannerWriteBatchSizeBytes();

    void setSpannerWriteBatchSizeBytes(Long value);

    /**
     * Whether to create a database and tables as part of the backup process. If this option is set
     * to true and the database or table exist, then we will fail the entire restore operation as we
     * do not want to risk inadvertantly altering database structure or contents. In other words, if
     * you set this option to true, there cannot be an existing database of the name specified in
     * --outputSpannerDatabaseInstanceId
     */
    @Description("Whether to create database and tables if they do not exist.")
    @Default.Boolean(true)
    Boolean getShouldCreateDatabaseAndTables();

    void setShouldCreateDatabaseAndTables(Boolean value);

    /**
     * List of tables to include in the restore. If this is set, only these tables will be included
     * in the database restore. This value cannot be set if tablesToExcludeFromRestore is also set.
     */
    @Description("List of tables to include in restore. If set, only these tables included.")
    String[] getTablesToIncludeInRestore();

    void setTablesToIncludeInRestore(String[] value);

    /**
     * List of tables to exclude from the restore. If this is set, all tables in the database except
     * these tables will be included in the database restore. This value cannot be set if
     * tablesToIncludeInRestore is also set.
     */
    @Description("List of tables to exclude from backup. If set, all tables but these included.")
    String[] getTablesToExcludeFromRestore();

    void setTablesToExcludeFromRestore(String[] value);
  }

  /**
   * Since Cloud Spanner supports parent-child relationships
   * https://cloud.google.com/spanner/docs/schema-and-data-model#parent-child_table_relationships
   * and since the parent table must be populated before the child table can be populated, we need
   * to be very careful to perform the restore of the table contents in a very specific order.
   *
   * <p>This method returns a list of root tables. For each root table, there is a linked list of
   * children tables in descending order (i.e., the first child will be listed in the linked list
   * before the first grandchild).
   *
   * <p>For example: rootTable1 => [rootTable1] // Table has no children dependencies rootTable2 =>
   * [rootTable2 -> child2 -> grandchild2]
   *
   * <p>If the user specified a list of tables to include in backup, return only these tables after
   * verifying their name is valid in the database. No other tables will be returned.
   *
   * <p>If the user specified a list of tables to exclude from backup, return all tables except for
   * these excluded tables.
   *
   * <p>{@param tableNamesToIncludeInRestore} and {@param tableNamesToExcludeFromRestore} cannot
   * both be set and populated.
   */
  public static LinkedHashMap<String, LinkedList<String>> queryListOfTablesToRestore(
      String projectId,
      String inputGcsPath,
      String[] tableNamesToIncludeInRestore,
      String[] tableNamesToExcludeFromRestore,
      Util util)
      throws Exception {

    boolean isTablesToIncludeSet =
        (tableNamesToIncludeInRestore != null && tableNamesToIncludeInRestore.length > 0);
    boolean isTablesToExcludeSet =
        (tableNamesToExcludeFromRestore != null && tableNamesToExcludeFromRestore.length > 0);
    if (isTablesToIncludeSet && isTablesToExcludeSet) {
      throw new Exception("Cannot set a table inclusion list AND a table exclusion list");
    }

    // Step 1: Fetch and parse file.
    String rawFileContents =
        util.getContentsOfFileFromGcs(
            projectId,
            Util.getGcsBucketNameFromDatabaseBackupLocation(inputGcsPath),
            Util.getGcsFolderPathFromDatabaseBackupLocation(inputGcsPath),
            Util.FILE_PATH_FOR_DATABASE_TABLE_NAMES);
    String lines[] = rawFileContents.split("\\r?\\n");

    // Step 2: Store all table names without a parent as well as
    // a multi-map mapping parent -> children.
    Set<String> rootTables = Sets.newHashSet();
    HashMultimap<String, String> miltiMapOfParentTableToChildrenTables = HashMultimap.create();
    for (String line : lines) {
      line = line.trim();
      String tableName = line.substring(0, line.indexOf(','));
      String parentTableName = line.substring(line.indexOf(',') + 1);
      boolean isParentTableNameSet = (line.length() - 1) > tableName.length();

      // If a list of tables to include or exclude in the restore job is set, check the
      // current table against that now.
      // If an inclusion list is set, do not restore tables outside that list.
      // If an exclusion list is set, do not restore tables inside that list.
      if ((isTablesToIncludeSet && !Arrays.asList(tableNamesToIncludeInRestore).contains(tableName))
          || (isTablesToExcludeSet
              && Arrays.asList(tableNamesToExcludeFromRestore).contains(tableName))) {
        continue;
      }

      // If parentTableName exists AND tableName is contained in either an inclusion or exclusion
      // table list, then both the parent table name and child table name must be contained
      // within the inclusion or exclusion list. I.e., we cannot have a situation where an inclusion
      // list contains the child table but not its parent because this would orphan the child table
      // and the restore job would fail.
      if (isParentTableNameSet) {
        if (isTablesToIncludeSet
            && !Arrays.asList(tableNamesToIncludeInRestore).contains(parentTableName)) {
          throw new Exception(
              "Parent-child table relationship exists for table "
                  + tableName
                  + ", yet tables to"
                  + " include in restore list does not contain parent table "
                  + parentTableName);
        }
        if (isTablesToExcludeSet
            && Arrays.asList(tableNamesToExcludeFromRestore).contains(parentTableName)) {
          throw new Exception(
              "Parent-child table relationship exists for table "
                  + tableName
                  + ", yet tables to"
                  + " exclude from restore list contains parent table "
                  + parentTableName);
        }
      }

      // Check whether a parent table exists
      // if a parent table exists, line is formatted: "childTableName,parentTableName"
      // if no parent table exists, line is formatted: "tableName,"
      if (isParentTableNameSet) {
        // table has a parent table
        miltiMapOfParentTableToChildrenTables.put(parentTableName, tableName);
      } else {
        // Line only has one table name, so table has no parent. So, table is a root.
        rootTables.add(tableName);
      }
    }

    // Step 3: Starting with the tables without a parent, walk down the tree adding each table
    // as you go.
    LinkedHashMap<String, LinkedList<String>> mapOfParentToAllChildrenTablesInOrderToFetch =
        new LinkedHashMap();

    for (String rootTable : rootTables) {
      // Create a LinkedList for each root note and populate that list with the root node
      mapOfParentToAllChildrenTablesInOrderToFetch.put(rootTable, new LinkedList<String>());
      mapOfParentToAllChildrenTablesInOrderToFetch.get(rootTable).add(rootTable);

      // Check whether the root table has any children. If it does, walk down that tree.
      if (miltiMapOfParentTableToChildrenTables.get(rootTable).size() > 0) {
        LinkedList<String> tablesToExamine = new LinkedList<String>();
        Set<String> firstChildrenTables = miltiMapOfParentTableToChildrenTables.get(rootTable);
        tablesToExamine.addAll(firstChildrenTables);

        while (!tablesToExamine.isEmpty()) {
          // For each table, walk down a level in the tree.
          String tableToExamine = tablesToExamine.remove();
          mapOfParentToAllChildrenTablesInOrderToFetch.get(rootTable).add(tableToExamine);
          Set<String> grandChildrenTables =
              miltiMapOfParentTableToChildrenTables.get(tableToExamine);
          tablesToExamine.addAll(grandChildrenTables);
        }
      }
    }

    return mapOfParentToAllChildrenTablesInOrderToFetch;
  }

  public static void createDatabaseAndTables(
      String projectId, String instanceId, String databaseId, String inputFolderPath, Util util)
      throws Exception {
    // STEP 1: Check whether DDL is backed-up in GCS. If not, fail out.
    String backedUpDdlFromGcs =
        util.getContentsOfFileFromGcs(
            projectId,
            Util.getGcsBucketNameFromDatabaseBackupLocation(inputFolderPath),
            Util.getGcsFolderPathFromDatabaseBackupLocation(inputFolderPath),
            Util.FILE_PATH_FOR_DATABASE_DDL);
    if (backedUpDdlFromGcs.length() < 1) {
      throw new Exception("Serious error. Unable to fetch backed-up DDL in: " + inputFolderPath);
    }
    ImmutableList<String> backedupDdl = Util.convertRawDdlIntoDdlList(backedUpDdlFromGcs);

    // STEP 2: Re-create database and apply DDL.
    util.createDatabaseAndTables(projectId, instanceId, databaseId, backedupDdl);
  }

  public static void main(String[] args) throws Exception {
    // STEP 1a: Parse Pipeline options
    final SpannerRestoreOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(SpannerRestoreOptions.class);
    final Util util = new Util();

    // STEP 1b: Setup Spanner configuration
    final SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withInstanceId(options.getOutputSpannerInstanceId())
            .withDatabaseId(options.getOutputSpannerDatabaseId());

    // STEP 2: Check whether to re-create database and tables
    if (options.getShouldCreateDatabaseAndTables()) {
      createDatabaseAndTables(
          options.getProjectId(),
          options.getOutputSpannerInstanceId(),
          options.getOutputSpannerDatabaseId(),
          options.getInputFolder(),
          util);
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
            util);

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
    final SpannerRestoreOptions options;
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
          PipelineOptionsFactory.fromArgs(args).withValidation().as(SpannerRestoreOptions.class);
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
                    .from(this.options.getInputFolder() + "tables/" + childTableName + "/*")
                    .withCompressionType(TextIO.CompressionType.GZIP));
        PCollection<Mutation> mutations =
            rows.apply(
                Util.MAP_DATA_TRANSFORM_NODE_NAME
                    + Util.TRANSFORM_NODE_NAME_DELIMITER
                    + childTableName,
                MapElements.via(new FormatTextAsGenericSpannerMutationFn(childTableName)));
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
