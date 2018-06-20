/*
 * Copyright 2018 Google LLC
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.pontem;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Helper class for performing end to end tests.
 *
 * <pre>
 * mvn compile exec:java \
 *   -Dexec.mainClass=com.google.cloud.pontem.EndToEndHelper \
 *   -Dexec.args="--projectId=my-cloud-spanner-project \
 *                --gcsRootBackupFolderPath=gs://my-cloud-spanner-project/multi-backup \
 *                --databaseInstanceId=my-cloud-spanner-instance \
 *                --databaseId=my-database \
 *                --operation=teardown"
 * </pre>
 */
public class EndToEndHelper {
  private static final Logger LOG = Logger.getLogger(EndToEndHelper.class.getName());

  public static final String FOO_TABLE_NAME = "foo_table";
  public static final String PARENT_TABLE_NAME = "parent_table";
  public static final String CHILD_TABLE_NAME = "child_table";
  public static final ImmutableSet<String> TABLE_NAMES =
      ImmutableSet.of(FOO_TABLE_NAME, PARENT_TABLE_NAME, CHILD_TABLE_NAME);

  private static final double FOO_TABLE_MUTATION_0__COL_FLOAT = 32.53829d;
  private static final Timestamp FOO_TABLE_MUTATION_0__COL_TIMESTAMP =
      Timestamp.parseTimestamp("2017-09-15T00:00:00.111111Z");
  private static final Date FOO_TABLE_MUTATION_0__COL_DATE = Date.fromYearMonthDay(2017, 11, 11);
  private static final String FOO_TABLE_MUTATION_0__COL_STRING = "helloString";
  private static final Integer FOO_TABLE_MUTATION_0__COL_INT = 102;
  public static final Mutation FOO_TABLE_MUTATION_0 =
      Mutation.newInsertBuilder(FOO_TABLE_NAME)
          .set("colFloat")
          .to(FOO_TABLE_MUTATION_0__COL_FLOAT)
          .set("colTimestamp")
          .to(FOO_TABLE_MUTATION_0__COL_TIMESTAMP)
          .set("colDate")
          .to(FOO_TABLE_MUTATION_0__COL_DATE)
          .set("colString")
          .to(FOO_TABLE_MUTATION_0__COL_STRING)
          .set("colInt")
          .to(FOO_TABLE_MUTATION_0__COL_INT)
          .build();

  private static final double FOO_TABLE_MUTATION_1__COL_FLOAT = 309.53829d;
  private static final Timestamp FOO_TABLE_MUTATION_1__COL_TIMESTAMP =
      Timestamp.parseTimestamp("2018-01-21T00:00:00.111111Z");
  private static final Date FOO_TABLE_MUTATION_1__COL_DATE = Date.fromYearMonthDay(2018, 1, 21);
  private static final String FOO_TABLE_MUTATION_1__COL_STRING = "helloString2";
  public static final Mutation FOO_TABLE_MUTATION_1 =
      Mutation.newInsertBuilder(FOO_TABLE_NAME)
          .set("colFloat")
          .to(FOO_TABLE_MUTATION_1__COL_FLOAT)
          .set("colTimestamp")
          .to(FOO_TABLE_MUTATION_1__COL_TIMESTAMP)
          .set("colDate")
          .to(FOO_TABLE_MUTATION_1__COL_DATE)
          .set("colString")
          .to(FOO_TABLE_MUTATION_1__COL_STRING)
          .build();

  private static final double FOO_TABLE_MUTATION_2__COL_FLOAT = 3009.53829d;
  private static final Timestamp FOO_TABLE_MUTATION_2__COL_TIMESTAMP =
      Timestamp.parseTimestamp("2017-09-15T00:00:00.111111Z");
  private static final Date FOO_TABLE_MUTATION_2__COL_DATE = Date.fromYearMonthDay(2017, 9, 15);
  private static final String FOO_TABLE_MUTATION_2__COL_STRING = "helloString3";
  private static final List<Long> FOO_TABLE_MUTATION_2__COL_ARRAY_INT =
      ImmutableList.of(329L, 2329302L);
  private static final List<Boolean> FOO_TABLE_MUTATION_2__COL_ARRAY_BOOL =
      ImmutableList.of(true, false, true, true);
  private static final Integer FOO_TABLE_MUTATION_2__COL_INT = 10232;
  public static final Mutation FOO_TABLE_MUTATION_2 =
      Mutation.newInsertBuilder(FOO_TABLE_NAME)
          .set("colFloat")
          .to(FOO_TABLE_MUTATION_2__COL_FLOAT)
          .set("colString")
          .to(FOO_TABLE_MUTATION_2__COL_STRING)
          .set("colTimestamp")
          .to(FOO_TABLE_MUTATION_2__COL_TIMESTAMP)
          .set("colDate")
          .to(FOO_TABLE_MUTATION_2__COL_DATE)
          .set("colArrayInt")
          .toInt64Array(FOO_TABLE_MUTATION_2__COL_ARRAY_INT)
          .set("colArrayBool")
          .toBoolArray(FOO_TABLE_MUTATION_2__COL_ARRAY_BOOL)
          .set("colInt")
          .to(FOO_TABLE_MUTATION_2__COL_INT)
          .build();

  private static final Long PARENT_TABLE_MUTATION_0__COL_FOO_ID = 10l;
  private static final String PARENT_TABLE_MUTATION_0__COL_BAR_STRING = "hello";
  public static final Mutation PARENT_TABLE_MUTATION_0 =
      Mutation.newInsertBuilder(PARENT_TABLE_NAME)
          .set("foo_id")
          .to(PARENT_TABLE_MUTATION_0__COL_FOO_ID)
          .set("bar_string")
          .to(PARENT_TABLE_MUTATION_0__COL_BAR_STRING)
          .build();

  private static final Long PARENT_TABLE_MUTATION_1__COL_FOO_ID = Long.MAX_VALUE;
  private static final String PARENT_TABLE_MUTATION_1__COL_BAR_STRING = "hello_2";
  public static final Mutation PARENT_TABLE_MUTATION_1 =
      Mutation.newInsertBuilder(PARENT_TABLE_NAME)
          .set("foo_id")
          .to(PARENT_TABLE_MUTATION_1__COL_FOO_ID)
          .set("bar_string")
          .to(PARENT_TABLE_MUTATION_1__COL_BAR_STRING)
          .build();

  private static final Long CHILD_TABLE_MUTATION_0__COL_FOO_ID = 10l;
  private static final String CHILD_TABLE_MUTATION_0_COL_CHILD_BAR_STRING = "child_hello";
  public static final Mutation CHILD_TABLE_MUTATION_0 =
      Mutation.newInsertBuilder(CHILD_TABLE_NAME)
          .set("foo_id")
          .to(CHILD_TABLE_MUTATION_0__COL_FOO_ID)
          .set("child_bar_string")
          .to(CHILD_TABLE_MUTATION_0_COL_CHILD_BAR_STRING)
          .build();

  public static final ImmutableList<Mutation> MUTATIONS =
      ImmutableList.of(
          FOO_TABLE_MUTATION_0,
          FOO_TABLE_MUTATION_1,
          FOO_TABLE_MUTATION_2,
          PARENT_TABLE_MUTATION_0,
          PARENT_TABLE_MUTATION_1,
          CHILD_TABLE_MUTATION_0);
  public static final ImmutableList<Struct> FOO_TABLE_STRUCTS =
      ImmutableList.of(
          Struct.newBuilder()
              .add("colFloat", Value.float64(FOO_TABLE_MUTATION_0__COL_FLOAT))
              .add("colTimestamp", Value.timestamp(FOO_TABLE_MUTATION_0__COL_TIMESTAMP))
              .add("colDate", Value.date(FOO_TABLE_MUTATION_0__COL_DATE))
              .add("colString", Value.string(FOO_TABLE_MUTATION_0__COL_STRING))
              .add("colInt", Value.int64(FOO_TABLE_MUTATION_0__COL_INT))
              .build(),
          Struct.newBuilder()
              .add("colFloat", Value.float64(FOO_TABLE_MUTATION_1__COL_FLOAT))
              .add("colString", Value.string(FOO_TABLE_MUTATION_1__COL_STRING))
              .add("colTimestamp", Value.timestamp(FOO_TABLE_MUTATION_1__COL_TIMESTAMP))
              .add("colDate", Value.date(FOO_TABLE_MUTATION_1__COL_DATE))
              .build(),
          Struct.newBuilder()
              .add("colFloat", Value.float64(FOO_TABLE_MUTATION_2__COL_FLOAT))
              .add("colString", Value.string(FOO_TABLE_MUTATION_2__COL_STRING))
              .add("colTimestamp", Value.timestamp(FOO_TABLE_MUTATION_2__COL_TIMESTAMP))
              .add("colDate", Value.date(FOO_TABLE_MUTATION_2__COL_DATE))
              .add("colArrayInt", Value.int64Array(FOO_TABLE_MUTATION_2__COL_ARRAY_INT))
              .add("colArrayBool", Value.boolArray(FOO_TABLE_MUTATION_2__COL_ARRAY_BOOL))
              .add("colInt", Value.int64(FOO_TABLE_MUTATION_2__COL_INT))
              .build());

  public static final ImmutableList<Struct> PARENT_TABLE_STRUCTS =
      ImmutableList.of(
          Struct.newBuilder()
              .add("foo_id", Value.int64(PARENT_TABLE_MUTATION_0__COL_FOO_ID))
              .add("bar_string", Value.string(PARENT_TABLE_MUTATION_0__COL_BAR_STRING))
              .build(),
          Struct.newBuilder()
              .add("foo_id", Value.int64(PARENT_TABLE_MUTATION_1__COL_FOO_ID))
              .add("bar_string", Value.string(PARENT_TABLE_MUTATION_1__COL_BAR_STRING))
              .build());
  public static final ImmutableList<Struct> CHILD_TABLE_STRUCTS =
      ImmutableList.of(
          Struct.newBuilder()
              .add("foo_id", Value.int64(CHILD_TABLE_MUTATION_0__COL_FOO_ID))
              .add("child_bar_string", Value.string(CHILD_TABLE_MUTATION_0_COL_CHILD_BAR_STRING))
              .build());

  public static final ImmutableList<String> GOOGLE_CLOUD_SPANNER_DDL =
      ImmutableList.of(
          "CREATE TABLE foo_table (\n"
              + "  colFloat FLOAT64 NOT NULL,\n"
              + "  colArrayInt ARRAY<INT64>,\n"
              + "  colBool BOOL,\n"
              + "  colBytes BYTES(MAX),\n"
              + "  colDate DATE NOT NULL,\n"
              + "  colString STRING(MAX) NOT NULL,\n"
              + "  colTimestamp TIMESTAMP NOT NULL,\n"
              + "  colInt INT64,\n"
              + "  colArrayBool ARRAY<BOOL>,\n"
              + ") PRIMARY KEY(colFloat)",
          "CREATE TABLE parent_table (\n"
              + "  foo_id INT64 NOT NULL,\n"
              + "  bar_string STRING(MAX),\n"
              + ") PRIMARY KEY(foo_id)",
          "CREATE NULL_FILTERED INDEX parent_table_index ON parent_table(foo_id DESC)",
          "CREATE TABLE child_table (\n"
              + "  foo_id INT64 NOT NULL,\n"
              + "  child_bar_string STRING(MAX),\n"
              + ") PRIMARY KEY(foo_id, child_bar_string DESC),\n"
              + "  INTERLEAVE IN PARENT parent_table ON DELETE CASCADE");

  public static Options configureCommandlineOptions() {
    Options options = new Options();

    /** Google Cloud Storage Absolute Path to Backup Folder. */
    Option gcsRootBackupFolderPath = new Option("b", "gcsRootBackupFolderPath", true, "GCS Folder");
    gcsRootBackupFolderPath.setRequired(true);
    options.addOption(gcsRootBackupFolderPath);

    /** Google Cloud project ID. */
    Option projectId = new Option("p", "projectId", true, "Google Cloud Project Id");
    projectId.setRequired(true);
    options.addOption(projectId);

    /** The Google Cloud Spanner database instance id */
    Option databaseInstanceId =
        new Option("i", "databaseInstanceId", true, "Google Cloud Spanner Instance ID");
    databaseInstanceId.setRequired(true);
    options.addOption(databaseInstanceId);

    /** The Google Cloud Spanner database id (i.e., name) */
    Option databaseid = new Option("d", "databaseId", true, "Google Cloud Spanner Database ID");
    databaseid.setRequired(true);
    options.addOption(databaseid);

    /** The operation for the end to end test helper to perform */
    Option operation = new Option("o", "operation", true, "End to End Helper Operation Requested");
    operation.setRequired(true);
    options.addOption(operation);

    /** The operation should fail if it content (e.g., test database) already exists */
    Option shouldFailIfContentAlreadyExists =
        new Option(
            "s", "shouldFailIfContentAlreadyExists", true, "End to End Helper Operation Requested");
    shouldFailIfContentAlreadyExists.setRequired(false);
    options.addOption(shouldFailIfContentAlreadyExists);

    return options;
  }

  public static void main(String[] args) throws Exception {
    // STEP 1: Parse inputs.
    Options options = configureCommandlineOptions();

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      LOG.info(e.getMessage());
      formatter.printHelp("utility-name", options);

      System.exit(1);
      return;
    }

    try {
      final String operation = cmd.getOptionValue("operation");
      final String databaseId = cmd.getOptionValue("databaseId");
      final String instanceId = cmd.getOptionValue("databaseInstanceId");
      final String projectId = cmd.getOptionValue("projectId");
      final String gcsRootBackupFolderPath = cmd.getOptionValue("gcsRootBackupFolderPath");
      final boolean shouldFailIfContentAlreadyExists =
          Boolean.valueOf(cmd.getOptionValue("shouldFailIfContentAlreadyExists", "false"));
      final Util util = new Util();
      final SpannerUtil spannerUtil = new SpannerUtil();
      if (operation.equals("setup")) {
        setupEnvironmentForEndToEndTests(
            projectId,
            instanceId,
            databaseId,
            Util.getGcsBucketNameFromDatabaseBackupLocation(gcsRootBackupFolderPath),
            shouldFailIfContentAlreadyExists);
      } else if (operation.equals("teardown")) {
        tearDownEnvironmentForEndToEndTests(
            projectId,
            instanceId,
            databaseId,
            Util.getGcsBucketNameFromDatabaseBackupLocation(gcsRootBackupFolderPath),
            Util.getGcsFolderPathFromDatabaseBackupLocation(gcsRootBackupFolderPath));
      } else if (operation.equals("verifyDatabase")) {
        verifyDatabaseStructureAndContent(projectId, instanceId, databaseId, spannerUtil);
      } else if (operation.equals("verifyGcsBackup")) {
        verifyGcsBackupMetaData(projectId, gcsRootBackupFolderPath, util);
      } else if (operation.equals("teardownDatabase")) {
        deleteCloudSpannerDatabase(projectId, instanceId, databaseId);
      } else {
        throw new Exception("Unable to execute operation: " + operation);
      }
    } catch (Exception e) {
      LOG.warning(e.getMessage() + "\n" + Arrays.toString(e.getStackTrace()));
      throw e;
    }
  }

  public static void tearDownEnvironmentForEndToEndTests(
      String projectId,
      String instanceId,
      String databaseId,
      String gcsBucketName,
      String gcsFolderPath) {
    deleteCloudSpannerDatabase(projectId, instanceId, databaseId);

    if (gcsFolderPath.charAt(0) == '/') {
      gcsFolderPath = gcsFolderPath.substring(1);
    }
    if (gcsFolderPath.charAt(gcsFolderPath.length() - 1) == '/') {
      gcsFolderPath = gcsFolderPath.substring(0, gcsFolderPath.length() - 1);
    }
    deleteGcsFolder(projectId, gcsBucketName, gcsFolderPath);
  }

  public static void setupEnvironmentForEndToEndTests(
      String projectId,
      String instanceId,
      String databaseId,
      String gcsBucketName,
      boolean shouldFailIfAlreadyExists)
      throws Exception {
    createGcsBucket(projectId, gcsBucketName, shouldFailIfAlreadyExists);

    createCloudSpannerDatabaseAndTableStructure(
        projectId, instanceId, databaseId, shouldFailIfAlreadyExists);
    populateCloudSpannerDatabaseWithBasicContent(
        projectId, instanceId, databaseId, shouldFailIfAlreadyExists);
  }

  public static void deleteCloudSpannerDatabase(
      String projectId, String instanceId, String databaseId) {
    LOG.info("Beginning deletion of Cloud Spanner database " + databaseId);
    SpannerOptions options = SpannerUtil.getSpannerOptionsBuilder().build();
    Spanner spanner = options.getService();
    try {
      DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);
      DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();

      dbAdminClient = spanner.getDatabaseAdminClient();
      LOG.info(
          "Attempting to drop database named '"
              + databaseId
              + "' in instance '"
              + instanceId
              + "' and project '"
              + projectId
              + "'");
      dbAdminClient.dropDatabase(instanceId, databaseId);
    } catch (SpannerException e) {
      LOG.info("Error dropping database " + databaseId + ":\n" + e.toString());
    } finally {
      LOG.info("Finished deletion of Cloud Spanner database " + databaseId);
      spanner.close();
    }
    ImmutableList<String> databaseNames =
        SpannerUtil.getListOfDatabaseNames(projectId, instanceId, 10);
    LOG.info(
        "Database names remaining in instance " + instanceId + ":\n" + databaseNames.toString());
    LOG.info("End deletion of Cloud Spanner database " + databaseId);
  }

  public static void deleteGcsFolder(String projectId, String gcsBucketName, String gcsFolderPath) {
    LOG.info("Begin deletion of content in GCS bucket " + gcsBucketName);
    LOG.info("Begin deletion of GCS folder " + gcsFolderPath);
    StorageOptions.Builder optionsBuilder = StorageOptions.newBuilder();
    StorageOptions storageOptions = optionsBuilder.setProjectId(projectId).build();
    Storage storage = storageOptions.getService();

    Iterable<Blob> blobs =
        storage.list(gcsBucketName, Storage.BlobListOption.prefix(gcsFolderPath)).iterateAll();
    for (Blob blob : blobs) {
      blob.delete(Blob.BlobSourceOption.generationMatch());
    }

    LOG.info("End deletion of content in GCS bucket " + gcsBucketName);
  }

  private static void createCloudSpannerDatabaseAndTableStructure(
      String projectId, String instanceId, String databaseId, boolean shouldFailIfAlreadyCreated)
      throws Exception {
    LOG.info("Begin creation of Cloud Spanner database " + databaseId);
    SpannerUtil spannerUtil = new SpannerUtil();
    try {
      spannerUtil.createDatabaseAndTables(
          projectId, instanceId, databaseId, GOOGLE_CLOUD_SPANNER_DDL);
    } catch (SpannerException e) {
      if (e.getMessage().contains("ALREADY_EXISTS") && !shouldFailIfAlreadyCreated) {
        LOG.info("Cloud Spanner database already exists.");
      } else {
        throw e;
      }
    }
    LOG.info("End creation of Cloud Spanner database " + databaseId);
  }

  private static void createGcsBucket(
      String projectId, String bucketName, boolean shouldFailIfAlreadyCreated) {
    LOG.info("Begin creation of GCS bucket " + bucketName);
    StorageOptions.Builder optionsBuilder = StorageOptions.newBuilder();
    StorageOptions storageOptions = optionsBuilder.setProjectId(projectId).build();
    Storage storage = storageOptions.getService();

    try {
      Bucket newBucket =
          storage.create(
              BucketInfo.newBuilder(bucketName)
                  // See here for possible values: http://g.co/cloud/storage/docs/storage-classes
                  .setStorageClass(StorageClass.REGIONAL)
                  // Possible values: http://g.co/cloud/storage/docs/bucket-locations#location-mr
                  .setLocation("us-central1")
                  .build());
    } catch (StorageException e) {
      if (e.getMessage().equals("You already own this bucket. Please select another name.")
          && !shouldFailIfAlreadyCreated) {
        LOG.info("Bucket already exists.");
      } else {
        throw e;
      }
    }
    LOG.info("End deletion of GCS bucket name " + bucketName);
  }

  private static void populateCloudSpannerDatabaseWithBasicContent(
      String projectId, String instanceId, String databaseId, boolean shouldFailIfAlreadyPopulated)
      throws Exception {
    LOG.info("Begin population of Cloud Spanner database with basic content: " + databaseId);
    SpannerOptions options = SpannerUtil.getSpannerOptionsBuilder().build();
    Spanner spanner = options.getService();
    try {
      DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);
      DatabaseClient dbClient = spanner.getDatabaseClient(db);

      Timestamp commitTimestamp = dbClient.write(MUTATIONS);
      LOG.info("Wrote " + MUTATIONS.size() + " mutations at " + commitTimestamp.toString());
    } catch (SpannerException e) {
      if (e.getMessage().contains("ALREADY_EXISTS") && !shouldFailIfAlreadyPopulated) {
        LOG.info("Cloud Spanner mutations already populated.\n" + e.getMessage());
      } else {
        throw e;
      }
    } finally {
      spanner.close();
    }
    LOG.info("End population of Cloud Spanner database with basic content: " + databaseId);
  }

  public static void verifyGcsBackupMetaData(
      String projectId, String gcsRootBackupFolderPath, Util util) throws Exception {
    LOG.info("Begin verify of GCS backup");
    String rawFileContentsOfTableList =
        util.getContentsOfFileFromGcs(
            projectId,
            Util.getGcsBucketNameFromDatabaseBackupLocation(gcsRootBackupFolderPath),
            Util.getGcsFolderPathFromDatabaseBackupLocation(gcsRootBackupFolderPath),
            Util.FILE_PATH_FOR_DATABASE_TABLE_NAMES);
    String tables[] = rawFileContentsOfTableList.split("\\r?\\n");
    if (tables.length != TABLE_NAMES.size()) {
      throw new Exception("Table names not backed-up");
    }
    String tableName0 = tables[0].substring(0, tables[0].indexOf(',')).trim();
    String tableName1 = tables[1].substring(0, tables[1].indexOf(',')).trim();
    String tableName2 = tables[2].substring(0, tables[2].indexOf(',')).trim();
    if (!TABLE_NAMES.contains(tableName0)
        || !TABLE_NAMES.contains(tableName1)
        || !TABLE_NAMES.contains(tableName2)) {
      throw new Exception(
          "Table names not backed-up properly."
              + "\nFound:\n"
              + tableName0
              + "\n"
              + tableName1
              + "\n"
              + tableName2
              + "\nExpected:\n"
              + PARENT_TABLE_NAME
              + "\n"
              + CHILD_TABLE_NAME
              + "\n"
              + FOO_TABLE_NAME);
    }
    LOG.info("End verify of GCS backup");
  }

  public static void verifyDatabaseStructureAndContent(
      String projectId, String instanceId, String databaseId, SpannerUtil spannerUtil)
      throws Exception {
    // STEP 1: Check DDL
    ImmutableList<String> ddl = spannerUtil.queryDatabaseDdl(projectId, instanceId, databaseId);
    LOG.info("DDL in database " + databaseId + " with " + ddl.size() + " statements");
    if (!ddl.equals(GOOGLE_CLOUD_SPANNER_DDL)) {
      LOG.info("Expected:\n" + GOOGLE_CLOUD_SPANNER_DDL.toString());
      LOG.info("Actual:\n" + ddl.toString());
      throw new Exception("Database does not appear to have expected DDL");
    }

    // STEP 2: Check Database Content
    ImmutableList<Struct> fooResultSet =
        spannerUtil.performSingleSpannerReadQuery(
            projectId, instanceId, databaseId, "SELECT * FROM " + FOO_TABLE_NAME + ";");
    LOG.info("Number of rows in table " + FOO_TABLE_NAME + " = " + fooResultSet.size());

    ImmutableList<Struct> parentResultSet =
        spannerUtil.performSingleSpannerReadQuery(
            projectId, instanceId, databaseId, "SELECT * FROM " + PARENT_TABLE_NAME + ";");
    LOG.info("Number of rows in table " + PARENT_TABLE_NAME + " = " + parentResultSet.size());

    ImmutableList<Struct> childResultSet =
        spannerUtil.performSingleSpannerReadQuery(
            projectId, instanceId, databaseId, "SELECT * FROM " + CHILD_TABLE_NAME + ";");
    LOG.info("Number of rows in table " + CHILD_TABLE_NAME + " = " + childResultSet.size());

    // STEP 3: Check Row Results
    // STEP 3a: Check Row Result Count
    int actualRowCount = fooResultSet.size() + parentResultSet.size() + childResultSet.size();
    if (actualRowCount != MUTATIONS.size()) {
      throw new Exception(
          "Number of rows in database ("
              + actualRowCount
              + ") "
              + "not as expected: "
              + MUTATIONS.size());
    }
    // STEP 3b: Check content
    if (!fooResultSet.get(0).getString("colString").equals(FOO_TABLE_MUTATION_0__COL_STRING)) {
      throw new Exception("Contents of colString do not match");
    }
    if (fooResultSet.get(0).getDouble("colFloat") != FOO_TABLE_MUTATION_0__COL_FLOAT) {
      throw new Exception("Contents of colFloat do not match");
    }
    if (!fooResultSet
        .get(0)
        .getTimestamp("colTimestamp")
        .equals(FOO_TABLE_MUTATION_0__COL_TIMESTAMP)) {
      throw new Exception("Contents of colTimestamp do not match");
    }
    if (fooResultSet.get(0).getLong("colInt") != FOO_TABLE_MUTATION_0__COL_INT) {
      throw new Exception("Contents of colInt do not match");
    }
    if (!fooResultSet.get(0).getDate("colDate").equals(FOO_TABLE_MUTATION_0__COL_DATE)) {
      throw new Exception("Contents of colDate do not match");
    }

    if (!Double.valueOf(fooResultSet.get(1).getDouble("colFloat"))
        .equals(FOO_TABLE_MUTATION_1__COL_FLOAT)) {
      throw new Exception("Contents of colFloat do not match.");
    }
    if (!fooResultSet.get(1).getString("colString").equals(FOO_TABLE_MUTATION_1__COL_STRING)) {
      throw new Exception("Contents of colString do not match");
    }
    if (!fooResultSet
        .get(1)
        .getTimestamp("colTimestamp")
        .equals(FOO_TABLE_MUTATION_1__COL_TIMESTAMP)) {
      throw new Exception("Contents of colTimestamp do not match");
    }

    if (!Double.valueOf(fooResultSet.get(2).getDouble("colFloat"))
        .equals(FOO_TABLE_MUTATION_2__COL_FLOAT)) {
      throw new Exception("Contents of colFloat do not match.");
    }
    if (!fooResultSet.get(2).getString("colString").equals(FOO_TABLE_MUTATION_2__COL_STRING)) {
      throw new Exception("Contents of colString do not match");
    }
    if (!fooResultSet
        .get(2)
        .getTimestamp("colTimestamp")
        .equals(FOO_TABLE_MUTATION_2__COL_TIMESTAMP)) {
      throw new Exception("Contents of colTimestamp do not match");
    }
    if (!fooResultSet
        .get(2)
        .getLongList("colArrayInt")
        .equals(FOO_TABLE_MUTATION_2__COL_ARRAY_INT)) {
      throw new Exception("Contents of colArrayInt do not match");
    }
    if (!fooResultSet
        .get(2)
        .getBooleanList("colArrayBool")
        .equals(FOO_TABLE_MUTATION_2__COL_ARRAY_BOOL)) {
      throw new Exception("Contents of colArrayBool do not match");
    }

    if (!childResultSet
        .get(0)
        .getString("child_bar_string")
        .equals(CHILD_TABLE_MUTATION_0_COL_CHILD_BAR_STRING)) {
      throw new Exception("Contents of child_bar_string do not match");
    }
    if (!parentResultSet
        .get(0)
        .getString("bar_string")
        .equals(PARENT_TABLE_MUTATION_0__COL_BAR_STRING)) {
      throw new Exception("Contents of parent bar_string do not match");
    }
  }
}
