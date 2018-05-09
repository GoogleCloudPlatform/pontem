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
  public static final Mutation FOO_TABLE_MUTATION_1 =
      Mutation.newInsertBuilder(FOO_TABLE_NAME)
          .set("colFloat")
          .to(32.53829d)
          .set("colTimestamp")
          .to(Timestamp.parseTimestamp("2017-09-15T00:00:00.111111Z"))
          .set("colDate")
          .to(Date.fromYearMonthDay(2017, 11, 11))
          .set("colString")
          .to("helloString")
          .set("colInt")
          .to(102)
          .build();
  public static final Mutation FOO_TABLE_MUTATION_2 =
      Mutation.newInsertBuilder(FOO_TABLE_NAME)
          .set("colFloat")
          .to(34.35d)
          .set("colTimestamp")
          .to(Timestamp.parseTimestamp("2016-09-15T00:00:00.111111Z"))
          .set("colDate")
          .to(Date.fromYearMonthDay(2018, 11, 11))
          .set("colString")
          .to("hello world")
          .build();
  public static final Mutation PARENT_TABLE_MUTATION_1 =
      Mutation.newInsertBuilder(PARENT_TABLE_NAME)
          .set("foo_id")
          .to(10L)
          .set("bar_string")
          .to("hello")
          .build();
  public static final Mutation PARENT_TABLE_MUTATION_2 =
      Mutation.newInsertBuilder(PARENT_TABLE_NAME)
          .set("foo_id")
          .to(100L)
          .set("bar_string")
          .to("hello_2")
          .build();
  public static final Mutation CHILD_TABLE_MUTATION =
      Mutation.newInsertBuilder(CHILD_TABLE_NAME)
          .set("foo_id")
          .to(10L)
          .set("child_bar_string")
          .to("child_hello")
          .build();
  public static final ImmutableList<Mutation> MUTATIONS =
      ImmutableList.of(
          FOO_TABLE_MUTATION_1,
          FOO_TABLE_MUTATION_2,
          PARENT_TABLE_MUTATION_1,
          PARENT_TABLE_MUTATION_2,
          CHILD_TABLE_MUTATION);
  public static final ImmutableList<Struct> FOO_TABLE_STRUCTS =
      ImmutableList.of(
          Struct.newBuilder()
              .add("colFloat", Value.float64(32.53829d))
              .add("colString", Value.string("helloString"))
              .add("colTimestamp", Value.date(Date.fromYearMonthDay(2018, 10, 1)))
              .build(),
          Struct.newBuilder()
              .add("colFloat", Value.float64(34.35d))
              .add("colString", Value.string("hello world"))
              .add("colTimestamp", Value.date(Date.fromYearMonthDay(2017, 10, 1)))
              .build());
  public static final ImmutableList<Struct> PARENT_TABLE_STRUCTS =
      ImmutableList.of(
          Struct.newBuilder()
              .add("foo_id", Value.int64(10L))
              .add("bar_string", Value.string("hello"))
              .build(),
          Struct.newBuilder()
              .add("foo_id", Value.int64(100L))
              .add("bar_string", Value.string("hello_2"))
              .build());
  public static final ImmutableList<Struct> CHILD_TABLE_STRUCTS =
      ImmutableList.of(
          Struct.newBuilder()
              .add("foo_id", Value.int64(10L))
              .add("child_bar_string", Value.string("child_hello"))
              .build());

  public static final ImmutableList<String> GOOGLE_CLOUD_SPANNER_DDL =
      ImmutableList.of(
          "CREATE TABLE foo_table (\n"
              + "  colFloat FLOAT64 NOT NULL,\n"
              + "  colArray ARRAY<INT64>,\n"
              + "  colBool BOOL,\n"
              + "  colBytes BYTES(MAX),\n"
              + "  colDate DATE NOT NULL,\n"
              + "  colString STRING(MAX) NOT NULL,\n"
              + "  colTimestamp TIMESTAMP NOT NULL,\n"
              + "  colInt INT64,\n"
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

  private static Options configureCommandlineOptions() {
    Options options = new Options();

    /** Google Cloud Storage Bucket. */
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

    /** The Google Cloud Spanner database id */
    Option databaseid = new Option("d", "databaseId", true, "Google Cloud Spanner Database ID");
    databaseid.setRequired(true);
    options.addOption(databaseid);

    /** The operation for the end to end test helper to perform */
    Option operation = new Option("o", "operation", true, "End to End Helper Operation Requested");
    operation.setRequired(true);
    options.addOption(operation);

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

    final String operation = cmd.getOptionValue("operation");
    final String databaseId = cmd.getOptionValue("databaseId");
    final String instanceId = cmd.getOptionValue("databaseInstanceId");
    final String projectId = cmd.getOptionValue("projectId");
    final String gcsRootBackupFolderPath = cmd.getOptionValue("gcsRootBackupFolderPath");
    final Util util = new Util();
    if (operation.equals("setup")) {
      setupEnvironmentForEndToEndTests(
          projectId,
          instanceId,
          databaseId,
          Util.getGcsBucketNameFromDatabaseBackupLocation(gcsRootBackupFolderPath),
          false);
    } else if (operation.equals("teardown")) {
      tearDownEnvironmentForEndToEndTests(
          projectId,
          instanceId,
          databaseId,
          Util.getGcsBucketNameFromDatabaseBackupLocation(gcsRootBackupFolderPath));
    } else if (operation.equals("verifyDatabase")) {
      verifyDatabaseStructureAndContent(projectId, instanceId, databaseId, util);
    } else if (operation.equals("verifyGcsBackup")) {
      verifyGcsBackupMetaData(projectId, gcsRootBackupFolderPath, util);
    } else if (operation.equals("teardownDatabase")) {
      deleteCloudSpannerDatabase(projectId, instanceId, databaseId);
    } else {
      throw new Exception("Unable to execute operation: " + operation);
    }
  }

  public static void tearDownEnvironmentForEndToEndTests(
      String projectId, String instanceId, String databaseId, String gcsBucketName) {
    deleteCloudSpannerDatabase(projectId, instanceId, databaseId);
    deleteGcsBucket(projectId, gcsBucketName);
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

  private static void deleteCloudSpannerDatabase(
      String projectId, String instanceId, String databaseId) {
    LOG.info("Beginning deletion of Cloud Spanner database " + databaseId);
    SpannerOptions options = Util.getSpannerOptionsBuilder().build();
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
      // Pause for 60 seconds to allow Cloud Spanner database to update.
      Thread.sleep(60000);
    } catch (SpannerException e) {
      LOG.info("Error dropping database " + databaseId + ":\n" + e.toString());
    } catch (InterruptedException e) {
      LOG.info(e.toString());
    } finally {
      LOG.info("Finished deletion of Cloud Spanner database " + databaseId);
      spanner.close();
    }
    ImmutableList<String> databaseNames = Util.getListOfDatabaseNames(projectId, instanceId, 10);
    LOG.info(
        "Database names remaining in instance " + instanceId + ":\n" + databaseNames.toString());
    LOG.info("End deletion of Cloud Spanner database " + databaseId);
  }

  private static void deleteGcsBucket(String projectId, String gcsBucketName) {
    LOG.info("Begin deletion of GCS bucket " + gcsBucketName);
    StorageOptions.Builder optionsBuilder = StorageOptions.newBuilder();
    StorageOptions storageOptions = optionsBuilder.setProjectId(projectId).build();
    Storage storage = storageOptions.getService();

    Iterable<Blob> blobs =
        storage.list(gcsBucketName, Storage.BlobListOption.prefix("")).iterateAll();
    for (Blob blob : blobs) {
      blob.delete(Blob.BlobSourceOption.generationMatch());
    }

    storage.delete(gcsBucketName);
    LOG.info("End deletion of GCS bucket " + gcsBucketName);
  }

  private static void createCloudSpannerDatabaseAndTableStructure(
      String projectId, String instanceId, String databaseId, boolean shouldFailIfAlreadyCreated)
      throws Exception {
    LOG.info("Begin creation of Cloud Spanner database " + databaseId);
    Util util = new Util();
    try {
      util.createDatabaseAndTables(projectId, instanceId, databaseId, GOOGLE_CLOUD_SPANNER_DDL);
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
    SpannerOptions options = Util.getSpannerOptionsBuilder().build();
    Spanner spanner = options.getService();
    try {
      DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);
      DatabaseClient dbClient = spanner.getDatabaseClient(db);

      Timestamp commitTimestamp = dbClient.write(MUTATIONS);
      LOG.info("Wrote mutations at " + commitTimestamp.toString());
    } catch (SpannerException e) {
      if (e.getMessage().contains("ALREADY_EXISTS") && !shouldFailIfAlreadyPopulated) {
        LOG.info("Cloud Spanner mutations already populated.");
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
      String projectId, String instanceId, String databaseId, Util util) throws Exception {
    // STEP 1: Check DDL
    ImmutableList<String> ddl = util.queryDatabaseDdl(projectId, instanceId, databaseId);
    LOG.info("DDL in database " + databaseId + " with " + ddl.size() + " statements");
    if (!ddl.equals(GOOGLE_CLOUD_SPANNER_DDL)) {
      LOG.info("Expected:\n" + GOOGLE_CLOUD_SPANNER_DDL.toString());
      LOG.info("Actual:\n" + ddl.toString());
      throw new Exception("Database does not appear to have expected DDL");
    }

    // STEP 2: Check Database Content
    ImmutableList<Struct> fooResultSet =
        util.performSingleSpannerQuery(
            projectId, instanceId, databaseId, "SELECT * FROM " + FOO_TABLE_NAME + ";");
    LOG.info("Number of rows in table " + FOO_TABLE_NAME + " = " + fooResultSet.size());

    ImmutableList<Struct> parentResultSet =
        util.performSingleSpannerQuery(
            projectId, instanceId, databaseId, "SELECT * FROM " + PARENT_TABLE_NAME + ";");
    LOG.info("Number of rows in table " + PARENT_TABLE_NAME + " = " + parentResultSet.size());

    ImmutableList<Struct> childResultSet =
        util.performSingleSpannerQuery(
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
    if (!fooResultSet.get(0).getString("colString").equals("helloString")) {
      throw new Exception("Contents of foo string do not match");
    }
    if (fooResultSet.get(0).getDouble("colFloat") != 32.53829d) {
      throw new Exception("Contents of col float do not match");
    }
    if (!fooResultSet.get(1).getString("colString").equals("hello world")) {
      throw new Exception("Contents of foo string do not match");
    }
    if (!childResultSet.get(0).getString("child_bar_string").equals("child_hello")) {
      throw new Exception("Contents of child string do not match");
    }
    if (!parentResultSet.get(0).getString("bar_string").equals("hello")) {
      throw new Exception("Contents of parent string do not match");
    }
  }
}
