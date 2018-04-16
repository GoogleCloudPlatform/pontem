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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 */
public class EndToEndHelper {
  private static final Logger LOG = LoggerFactory.getLogger(EndToEndHelper.class);

  public static final String PARENT_TABLE_NAME = "parent_table";
  public static final String CHILD_TABLE_NAME = "child_table";
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
      ImmutableList.of(PARENT_TABLE_MUTATION_1, PARENT_TABLE_MUTATION_2, CHILD_TABLE_MUTATION);
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
      System.out.println(e.getMessage());
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
    SpannerOptions options =
        SpannerOptions.newBuilder().setUserAgentPrefix(Util.USER_AGENT_PREFIX).build();
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
      spanner.close();
    }
  }

  private static void deleteGcsBucket(String projectId, String gcsBucketName) {
    StorageOptions.Builder optionsBuilder = StorageOptions.newBuilder();
    StorageOptions storageOptions = optionsBuilder.setProjectId(projectId).build();
    Storage storage = storageOptions.getService();

    Iterable<Blob> blobs =
        storage.list(gcsBucketName, Storage.BlobListOption.prefix("")).iterateAll();
    for (Blob blob : blobs) {
      blob.delete(Blob.BlobSourceOption.generationMatch());
    }

    storage.delete(gcsBucketName, Storage.BucketSourceOption.userProject(projectId));
  }

  private static void createCloudSpannerDatabaseAndTableStructure(
      String projectId, String instanceId, String databaseId, boolean shouldFailIfAlreadyCreated)
      throws Exception {
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
  }

  private static void createGcsBucket(
      String projectId, String bucketName, boolean shouldFailIfAlreadyCreated) {
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
  }

  private static void populateCloudSpannerDatabaseWithBasicContent(
      String projectId, String instanceId, String databaseId, boolean shouldFailIfAlreadyPopulated)
      throws Exception {
    SpannerOptions options =
        SpannerOptions.newBuilder().setUserAgentPrefix(Util.USER_AGENT_PREFIX).build();
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
  }

  public static void verifyGcsBackupMetaData(
      String projectId, String gcsRootBackupFolderPath, Util util) throws Exception {
    String rawFileContentsOfTableList =
        util.getContentsOfFileFromGcs(
            projectId,
            Util.getGcsBucketNameFromDatabaseBackupLocation(gcsRootBackupFolderPath),
            Util.getGcsFolderPathFromDatabaseBackupLocation(gcsRootBackupFolderPath),
            Util.FILE_PATH_FOR_DATABASE_TABLE_NAMES);
    String tables[] = rawFileContentsOfTableList.split("\\r?\\n");
    if (tables.length != 2) {
      throw new Exception("Table names not backed-up");
    }
    String tableName0 = tables[0].substring(0, tables[0].indexOf(','));
    String tableName1 = tables[1].substring(0, tables[1].indexOf(','));
    if (!tableName1.equals(PARENT_TABLE_NAME) || !tableName0.equals(CHILD_TABLE_NAME)) {
      throw new Exception("Table names not backed-up properly.\n" + tableName0 + "\n" + tableName1);
    }
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
    ImmutableList<Struct> parentResultSet =
        util.performSingleSpannerQuery(
            projectId, instanceId, databaseId, "SELECT * FROM " + PARENT_TABLE_NAME + ";");

    ImmutableList<Struct> childResultSet =
        util.performSingleSpannerQuery(
            projectId, instanceId, databaseId, "SELECT * FROM " + CHILD_TABLE_NAME + ";");

    // STEP 3: Check Row Results
    // STEP 3a: Check Row Result Count
    if (parentResultSet.size() + childResultSet.size() != MUTATIONS.size()) {
      throw new Exception("Number of rows in database is not as expected");
    }
    // STEP 3b: Check content
    if (!childResultSet.get(0).getString("child_bar_string").equals("child_hello")) {
      throw new Exception("Contents of child string do not match");
    }
    if (!parentResultSet.get(0).getString("bar_string").equals("hello")) {
      throw new Exception("Contents of parent string do not match");
    }
  }
}
