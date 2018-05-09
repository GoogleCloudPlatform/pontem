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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.JobMetrics;
import com.google.api.services.dataflow.model.MetricStructuredName;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/** Utility function for Cloud Spanner backup and restore. */
public class Util {
  private static final Logger LOG = Logger.getLogger(Util.class.getName());

  public static final String READ_DATA_TRANSFORM_NODE_NAME = "Read_Data";
  public static final String WRITE_DATA_TRANSFORM_NODE_NAME = "Write_Data";
  public static final String MAP_DATA_TRANSFORM_NODE_NAME = "Map_Data_to_Write";
  public static final String FILE_PATH_FOR_DATABASE_TABLE_NAMES =
      "metadata/database_table_names.txt";
  public static final String FILE_PATH_FOR_DATABASE_TABLE_NAMES_ROW_COUNTS =
      "metadata/database_tablerow_counts.txt";
  public static final String FILE_PATH_FOR_DATABASE_DDL = "metadata/database_ddl.txt";
  public static final String FILE_PATH_FOR_DATABASE_TABLE_SCHEMAS_FOLDER =
      "metadata/table_schemas/";
  public static final String FILE_PATH_FOR_TABLE_NAMES_ROW_COUNTS_FROM_JOB_METRICS =
      "metadata/table_row_counts_from_backup_job_metrics.txt";
  public static final String USER_AGENT_PREFIX = "pontem/0.0.1";

  // Use a delimeter that cannot be a part of a Cloud Spanner table name.
  public static final String TRANSFORM_NODE_NAME_DELIMITER = "-#-";

  // Use a delimeter to delinate the statements in a database DDL.
  public static final String DDL_DELIMITER = "\n##\n";

  /**
   * Fetch the DDL for the database. See https://bit.ly/2qVpToj for more.
   *
   * @return DDL for the database in sequential order.
   */
  public ImmutableList<String> queryDatabaseDdl(
      String projectId, String instance, String databaseId) {
    SpannerOptions options =
        SpannerOptions.newBuilder().setUserAgentPrefix(Util.USER_AGENT_PREFIX).build();
    Spanner spanner = options.getService();

    List<String> ddl = Lists.newArrayList();
    try {
      DatabaseId db = DatabaseId.of(projectId, instance, databaseId);
      DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
      ddl = dbAdminClient.getDatabaseDdl(instance, databaseId);
    } finally {
      spanner.close();
    }
    LOG.info("Raw DDL statements from database " + databaseId + ":");
    for (String ddlStatement : ddl) {
      LOG.info(ddlStatement + "\n");
    }
    return ImmutableList.copyOf(ddl);
  }

  /**
   * Return structs from a single query. Result structs must fit into memory, so these cannot be
   * huge queries.
   */
  public ImmutableList<Struct> performSingleSpannerQuery(
      String projectId, String instance, String databaseId, String querySql) {
    SpannerOptions options =
        SpannerOptions.newBuilder().setUserAgentPrefix(Util.USER_AGENT_PREFIX).build();
    Spanner spanner = options.getService();

    List<Struct> resultsAsStruct = Lists.newArrayList();
    try {
      DatabaseId db = DatabaseId.of(projectId, instance, databaseId);
      DatabaseClient dbClient = spanner.getDatabaseClient(db);

      ResultSet resultSet = dbClient.singleUse().executeQuery(Statement.of(querySql));
      while (resultSet.next()) {
        resultsAsStruct.add(resultSet.getCurrentRowAsStruct());
      }
    } finally {
      spanner.close();
    }
    return ImmutableList.copyOf(resultsAsStruct);
  }

  /**
   * Create database and populate it with a specific database definition language.
   *
   * <p>See https://cloud.google.com/spanner/docs/data-definition-language
   */
  public void createDatabaseAndTables(
      String projectId, String instanceId, String databaseId, ImmutableList<String> databaseDdl)
      throws Exception {
    SpannerOptions options =
        SpannerOptions.newBuilder().setUserAgentPrefix(Util.USER_AGENT_PREFIX).build();
    Spanner spanner = options.getService();
    try {
      DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);
      DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();

      dbAdminClient = spanner.getDatabaseAdminClient();
      LOG.info(
          "Attempting to create database named '"
              + databaseId
              + "' in instance '"
              + instanceId
              + "' and project '"
              + projectId
              + "'");
      Operation<Database, CreateDatabaseMetadata> operation =
          dbAdminClient.createDatabase(instanceId, databaseId, databaseDdl).waitFor();
      // If the database already exists, then this attempt to create it again will fail and
      // throw an exception.
      if (!operation.isSuccessful()) {
        throw new Exception(
            "Failure creating database named '"
                + databaseId
                + "' in instance '"
                + instanceId
                + "' and project '"
                + projectId
                + "'");
      }
    } catch (SpannerException e) {
      LOG.info("Error creating database " + databaseId + ":\n" + e.toString());
      throw e;
    } finally {
      spanner.close();
    }
  }

  public static String convertDdlListIntoRawText(ImmutableList<String> ddlStatements) {
    String databaseDdlAsString = String.join(DDL_DELIMITER, ddlStatements);
    return databaseDdlAsString;
  }

  /**
   * Take in raw text version of a database's DDL and return a list of DDL statements. The list of
   * DDL statements can be executed sequentially.
   *
   * <p>See https://cloud.google.com/spanner/docs/data-definition-language
   */
  public static ImmutableList<String> convertRawDdlIntoDdlList(String rawDdl) {
    List<String> statements = new ArrayList<String>(Arrays.asList(rawDdl.split(DDL_DELIMITER)));
    return ImmutableList.copyOf(statements);
  }

  /**
   * Fetch the contents of a text file from Google Cloud Storage and return it as a string.
   *
   * @param pathToRootOfBackup e.g., "/backups/latest/" if the file in question is
   *     "gs://my-cloud-spanner-project/backup_today/table_schema/mytable.txt"
   * @param filenameFromRootOfBackup e.g., "table_schema/mytable.txt" if the file in question is
   *     "gs://my-cloud-spanner-project/backup_today/table_schema/mytable.txt"
   */
  public String getContentsOfFileFromGcs(
      String projectId,
      String bucketName,
      String pathToRootOfBackup,
      String filenameFromRootOfBackup)
      throws Exception {
    StorageOptions.Builder optionsBuilder = StorageOptions.newBuilder();
    StorageOptions storageOptions = optionsBuilder.setProjectId(projectId).build();
    Storage storage = storageOptions.getService();

    // Blob path should not start with '/'
    if (pathToRootOfBackup.charAt(0) == '/') {
      pathToRootOfBackup = pathToRootOfBackup.substring(1);
    }

    BlobId blobId = BlobId.of(bucketName, pathToRootOfBackup + filenameFromRootOfBackup);
    Blob blob = storage.get(blobId);
    if (blob == null) {
      throw new Exception(
          "No such object in GCS:\nBucketName: "
              + bucketName
              + "\nPathToRoot: "
              + pathToRootOfBackup
              + "\nFilenameFromRoot: "
              + filenameFromRootOfBackup
              + "\n\nBlobId: "
              + blobId.toString());
    }

    if (blob.getSize() < 1_000_000) {
      // Blob is small, so read all its content in one request
      byte[] content = blob.getContent();
      String fileContents = new String(content);
      return fileContents;
    }
    throw new Exception("Metadata file is unexpectedly large");
  }

  /**
   * Write the contents of a file to disk as text in GCS.
   *
   * @param pathToRootOfBackup e.g., "/backups/latest/" if the file in question is
   *     "gs://my-cloud-spanner-project/backup_today/table_schema/mytable.txt"
   * @param filenameFromRootOfBackup e.g., "table_schema/mytable.txt" if the file in question is
   *     "gs://my-cloud-spanner-project/backup_today/table_schema/mytable.txt"
   */
  public void writeContentsToGcs(
      String contents,
      String projectId,
      String bucketName,
      String pathToRootOfBackup,
      String filenameFromRootOfBackup) {

    StorageOptions.Builder optionsBuilder = StorageOptions.newBuilder();
    StorageOptions storageOptions = optionsBuilder.setProjectId(projectId).build();
    Storage storage = storageOptions.getService();

    // Blob path should not start with '/'
    if (pathToRootOfBackup.charAt(0) == '/') {
      pathToRootOfBackup = pathToRootOfBackup.substring(1);
    }

    BlobId blobId = BlobId.of(bucketName, pathToRootOfBackup + filenameFromRootOfBackup);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();

    byte[] bytes = contents.getBytes();
    // since file will be less than 1MB, create the blob in one request.
    storage.create(blobInfo, bytes);
  }

  /**
   * @param databaseBackupLocation E.g., gs://my-cloud-spanner-project/multi-backup
   * @return GCS bucket E.g., "my-cloud-spanner-project"
   */
  public static String getGcsBucketNameFromDatabaseBackupLocation(String databaseBackupLocation)
      throws Exception {
    URI uri = new URI(databaseBackupLocation);
    if (!uri.getScheme().toLowerCase().equals("gs")) {
      throw new Exception("Database backup location looks malformed");
    }
    return uri.getAuthority();
  }

  /**
   * @param databaseBackupLocation E.g., gs://my-cloud-spanner-project/multi-backup
   * @return Path from root of GCS bucket to root of database backup E.g., /multi-backup/
   */
  public static String getGcsFolderPathFromDatabaseBackupLocation(String databaseBackupLocation)
      throws Exception {
    URI uri = new URI(databaseBackupLocation);
    if (!uri.getScheme().toLowerCase().equals("gs")) {
      throw new Exception("Database backup location looks malformed");
    }
    String path = uri.getPath();
    if (path.length() == 0) {
      path = "/";
    }
    if (path.length() > 1 && path.charAt(0) != '/') {
      path = '/' + path;
    }
    if (path.charAt(path.length() - 1) != '/') {
      path += '/';
    }
    return path;
  }

  /**
   * Receive contents of meta-data file listing table names and return {@code Set} of table names.
   */
  public static Set<String> convertTablenamesIntoSet(String rawFileContents) {
    String lines[] = rawFileContents.split("\\r?\\n");
    Set<String> tableNames = new HashSet<String>();
    for (int i = 0; i < lines.length; i++) {
      if (lines[i].indexOf(',') < 0) {
        tableNames.add(lines[i]);
      } else {
        tableNames.add(lines[i].substring(0, lines[i].indexOf(',')));
      }
    }
    return tableNames;
  }

  /**
   * Receive contents of meta-data file listing table names and number of rows. Return a {@code Map}
   * that maps the table name to its corresponding number of rows.
   */
  public static Map<String, Long> convertTableMetadataContentsToMap(String rawFileContents)
      throws Exception {
    Map<String, Long> metadataMap = new HashMap<String, Long>();
    String lines[] = rawFileContents.split("\\r?\\n");
    for (int i = 0; i < lines.length; i++) {
      if (lines[i].indexOf(',') < 0) {
        throw new Exception("Formatting issue in metadata file");
      }
      String tableName = lines[i].substring(0, lines[i].indexOf(','));
      Long numRows = Long.parseLong(lines[i].substring(lines[i].indexOf(',') + 1));
      metadataMap.put(tableName, numRows);
    }
    if (metadataMap.size() != lines.length) {
      throw new Exception("Mismatch parsing metadata");
    }
    LOG.warning("Found " + metadataMap.size() + " tables in metadata");
    return metadataMap;
  }

  /**
   * Fetch Google Cloud Dataflow job metrics {@code JobMetrics} for a specific Dataflow job that is
   * a part of a Cloud project.
   */
  public JobMetrics fetchMetricsForDataflowJob(String projectId, String jobId) throws Exception {
    // Authentication is provided by gcloud tool when running locally
    // and by built-in service accounts when running on GAE, GCE or GKE.
    GoogleCredential credential = GoogleCredential.getApplicationDefault();

    // The createScopedRequired method returns true when running on GAE or a local developer
    // machine. In that case, the desired scopes must be passed in manually. When the code is
    // running in GCE, GKE or a Managed VM, the scopes are pulled from the GCE metadata server.
    // See https://developers.google.com/identity/protocols/application-default-credentials for more
    // information.
    if (credential.createScopedRequired()) {
      credential =
          credential.createScoped(
              Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    }

    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
    Dataflow dataflowService =
        new Dataflow.Builder(httpTransport, jsonFactory, credential)
            .setApplicationName("Cloud Spanner Backup & Restore")
            .build();

    Dataflow.Projects.Jobs.GetMetrics request =
        dataflowService.projects().jobs().getMetrics(projectId, jobId);
    JobMetrics jobMetrics = request.execute();
    return jobMetrics;
  }

  /**
   * The exact version of Apache Beam used to execute the backup job will have an effect on dataflow
   * step order and step names (and therefore will affect parsing).
   *
   * <p>You can try out this API's getMetrics call manually for debugging using the following URL:
   * https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.jobs/getMetrics .
   *
   * <p>See https://goo.gl/qexnaZ
   */
  public static Map<String, Long> getTableRowCountsFromJobMetrics(JobMetrics jobMetrics)
      throws DataIntegrityErrorException {
    Map<String, Long> tableRowCounts = new HashMap<String, Long>();

    List<MetricUpdate> metrics = jobMetrics.getMetrics();
    // Sample MetricUpdate
    //
    // {
    //   "name": {
    //     "origin": "dataflow/v1b3",
    //     "name": "MeanByteCount",
    //     "context": {
    //       "output_user_name": "Read Table List/SpannerIO.CreateTransaction/Create
    // transaction-out0",
    //       "original_name": "Read Table List/SpannerIO.CreateTransaction/Create
    // transaction-out0-MeanByteCount",
    //       "tentative": "true"
    //     }
    //   },
    //   "scalar": 267,
    //   "updateTime": "2017-10-23T19:10:48.566Z"
    // }

    for (MetricUpdate metricUpdate : metrics) {
      MetricStructuredName metricStructuredName = metricUpdate.getName();
      if (metricStructuredName.getName().equals("ElementCount")) {
        Map<String, String> individualMetrics = metricStructuredName.getContext();
        // Sample Context
        //
        // "context": {
        //   "output_user_name": "Read Table List/SpannerIO.CreateTransaction/Create
        // transaction-out0",
        //   "original_name": "Read Table List/SpannerIO.CreateTransaction/Create
        // transaction-out0-MeanByteCount",
        //   "tentative": "true"
        // }
        if (!individualMetrics.containsKey("tentative")
            && (individualMetrics.containsKey("original_name")
                || individualMetrics.containsKey("output_user_name"))) {

          // Looking for lines such as:
          // "Read_Data-#-seven_words/Execute query-out0" corresponds to [num elems read from table]
          String prefixToSearchFor = "Read_Data" + Util.TRANSFORM_NODE_NAME_DELIMITER;
          String suffixToSearchFor = "/Execute query-out0";
          String metricValue = individualMetrics.get("output_user_name");
          if (metricValue.substring(0, prefixToSearchFor.length()).equals(prefixToSearchFor)
              && metricValue
                  .substring(metricValue.length() - suffixToSearchFor.length())
                  .equals(suffixToSearchFor)) {
            parseThenSetTableInfoInMap(tableRowCounts, metricUpdate, metricValue);
          }

          // Looking for lines such as:
          // "Map_Data_to_Write-#-six_words/Map-out0" corresponds to [num elems written to disk]
          prefixToSearchFor = "Map_Data_to_Write" + Util.TRANSFORM_NODE_NAME_DELIMITER;
          suffixToSearchFor = "/Map-out0";
          metricValue = individualMetrics.get("output_user_name");
          if (metricValue.substring(0, prefixToSearchFor.length()).equals(prefixToSearchFor)
              && metricValue
                  .substring(metricValue.length() - suffixToSearchFor.length())
                  .equals(suffixToSearchFor)) {
            parseThenSetTableInfoInMap(tableRowCounts, metricUpdate, metricValue);
          }
        }
      }
    }

    if (tableRowCounts.size() == 0) {
      LOG.warning(
          "Found 0 tables when parsing Job Metrics." + " Likely an error parsing Job Metrics.");
    }
    return tableRowCounts;
  }

  private static void parseThenSetTableInfoInMap(
      Map<String, Long> tableRowCounts, MetricUpdate metricUpdate, String metricValue)
      throws DataIntegrityErrorException {
    Long rowCount = ((BigDecimal) metricUpdate.getScalar()).longValue();
    // Avoid using overhead of RegEx
    // Tablename must be in format of "[delimiter][table name]/"
    String modifiedMetricValue =
        metricValue.substring(
            metricValue.indexOf(Util.TRANSFORM_NODE_NAME_DELIMITER)
                + Util.TRANSFORM_NODE_NAME_DELIMITER.length());
    String tableName = modifiedMetricValue.substring(0, modifiedMetricValue.indexOf('/'));
    if (tableRowCounts.containsKey(tableName) && tableRowCounts.get(tableName) != rowCount) {
      throw new DataIntegrityErrorException(
          "Inconsistent row count values in metrics for"
              + " table '"
              + tableName
              + "' Attempting to put row count value "
              + rowCount
              + " into table when existing"
              + " value is "
              + tableRowCounts.get(tableName));
    }
    tableRowCounts.put(tableName, rowCount);
  }
}
