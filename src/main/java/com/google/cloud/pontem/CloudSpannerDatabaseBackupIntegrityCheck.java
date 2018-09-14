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

import com.google.api.services.dataflow.model.JobMetrics;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Verify the integrity of a backup of a Cloud Spanner database.
 *
 * <p>Database Backup Integrity Check verifies that the backup completed successfully. In
 * particular, this checks the list of tables backed-up and the number of rows backed-up. It does
 * not check the cell-level data values.
 *
 * <p>A sample run:
 *
 * <pre>
 * mvn compile exec:java \
 *   -Dexec.mainClass=com.google.cloud.pontem.CloudSpannerDatabaseBackupIntegrityCheck \
 *   -Dexec.args="--project=my-cloud-spanner-project \
 *                --databaseBackupLocation=gs://my-cloud-spanner-project/multi-backup \
 *                --job=2017-10-25_11_18_28-6233650047978038157"
 * </pre>
 *
 * </pre>
 */
public class CloudSpannerDatabaseBackupIntegrityCheck {

  public static Options configureCommandlineOptions() {
    Options options = new Options();

    /** Google Cloud project ID. */
    Option projectId = new Option("p", "project", true, "Google Cloud Project Id");
    projectId.setRequired(true);
    options.addOption(projectId);

    /** The GCS path to the folder that contains the backup of a Cloud Spanner database. */
    Option databaseBackupLocation =
        new Option(
            "d",
            "databaseBackupLocation",
            true,
            "Path to GCS folder that contains the Cloud Spanner database backup");
    databaseBackupLocation.setRequired(true);
    options.addOption(databaseBackupLocation);

    /** The Dataflow job id for the job that performed the Spanner database backup. */
    Option jobId = new Option("j", "job", true, "Google Cloud Dataflow Job Id");
    jobId.setRequired(true);
    options.addOption(jobId);

    /**
     * Whether to write to GCS a file containing the table row counts based upon the backup job
     * metrics (i.e., the metrics that are generated in executing the backup job -- metrics such as
     * rows read from the Dataflow source connector). The reason for this is that the Dataflow
     * metrics (e.g., rows read, bytes written) expire after a month. So, if a restoration job is
     * undertaken and we want to verify that restoration job, the corresponding backup row counts
     * from the backup dataflow job metrics would no longer exist. So, to get around this, during
     * the backup verification, write the row counts to disk in GCS to make them available later. By
     * default, we do not skip writing row counts (i.e., we write row counts by default).
     */
    Option skipWriteRowCountsOfVerifiedBackupToGcs =
        new Option(
            "skipWriteRowCountsOfVerifiedBackupToGcs",
            "Whether to skip writingthe row counts for each table to disk (GCS)");
    options.addOption(skipWriteRowCountsOfVerifiedBackupToGcs);

    return options;
  }

  private static final Logger LOG =
      Logger.getLogger(CloudSpannerDatabaseBackupIntegrityCheck.class.getName());

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

    String projectId = cmd.getOptionValue("project");
    String jobId = cmd.getOptionValue("job");
    String gcsBucketName =
        GcsUtil.getGcsBucketNameFromDatabaseBackupLocation(
            cmd.getOptionValue("databaseBackupLocation"));
    String gcsFolderPath =
        GcsUtil.getGcsFolderPathFromDatabaseBackupLocation(
            cmd.getOptionValue("databaseBackupLocation"));
    boolean shouldSkipWriteRowCountsOfVerifiedBackupToGcs =
        Boolean.valueOf(cmd.getOptionValue("skipWriteRowCountsOfVerifiedBackupToGcs"));
    GcsUtil gcsUtil = new GcsUtil();
    Util util = new Util();

    performDatabaseBackupIntegrityCheck(
        projectId,
        jobId,
        gcsBucketName,
        gcsFolderPath,
        shouldSkipWriteRowCountsOfVerifiedBackupToGcs,
        gcsUtil,
        util);

    System.out.println("Database Backup Integrity Check Complete");
  }

  /**
   * Perform database backup integrity check to validate that the appropriate number of rows and
   * tables were restored.
   */
  public static boolean performDatabaseBackupIntegrityCheck(
      String projectId,
      String jobId,
      String gcsBucketName,
      String gcsFolderPath,
      boolean shouldSkipWriteRowCountsOfVerifiedBackupToGcs,
      GcsUtil gcsUtill,
      Util util)
      throws Exception {

    // STEP 2: Pull metadata about backup from GCS.
    // STEP 2a: Fetch file from GCS.
    String rawContentsOfTableNames =
        gcsUtill.getContentsOfFileFromGcs(
            projectId, gcsBucketName, gcsFolderPath, Util.FILE_PATH_FOR_DATABASE_TABLE_NAMES);

    // STEP 2b: Parse table names file into Set
    Set<String> tableNamesFromGcs = Util.convertTablenamesIntoSet(rawContentsOfTableNames);

    // STEP 3: Perform rudamentary check to ensure table names match
    boolean areTableNamesValid = false;

    // STEP 4: Check Dataflow job metrics for elements read/written against expected values.
    // STEP 4a: Get all job metrics
    JobMetrics jobMetrics = util.fetchMetricsForDataflowJob(projectId, jobId);

    // STEP 4b: Get row counts per table
    Map<String, Long> tableNameToNumRowsFromJobMetrics =
        Util.getTableRowCountsFromJobMetrics(jobMetrics);

    // STEP 4c: Validate table data
    boolean areTableRowCountsValid = false;
    // No metadata file with row counts exists, so fetch only check table names.
    areTableNamesValid =
        validateTableNamesOnly(tableNameToNumRowsFromJobMetrics, tableNamesFromGcs);
    if (!areTableNamesValid) {
      throw new DataIntegrityErrorException("Number of tables does not match");
    }

    // STEP 5: Write the verified backup row counts to disk for use later.
    if (!shouldSkipWriteRowCountsOfVerifiedBackupToGcs) {
      String rowCountContents = "";
      for (Map.Entry<String, Long> table : tableNameToNumRowsFromJobMetrics.entrySet()) {
        rowCountContents += table.getKey() + "," + table.getValue() + "\n";
      }
      rowCountContents = rowCountContents.trim();
      gcsUtill.writeContentsToGcs(
          rowCountContents,
          projectId,
          gcsBucketName,
          gcsFolderPath,
          Util.FILE_PATH_FOR_TABLE_NAMES_ROW_COUNTS_FROM_JOB_METRICS);
    }

    return true;
  }

  private static boolean validateTableNamesOnly(
      Map<String, Long> tableNameToNumRowsFromJobMetrics, Set<String> tableNames) {
    boolean isDataValid = true;
    if (tableNameToNumRowsFromJobMetrics.size() != tableNames.size()) {
      System.out.println("Table names validation valied due to different number of tables.");
      isDataValid = false;
    }
    for (String tableName : tableNames) {
      if (!tableNameToNumRowsFromJobMetrics.containsKey(tableName)) {
        System.out.println("Table name " + tableName + " not found.");
        isDataValid = false;
      }
    }
    return isDataValid;
  }
}
