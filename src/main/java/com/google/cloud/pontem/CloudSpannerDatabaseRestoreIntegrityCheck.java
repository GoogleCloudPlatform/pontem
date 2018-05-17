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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Verify the integrity of a restore of a Cloud Spanner database.
 *
 * <p>Integrity Check which verifies that the restore completed successfully. In particular, this
 * checks the list of tables restored and the number of rows restored. It does not check the
 * cell-level data values.
 *
 * <p>A sample run:
 *
 * <pre>
 * mvn compile exec:java \
 *   -Dexec.mainClass=com.google.cloud.pontem.CloudSpannerDatabaseRestoreIntegrityCheck \
 *   -Dexec.args="--project=my-cloud-spanner-project \
 *                --backupJobId=2017-12-28_08_21_49-14004506096652894727 \
 *                --restoreJobId=2017-12-28_10_43_41-10818313728228686289 \
 *                --restoreJobId=2017-12-28_10_46_20-2492938473109299932 \
 *                --restoreJobId=2017-12-28_10_48_47-17657844663911609681 \
 *                --restoreJobId=2017-12-28_10_51_16-6862684906618456059 \
 *                --restoreJobId=2017-12-28_10_53_55-7680195084002915695 \
 *                --restoreJobId=2017-12-28_10_43_41-17045671774098975520 \
 *                --restoreJobId=2017-12-28_10_43_41-16236058809269230845 \
 *                --databaseBackupLocation=gs://my-cloud-spanner-project/words_db_apache2.2.0_b/ \
 *                --areAllTablesRestored=false"
 * </pre>
 *
 * <p>A sample run that requires every table to have been restored:
 *
 * <pre>
 * mvn compile exec:java \
 *   -Dexec.mainClass=com.google.cloud.pontem.CloudSpannerDatabaseRestoreIntegrityCheck \
 *   -Dexec.args="--project=my-cloud-spanner-project \
 *                --backupJobId=2017-12-28_08_21_49-14004506096652894727 \
 *                --restoreJobId=2017-12-28_10_43_41-10818313728228686289 \
 *                --restoreJobId=2017-12-28_10_46_20-2492938473109299932 \
 *                --restoreJobId=2017-12-28_10_48_47-17657844663911609681 \
 *                --restoreJobId=2017-12-28_10_51_16-6862684906618456059 \
 *                --restoreJobId=2017-12-28_10_53_55-7680195084002915695 \
 *                --restoreJobId=2017-12-28_10_43_41-17045671774098975520 \
 *                --restoreJobId=2017-12-28_10_43_47-170458491231849489454 \
 *                --restoreJobId=2017-12-28_10_43_41-16236058809269230845 \
 *                --databaseBackupLocation=gs://my-cloud-spanner-project/words_db_apache2.2.0_b/ \
 *                --areAllTablesRestored=true"
 * </pre>
 */
public class CloudSpannerDatabaseRestoreIntegrityCheck {

  public static Options configureCommandlineOptions() {
    Options options = new Options();

    /** The Google Cloud project id. */
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

    /**
     * The Dataflow job id for the job that performed the Spanner database backup. If the Dataflow
     * job history/meta-data is no longer available (Dataflow job history expires in a month), this
     * program may/may not work depending upon whether data from the backup job is available
     * elsewhere.
     */
    Option backupJobId =
        new Option("b", "backupJobId", true, "Google Cloud Dataflow Backup Job Id");
    backupJobId.setRequired(true);
    options.addOption(backupJobId);

    /**
     * Since restoring a single database involves multiple pipelines (dataflow jobs), multiple job
     * ids need to be acceptable. Do not pass all restore Dataflow job IDs to a single flag as a CSV
     * but rather pass each restore job ID to another instance of this flag.
     */
    Option restoreJobId =
        new Option("r", "restoreJobId", true, "Google Cloud Dataflow Restore Job Id");
    restoreJobId.setRequired(true);
    options.addOption(restoreJobId);

    /**
     * Whether all the tables backed-up were restored (and therefore should be validated). If you
     * only restored 25% of the tables you backed-up, then this needs to be set to false.
     */
    Option areAllTablesRestored =
        new Option("a", "areAllTablesRestored", true, "Whether all tables must be restored");
    areAllTablesRestored.setRequired(true);
    options.addOption(areAllTablesRestored);

    return options;
  }

  private static final Logger LOG =
      Logger.getLogger(CloudSpannerDatabaseRestoreIntegrityCheck.class.getName());

  public static void main(String[] args) throws Exception {
    // STEP 1: Parse inputs.
    Options options = configureCommandlineOptions();

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      LOG.warning(e.getMessage());
      formatter.printHelp("utility-name", options);

      System.exit(1);
      return;
    }

    String projectId = cmd.getOptionValue("project");
    String[] restoreJobIds = cmd.getOptionValues("restoreJobId");
    String backupJobId = cmd.getOptionValue("backupJobId");
    String databaseBackupLocation = cmd.getOptionValue("databaseBackupLocation");
    String gcsBucketName =
        Util.getGcsBucketNameFromDatabaseBackupLocation(
            cmd.getOptionValue("databaseBackupLocation"));
    String gcsFolderPath =
        Util.getGcsFolderPathFromDatabaseBackupLocation(
            cmd.getOptionValue("databaseBackupLocation"));
    boolean requireAllTablesRestored =
        Boolean.parseBoolean(cmd.getOptionValue("areAllTablesRestored"));
    Util util = new Util();

    performDatabaseRestoreIntegrityCheck(
        projectId,
        restoreJobIds,
        backupJobId,
        gcsBucketName,
        gcsFolderPath,
        requireAllTablesRestored,
        util);

    LOG.info("Database Restore Integrity Check Complete");
  }

  /**
   * Perform database restoration integrity check to validate that the appropriate number of rows
   * and tables were restored.
   */
  public static boolean performDatabaseRestoreIntegrityCheck(
      String projectId,
      String[] restoreJobIds,
      String backupJobId,
      String gcsBucketName,
      String gcsFolderPath,
      boolean requireAllTablesRestored,
      Util util)
      throws Exception {

    // STEP 2: Get backup data.
    // STEP 2a: Get backup metrics from dataflow job.
    Map<String, Long> tableNameToNumRowsFromBackup = new HashMap<String, Long>();
    try {
      LOG.info("Fetching metrics for jobid " + backupJobId);
      JobMetrics backupJobMetrics = util.fetchMetricsForDataflowJob(projectId, backupJobId);

      // STEP 2b: Parse data from backup using job metrics
      tableNameToNumRowsFromBackup = Util.getTableRowCountsFromJobMetrics(backupJobMetrics);
    } catch (Exception e) {
      // Unable to fetch backup job metrics from dataflow, so fallback to
      // the data written to disk during the backup integrity check.
      String tableRowCountsFileContents =
          util.getContentsOfFileFromGcs(
              projectId,
              gcsBucketName,
              gcsFolderPath,
              Util.FILE_PATH_FOR_TABLE_NAMES_ROW_COUNTS_FROM_JOB_METRICS);

      tableNameToNumRowsFromBackup =
          Util.convertTableMetadataContentsToMap(tableRowCountsFileContents);
    }

    // STEP 3: Get restore data.
    // STEP 3a: Get restore metrics from dataflow job.
    List<JobMetrics> restoreJobMetrics = new ArrayList<JobMetrics>();
    for (String restoreJobId : restoreJobIds) {
      LOG.info("Fetching metrics for jobid " + restoreJobId);
      restoreJobMetrics.add(util.fetchMetricsForDataflowJob(projectId, restoreJobId));
    }

    // STEP 3a: Parse data metrics from restore job.
    Map<String, Long> tableNameToNumRowsFromRestoreJobMetrics = new HashMap<String, Long>();
    for (JobMetrics restoreJobMetric : restoreJobMetrics) {
      tableNameToNumRowsFromRestoreJobMetrics.putAll(
          Util.getTableRowCountsFromJobMetrics(restoreJobMetric));
    }

    // STEP 4: Compare table names and (possibly) row counts between backup & restore
    // STEP 4a: If all tables are required to be backed-up, then ensure that all the backed-up
    // table names are present in the restore.
    if (requireAllTablesRestored
        && tableNameToNumRowsFromBackup.size() != tableNameToNumRowsFromRestoreJobMetrics.size()) {
      throw new DataIntegrityErrorException(
          "Database backup integrity issue found: Number of tables restored"
              + " ("
              + tableNameToNumRowsFromRestoreJobMetrics.size()
              + ") does not equal the number of tables backed-up"
              + " ("
              + tableNameToNumRowsFromBackup.size()
              + ")");
    }

    for (Map.Entry<String, Long> entry : tableNameToNumRowsFromRestoreJobMetrics.entrySet()) {
      if (tableNameToNumRowsFromBackup.get(entry.getKey()) != entry.getValue()) {
        throw new DataIntegrityErrorException(
            "Table row counts between backup and restore do not match for table " + entry.getKey());
      }
    }

    return true;
  }
}
