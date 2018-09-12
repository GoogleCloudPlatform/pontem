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
import java.math.BigDecimal;
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
  public static final String FILE_PATH_FOR_TABLE_NAMES_ROW_COUNTS_FROM_JOB_METRICS =
      "metadata/table_row_counts_from_backup_job_metrics.txt";
  public static final String USER_AGENT_PREFIX = "pontem/0.0.3";

  // Use a delimeter that cannot be a part of a Cloud Spanner table name.
  public static final String TRANSFORM_NODE_NAME_DELIMITER = "-#-";

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
