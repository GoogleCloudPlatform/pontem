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

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/**
 * Dataflow job configuration options. Inherits standard configuration options from {@code
 * PipelineOptions}.
 */
public interface BaseCloudSpannerBackupOptions extends PipelineOptions {
  /** Get the Google Cloud project id. */
  @Description("Google Cloud project id")
  @Required
  String getProjectId();

  void setProjectId(String value);

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

  /**
   * Whether to overwrite existing GCS file contents (if any contents exist). This prevents
   * unintended overwriting.
   */
  @Description("Whether to overwrite GCS file contents.")
  @Default.Boolean(false)
  Boolean getShouldOverwriteGcsFileBackup();

  void setShouldOverwriteGcsFileBackup(Boolean value);

  /**
   * Whether to backup DDL.
   *
   * <p>Cloud Spanner creates tables and indexes within a database using DDL (Data Definition
   * Language). This is the series of CREATE TABLE and CREATE INDEX commands that can be run. We can
   * backup these statements so the tables can be re-created.
   *
   * <p>WARNING: The backup of DDL relies on a Cloud Spanner API method and not a SQL query run in
   * Dataflow's SpannerIO.read() connector. Consequently, the Cloud Spanner API query to fetch the
   * DDL will not run at the exact same time as the backup of the underlying contents. Consequently,
   * it is possible that a database admin could alter the schema of the table between the time the
   * DDL is backed-up using the Cloud Spanner API and the time Dataflow takes a snapshot of the
   * database for use by SpannerIO.read(). This is highly unlikely but it is theoritically possible.
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
   * List of tables to include in the backup. If this is set, only these tables will be included in
   * the database backup. This value cannot be set if tablesToExcludeFromBackup is also set.
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

  @Description("Cloud Spanner host")
  @Required
  @Default.String(SpannerUtil.CLOUD_SPANNER_API_ENDPOINT_HOSTNAME)
  String getSpannerHost();

  void setSpannerHost(String value);
}
