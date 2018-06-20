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
 * Dataflow job configuration options. Inherits standard {@code PipelineOptions} configuration
 * options.
 */
public interface BaseCloudSpannerRestoreOptions extends PipelineOptions {
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
   * Where to read the backup from. This should be a GCS bucket and it could also include a subpath
   * folder such as "gs://cloud-spanner-backup-test/backup-location"
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
   * Whether to create a database and tables as part of the backup process. If this option is set to
   * true and the database or table exist, then we will fail the entire restore operation as we do
   * not want to risk inadvertantly altering database structure or contents. In other words, if you
   * set this option to true, there cannot be an existing database of the name specified in
   * --outputSpannerDatabaseInstanceId
   */
  @Description("Whether to create database and tables if they do not exist.")
  @Default.Boolean(true)
  Boolean getShouldCreateDatabaseAndTables();

  void setShouldCreateDatabaseAndTables(Boolean value);

  /**
   * List of tables to include in the restore. If this is set, only these tables will be included in
   * the database restore. This value cannot be set if tablesToExcludeFromRestore is also set.
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

  @Description("Cloud Spanner host")
  @Required
  @Default.String(SpannerUtil.CLOUD_SPANNER_API_ENDPOINT_HOSTNAME)
  String getSpannerHost();

  void setSpannerHost(String value);
}
