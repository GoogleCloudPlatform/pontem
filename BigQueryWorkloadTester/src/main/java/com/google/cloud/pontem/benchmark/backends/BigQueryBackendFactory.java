/*
 * Copyright 2019 Google LLC
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

package com.google.cloud.pontem.benchmark.backends;

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.pontem.auth.BigQueryCredentialManager;
import com.google.cloud.pontem.config.WorkloadSettings;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.logging.Logger;

/** A factory for BigQueryBackend objects. */
public class BigQueryBackendFactory {

  private static final Logger logger = Logger.getLogger(BigQueryBackendFactory.class.getName());

  private static final String USER_AGENT_HEADER = "user-agent";
  private static final String USER_AGENT_VALUE = "pontem,bigquery-workload/0.0.1";

  /**
   * Ensures that the BigQueryBackend and it's dependencies are properly built and configured.
   *
   * @return a properly configured BigQueryBackend
   */
  public static BigQueryBackend getBigQueryBackend(
      final BigQueryCredentialManager bigQueryCredentialManager, final WorkloadSettings workload)
      throws IOException {
    logger.fine("Building BigQueryBackend.");

    GoogleCredentials googleCredentials =
        bigQueryCredentialManager.getCredentialsFromFile(workload.getCloudCredentialsFile());

    HeaderProvider headerProvider =
        FixedHeaderProvider.create(ImmutableMap.of(USER_AGENT_HEADER, USER_AGENT_VALUE));

    BigQuery bigQuery =
        BigQueryOptions.newBuilder()
            .setProjectId(workload.getProjectId())
            .setCredentials(googleCredentials)
            .setHeaderProvider(headerProvider)
            .build()
            .getService();

    return new BigQueryBackend(bigQuery);
  }
}
