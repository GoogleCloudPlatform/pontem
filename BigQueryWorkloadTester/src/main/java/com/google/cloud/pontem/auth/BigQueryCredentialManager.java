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

package com.google.cloud.pontem.auth;

import com.google.auth.oauth2.GoogleCredentials;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/** Exchanges credentials that can be used to authenticate against the BigQuery backend. */
public class BigQueryCredentialManager {

  private static final List<String> BIGQUERY_SCOPES =
      Arrays.asList("https://www.googleapis.com/auth/bigquery");

  /**
   * Retrieves GoogleCredentials if credentialsFile is empty it will fall-back to the
   * default config.
   *
   * @param credentialsFile Optional containing the credentials file path
   * @return the properly configured GoogleCredentials object
   * @throws IOException if file could not be read
   */
  public GoogleCredentials getCredentialsFromFile(final Optional<String> credentialsFile)
      throws IOException {
    GoogleCredentials googleCredentials;

    if (credentialsFile.isPresent()) {
      InputStream inputStream = new FileInputStream(credentialsFile.get());
      googleCredentials = GoogleCredentials.fromStream(inputStream).createScoped(BIGQUERY_SCOPES);
    } else {
      // Do we need scopes or are they read from the default
      googleCredentials = GoogleCredentials.getApplicationDefault().createScoped(BIGQUERY_SCOPES);
    }

    return googleCredentials;
  }
}
