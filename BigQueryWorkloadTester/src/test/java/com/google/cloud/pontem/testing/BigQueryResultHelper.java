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

package com.google.cloud.pontem.testing;

import com.google.cloud.pontem.model.BigQueryResult;
import com.google.cloud.pontem.model.Status;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Helper that builds BigQueryResult objects for testing. */
public class BigQueryResultHelper {

  public static BigQueryResult getSuccessfulBigQueryResult() {
    return getSuccessfulBigQueryResult(TestConstants.JOB_ID.toString());
  }

  /**
   * Returns a successful BigQueryResult.
   *
   * @param jobId The JobId
   * @return BigQueryResult
   */
  public static BigQueryResult getSuccessfulBigQueryResult(final String jobId) {
    return getBigQuerySharedValues(jobId)
        .setStatus(Status.SUCCESS)
        .setErrors(new ArrayList<>())
        .setRuntime(TestConstants.SUCCESSFUL_QUERY_RUNTIME)
        .build();
  }

  /**
   * Returns a BigQueryResult with query errors.
   *
   * @return BigQueryResult
   */
  public static BigQueryResult getBigQueryResultWithQueryErrors() {
    return getBigQueryResultWithQueryErrors(
        TestConstants.JOB_ID.toString(), TestConstants.ERRORS.get(0).getMessage());
  }

  /**
   * Returns a BigQueryResult with query errors and an error message.
   * @param jobId The JobId
   * @param errorMessage A error message to include in the BigQueryResult
   * @return
   */
  public static BigQueryResult getBigQueryResultWithQueryErrors(
      final String jobId, String errorMessage) {
    return getBigQuerySharedValues(jobId)
        .setStatus(Status.ERROR)
        .setErrors(Arrays.asList(errorMessage))
        .setRuntime(TestConstants.FAILING_QUERY_RUNTIME)
        .build();
  }

  public static BigQueryResult getBigQueryResultWithMultipleQueryErrors() {
    return getBigQueryResultWithMultipleQueryErrors(TestConstants.JOB_ID.toString());
  }

  public static BigQueryResult getBigQueryResultWithMultipleQueryErrors(final String jobId) {
    return getBigQueryResultWithMultipleQueryErrors(jobId, TestConstants.ERROR_MESSAGES);
  }

  /**
   * Returns a BigQueryResult with multiple query errors.
   * @param jobId The JobId
   * @param errorMessages A List of Strings containing error messages
   * @return
   */
  public static BigQueryResult getBigQueryResultWithMultipleQueryErrors(
      final String jobId, final List<String> errorMessages) {
    return getBigQuerySharedValues(jobId)
        .setStatus(Status.ERROR)
        .setErrors(errorMessages)
        .setRuntime(TestConstants.FAILING_QUERY_RUNTIME)
        .build();
  }

  private static BigQueryResult.Builder getBigQuerySharedValues(String jobId) {
    return BigQueryResult.newBuilder().setId(jobId).setCreationTime(TestConstants.CREATION_TIME);
  }
}
