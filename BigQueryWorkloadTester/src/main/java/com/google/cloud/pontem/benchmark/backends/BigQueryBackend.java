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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatistics;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryJobConfiguration.Priority;
import com.google.cloud.pontem.model.BigQueryResult;
import com.google.cloud.pontem.model.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

/** Backend that allows users to run interactive queries in BigQuery. */
public class BigQueryBackend {

  private static final Logger logger = Logger.getLogger(BigQueryBackend.class.getName());

  private static final String PONTEM_JOB_ID_PREFIX = "Pontem_BigQuery_WorkloadTester_";
  private final BigQuery bigQuery;

  public BigQueryBackend(BigQuery bigQuery) {
    this.bigQuery = bigQuery;
  }

  /**
   * Runs interactive queries in BigQuery.
   *
   * <p>Interactive queries are executed as soon as possible and should not be batched. They count
   * towards your concurrent rate limit and your daily limit.
   *
   * @param query The query to execute.
   * @return a BigQueryResult object containing execution details and whether execution was
   *     successful or not.
   * @throws BackendException if there were any BigQuery issues while executing the queries.
   */
  public BigQueryResult executeQuery(String query) throws BackendException {
    logger.fine("Executing BigQuery query");
    logger.finest("Query to execute: " + query);

    Job job = getJob(query);
    try {
      logger.fine("Waiting for Job to complete");
      // TODO(ldanielmadariaga): Use timeout? The default wait is 12h
      job = job.waitFor();
    } catch (BigQueryException | InterruptedException e) {
      throw new BackendException(e);
    }

    if (job == null) {
      throw new BackendException("BigQuery job is empty!");
    }

    return getBigQueryResult(job);
  }

  private Job getJob(final String query) throws BackendException {
    // Create a random Job ID so that we can safely retry.
    JobId jobId = JobId.of(PONTEM_JOB_ID_PREFIX + UUID.randomUUID().toString());
    QueryJobConfiguration queryConfig = getQueryJobConfiguration(query);
    JobInfo jobInfo = getJobInfo(jobId, queryConfig);

    Job job;
    try {
      job = bigQuery.create(jobInfo);
    } catch (BigQueryException e) {
      throw new BackendException(e);
    }

    return job;
  }

  private QueryJobConfiguration getQueryJobConfiguration(String query) {
    return QueryJobConfiguration.newBuilder(query)
        // TODO(ldanielmadariaga): Set User Agent for tracking
        .setUseLegacySql(false)
        .setUseQueryCache(false)
        .setPriority(Priority.INTERACTIVE)
        // TODO(ldanielmadariaga): allow large result?
        .build();
  }

  private JobInfo getJobInfo(JobId jobId, QueryJobConfiguration queryConfig) {
    return JobInfo.newBuilder(queryConfig).setJobId(jobId).build();
  }

  private BigQueryResult getBigQueryResult(final Job job) {
    // TODO(ldanielmadariaga): How do we read TimelineSample.activeUnits?

    Status status = Status.SUCCESS;
    List<String> errors = new ArrayList<>();
    JobStatus jobStatus = job.getStatus();
    if (jobStatus.getError() != null) {
      status = Status.ERROR;

      jobStatus.getExecutionErrors().forEach(e -> errors.add(e.getMessage()));
    }

    JobStatistics statistics = job.getStatistics();
    String jobId = job.getJobId().toString();

    return BigQueryResult.newBuilder()
        .setId(jobId)
        .setStatus(status)
        .setErrors(errors)
        .setCreationTime(statistics.getCreationTime())
        .setRuntime(statistics.getEndTime() - statistics.getStartTime())
        .build();
  }
}
