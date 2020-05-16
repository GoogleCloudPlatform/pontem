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

package com.google.cloud.pontem.config;

import com.google.common.base.Strings;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/** Workload specific configuration settings. */
public class WorkloadSettings {

  private static final Logger logger = Logger.getLogger(WorkloadSettings.class.getName());

  private boolean isFirstGetQueries = true;

  private String name = "";
  private String projectId = "";
  private String cloudCredentialsFile = "";
  private String outputFileName = "";
  private List<String> queries = new ArrayList<>();
  private List<String> queryFiles = new ArrayList<>();

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getProjectId() {
    return this.projectId;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  /**
   * Return an Optional with a path to a credentials file.
   * @return Optional&ltString&gt
   */
  public Optional<String> getCloudCredentialsFile() {
    Optional<String> optional = Optional.empty();

    if (!Strings.isNullOrEmpty(cloudCredentialsFile)) {
      optional = Optional.of(cloudCredentialsFile);
    }

    return optional;
  }

  public void setCloudCredentialsFile(String cloudCredentialsFile) {
    this.cloudCredentialsFile = cloudCredentialsFile;
  }

  public String getOutputFileName() {
    return this.outputFileName;
  }

  public void setOutputFileName(String outputFileName) {
    this.outputFileName = outputFileName;
  }

  public void setQueryFiles(List<String> queryFiles) {
    this.queryFiles = queryFiles;
  }

  /**
   * Return a List of queries.
   * @return List&ltString&gt
   */
  public List<String> getQueries() {
    if (isFirstGetQueries) {
      queries = queries.stream().filter(q -> q != null).collect(Collectors.toList());
      queryFiles = queryFiles.stream().filter(q -> q != null).collect(Collectors.toList());

      List<String> queriesFromFiles = getQueriesFromFiles();
      if (queries != null) {
        queries.addAll(queriesFromFiles);
      } else {
        queries = queriesFromFiles;
      }

      isFirstGetQueries = false;
    }

    return this.queries;
  }

  public void setQueries(List<String> queries) {
    this.queries = queries;
  }

  private List<String> getQueriesFromFiles() {
    List<String> queriesFromFiles = new ArrayList<>();

    for (String queryFile : queryFiles) {
      queriesFromFiles.addAll(readQueryFile(queryFile));
    }

    return queriesFromFiles;
  }

  private List<String> readQueryFile(final String queryFile) {
    List<String> queries = new ArrayList<>();

    try {
      queries =
          Resources.readLines(
              Resources.getResource(queryFile),
              // TODO(ldanielmadariaga): Handle other charsets
              Charset.defaultCharset());
    } catch (IOException e) {
      logger.warning("Failed to read file " + queryFile + " moving on to the next file");
      logger.log(Level.FINE, "Failed to write to query files: ", e);
    }

    return queries;
  }
}
