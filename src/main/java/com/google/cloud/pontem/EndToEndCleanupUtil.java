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

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Util class for cleaning up End to End tests.
 *
 * <pre>
 * mvn compile exec:java \
 *   -Dexec.mainClass=com.google.cloud.pontem.EndToEndCleanupUtil \
 *   -Dexec.args="--projectId=my-cloud-spanner-project \
 *                --gcsBucket=my-cloud-spanner-project \
 *                --databaseInstanceId=my-cloud-spanner-instance \
 *                --operation=cleanupAll"
 * </pre>
 */
public class EndToEndCleanupUtil {
  private static final Logger LOG = Logger.getLogger(EndToEndCleanupUtil.class.getName());

  public static Options configureCommandlineOptions() {
    Options options = new Options();

    /** Google Cloud Storage Bucket. */
    Option gcsBucket = new Option("b", "gcsBucket", true, "GCS Bucket");
    gcsBucket.setRequired(true);
    options.addOption(gcsBucket);

    /** Google Cloud project ID. */
    Option projectId = new Option("p", "projectId", true, "Google Cloud Project Id");
    projectId.setRequired(true);
    options.addOption(projectId);

    /** The Google Cloud Spanner database instance id */
    Option databaseInstanceId =
        new Option("i", "databaseInstanceId", true, "Google Cloud Spanner Instance ID");
    databaseInstanceId.setRequired(true);
    options.addOption(databaseInstanceId);

    /** The operation for the end to end test cleanup util to perform */
    Option operation = new Option("o", "operation", true, "Util Operation Requested");
    operation.setRequired(true);
    options.addOption(operation);

    return options;
  }

  public static void main(String[] args) throws Exception {
    // STEP 1: Parse inputs.
    Options options = configureCommandlineOptions();

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      LOG.info(e.getMessage());
      formatter.printHelp("utility-name", options);

      System.exit(1);
      return;
    }

    final String operation = cmd.getOptionValue("operation");
    final String instanceId = cmd.getOptionValue("databaseInstanceId");
    final String projectId = cmd.getOptionValue("projectId");
    final String gcsBucket = cmd.getOptionValue("gcsBucket");

    try {
      if (operation.equals("cleanupAll")) {
        cleanupDatabase(projectId, instanceId);
        cleanupGcs(projectId, gcsBucket);
      } else if (operation.equals("cleanupDatabase")) {
        cleanupDatabase(projectId, instanceId);
      } else if (operation.equals("cleanupGcs")) {
        cleanupGcs(projectId, gcsBucket);
      } else {
        throw new Exception("Unable to execute operation: " + operation);
      }
    } catch (Exception e) {
      LOG.warning(e.getMessage() + "\n" + Arrays.toString(e.getStackTrace()));
      throw e;
    }
  }

  /** Cleanup databases in an instance. */
  public static void cleanupDatabase(String projectId, String instanceId) {
    LOG.info("Begin deletion of databases.");
    ImmutableList<String> allDatabaseNames =
        Util.getListOfDatabaseNames(projectId, instanceId, 1000);
    for (String databaseName : allDatabaseNames) {
      EndToEndHelper.deleteCloudSpannerDatabase(projectId, instanceId, databaseName);
    }
    LOG.info("End deletion of databases.");
  }

  /** Cleanup folders/files in a GCS bucket. */
  public static void cleanupGcs(String projectId, String gcsBucket) {
    EndToEndHelper.deleteGcsFolder(projectId, gcsBucket, "");
  }
}
