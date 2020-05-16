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

package com.google.cloud.pontem.result;

import com.google.cloud.pontem.model.WorkloadResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.List;
import java.util.logging.Logger;

/** Transforms Workload Benchmark results and saves them into a JSON file. */
public class JsonResultProcessor {

  private static final Logger logger = Logger.getLogger(JsonResultProcessor.class.getName());

  private final ResultFileWriter resultFileWriter;

  public JsonResultProcessor(ResultFileWriter resultFileWriter) {
    this.resultFileWriter = resultFileWriter;
  }

  /**
   * Converts and saves a list of WorkloadResult objects into JSON files.
   *
   * @param destination the destination where to write the results
   * @param results the results to convert into JSON
   */
  public void run(final String destination, final List<WorkloadResult> results) {
    if (results == null || results.size() == 0) {
      logger.warning("No result received writing empty file.");
    }

    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    String toJson = gson.toJson(results);
    logger.fine("Attempting to write the following JSON file: " + toJson);

    resultFileWriter.write(destination, toJson);
  }
}
