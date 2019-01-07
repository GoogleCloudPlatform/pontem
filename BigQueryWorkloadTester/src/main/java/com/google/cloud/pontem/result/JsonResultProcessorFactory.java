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

import java.util.logging.Logger;

/** A factory for JsonResultProcessor objects. */
public class JsonResultProcessorFactory {

  private static final Logger logger = Logger.getLogger(JsonResultProcessorFactory.class.getName());

  /**
   * Ensures that the JsonResultProcessor and it's dependencies are properly built and configured.
   *
   * @return a properly configured JsonResultProcessor
   */
  public static JsonResultProcessor get() {
    logger.fine("Building JsonResultProcessor.");
    ResultFileWriter resultFileWriter = new ResultFileWriter();

    return new JsonResultProcessor(resultFileWriter);
  }
}
