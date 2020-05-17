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

import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Writes Results into a file while ensuring the underlying writer is properly closed and errors
 * handled.
 */
public class ResultFileWriter {

  private static final Logger logger = Logger.getLogger(ResultFileWriter.class.getName());

  /**
   * Writes the provided content into a file, if the file existed previous to this invocation it's
   * contents are overwritten.
   *
   * @param fileName the name of the file to write
   * @param content the content to write into the file
   */
  public void write(final String fileName, final String content) {

    FileWriter fileWriter = null;

    try {
      fileWriter = new FileWriter(fileName);
      fileWriter.write(content);
      fileWriter.flush();
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Failed to write to result file: ", e);
    } finally {
      closeWriter(fileWriter);
    }
  }

  private void closeWriter(final FileWriter fileWriter) {
    if (fileWriter != null) {
      try {
        fileWriter.close();
      } catch (IOException e) {
        logger.log(Level.WARNING, "Failed to close file writer: ", e);
      }
    }
  }
}
