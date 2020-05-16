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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.pontem.model.WorkloadResult;
import com.google.cloud.pontem.testing.WorkloadResultHelper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link JsonResultProcessor}. */
@RunWith(JUnit4.class)
public class JsonResultProcessorTest {

  private static String DESTINATION = "/destination/file.json";
  private static Gson GSON = new GsonBuilder().setPrettyPrinting().create();

  private ResultFileWriter resultFileWriterMock;

  private JsonResultProcessor jsonResultProcessor;

  @Before
  public void setUp() {
    resultFileWriterMock = mock(ResultFileWriter.class);

    jsonResultProcessor = new JsonResultProcessor(resultFileWriterMock);
  }

  @Test
  public void savesJsonFile() {
    runAndVerify(WorkloadResultHelper.getWorkloadResultsForConcurrencyLevel());
  }

  @Test
  public void savesMultipleJsonFiles() {
    runAndVerify(WorkloadResultHelper.getConcurrentWorkloadResults());
  }

  @Test
  public void ignoresEmptyResults() {
    runAndVerify(null);
  }

  @Test
  public void ignoresResultListWithNull() {
    List<WorkloadResult> results = new ArrayList<>();
    results.add(null);

    runAndVerify(results);
  }

  private void runAndVerify(final List<WorkloadResult> results) {
    jsonResultProcessor.run(DESTINATION, results);

    verify(resultFileWriterMock, times(1)).write(DESTINATION, GSON.toJson(results));
  }
}
