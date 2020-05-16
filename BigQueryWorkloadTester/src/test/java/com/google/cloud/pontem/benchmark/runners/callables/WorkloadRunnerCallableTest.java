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

package com.google.cloud.pontem.benchmark.runners.callables;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.pontem.benchmark.runners.SerialQueryRunner;
import com.google.cloud.pontem.config.WorkloadSettings;
import com.google.cloud.pontem.model.WorkloadResult;
import com.google.cloud.pontem.testing.QueryResultHelper;
import com.google.cloud.pontem.testing.TestConstants;
import com.google.cloud.pontem.testing.WorkloadResultHelper;
import com.google.cloud.pontem.testing.WorkloadSettingsHelper;
import com.google.common.base.Stopwatch;
import com.google.common.testing.FakeTicker;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link WorkloadRunnerCallable}. */
@RunWith(JUnit4.class)
public class WorkloadRunnerCallableTest {

  private SerialQueryRunner serialQueryRunnerMock;
  private WorkloadSettings workloadMock;
  private FakeTicker fakeTicker;
  private Stopwatch stopwatch;

  private WorkloadRunnerCallable workloadRunnerCallable;

  @Before
  public void setUp() {
    this.serialQueryRunnerMock = mock(SerialQueryRunner.class);
    this.workloadMock = WorkloadSettingsHelper.getMultipleQueries();
    this.fakeTicker = new FakeTicker();
    this.stopwatch = Stopwatch.createUnstarted(fakeTicker);

    this.workloadRunnerCallable =
        new WorkloadRunnerCallable(
            serialQueryRunnerMock,
            stopwatch,
            workloadMock,
            TestConstants.CONCURRENCY_LEVEL_BELOW_LIMIT);
  }

  @Test
  public void getAndRunSuccessfully() {
    fakeTicker.setAutoIncrementStep(
        TestConstants.SUCCESSFUL_WORKLOAD_WALLTIME, TimeUnit.MILLISECONDS);
    when(serialQueryRunnerMock.run(TestConstants.TEST_QUERIES))
        .thenReturn(QueryResultHelper.getSuccessfulQueryResults());

    WorkloadResult workloadResult = workloadRunnerCallable.call();

    assertThat(
        workloadResult, is(WorkloadResultHelper.getWorkloadResultMultipleSuccessfulQueries()));
    verify(serialQueryRunnerMock).run(TestConstants.TEST_QUERIES);
  }

  @Test
  public void getAndRunWithQueryErrors() {
    fakeTicker.setAutoIncrementStep(TestConstants.FAILING_WORKLOAD_WALLTIME, TimeUnit.MILLISECONDS);
    when(serialQueryRunnerMock.run(TestConstants.TEST_QUERIES))
        .thenReturn(QueryResultHelper.getQueryResultsWithSingleError());

    WorkloadResult workloadResult = workloadRunnerCallable.call();

    assertThat(workloadResult, is(WorkloadResultHelper.getWorkloadResultMultipleQueryErrors()));
    verify(serialQueryRunnerMock).run(TestConstants.TEST_QUERIES);
  }
}
