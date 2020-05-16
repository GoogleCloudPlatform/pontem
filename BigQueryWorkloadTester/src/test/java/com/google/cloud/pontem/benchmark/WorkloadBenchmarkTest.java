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

package com.google.cloud.pontem.benchmark;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.pontem.benchmark.runners.ConcurrentWorkloadRunner;
import com.google.cloud.pontem.benchmark.runners.ConcurrentWorkloadRunnerFactory;
import com.google.cloud.pontem.config.WorkloadSettings;
import com.google.cloud.pontem.model.WorkloadResult;
import com.google.cloud.pontem.testing.TestConstants;
import com.google.cloud.pontem.testing.WorkloadResultHelper;
import com.google.cloud.pontem.testing.WorkloadSettingsHelper;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link WorkloadBenchmark}. */
@RunWith(JUnit4.class)
public class WorkloadBenchmarkTest {

  private ConcurrentWorkloadRunnerFactory concurrentWorkloadRunnerFactoryMock;
  private ConcurrentWorkloadRunner concurrentWorkloadRunnerMock;

  private WorkloadBenchmark workloadBenchmark;

  @Before
  public void setUp() {
    this.concurrentWorkloadRunnerFactoryMock = mock(ConcurrentWorkloadRunnerFactory.class);
    this.concurrentWorkloadRunnerMock = mock(ConcurrentWorkloadRunner.class);

    this.workloadBenchmark = new WorkloadBenchmark(concurrentWorkloadRunnerFactoryMock);
  }

  @Test
  public void runsBenchmark() {
    int concurrencyLevel = TestConstants.CONCURRENCY_LEVEL_BELOW_LIMIT;
    final WorkloadSettings workload = WorkloadSettingsHelper.getMultipleQueries();

    when(concurrentWorkloadRunnerFactoryMock.getConcurrentWorkloadRunner(
            workload, concurrencyLevel))
        .thenReturn(concurrentWorkloadRunnerMock);
    when(concurrentWorkloadRunnerMock.run(workload, concurrencyLevel))
        .thenReturn(WorkloadResultHelper.getConcurrentWorkloadResults());

    List<WorkloadResult> workloadResults = workloadBenchmark.run(workload, concurrencyLevel);

    assertThat(workloadResults, is(WorkloadResultHelper.getConcurrentWorkloadResults()));

    verify(concurrentWorkloadRunnerFactoryMock, times(1))
        .getConcurrentWorkloadRunner(workload, concurrencyLevel);
    verify(concurrentWorkloadRunnerMock, times(1)).run(workload, concurrencyLevel);
  }

  @Test
  public void runsBenchmarkWithWorkflowWithQueryErrors() {
    int concurrencyLevel = TestConstants.CONCURRENCY_LEVEL_BELOW_LIMIT;
    WorkloadSettings workload = WorkloadSettingsHelper.getMultipleQueries();

    when(concurrentWorkloadRunnerFactoryMock.getConcurrentWorkloadRunner(
            workload, concurrencyLevel))
        .thenReturn(concurrentWorkloadRunnerMock);
    when(concurrentWorkloadRunnerMock.run(workload, concurrencyLevel))
        .thenReturn(WorkloadResultHelper.getConcurrentWorkloadResultsWithQueryErrors());

    List<WorkloadResult> workloadResults = workloadBenchmark.run(workload, concurrencyLevel);

    assertThat(
        workloadResults, is(WorkloadResultHelper.getConcurrentWorkloadResultsWithQueryErrors()));

    verify(concurrentWorkloadRunnerFactoryMock, times(1))
        .getConcurrentWorkloadRunner(workload, concurrencyLevel);
    verify(concurrentWorkloadRunnerMock, times(1)).run(workload, concurrencyLevel);
  }

  @Test
  public void runsBenchmarkWithWorkflowFailure() {
    int concurrencyLevel = TestConstants.CONCURRENCY_LEVEL_BELOW_LIMIT;
    WorkloadSettings workload = WorkloadSettingsHelper.getMultipleQueries();

    when(concurrentWorkloadRunnerFactoryMock.getConcurrentWorkloadRunner(
            workload, concurrencyLevel))
        .thenReturn(concurrentWorkloadRunnerMock);
    when(concurrentWorkloadRunnerMock.run(workload, concurrencyLevel))
        .thenReturn(WorkloadResultHelper.getConcurrentWorkloadExceptionResults());

    List<WorkloadResult> workloadResults = workloadBenchmark.run(workload, concurrencyLevel);

    assertThat(workloadResults, is(WorkloadResultHelper.getConcurrentWorkloadExceptionResults()));

    verify(concurrentWorkloadRunnerFactoryMock, times(1))
        .getConcurrentWorkloadRunner(workload, concurrencyLevel);
    verify(concurrentWorkloadRunnerMock, times(1)).run(workload, concurrencyLevel);
  }
}
