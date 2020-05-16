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

package com.google.cloud.pontem.benchmark.runners;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.pontem.benchmark.runners.callables.WorkloadRunnerCallable;
import com.google.cloud.pontem.model.WorkloadResult;
import com.google.cloud.pontem.testing.TestConstants;
import com.google.cloud.pontem.testing.WorkloadResultHelper;
import com.google.cloud.pontem.testing.WorkloadSettingsHelper;
import com.google.common.base.Stopwatch;
import com.google.common.testing.FakeTicker;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ConcurrentWorkloadRunner}. */
@RunWith(JUnit4.class)
public class ConcurrentWorkloadRunnerTest {

  private ExecutorService executorServiceMock;
  private FakeTicker fakeTicker;
  private Stopwatch stopwatch;

  @Before
  public void setUp() {
    this.executorServiceMock = mock(ExecutorService.class);
    this.fakeTicker = new FakeTicker();
    this.stopwatch = Stopwatch.createUnstarted(fakeTicker);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void runConcurrentWorkload() throws Exception {
    int concurrencyLevel = TestConstants.CONCURRENCY_LEVEL_BELOW_LIMIT;

    List<WorkloadRunnerCallable> callableMocks = new ArrayList<>();
    for (int i = 0; i < concurrencyLevel; i++) {
      callableMocks.add(mock(WorkloadRunnerCallable.class));
    }

    List<Future<WorkloadResult>> futureMocks = new ArrayList<>();
    for (int i = 0; i < concurrencyLevel; i++) {
      futureMocks.add(mock(Future.class));
    }

    when(executorServiceMock.invokeAll(callableMocks)).thenReturn(futureMocks);

    for (Future<WorkloadResult> futureMock : futureMocks) {
      when(futureMock.isDone()).thenReturn(true);
      when(futureMock.get())
          .thenReturn(WorkloadResultHelper.getWorkloadResultMultipleSuccessfulQueries());
    }

    ConcurrentWorkloadRunner<WorkloadResult> concurrentWorkloadRunner =
        new ConcurrentWorkloadRunner(executorServiceMock, callableMocks, stopwatch);
    List<WorkloadResult> workloadResults =
        concurrentWorkloadRunner.run(WorkloadSettingsHelper.getMultipleQueries(), concurrencyLevel);

    assertThat(workloadResults.size(), is(concurrencyLevel));
    assertThat(workloadResults, is(WorkloadResultHelper.getConcurrentWorkloadResults()));

    verify(executorServiceMock, times(1)).invokeAll(callableMocks);
    for (Future<WorkloadResult> futureMock : futureMocks) {
      verify(futureMock, times(1)).isDone();
      verify(futureMock, times(1)).get();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void runConcurrentWorkloadExceedingConcurrencyLevel() throws Exception {
    int expectedConcurrencyLevel = TestConstants.CONCURRENCY_LEVEL_LIMIT;

    List<WorkloadRunnerCallable> callableMocks = new ArrayList<>();
    for (int i = 0; i < expectedConcurrencyLevel; i++) {
      callableMocks.add(mock(WorkloadRunnerCallable.class));
    }

    List<Future<WorkloadResult>> futureMocks = new ArrayList<>();
    for (int i = 0; i < expectedConcurrencyLevel; i++) {
      futureMocks.add(mock(Future.class));
    }

    when(executorServiceMock.invokeAll(callableMocks)).thenReturn(futureMocks);

    for (Future<WorkloadResult> futureMock : futureMocks) {
      when(futureMock.isDone()).thenReturn(true);
      when(futureMock.get())
          .thenReturn(WorkloadResultHelper.getWorkloadResultMultipleSuccessfulQueries());
    }

    ConcurrentWorkloadRunner<WorkloadResult> concurrentWorkloadRunner =
        new ConcurrentWorkloadRunner(executorServiceMock, callableMocks, stopwatch);
    List<WorkloadResult> workloadResults =
        concurrentWorkloadRunner.run(
            WorkloadSettingsHelper.getMultipleQueries(),
            TestConstants.CONCURRENCY_LEVEL_ABOVE_LIMIT);

    assertThat(workloadResults.size(), is(expectedConcurrencyLevel));
    assertThat(workloadResults, is(WorkloadResultHelper.getConcurrentWorkloadResultsOverLimit()));

    verify(executorServiceMock, times(1)).invokeAll(callableMocks);
    for (Future<WorkloadResult> futureMock : futureMocks) {
      verify(futureMock, times(1)).isDone();
      verify(futureMock, times(1)).get();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void runConcurrentWorkloadWithQueryErrors() throws Exception {
    int concurrencyLevel = TestConstants.CONCURRENCY_LEVEL_BELOW_LIMIT;

    List<WorkloadRunnerCallable> callableMocks = new ArrayList<>();
    for (int i = 0; i < concurrencyLevel; i++) {
      callableMocks.add(mock(WorkloadRunnerCallable.class));
    }

    List<Future<WorkloadResult>> futureMocks = new ArrayList<>();
    for (int i = 0; i < concurrencyLevel; i++) {
      futureMocks.add(mock(Future.class));
    }

    when(executorServiceMock.invokeAll(callableMocks)).thenReturn(futureMocks);

    for (Future<WorkloadResult> futureMock : futureMocks) {
      when(futureMock.isDone()).thenReturn(true);
      when(futureMock.get())
          .thenReturn(WorkloadResultHelper.getWorkloadResultMultipleQueryErrors());
    }

    ConcurrentWorkloadRunner<WorkloadResult> concurrentWorkloadRunner =
        new ConcurrentWorkloadRunner(executorServiceMock, callableMocks, stopwatch);
    List<WorkloadResult> workloadResults =
        concurrentWorkloadRunner.run(WorkloadSettingsHelper.getMultipleQueries(), concurrencyLevel);

    assertThat(workloadResults.size(), is(concurrencyLevel));
    assertThat(
        workloadResults, is(WorkloadResultHelper.getConcurrentWorkloadResultsWithQueryErrors()));

    verify(executorServiceMock, times(1)).invokeAll(callableMocks);
    for (Future<WorkloadResult> futureMock : futureMocks) {
      verify(futureMock, times(1)).isDone();
      verify(futureMock, times(1)).get();
    }
  }

  @Test
  public void runConcurrentWorkloadWithExecutorExceptions() throws Exception {
    int concurrencyLevel = TestConstants.CONCURRENCY_LEVEL_BELOW_LIMIT;

    List<WorkloadRunnerCallable> callableMocks = new ArrayList<>();
    for (int i = 0; i < concurrencyLevel; i++) {
      callableMocks.add(mock(WorkloadRunnerCallable.class));
    }

    when(executorServiceMock.invokeAll(callableMocks))
        .thenThrow(new InterruptedException(TestConstants.ERROR_MESSAGE_1));

    ConcurrentWorkloadRunner<WorkloadResult> concurrentWorkloadRunner =
        new ConcurrentWorkloadRunner(executorServiceMock, callableMocks, stopwatch);
    List<WorkloadResult> workloadResults =
        concurrentWorkloadRunner.run(WorkloadSettingsHelper.getMultipleQueries(), concurrencyLevel);

    assertThat(workloadResults.size(), is(1));
    assertThat(workloadResults, is(WorkloadResultHelper.getConcurrentWorkloadExceptionResults()));

    verify(executorServiceMock, times(1)).invokeAll(callableMocks);
  }

  @Test
  public void runConcurrentWorkloadWithFutureExceptions() throws Exception {
    int concurrencyLevel = TestConstants.CONCURRENCY_LEVEL_BELOW_LIMIT;

    List<WorkloadRunnerCallable> callableMocks = new ArrayList<>();
    for (int i = 0; i < concurrencyLevel; i++) {
      callableMocks.add(mock(WorkloadRunnerCallable.class));
    }

    List<Future<WorkloadResult>> futureMocks = new ArrayList<>();
    for (int i = 0; i < concurrencyLevel; i++) {
      futureMocks.add(mock(Future.class));
    }

    when(executorServiceMock.invokeAll(callableMocks)).thenReturn(futureMocks);
    for (Future<WorkloadResult> futureMock : futureMocks) {
      when(futureMock.isDone()).thenReturn(true);
      when(futureMock.get())
          .thenThrow(
              new ExecutionException(TestConstants.ERROR_MESSAGE_1, new NullPointerException()));
    }

    ConcurrentWorkloadRunner concurrentWorkloadRunner =
        new ConcurrentWorkloadRunner(executorServiceMock, callableMocks, stopwatch);
    List<WorkloadResult> workloadResults =
        concurrentWorkloadRunner.run(WorkloadSettingsHelper.getMultipleQueries(), concurrencyLevel);

    assertThat(workloadResults.size(), is(concurrencyLevel));
    for (WorkloadResult workloadResult : workloadResults) {
      assertThat(workloadResult, is(WorkloadResultHelper.getConcurrentWorkloadExceptionResult()));
    }

    verify(executorServiceMock, times(1)).invokeAll(callableMocks);
    for (Future<WorkloadResult> futureMock : futureMocks) {
      verify(futureMock, times(1)).isDone();
      verify(futureMock, times(1)).get();
    }
  }
}
