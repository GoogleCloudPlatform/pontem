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
import static org.mockito.AdditionalAnswers.returnsElementsOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.pontem.benchmark.runners.ConcurrentWorkloadRunner;
import com.google.cloud.pontem.benchmark.runners.ConcurrentWorkloadRunnerFactory;
import com.google.cloud.pontem.config.WorkloadSettings;
import com.google.cloud.pontem.model.WorkloadResult;
import com.google.cloud.pontem.testing.MapBuilder;
import com.google.cloud.pontem.testing.TestConstants;
import com.google.cloud.pontem.testing.WorkloadResultHelper;
import com.google.cloud.pontem.testing.WorkloadSettingsHelper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link RatioBasedWorkloadBenchmark}. */
@RunWith(JUnit4.class)
public class RatioBasedWorkloadBenchmarkTest {

  private ConcurrentWorkloadRunnerFactory concurrentWorkloadRunnerFactoryMock;

  private RatioBasedWorkloadBenchmark ratioBasedWorkloadBenchmark;

  @Before
  public void setUp() {
    this.concurrentWorkloadRunnerFactoryMock = mock(ConcurrentWorkloadRunnerFactory.class);

    this.ratioBasedWorkloadBenchmark =
        new RatioBasedWorkloadBenchmark(concurrentWorkloadRunnerFactoryMock);
  }

  @Test
  public void runsBenchmarkWithAllPercentages() {
    int concurrencyLevel = 15;
    WorkloadSettings workload = WorkloadSettingsHelper.getMultipleQueries();

    List<Integer> expectedConcurrencyLevels = new ArrayList<>();
    for (double percentage : RatioBasedWorkloadBenchmark.CONCURRENCY_PERCENTAGES) {
      int expectedConcurrencyLevel = (int) Math.ceil(concurrencyLevel * percentage);
      expectedConcurrencyLevels.add(expectedConcurrencyLevel);
    }

    List<Integer> concurrencyLimits = new ArrayList<>();
    List<ConcurrentWorkloadRunner> concurrentWorkloadRunnerMocks = new ArrayList<>();
    List<List<WorkloadResult>> workloadResultList = new ArrayList<>();
    for (Integer expectedConcurrencyLevel : expectedConcurrencyLevels) {
      int concurrencyLimit =
          Math.min(expectedConcurrencyLevel, TestConstants.CONCURRENCY_LEVEL_LIMIT);
      List<WorkloadResult> results =
          WorkloadResultHelper.getWorkloadResultsForConcurrencyLevel(expectedConcurrencyLevel);

      concurrencyLimits.add(concurrencyLimit);
      concurrentWorkloadRunnerMocks.add(mock(ConcurrentWorkloadRunner.class));
      workloadResultList.add(results);
    }

    Map<ConcurrentWorkloadRunner, List<WorkloadResult>> mocksAndResults =
        MapBuilder.getImmutableMapFromLists(concurrentWorkloadRunnerMocks, workloadResultList);

    when(concurrentWorkloadRunnerFactoryMock.get(any(WorkloadSettings.class), anyInt()))
        .thenAnswer(returnsElementsOf(concurrentWorkloadRunnerMocks));
    when(concurrentWorkloadRunnerFactoryMock.getConcurrencyLimit(anyInt()))
        .thenAnswer(returnsElementsOf(concurrencyLimits));
    for (Entry<ConcurrentWorkloadRunner, List<WorkloadResult>> entry : mocksAndResults.entrySet()) {
      when(entry.getKey().run(any(WorkloadSettings.class), anyInt())).thenReturn(entry.getValue());
    }

    List<WorkloadResult> workloadResults =
        ratioBasedWorkloadBenchmark.run(workload, concurrencyLevel);

    List<WorkloadResult> expectedWorkloadResults =
        workloadResultList.stream().flatMap(List::stream).collect(Collectors.toList());
    assertThat(workloadResults, is(expectedWorkloadResults));

    for (ConcurrentWorkloadRunner concurrentWorkloadRunnerMock : concurrentWorkloadRunnerMocks) {
      verify(concurrentWorkloadRunnerMock, times(1)).run(any(WorkloadSettings.class), anyInt());
    }
  }

  @Test
  public void runsBenchmarkUntilLimitIsReached() {
    int concurrencyLevel = 90;
    WorkloadSettings workload = WorkloadSettingsHelper.getMultipleQueries();

    List<Integer> expectedConcurrencyLevels = new ArrayList<>();
    for (double percentage : RatioBasedWorkloadBenchmark.CONCURRENCY_PERCENTAGES.subList(0, 4)) {
      int expectedConcurrencyLevel = (int) Math.ceil(concurrencyLevel * percentage);
      expectedConcurrencyLevels.add(expectedConcurrencyLevel);
    }
    expectedConcurrencyLevels.add(TestConstants.CONCURRENCY_LEVEL_LIMIT);

    List<Integer> concurrencyLimits = new ArrayList<>();
    List<ConcurrentWorkloadRunner> concurrentWorkloadRunnerMocks = new ArrayList<>();
    List<List<WorkloadResult>> workloadResultList = new ArrayList<>();
    for (Integer expectedConcurrencyLevel : expectedConcurrencyLevels) {
      int concurrencyLimit =
          Math.min(expectedConcurrencyLevel, TestConstants.CONCURRENCY_LEVEL_LIMIT);
      List<WorkloadResult> results =
          WorkloadResultHelper.getWorkloadResultsForConcurrencyLevel(expectedConcurrencyLevel);

      concurrencyLimits.add(concurrencyLimit);
      concurrentWorkloadRunnerMocks.add(mock(ConcurrentWorkloadRunner.class));
      workloadResultList.add(results);
    }

    Map<ConcurrentWorkloadRunner, List<WorkloadResult>> mocksAndResults =
        MapBuilder.getImmutableMapFromLists(concurrentWorkloadRunnerMocks, workloadResultList);

    when(concurrentWorkloadRunnerFactoryMock.get(any(WorkloadSettings.class), anyInt()))
        .thenAnswer(returnsElementsOf(concurrentWorkloadRunnerMocks));
    when(concurrentWorkloadRunnerFactoryMock.getConcurrencyLimit(anyInt()))
        .thenAnswer(returnsElementsOf(concurrencyLimits));
    for (Entry<ConcurrentWorkloadRunner, List<WorkloadResult>> entry : mocksAndResults.entrySet()) {
      when(entry.getKey().run(any(WorkloadSettings.class), anyInt())).thenReturn(entry.getValue());
    }

    List<WorkloadResult> workloadResults =
        ratioBasedWorkloadBenchmark.run(workload, concurrencyLevel);

    List<WorkloadResult> expectedWorkloadResults =
        workloadResultList.stream().flatMap(List::stream).collect(Collectors.toList());
    assertThat(workloadResults, is(expectedWorkloadResults));

    for (ConcurrentWorkloadRunner concurrentWorkloadRunnerMock : concurrentWorkloadRunnerMocks) {
      verify(concurrentWorkloadRunnerMock, times(1)).run(any(WorkloadSettings.class), anyInt());
    }
  }

  @Test
  public void runsBenchmarkIgnoringRepeatedLevels() {
    int concurrencyLevel = 4;
    WorkloadSettings workload = WorkloadSettingsHelper.getMultipleQueries();

    List<Integer> expectedConcurrencyLevels = new ArrayList<>();
    for (double percentage : RatioBasedWorkloadBenchmark.CONCURRENCY_PERCENTAGES.subList(2, 7)) {
      int expectedConcurrencyLevel = (int) Math.ceil(concurrencyLevel * percentage);
      expectedConcurrencyLevels.add(expectedConcurrencyLevel);
    }

    List<Integer> concurrencyLimits = new ArrayList<>();
    List<ConcurrentWorkloadRunner> concurrentWorkloadRunnerMocks = new ArrayList<>();
    List<List<WorkloadResult>> workloadResultList = new ArrayList<>();
    for (Integer expectedConcurrencyLevel : expectedConcurrencyLevels) {
      int concurrencyLimit =
          Math.min(expectedConcurrencyLevel, TestConstants.CONCURRENCY_LEVEL_LIMIT);
      List<WorkloadResult> results =
          WorkloadResultHelper.getWorkloadResultsForConcurrencyLevel(expectedConcurrencyLevel);

      concurrencyLimits.add(concurrencyLimit);
      concurrentWorkloadRunnerMocks.add(mock(ConcurrentWorkloadRunner.class));
      workloadResultList.add(results);
    }

    Map<ConcurrentWorkloadRunner, List<WorkloadResult>> mocksAndResults =
        MapBuilder.getImmutableMapFromLists(concurrentWorkloadRunnerMocks, workloadResultList);

    when(concurrentWorkloadRunnerFactoryMock.get(any(WorkloadSettings.class), anyInt()))
        .thenAnswer(returnsElementsOf(concurrentWorkloadRunnerMocks));
    when(concurrentWorkloadRunnerFactoryMock.getConcurrencyLimit(anyInt()))
        .thenAnswer(returnsElementsOf(concurrencyLimits));
    for (Entry<ConcurrentWorkloadRunner, List<WorkloadResult>> entry : mocksAndResults.entrySet()) {
      when(entry.getKey().run(any(WorkloadSettings.class), anyInt())).thenReturn(entry.getValue());
    }

    List<WorkloadResult> workloadResults =
        ratioBasedWorkloadBenchmark.run(workload, concurrencyLevel);

    List<WorkloadResult> expectedWorkloadResults =
        workloadResultList.stream().flatMap(List::stream).collect(Collectors.toList());
    assertThat(workloadResults, is(expectedWorkloadResults));

    for (ConcurrentWorkloadRunner concurrentWorkloadRunnerMock : concurrentWorkloadRunnerMocks) {
      verify(concurrentWorkloadRunnerMock, times(1)).run(any(WorkloadSettings.class), anyInt());
    }
  }

  @Test
  public void onlyRunsBenchmarkAtTheConcurrencyLimit() {
    int concurrencyLevel = 9999;
    int concurrencyLimit = TestConstants.CONCURRENCY_LEVEL_LIMIT;

    WorkloadSettings workload = WorkloadSettingsHelper.getMultipleQueries();
    List<WorkloadResult> workloadResultsForLimit =
        WorkloadResultHelper.getWorkloadResultsForConcurrencyLevel(concurrencyLimit);
    ConcurrentWorkloadRunner concurrentWorkloadRunnerMock = mock(ConcurrentWorkloadRunner.class);

    when(concurrentWorkloadRunnerFactoryMock.get(any(WorkloadSettings.class), anyInt()))
        .thenReturn(concurrentWorkloadRunnerMock);
    when(concurrentWorkloadRunnerFactoryMock.getConcurrencyLimit(anyInt()))
        .thenReturn(concurrencyLimit);
    when(concurrentWorkloadRunnerMock.run(any(WorkloadSettings.class), anyInt()))
        .thenReturn(workloadResultsForLimit);

    List<WorkloadResult> workloadResults =
        ratioBasedWorkloadBenchmark.run(workload, concurrencyLevel);

    assertThat(workloadResults.size(), is(1));
    assertThat(workloadResults, is(workloadResultsForLimit));

    verify(concurrentWorkloadRunnerMock, times(1)).run(workload, concurrencyLimit);
  }
}
