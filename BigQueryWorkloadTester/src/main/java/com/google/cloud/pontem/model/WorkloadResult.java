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
package com.google.cloud.pontem.model;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import java.util.List;

/**
 * Holds the execution result of running a Workload (a set of queries) against a Workload Tester
 * compatible backend.
 */
@AutoValue
public abstract class WorkloadResult {

  public static Builder newBuilder() {
    return new AutoValue_WorkloadResult.Builder();
  }

  public abstract String getWorkloadName();

  public abstract String getProjectId();

  public abstract int getConcurrencyLevel();

  public abstract Status getStatus();

  public abstract String getError();

  public abstract ImmutableList<QueryResult> getQueryResults();

  public abstract long getWallTime();

  public abstract long getRunTime();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setWorkloadName(String value);

    public abstract Builder setProjectId(String value);

    public abstract Builder setConcurrencyLevel(int value);

    public abstract Builder setStatus(Status value);

    public abstract Builder setError(String value);

    public abstract Builder setQueryResults(List<QueryResult> value);

    public abstract Builder setRunTime(long value);

    public abstract Builder setWallTime(long value);

    public abstract WorkloadResult build();
  }
}
