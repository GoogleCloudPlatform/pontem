/*
 * Copyright 2018 Google LLC
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

@AutoValue
/** Holds the execution results of running an interactive query against BigQuery. */
public abstract class BigQueryResult {

  public static Builder newBuilder() {
    return new AutoValue_BigQueryResult.Builder();
  }

  public abstract String getId();

  public abstract Long getStartTime();

  public abstract Long getEndTime();

  public abstract Long getCreationTime();

  public abstract Status getStatus();

  public abstract ImmutableList<String> getErrors();

  /**
   * Calculates the runtime in seconds from the start and end time.
   *
   * @return The runtime in seconds.
   */
  public Double getRuntime() {
    return (getEndTime() - getStartTime()) / 1000.0;
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setId(String value);

    public abstract Builder setStartTime(Long value);

    public abstract Builder setEndTime(Long value);

    public abstract Builder setCreationTime(Long value);

    public abstract Builder setStatus(Status value);

    public abstract Builder setErrors(List<String> value);

    public abstract BigQueryResult build();
  }
}
