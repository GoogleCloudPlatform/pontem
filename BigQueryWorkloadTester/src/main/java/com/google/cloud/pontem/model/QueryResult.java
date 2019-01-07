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

/** Holds the execution result of running a Query against a Workload Tester compatible backend. */
@AutoValue
public abstract class QueryResult {

  public static Builder newBuilder() {
    return new AutoValue_QueryResult.Builder();
  }

  public abstract String getId();

  public abstract Long getCreationTime();

  public abstract Long getRuntime();

  public abstract Long getWallTime();

  public abstract String getQuery();

  public abstract Status getStatus();

  public abstract ImmutableList<String> getErrors();

  @AutoValue.Builder
  public abstract static class Builder {

    protected abstract ImmutableList.Builder<String> errorsBuilder();

    public abstract Builder setId(String value);

    public abstract Builder setQuery(String value);

    public abstract Builder setStatus(Status value);

    public abstract Builder setCreationTime(Long value);

    public abstract Builder setRuntime(Long value);

    public abstract Builder setWallTime(Long value);

    public abstract Builder setErrors(List<String> value);

    public Builder addError(String error) {
      errorsBuilder().add(error);
      return this;
    }

    public abstract QueryResult build();
  }
}
