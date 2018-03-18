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
package com.google.cloud.pontem;

import com.google.cloud.spanner.Struct;
import org.apache.beam.sdk.transforms.SimpleFunction;

/**
 * A {@code SimpleFunction} that converts a Spanner {@code Struct} with a list of Spanner table
 * names to text which can be written to disk.
 */
public class FormatSpannerTablesListStructAsTextFn extends SimpleFunction<Struct, String> {
  @Override
  public String apply(Struct inputRow) {
    String parentTableName = "";
    if (!inputRow.isNull("parent_table_name")) {
      parentTableName = inputRow.getString("parent_table_name");
    }
    return inputRow.getString("table_name") + "," + parentTableName;
  }
}
