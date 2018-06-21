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

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import javax.xml.bind.DatatypeConverter;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.SimpleFunction;

/**
 * A {@code SimpleFunction} that converts a {@code String} to a {@code Mutation}. Code takes
 * base64-encoded binary data from Cloud Spanner and turns it into a Cloud Spanner {@code Mutation}
 * which can be written into Cloud Spanner.
 *
 * <p>See https://cloud.google.com/spanner/docs/data-types
 */
public class FormatStringAsSpannerMutationFn extends SimpleFunction<String, Mutation> {
  private final Counter tableRowsReadCounter;
  private final String tableName;

  FormatStringAsSpannerMutationFn(final String tableName) {
    this.tableRowsReadCounter =
        Metrics.counter(FormatStringAsSpannerMutationFn.class, "rowsRead_" + tableName);
    this.tableName = tableName;
  }

  @Override
  public Mutation apply(String inputRow) {
    try {
      // STEP 1: Convert binary base64-encoded serialzied data to a Struct
      // Data is stored on disk serialized as a Struct (a Struct is essentially a row of data)
      byte[] inputRowBytes = DatatypeConverter.parseBase64Binary(inputRow);
      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(inputRowBytes));
      Struct struct = (Struct) ois.readObject();

      return SpannerUtil.convertStructToMutation(struct, this.tableName);
    } catch (Exception e) {
      throw new RuntimeException("Serious error:\n" + e);
    }
  }
}
