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
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import javax.xml.bind.DatatypeConverter;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.SimpleFunction;

/**
 * A {@code SimpleFunction} that converts a {@code Struct} containing any type of data to a text
 * file that can be written.
 */
public class FormatSpannerStructAsStringFn extends SimpleFunction<Struct, String> {
  private final Counter tableRowsReadCounter;

  FormatSpannerStructAsStringFn(final String tableName) throws Exception {
    this.tableRowsReadCounter =
        Metrics.counter(FormatSpannerStructAsStringFn.class, "rowsRead_" + tableName);
  }

  @Override
  public String apply(Struct inputRow) {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(inputRow);
      oos.flush();
      byte[] binary = baos.toByteArray();
      oos.close();
      return DatatypeConverter.printBase64Binary(binary);
    } catch (Exception e) {
      throw new RuntimeException("Error converting Struct to serialized String\n" + e);
    }
  }
}
