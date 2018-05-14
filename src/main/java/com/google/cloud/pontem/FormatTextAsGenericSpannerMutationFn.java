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

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;
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
public class FormatTextAsGenericSpannerMutationFn extends SimpleFunction<String, Mutation> {
  private final Counter tableRowsReadCounter;
  private final String tableName;

  FormatTextAsGenericSpannerMutationFn(final String tableName) {
    this.tableRowsReadCounter =
        Metrics.counter(FormatTextAsGenericSpannerMutationFn.class, "rowsRead_" + tableName);
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

      // STEP 2: Convert a Struct to a Mutation
      Mutation.WriteBuilder mutationBuilder = Mutation.newInsertOrUpdateBuilder(tableName);

      Type tableType = struct.getType();

      for (Type.StructField field : tableType.getStructFields()) {
        Type type = field.getType();
        String name = field.getName();

        // If a column's value is NULL, skip it.
        if (struct.isNull(name)) {
          continue;
        }

        // Using switch produced errors switching on an enum
        // see https://cloud.google.com/spanner/docs/data-types
        if (type.getCode() == Code.STRING) {
          mutationBuilder.set(name).to((String) struct.getString(name));
        } else if (type.getCode() == Code.BOOL) {
          mutationBuilder.set(name).to((boolean) struct.getBoolean(name));
        } else if (type.getCode() == Code.BYTES) {
          mutationBuilder.set(name).to((ByteArray) struct.getBytes(name));
        } else if (type.getCode() == Code.DATE) {
          mutationBuilder.set(name).to((Date) struct.getDate(name));
        } else if (type.getCode() == Code.FLOAT64) {
          mutationBuilder.set(name).to((double) struct.getDouble(name));
        } else if (type.getCode() == Code.TIMESTAMP) {
          mutationBuilder.set(name).to((Timestamp) struct.getTimestamp(name));
        } else if (type.getCode() == Code.INT64) {
          mutationBuilder.set(name).to((long) struct.getLong(name));
        } else if (type.getCode() == Code.ARRAY) {
          // Go through different types of arrays.
          if (type.getArrayElementType().getCode() == Code.STRING) {
            mutationBuilder.set(name).toStringArray(struct.getStringList(name));
          } else if (type.getArrayElementType().getCode() == Code.BOOL) {
            mutationBuilder.set(name).toBoolArray(struct.getBooleanList(name));
          } else if (type.getArrayElementType().getCode() == Code.BYTES) {
            mutationBuilder.set(name).toBytesArray(struct.getBytesList(name));
          } else if (type.getArrayElementType().getCode() == Code.DATE) {
            mutationBuilder.set(name).toDateArray(struct.getDateList(name));
          } else if (type.getArrayElementType().getCode() == Code.FLOAT64) {
            mutationBuilder.set(name).toFloat64Array(struct.getDoubleList(name));
          } else if (type.getArrayElementType().getCode() == Code.INT64) {
            mutationBuilder.set(name).toInt64Array(struct.getLongArray(name));
          } else if (type.getArrayElementType().getCode() == Code.TIMESTAMP) {
            mutationBuilder.set(name).toTimestampArray(struct.getTimestampList(name));
          }
        } else {
          throw new RuntimeException(
              "Not handling type for field '" + name + "' of type " + type.getCode());
        }
      }
      return mutationBuilder.build();
    } catch (Exception e) {
      throw new RuntimeException("Serious error:\n" + e);
    }
  }
}
