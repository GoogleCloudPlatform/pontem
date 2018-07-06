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
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.SimpleFunction;

/**
 * A {@code SimpleFunction} that converts a {@code GenericRecord} to a {@code Mutation}.
 *
 * <p>See https://cloud.google.com/spanner/docs/data-types
 */
public class FormatGenericRecordAsSpannerMutationFn
    extends SimpleFunction<GenericRecord, Mutation> {

  private final Counter tableRowsReadCounter;
  private final String tableName;
  private final ImmutableMap<String, Type> mapOfColumnNamesToSpannerTypes;

  FormatGenericRecordAsSpannerMutationFn(
      String tableName, ImmutableMap<String, Type> mapOfColumnNamesToSpannerTypes) {
    this.tableRowsReadCounter =
        Metrics.counter(FormatGenericRecordAsSpannerMutationFn.class, "recordsRead_" + tableName);
    this.tableName = tableName;
    this.mapOfColumnNamesToSpannerTypes = mapOfColumnNamesToSpannerTypes;
  }

  @Override
  public Mutation apply(GenericRecord inputRow) {
    Mutation.WriteBuilder mutationBuilder = Mutation.newInsertOrUpdateBuilder(this.tableName);

    Schema tableSchema = inputRow.getSchema();
    List<Schema.Field> tableFields = tableSchema.getFields();
    for (Schema.Field tableField : tableFields) {

      String fieldName = tableField.name();
      Type spannerFieldType = this.mapOfColumnNamesToSpannerTypes.get(fieldName);
      Schema columnSchema = tableField.schema();
      // Since the Avro column schema could be a UNION of types (e.g., [STRING, NULL]),
      // we need to unpack that UNION to simply be a noramlly recognized Avro type (e.g., STRING).
      if (columnSchema.getType() == Schema.Type.UNION) {
        columnSchema = AvroUtil.getSingleAvroTypeFromNullableUnion(columnSchema);
      }
      Schema.Type avroFieldType = columnSchema.getType();

      if (spannerFieldType.getCode() == Code.STRING && avroFieldType == Schema.Type.STRING) {
        // Sometimes, the row comes back as a Utf8 and sometimes as a String.
        Object object = inputRow.get(fieldName);
        if (object == null) {
          // Do nothing with NULL value.
        } else if (object instanceof String || object instanceof Utf8) {
          mutationBuilder.set(fieldName).to((String) object.toString());
        } else {
          throw new RuntimeException(
              "Unrecognized format in field "
                  + fieldName
                  + " for Avro String: "
                  + object.getClass());
        }
      } else if (spannerFieldType.getCode() == Code.BOOL && avroFieldType == Schema.Type.BOOLEAN) {
        Object object = inputRow.get(fieldName);
        if (object == null) {
          // Do nothing with NULL value.
        } else if (object instanceof Boolean) {
          mutationBuilder.set(fieldName).to((boolean) object);
        } else {
          throw new RuntimeException(
              "Unrecognized format in field " + fieldName + " for Avro Long: " + object.getClass());
        }
      } else if (spannerFieldType.getCode() == Code.INT64 && avroFieldType == Schema.Type.LONG) {
        Object longVal = inputRow.get(fieldName);
        if (longVal == null) {
          // Do nothing with NULL value.
        } else if (longVal instanceof Long) {
          Long castLongVal = new Long((long) longVal);
          mutationBuilder.set(fieldName).to(castLongVal);
        } else {
          throw new RuntimeException(
              "Unrecognized format in field "
                  + fieldName
                  + " for Avro Long: "
                  + longVal.getClass());
        }
      } else if (spannerFieldType.getCode() == Code.FLOAT64
          && avroFieldType == Schema.Type.DOUBLE) {
        Object doubleVal = inputRow.get(fieldName);
        if (doubleVal == null) {
          // Do nothing with NULL value.
        } else if (doubleVal instanceof Double) {
          mutationBuilder.set(fieldName).to((double) inputRow.get(fieldName));
        } else {
          throw new RuntimeException(
              "Unrecognized format in field "
                  + fieldName
                  + " for Avro Long: "
                  + doubleVal.getClass());
        }
      } else if (spannerFieldType.getCode() == Code.DATE && avroFieldType == Schema.Type.STRING) {
        // Sometimes, the row comes back as a Utf8 and sometimes as a String.
        Object object = inputRow.get(fieldName);
        if (object == null) {
          // Do nothing with NULL value.
        } else if (object instanceof Utf8 || object instanceof String) {
          mutationBuilder.set(fieldName).to(Date.parseDate((String) object.toString()));
        } else {
          throw new RuntimeException(
              "Unrecognized format in field "
                  + fieldName
                  + " for Avro String: "
                  + object.getClass());
        }
      } else if (spannerFieldType.getCode() == Code.TIMESTAMP
          && avroFieldType == Schema.Type.STRING) {
        // Sometimes, the row comes back as a Utf8 and sometimes as a String.
        Object object = inputRow.get(fieldName);
        if (object == null) {
          // Do nothing with NULL value.
        } else if (object instanceof String || object instanceof Utf8) {
          mutationBuilder.set(fieldName).to(Timestamp.parseTimestamp((String) object.toString()));
        } else {
          throw new RuntimeException(
              "Unrecognized format in field "
                  + fieldName
                  + " for Avro String: "
                  + object.getClass());
        }
      } else if (spannerFieldType.getCode() == Code.BYTES && avroFieldType == Schema.Type.BYTES) {
        Object object = inputRow.get(fieldName);
        if (object == null) {
          // Do nothing with NULL value.
        } else if (object instanceof ByteBuffer) {
          // WARNING: If you call ByteArray.copyFrom(ByteBuffer), you will change the pointer of
          // the ByteBuffer and consume its contents. This will mean that the GenericRecord that
          // you received at the beginning of the function is not the same as record at the end.
          // To prevent this, invoke asReadOnlyBuffer() on the ByteBuffer before passing it to
          // ByteArray.copyFrom()
          mutationBuilder
              .set(fieldName)
              .to(ByteArray.copyFrom(((ByteBuffer) object).asReadOnlyBuffer()));
        } else {
          throw new RuntimeException(
              "Unrecognized format in field " + fieldName + " for Avro: " + object.getClass());
        }
      } else if (spannerFieldType.getCode() == Code.ARRAY && avroFieldType == Schema.Type.ARRAY) {
        // Parse the Array.

        Schema nestedArrayAvroColumnType = columnSchema.getElementType();
        if (nestedArrayAvroColumnType.getType() == Schema.Type.UNION) {
          throw new RuntimeException("Cloud Spanner does not support null inside of an Array");
        }

        if (spannerFieldType.getArrayElementType().getCode() == Code.STRING
            && nestedArrayAvroColumnType.getType() == Schema.Type.STRING) {
          // Sometimes, the row comes back as a Utf8 and sometimes as a String.
          List<Object> object = (List<Object>) inputRow.get(fieldName);
          if (object == null) {
            // Do nothing with NULL value.
          } else if (object.get(0) instanceof String || object.get(0) instanceof Utf8) {
            mutationBuilder
                .set(fieldName)
                .toStringArray(
                    object
                        .stream()
                        .map(str -> str == null ? null : str.toString())
                        .collect(Collectors.toList()));
          } else {
            throw new RuntimeException(
                "Unrecognized format in array field "
                    + fieldName
                    + " for Avro String: "
                    + object.get(0).getClass());
          }
        } else if (spannerFieldType.getArrayElementType().getCode() == Code.BOOL
            && nestedArrayAvroColumnType.getType() == Schema.Type.BOOLEAN) {
          List<Boolean> object = (List<Boolean>) inputRow.get(fieldName);
          if (object == null) {
            // Do nothing with NULL value.
          } else {
            mutationBuilder.set(fieldName).toBoolArray(object);
          }
        } else if (spannerFieldType.getArrayElementType().getCode() == Code.INT64
            && nestedArrayAvroColumnType.getType() == Schema.Type.LONG) {
          List<Long> object = (List<Long>) inputRow.get(fieldName);
          if (object == null) {
            // Do nothing with NULL value.
          } else {
            mutationBuilder.set(fieldName).toInt64Array(object);
          }
        } else if (spannerFieldType.getArrayElementType().getCode() == Code.FLOAT64
            && nestedArrayAvroColumnType.getType() == Schema.Type.DOUBLE) {
          List<Double> object = (List<Double>) inputRow.get(fieldName);
          if (object == null) {
            // Do nothing with NULL value.
          } else {
            mutationBuilder.set(fieldName).toFloat64Array(object);
          }
        } else if (spannerFieldType.getArrayElementType().getCode() == Code.DATE
            && nestedArrayAvroColumnType.getType() == Schema.Type.STRING) {
          List<Object> object = (List<Object>) inputRow.get(fieldName);
          if (object == null) {
            // Do nothing with NULL value.
          } else if (object.get(0) instanceof String || object.get(0) instanceof Utf8) {
            mutationBuilder
                .set(fieldName)
                .toDateArray(
                    object
                        .stream()
                        .map(str -> str == null ? null : Date.parseDate(str.toString()))
                        .collect(Collectors.toList()));
          } else {
            throw new RuntimeException(
                "Unrecognized format in array field "
                    + fieldName
                    + " for Avro String Date: "
                    + object.get(0).getClass());
          }
        } else if (spannerFieldType.getArrayElementType().getCode() == Code.TIMESTAMP
            && nestedArrayAvroColumnType.getType() == Schema.Type.STRING) {
          List<Object> object = (List<Object>) inputRow.get(fieldName);
          if (object == null) {
            // Do nothing with NULL value.
          } else if (object.get(0) instanceof String || object.get(0) instanceof Utf8) {
            mutationBuilder
                .set(fieldName)
                .toTimestampArray(
                    object
                        .stream()
                        .map(str -> str == null ? null : Timestamp.parseTimestamp(str.toString()))
                        .collect(Collectors.toList()));
          } else {
            throw new RuntimeException(
                "Unrecognized format in field "
                    + fieldName
                    + " for Avro String Timestamp: "
                    + object.get(0).getClass());
          }
        } else if (spannerFieldType.getArrayElementType().getCode() == Code.BYTES
            && nestedArrayAvroColumnType.getType() == Schema.Type.BYTES) {
          List<ByteBuffer> object = (List<ByteBuffer>) inputRow.get(fieldName);
          if (object == null) {
            // Do nothing with NULL value.
          } else {
            mutationBuilder
                .set(fieldName)
                .toBytesArray(
                    object
                        .stream()
                        // WARNING: If you call ByteArray.copyFrom(ByteBuffer), you will change the
                        // pointer of the ByteBuffer and consume its contents.
                        // This will mean that the GenericRecord that you received at the
                        // beginning of the function is not the same as record at the end.
                        // To prevent this, invoke asReadOnlyBuffer() on the ByteBuffer before
                        // passing it to ByteArray.copyFrom()
                        .map(val -> ByteArray.copyFrom(val.asReadOnlyBuffer()))
                        .collect(Collectors.toList()));
          }
        } else {
          throw new RuntimeException(
              "Unrecognized type in field "
                  + fieldName
                  + " of Avro Array: "
                  + nestedArrayAvroColumnType.getType());
        }
      } else {
        throw new RuntimeException(
            "Error converting field named "
                + fieldName
                + " of Avro field type "
                + avroFieldType
                + " and of Spanner field type "
                + spannerFieldType.getCode());
      }
    }
    return mutationBuilder.build();
  }
}
