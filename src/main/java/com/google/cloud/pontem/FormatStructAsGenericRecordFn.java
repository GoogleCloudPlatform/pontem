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
import com.google.cloud.spanner.Type.Code;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.transforms.SimpleFunction;

/**
 * A {@code SimpleFunction} that converts a {@code Struct} containing any type of data to a {@code
 * GenericRecord} Avro file that can be written to disk.
 */
public class FormatStructAsGenericRecordFn extends SimpleFunction<Struct, GenericRecord> {
  private final String schemaAsString;

  FormatStructAsGenericRecordFn(String schemaAsString) throws Exception {
    // TODO: Use AvroUtils.serializableSchemaSupplier
    this.schemaAsString = schemaAsString;
  }

  @Override
  public GenericRecord apply(Struct inputRow) {
    Schema schema = new Schema.Parser().parse(this.schemaAsString);
    GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);

    for (Schema.Field column : schema.getFields()) {
      String columnName = column.name();
      Schema columnTypeSchema = column.schema();

      // Since the Avro column schema could be a UNION of types (e.g., [STRING, NULL]),
      // we need to unpack that UNION to simply be a noramlly recognized Avro type (e.g., STRING).
      if (columnTypeSchema.getType() == Schema.Type.UNION) {
        columnTypeSchema = AvroUtil.getSingleAvroTypeFromNullableUnion(columnTypeSchema);
      }

      boolean isSpannerColumnValueNull = inputRow.isNull(columnName);
      // WARNING: Calling getColumnType(columnName) on a {@type Struct} will have the effect of
      // changing the serialized value of the {@type Struct}. The underlying data will not change,
      // but the serialized value will. Consequently, every {@type Struct} that passes through
      // here will not have the same serialized value.
      if (columnTypeSchema.getType() == Schema.Type.STRING
          && inputRow.getColumnType(columnName).getCode() == Code.STRING) {
        genericRecordBuilder.set(
            column, isSpannerColumnValueNull ? null : inputRow.getString(columnName));
      } else if (columnTypeSchema.getType() == Schema.Type.BOOLEAN
          && inputRow.getColumnType(columnName).getCode() == Code.BOOL) {
        genericRecordBuilder.set(
            column, isSpannerColumnValueNull ? null : inputRow.getBoolean(columnName));
      } else if (columnTypeSchema.getType() == Schema.Type.LONG
          && inputRow.getColumnType(columnName).getCode() == Code.INT64) {
        genericRecordBuilder.set(
            column, isSpannerColumnValueNull ? null : inputRow.getLong(columnName));
      } else if (columnTypeSchema.getType() == Schema.Type.DOUBLE
          && inputRow.getColumnType(columnName).getCode() == Code.FLOAT64) {
        genericRecordBuilder.set(
            column, isSpannerColumnValueNull ? null : inputRow.getDouble(columnName));
      } else if (columnTypeSchema.getType() == Schema.Type.BYTES
          && inputRow.getColumnType(columnName).getCode() == Code.BYTES) {
        genericRecordBuilder.set(
            column,
            isSpannerColumnValueNull
                ? null
                : ByteBuffer.wrap(inputRow.getBytes(columnName).toByteArray()));
      } else if (columnTypeSchema.getType() == Schema.Type.STRING
          && inputRow.getColumnType(columnName).getCode() == Code.DATE) {
        genericRecordBuilder.set(
            column, isSpannerColumnValueNull ? null : inputRow.getDate(columnName).toString());
      } else if (columnTypeSchema.getType() == Schema.Type.STRING
          && inputRow.getColumnType(columnName).getCode() == Code.TIMESTAMP) {
        genericRecordBuilder.set(
            column, isSpannerColumnValueNull ? null : inputRow.getTimestamp(columnName).toString());
      } else if (columnTypeSchema.getType() == Schema.Type.ARRAY
          && inputRow.getColumnType(columnName).getCode() == Code.ARRAY) {
        // Parse Array Type
        if (columnTypeSchema.getElementType().getType() == Schema.Type.STRING
            && inputRow.getColumnType(columnName).getArrayElementType().getCode() == Code.STRING) {
          genericRecordBuilder.set(
              column, isSpannerColumnValueNull ? null : inputRow.getStringList(columnName));
        } else if (columnTypeSchema.getElementType().getType() == Schema.Type.BOOLEAN
            && inputRow.getColumnType(columnName).getArrayElementType().getCode() == Code.BOOL) {
          genericRecordBuilder.set(
              column, isSpannerColumnValueNull ? null : inputRow.getBooleanList(columnName));
        } else if (columnTypeSchema.getElementType().getType() == Schema.Type.LONG
            && inputRow.getColumnType(columnName).getArrayElementType().getCode() == Code.INT64) {
          genericRecordBuilder.set(
              column, isSpannerColumnValueNull ? null : inputRow.getLongList(columnName));
        } else if (columnTypeSchema.getElementType().getType() == Schema.Type.DOUBLE
            && inputRow.getColumnType(columnName).getArrayElementType().getCode() == Code.FLOAT64) {
          genericRecordBuilder.set(
              column, isSpannerColumnValueNull ? null : inputRow.getDoubleList(columnName));
        } else if (columnTypeSchema.getElementType().getType() == Schema.Type.BYTES
            && inputRow.getColumnType(columnName).getArrayElementType().getCode() == Code.BYTES) {
          if (!isSpannerColumnValueNull) {
            List<ByteBuffer> bytesList =
                inputRow
                    .getBytesList(columnName)
                    .stream()
                    .map(bytes -> ByteBuffer.wrap(bytes.toByteArray()))
                    .collect(Collectors.toList());
            genericRecordBuilder.set(column, bytesList);
          } else {
            genericRecordBuilder.set(column, null);
          }
        } else if (columnTypeSchema.getElementType().getType() == Schema.Type.STRING
            && inputRow.getColumnType(columnName).getArrayElementType().getCode() == Code.DATE) {
          if (!isSpannerColumnValueNull) {
            List<String> dates =
                inputRow
                    .getDateList(columnName)
                    .stream()
                    .map(date -> date.toString())
                    .collect(Collectors.toList());
            genericRecordBuilder.set(column, dates);
          } else {
            genericRecordBuilder.set(column, null);
          }
        } else if (columnTypeSchema.getElementType().getType() == Schema.Type.STRING
            && inputRow.getColumnType(columnName).getArrayElementType().getCode()
                == Code.TIMESTAMP) {
          if (!isSpannerColumnValueNull) {
            List<String> timestamps =
                inputRow
                    .getTimestampList(columnName)
                    .stream()
                    .map(timestamp -> timestamp.toString())
                    .collect(Collectors.toList());
            genericRecordBuilder.set(column, timestamps);
          } else {
            genericRecordBuilder.set(column, null);
          }
        } else {
          throw new RuntimeException(
              "Unrecognized array type for column name "
                  + columnName
                  + " and Avro Array type "
                  + columnTypeSchema.getElementType().getType());
        }
      } else {
        throw new RuntimeException(
            "Unrecognized type for column name "
                + columnName
                + " and AvroType "
                + columnTypeSchema.toString()
                + " and SpannerType "
                + inputRow.getColumnType(columnName).getCode());
      }
    }
    return genericRecordBuilder.build();
  }
}
