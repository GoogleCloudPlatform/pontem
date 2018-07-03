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

import com.google.cloud.spanner.Type;
import java.util.List;
import java.util.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

/** Utils for Avro */
public class AvroUtil {
  private static final Logger LOG = Logger.getLogger(AvroUtil.class.getName());
  public static final String AVRO_SCHEMA_FOLDER_LOCATION = "avro_schemas";
  public static final String AVRO_SCHEMA_NAMESPACE = "com.google.cloud.pontem";
  public static final String AVRO_RECORD_VERSION = "1";

  /**
   * Since the Avro column schema could be a UNION of types (e.g., [STRING, NULL]), we need to
   * unpack that UNION to simply be a noramlly recognized Avro type (e.g., STRING).
   */
  public static Schema getSingleAvroTypeFromNullableUnion(Schema columnSchema) {
    if (columnSchema.getType() == Schema.Type.UNION) {
      List<Schema> columnUnionAvroTypes = columnSchema.getTypes();
      if (columnUnionAvroTypes.size() != 2) {
        throw new IllegalArgumentException("AvroRecord contains an unexpected format");
      } else if (columnUnionAvroTypes.get(0).getType() == Schema.Type.NULL) {
        return columnUnionAvroTypes.get(1);
      } else if (columnUnionAvroTypes.get(1).getType() == Schema.Type.NULL) {
        return columnUnionAvroTypes.get(0);
      } else {
        throw new IllegalArgumentException("AvroRecord contains an unexpected format");
      }
    }
    throw new IllegalArgumentException("AvroRecord contains an unexpected format");
  }

  /**
   * Build the Avro schema for a specific Cloud Spanner table.
   *
   * @see http://avro.apache.org/docs/1.7.7/api/java/index.html?org/apache/avro/Schema.Type.html
   * @see /google-cloud-java/google-cloud-clients/apidocs/com/google/cloud/spanner/Type.Code.html
   */
  public static Schema buildSchemaForTable(String recordName, TableInformation tableInformation) {
    SchemaBuilder.RecordBuilder<Schema> baseRecordBuilder =
        SchemaBuilder.record(recordName).namespace(AVRO_SCHEMA_NAMESPACE);
    baseRecordBuilder.prop("pontemAvroSchemaFormattingVersion", AVRO_RECORD_VERSION);

    SchemaBuilder.FieldAssembler<Schema> schemaBuilderFieldAssembler = baseRecordBuilder.fields();

    for (String columnName : tableInformation.getColumnNames()) {
      schemaBuilderFieldAssembler
          .name(columnName)
          .type(tableInformation.getAvroTypeOfColumn(columnName))
          .noDefault();
    }

    Schema schema = schemaBuilderFieldAssembler.endRecord();
    LOG.info("Schema:\n" + schema.toString());
    return schema;
  }

  public static Schema getAvroTypeFromSpannerType(Type cloudSpannerType, boolean isNullable) {
    SchemaBuilder.TypeBuilder<Schema> builder = SchemaBuilder.builder();
    switch (cloudSpannerType.getCode()) {
      case STRING:
        if (isNullable) {
          return builder.nullable().stringType();
        } else {
          return builder.stringType();
        }
      case BOOL:
        if (isNullable) {
          return builder.nullable().booleanType();
        } else {
          return builder.booleanType();
        }
      case INT64:
        if (isNullable) {
          return builder.nullable().longType();
        } else {
          return builder.longType();
        }
      case FLOAT64:
        if (isNullable) {
          return builder.nullable().doubleType();
        } else {
          return builder.doubleType();
        }
      case BYTES:
        if (isNullable) {
          return builder.nullable().bytesType();
        } else {
          return builder.bytesType();
        }
      case TIMESTAMP:
        if (isNullable) {
          return builder.nullable().stringType();
        } else {
          return builder.stringType();
        }
      case DATE:
        if (isNullable) {
          return builder.nullable().stringType();
        } else {
          return builder.stringType();
        }
      case ARRAY:
        // Types contained in arrays cannot include "NOT NULL" in syntax, so they are not nullable.
        // I.e., DDL can only contain "ARRAY<STRING(MAX)> NOT NULL" and
        // NOT "ARRAY<STRING(MAX) NOT NULL> NOT NULL"
        Schema avroArrayType =
            getAvroTypeFromSpannerType(cloudSpannerType.getArrayElementType(), false);
        if (isNullable) {
          return SchemaBuilder.builder().nullable().array().items().type(avroArrayType);
        } else {
          return SchemaBuilder.builder().array().items().type(avroArrayType);
        }
      default:
        throw new IllegalArgumentException("Unknown Cloud Spanner type " + cloudSpannerType);
    }
  }

  public static String getAvroSchemaFileLocation(String tableName) {
    return "metadata/" + AvroUtil.AVRO_SCHEMA_FOLDER_LOCATION + "/" + tableName + ".json";
  }
}
