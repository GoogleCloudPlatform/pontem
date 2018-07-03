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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.Schema;

/** Wrapper class for storing information about a table. */
public class TableInformation {
  private static final Logger LOG = Logger.getLogger(TableInformation.class.getName());

  private final ImmutableMap<String, Type> mapOfColumnNamesToSpannerTypes;
  private final ImmutableMap<String, Schema> mapOfColumnNamesToAvroTypes;
  private final ImmutableMap<String, Boolean> mapOfColumnNamesToNullable;

  public TableInformation(String tableDdl) {

    // Parse out the specific column statements from the entire Table DDL.
    Pattern compiledPattern =
        Pattern.compile(
            "^CREATE TABLE[\\s]*[a-zA-Z0-9_]*[\\s]?\\((.*)\\)[\\s]*PRIMARY KEY.*$", Pattern.DOTALL);
    Matcher matcher = compiledPattern.matcher(tableDdl);
    if (!matcher.matches()) {
      throw new RuntimeException("Unparsable Table DDL:\n" + tableDdl);
    }

    // Look through each column statement, maping column name to type and nullability.
    Map<String, Type> mapOfColumnNamesToType = new HashMap<String, Type>();
    String fullColumnStatement = matcher.group(1).trim();
    if (fullColumnStatement.substring(fullColumnStatement.length() - 1).equals(",")) {
      fullColumnStatement = fullColumnStatement.substring(0, fullColumnStatement.length() - 1);
    }
    String[] columnStatements = fullColumnStatement.split(",");

    ImmutableMap.Builder<String, Type> spannerTypeBuilder =
        new ImmutableMap.Builder<String, Type>();
    ImmutableMap.Builder<String, Schema> avroTypeBuilder =
        new ImmutableMap.Builder<String, Schema>();
    ImmutableMap.Builder<String, Boolean> nullableBuilder =
        new ImmutableMap.Builder<String, Boolean>();

    for (String columnStatement : columnStatements) {
      columnStatement = columnStatement.trim();

      // parse out the name and type of column as well as nullability
      // E.g., "dateArray ARRAY<DATE> NOT NULL,"
      // E.g., "int64_ INT64 NOT NULL,"
      // E.g., "timestamp TIMESTAMP NOT NULL,"
      Pattern compiledColumnPattern =
          Pattern.compile(
              "^([a-zA-Z0-9_]+)[\\s]+([<>A-z0-9()]+)[\\s]?(NOT NULL)?.*$", Pattern.DOTALL);
      Matcher matcherColumnPattern = compiledColumnPattern.matcher(columnStatement);
      if (!matcherColumnPattern.matches()) {
        throw new RuntimeException("Unparsable Column Definition Statement:\n" + columnStatement);
      }
      String columnName = matcherColumnPattern.group(1);
      String colTypeAsString = matcherColumnPattern.group(2);
      boolean isNullable = true;
      try {
        String notNullStr = matcherColumnPattern.group(3);
        if (!Strings.isNullOrEmpty(notNullStr) && notNullStr.trim().equals("NOT NULL")) {
          isNullable = false;
        }
      } catch (IndexOutOfBoundsException e) {
        // Group does not exist.
      }

      LOG.info("Mapped " + columnName + " to Cloud Spanner Type, " + colTypeAsString);
      Type spannerColumnType = SpannerUtil.getSpannerType(colTypeAsString);
      spannerTypeBuilder.put(columnName, spannerColumnType);
      avroTypeBuilder.put(
          columnName, AvroUtil.getAvroTypeFromSpannerType(spannerColumnType, isNullable));
      nullableBuilder.put(columnName, isNullable);
    }

    mapOfColumnNamesToSpannerTypes = spannerTypeBuilder.build();
    mapOfColumnNamesToAvroTypes = avroTypeBuilder.build();
    mapOfColumnNamesToNullable = nullableBuilder.build();

    if (mapOfColumnNamesToSpannerTypes.size() != mapOfColumnNamesToAvroTypes.size()
        || mapOfColumnNamesToAvroTypes.size() != mapOfColumnNamesToNullable.size()) {
      throw new RuntimeException("Error occurred parsing table information.");
    }
  }

  public Set<String> getColumnNames() {
    return mapOfColumnNamesToSpannerTypes.keySet();
  }

  public ImmutableMap<String, Type> getMapOfColumnNamesToSpannerTypes() {
    return mapOfColumnNamesToSpannerTypes;
  }

  public ImmutableMap<String, Boolean> getMapOfColumnNamesToNullable() {
    return mapOfColumnNamesToNullable;
  }

  public ImmutableMap<String, Schema> getMapOfColumnNamesToAvroTypes() {
    return mapOfColumnNamesToAvroTypes;
  }

  public Type getSpannerTypeOfColumn(String columnName) {
    if (!mapOfColumnNamesToSpannerTypes.containsKey(columnName)) {
      throw new RuntimeException("Unable to find column name " + columnName);
    }
    return mapOfColumnNamesToSpannerTypes.get(columnName);
  }

  public Schema getAvroTypeOfColumn(String columnName) {
    if (!mapOfColumnNamesToAvroTypes.containsKey(columnName)) {
      throw new RuntimeException("Unable to find column name " + columnName);
    }
    return mapOfColumnNamesToAvroTypes.get(columnName);
  }

  public boolean isColumnNullable(String columnName) {
    if (!mapOfColumnNamesToNullable.containsKey(columnName)) {
      throw new RuntimeException("Unable to find column name " + columnName);
    }
    return mapOfColumnNamesToNullable.get(columnName);
  }
}
