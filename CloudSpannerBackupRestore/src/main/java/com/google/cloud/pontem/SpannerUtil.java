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

import com.google.api.gax.paging.Page;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Operation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utility function for Spanner operations. */
public class SpannerUtil {
  public static final String CLOUD_SPANNER_API_ENDPOINT_HOSTNAME = "https://spanner.googleapis.com";
  public static final String FILE_PATH_FOR_DATABASE_DDL = "metadata/database_ddl.txt";
  // Use a delimeter to delinate the statements in a database DDL.
  public static final String DDL_DELIMITER = "\n##\n";
  private static final Logger LOG = Logger.getLogger(SpannerUtil.class.getName());
  // parse out the name of the table
  private static final Pattern COMPILED_TABLE_PATTERN =
      Pattern.compile("^CREATE TABLE[\\s]*([a-zA-Z0-9_]+)[\\s]?\\(.*$", Pattern.DOTALL);
  // parse out the specific column statements
  private static final Pattern COMPILED_ARRAY_PATTERN =
      Pattern.compile("^ARRAY<([a-zA-Z0-9()]+)>$", Pattern.DOTALL);

  /**
   * Returns a SpannerOptions.Builder pre-configured with the correct API endpoint and Pontem's User
   * Agent
   */
  public static SpannerOptions.Builder getSpannerOptionsBuilder() {
    SpannerOptions.Builder options =
        SpannerOptions.newBuilder()
            .setHost(SpannerUtil.CLOUD_SPANNER_API_ENDPOINT_HOSTNAME);
            //.setUserAgentPrefix(Util.USER_AGENT_PREFIX);
    return options;
  }

  /**
   * Convert the Cloud Spanner type from {@type String} to {@type Type}.
   *
   * @see:
   *     https://googleapis.github.io/google-cloud-java/google-cloud-clients/apidocs/com/google/cloud/spanner/Type.Code.html
   */
  public static Type getSpannerType(String typeAsString) {
    typeAsString = typeAsString.trim();
    if (typeAsString.startsWith("STRING")) {
      return Type.string();
    } else if (typeAsString.startsWith("INT64")) {
      return Type.int64();
    } else if (typeAsString.startsWith("DATE")) {
      return Type.date();
    } else if (typeAsString.startsWith("BOOL")) {
      return Type.bool();
    } else if (typeAsString.startsWith("FLOAT64")) {
      return Type.float64();
    } else if (typeAsString.startsWith("BYTES")) {
      return Type.bytes();
    } else if (typeAsString.startsWith("TIMESTAMP")) {
      return Type.timestamp();
    } else if (typeAsString.startsWith("ARRAY<") && typeAsString.endsWith(">")) {
      // parse out the specific column statements
      Matcher matcher = COMPILED_ARRAY_PATTERN.matcher(typeAsString);
      if (!matcher.matches()) {
        throw new RuntimeException("Unparsable Array Type:\n" + typeAsString);
      }
      String subTypeAsString = matcher.group(1);
      return Type.array(getSpannerType(subTypeAsString));
    } else {
      throw new RuntimeException("Unparsable Cloud Spanner Type: " + typeAsString);
    }
  }

  /**
   * Read in a DDL for an entire database, parse it into a map of DDLs for each table. Ignore DDL
   * statements that are not tables (e.g., indexes).
   */
  public static ImmutableMap<String, String> convertEntireDtaabaseDdlIntoTableSpecificDdl(
      ImmutableList<String> ddl) {
    Map<String, String> map = new HashMap<String, String>();
    for (String ddlStatement : ddl) {
      ddlStatement = ddlStatement.trim();
      if (!ddlStatement.substring(0, 12).equals("CREATE TABLE")) {
        // DDL statement is not for a table, so move on.
        continue;
      }
      Matcher matcher = COMPILED_TABLE_PATTERN.matcher(ddlStatement);
      if (!matcher.matches()) {
        throw new RuntimeException("Unparsable DDL:\n" + ddlStatement);
      }
      String tableName = matcher.group(1);
      if (Strings.isNullOrEmpty(tableName)) {
        throw new RuntimeException("Unable to parse table name. DDL Statement:\n" + ddlStatement);
      }

      map.put(tableName, ddlStatement);
    }
    return ImmutableMap.copyOf(map);
  }

  /** Fetch a list of all database names. */
  public static ImmutableList<String> getListOfDatabaseNames(
      String projectId, String instanceId, int numDatabases) {
    LOG.info("Begin getting list of database names");
    SpannerOptions options = SpannerUtil.getSpannerOptionsBuilder().build();
    Spanner spanner = options.getService();

    DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();

    List<String> databaseNames = new ArrayList<>();

    try {
      Page<Database> page = dbAdminClient.listDatabases(instanceId, Options.pageSize(numDatabases));
      Iterable<Database> allDatabases = page.iterateAll();

      for (Database database : allDatabases) {
        // projects/project-name/instances/my-cloud-spanner-instance/databases/myDbName
        String fullDatabasePathName = database.getId().getName();
        String databaseName =
            fullDatabasePathName.substring(fullDatabasePathName.lastIndexOf("/") + 1);
        databaseNames.add(databaseName);
      }
    } finally {
      spanner.close();
    }
    LOG.info("End getting list of database names, size: " + databaseNames.size());
    return ImmutableList.copyOf(databaseNames);
  }

  public static String convertDdlListIntoRawText(ImmutableList<String> ddlStatements) {
    String databaseDdlAsString = String.join(DDL_DELIMITER, ddlStatements);
    return databaseDdlAsString;
  }

  /**
   * Take in raw text version of a database's DDL and return a list of DDL statements. The list of
   * DDL statements can be executed sequentially.
   *
   * @see: https://cloud.google.com/spanner/docs/data-definition-language
   */
  public static ImmutableList<String> convertRawDdlIntoDdlList(String rawDdl) {
    List<String> statements = new ArrayList<String>(Arrays.asList(rawDdl.split(DDL_DELIMITER)));
    return ImmutableList.copyOf(statements);
  }

  /** Convert a Spanner {@type Struct} into a {@type Mutation}. */
  public static Mutation convertStructToMutation(Struct struct, String tableName) {
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
  }

  /**
   * Fetch the DDL for the database. See https://bit.ly/2qVpToj for more.
   *
   * @return DDL for the database in sequential order.
   */
  public ImmutableList<String> queryDatabaseDdl(
      String projectId, String instance, String databaseId) {
    LOG.info("Query database DDL for database " + databaseId);
    SpannerOptions options = SpannerUtil.getSpannerOptionsBuilder().build();
    Spanner spanner = options.getService();

    List<String> ddl = Lists.newArrayList();
    try {
      DatabaseId db = DatabaseId.of(projectId, instance, databaseId);
      DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
      ddl = dbAdminClient.getDatabaseDdl(instance, databaseId);
    } finally {
      spanner.close();
    }
    LOG.info("Raw DDL statements from database " + databaseId + ":");
    for (String ddlStatement : ddl) {
      LOG.info(ddlStatement + "\n");
    }
    return ImmutableList.copyOf(ddl);
  }

  /**
   * Return structs from a single query. Result structs must fit into memory, so these cannot be
   * huge queries.
   */
  public ImmutableList<Struct> performSingleSpannerReadQuery(
      String projectId, String instance, String databaseId, String querySql) {
    return performSingleSpannerReadQueryAtTimestamp(
        projectId, instance, databaseId, querySql, Timestamp.now());
  }

  /**
   * Return structs from a single query. Result structs must fit into memory, so these cannot be
   * huge queries. If you want to just use the current timestamp, pass Timestamp.now()
   */
  public ImmutableList<Struct> performSingleSpannerReadQueryAtTimestamp(
      String projectId,
      String instance,
      String databaseId,
      String querySql,
      Timestamp readTimestamp) {
    LOG.info("Begin performing single Spanner query on database " + databaseId);
    SpannerOptions options = SpannerUtil.getSpannerOptionsBuilder().build();
    Spanner spanner = options.getService();

    List<Struct> resultsAsStruct = Lists.newArrayList();
    try {
      DatabaseId db = DatabaseId.of(projectId, instance, databaseId);
      DatabaseClient dbClient = spanner.getDatabaseClient(db);

      ResultSet resultSet =
          dbClient
              .singleUse(TimestampBound.ofReadTimestamp(readTimestamp))
              .executeQuery(Statement.of(querySql));
      while (resultSet.next()) {
        resultsAsStruct.add(resultSet.getCurrentRowAsStruct());
      }
    } finally {
      spanner.close();
    }
    return ImmutableList.copyOf(resultsAsStruct);
  }

  /**
   * Create database and populate it with a specific database definition language.
   *
   * <p>See https://cloud.google.com/spanner/docs/data-definition-language
   */
  public void createDatabaseAndTables(
      String projectId, String instanceId, String databaseId, ImmutableList<String> databaseDdl)
      throws Exception {
    LOG.info("Begin creating Cloud Spanner database " + databaseId);
    SpannerOptions options = SpannerUtil.getSpannerOptionsBuilder().build();
    Spanner spanner = options.getService();
    try {
      DatabaseId db = DatabaseId.of(projectId, instanceId, databaseId);
      DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();

      dbAdminClient = spanner.getDatabaseAdminClient();
      LOG.info(
          "Attempting to create database named '"
              + databaseId
              + "' in instance '"
              + instanceId
              + "' and project '"
              + projectId
              + "'");
      Operation<Database, CreateDatabaseMetadata> operation =
          dbAdminClient.createDatabase(instanceId, databaseId, databaseDdl).waitFor();
      // If the database already exists, then this attempt to create it again will fail and
      // throw an exception.
      if (!operation.isSuccessful()) {
        throw new Exception(
            "Failure creating database named '"
                + databaseId
                + "' in instance '"
                + instanceId
                + "' and project '"
                + projectId
                + "'");
      }
      LOG.info(
          "Successfully created database named '"
              + databaseId
              + "' in instance '"
              + instanceId
              + "' and project '"
              + projectId
              + "'");
    } catch (SpannerException e) {
      LOG.info("Error creating database " + databaseId + ":\n" + e.toString());
      throw e;
    } finally {
      spanner.close();
    }
    LOG.info("End creating Cloud Spanner database " + databaseId);
  }
}
