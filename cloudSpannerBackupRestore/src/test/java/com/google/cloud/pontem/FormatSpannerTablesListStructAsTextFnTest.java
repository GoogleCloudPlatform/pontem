/*
 * Copyright 2018 Google LLC
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.pontem;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.Struct;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FormatSpannerTablesListStructAsTextFn}. */
@RunWith(JUnit4.class)
public final class FormatSpannerTablesListStructAsTextFnTest {
  @Test
  public void testFormatSpannerTablesListStructAsTextFn_parent() throws Exception {
    String childTableName = "child_table";
    String parentTableName = "parent_table";
    Struct struct =
        Struct.newBuilder()
            .set("table_name")
            .to(childTableName)
            .set("parent_table_name")
            .to(parentTableName)
            .build();
    FormatSpannerTablesListStructAsTextFn simpleFn = new FormatSpannerTablesListStructAsTextFn();
    assertEquals(childTableName + "," + parentTableName, simpleFn.apply(struct));
  }

  @Test
  public void testFormatSpannerTablesListStructAsTextFn_basic() throws Exception {
    String childTableName = "child_table";
    String parentTableName = "";
    Struct struct =
        Struct.newBuilder()
            .set("table_name")
            .to(childTableName)
            .set("parent_table_name")
            .to(parentTableName)
            .build();
    FormatSpannerTablesListStructAsTextFn simpleFn = new FormatSpannerTablesListStructAsTextFn();
    assertEquals(childTableName + "," + parentTableName, simpleFn.apply(struct));
  }

  @Test
  public void testFormatSpannerTablesListStructAsTextFn_basicNull() throws Exception {
    String childTableName = "child_table";
    String parentTableName = null;
    Struct struct =
        Struct.newBuilder()
            .set("table_name")
            .to(childTableName)
            .set("parent_table_name")
            .to(parentTableName)
            .build();
    FormatSpannerTablesListStructAsTextFn simpleFn = new FormatSpannerTablesListStructAsTextFn();
    assertEquals(childTableName + ",", simpleFn.apply(struct));
  }
}
