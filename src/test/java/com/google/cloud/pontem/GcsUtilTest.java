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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Util}. */
@RunWith(JUnit4.class)
public class GcsUtilTest {

  @Test
  public void testGetOutputPath() {
    assertEquals("foo/bar/", GcsUtil.getFormattedOutputPath("foo/bar"));
    assertEquals("foo/bar/", GcsUtil.getFormattedOutputPath("foo/bar/"));
  }

  @Test
  public void testGetGcsBucketNameFromDatabaseBackupLocation() throws Exception {
    assertEquals(
        "Bucket name parsing failed",
        "cloud-spanner-backup-test",
        GcsUtil.getGcsBucketNameFromDatabaseBackupLocation(
            "gs://cloud-spanner-backup-test/multi-backup"));

    assertEquals(
        "Bucket name parsing failed",
        "bucketName",
        GcsUtil.getGcsBucketNameFromDatabaseBackupLocation("gs://bucketName/multi-backup/djskd"));

    assertEquals(
        "Bucket name parsing failed",
        "bucketName2",
        GcsUtil.getGcsBucketNameFromDatabaseBackupLocation("gs://bucketName2/"));
  }

  @Test(expected = Exception.class)
  public void testGetGcsBucketNameFromDatabaseBackupLocation_invalidScheme() throws Exception {
    GcsUtil.getGcsBucketNameFromDatabaseBackupLocation(
        "cs://cloud-spanner-backup-test/multi-backup");
  }

  @Test
  public void testGetGcsFolderPathFromDatabaseBackupLocation() throws Exception {
    assertEquals(
        "Folder path parsing failed",
        "/multi-backup/djskd/",
        GcsUtil.getGcsFolderPathFromDatabaseBackupLocation("gs://bucketName/multi-backup/djskd"));
    assertEquals(
        "Folder path parsing failed",
        "/multi-backup/",
        GcsUtil.getGcsFolderPathFromDatabaseBackupLocation("gs://bucketName/multi-backup/"));
    assertEquals(
        "Folder path parsing failed",
        "/",
        GcsUtil.getGcsFolderPathFromDatabaseBackupLocation("gs://bucketName"));
  }

  @Test(expected = Exception.class)
  public void testGetGcsFolderPathFromDatabaseBackupLocation_invalid() throws Exception {
    GcsUtil.getGcsFolderPathFromDatabaseBackupLocation("cs://bucketName/multi-backup/djskd");
  }
}
