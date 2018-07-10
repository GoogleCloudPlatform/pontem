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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.Iterables;
import java.net.URI;
import java.util.logging.Logger;

/** Utility function for GCS calls in Cloud Spanner backup and restore. */
public class GcsUtil {
  private static final Logger LOG = Logger.getLogger(GcsUtil.class.getName());
  /** Get the number of GCS blobs in a file path. */
  public int getNumGcsBlobsInGcsFilePath(
      String projectId, String gcsBucketName, String gcsFolderPath) {
    StorageOptions.Builder optionsBuilder = StorageOptions.newBuilder();
    StorageOptions storageOptions = optionsBuilder.setProjectId(projectId).build();
    Storage storage = storageOptions.getService();

    // Prefix cannot have "/" at beginning or end.
    if (gcsFolderPath.charAt(0) == '/') {
      gcsFolderPath = gcsFolderPath.substring(1);
    }
    if (gcsFolderPath.charAt(gcsFolderPath.length() - 1) == '/') {
      gcsFolderPath = gcsFolderPath.substring(0, gcsFolderPath.length() - 1);
    }

    Iterable<Blob> blobs =
        storage.list(gcsBucketName, Storage.BlobListOption.prefix(gcsFolderPath)).iterateAll();

    int numGcsBlobs = Iterables.size(blobs);
    LOG.info(
        "Number of GCS blobs in bucket '"
            + gcsBucketName
            + "' and folder '"
            + gcsFolderPath
            + "' is: "
            + numGcsBlobs);
    return numGcsBlobs;
  }

  /** Get path to write output of backup to. */
  public static String getFormattedOutputPath(String baseFolderPath) {
    if (baseFolderPath.endsWith("/")) {
      return baseFolderPath;
    } else {
      return baseFolderPath + "/";
    }
  }

  /**
   * Fetch the contents of a text file from Google Cloud Storage and return it as a string.
   *
   * @param pathToRootOfBackup e.g., "/backups/latest/" if the file in question is
   *     "gs://my-cloud-spanner-project/backup_today/table_schema/mytable.txt"
   * @param filenameFromRootOfBackup e.g., "table_schema/mytable.txt" if the file in question is
   *     "gs://my-cloud-spanner-project/backup_today/table_schema/mytable.txt"
   */
  public String getContentsOfFileFromGcs(
      String projectId,
      String bucketName,
      String pathToRootOfBackup,
      String filenameFromRootOfBackup)
      throws Exception {
    StorageOptions.Builder optionsBuilder = StorageOptions.newBuilder();
    StorageOptions storageOptions = optionsBuilder.setProjectId(projectId).build();
    Storage storage = storageOptions.getService();

    // Blob path should not start with '/'
    if (pathToRootOfBackup.charAt(0) == '/') {
      pathToRootOfBackup = pathToRootOfBackup.substring(1);
    }

    BlobId blobId = BlobId.of(bucketName, pathToRootOfBackup + filenameFromRootOfBackup);
    Blob blob = storage.get(blobId);
    if (blob == null) {
      throw new Exception(
          "No such object in GCS:\nBucketName: "
              + bucketName
              + "\nPathToRoot: "
              + pathToRootOfBackup
              + "\nFilenameFromRoot: "
              + filenameFromRootOfBackup
              + "\n\nBlobId: "
              + blobId.toString());
    }

    if (blob.getSize() < 1_000_000) {
      // Blob is small, so read all its content in one request
      byte[] content = blob.getContent();
      String fileContents = new String(content);
      return fileContents;
    }
    throw new Exception("Metadata file is unexpectedly large");
  }

  /**
   * Write the contents of a file to disk as text in GCS.
   *
   * @param pathToRootOfBackup e.g., "/backups/latest/" if the file in question is
   *     "gs://my-cloud-spanner-project/backup_today/table_schema/mytable.txt"
   * @param filenameFromRootOfBackup e.g., "table_schema/mytable.txt" if the file in question is
   *     "gs://my-cloud-spanner-project/backup_today/table_schema/mytable.txt"
   */
  public void writeContentsToGcs(
      String contents,
      String projectId,
      String bucketName,
      String pathToRootOfBackup,
      String filenameFromRootOfBackup) {

    StorageOptions.Builder optionsBuilder = StorageOptions.newBuilder();
    StorageOptions storageOptions = optionsBuilder.setProjectId(projectId).build();
    Storage storage = storageOptions.getService();

    // Blob path should not start with '/'
    if (pathToRootOfBackup.charAt(0) == '/') {
      pathToRootOfBackup = pathToRootOfBackup.substring(1);
    }

    BlobId blobId = BlobId.of(bucketName, pathToRootOfBackup + filenameFromRootOfBackup);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();

    byte[] bytes = contents.getBytes();
    // since file will be less than 1MB, create the blob in one request.
    storage.create(blobInfo, bytes);
  }

  /**
   * @param databaseBackupLocation E.g., gs://my-cloud-spanner-project/multi-backup
   * @return GCS bucket E.g., "my-cloud-spanner-project"
   */
  public static String getGcsBucketNameFromDatabaseBackupLocation(String databaseBackupLocation)
      throws Exception {
    URI uri = new URI(databaseBackupLocation);
    if (!uri.getScheme().toLowerCase().equals("gs")) {
      throw new Exception("Database backup location looks malformed");
    }
    return uri.getAuthority();
  }

  /**
   * @param databaseBackupLocation E.g., gs://my-cloud-spanner-project/multi-backup
   * @return Path from root of GCS bucket to root of database backup E.g., /multi-backup/
   */
  public static String getGcsFolderPathFromDatabaseBackupLocation(String databaseBackupLocation)
      throws Exception {
    URI uri = new URI(databaseBackupLocation);
    if (!uri.getScheme().toLowerCase().equals("gs")) {
      throw new Exception("Database backup location looks malformed");
    }
    String path = uri.getPath();
    if (path.length() == 0) {
      path = "/";
    }
    if (path.length() > 1 && path.charAt(0) != '/') {
      path = '/' + path;
    }
    if (path.charAt(path.length() - 1) != '/') {
      path += '/';
    }
    return path;
  }
}
