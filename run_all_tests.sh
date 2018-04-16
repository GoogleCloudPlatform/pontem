#!/bin/bash
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Parse Flags
for i in "$@"
do
case $i in
    -p=*|--project=*)
    GCP_PROJECT="${i#*=}"
    shift # past argument=value
    ;;
    -b=*|--bucket=*)
    GCP_BUCKET="${i#*=}"
    shift # past argument=value
    ;;
    -d=*|--database=*)
    DATABASE_INSTANCE="${i#*=}"
    shift # past argument=value
    ;;
    --default)
    DEFAULT=YES
    shift # past argument with no value
    ;;
    *)
          # unknown option
    ;;
esac
done

if [ -z ${GCP_PROJECT} ]; then
  echo "No project set."
  exit 1
else
  echo "GCP Project Set: ${GCP_PROJECT}"
fi

if [ -z ${GCP_BUCKET} ]; then
  echo "No bucket set."
  exit 1
else
  echo "GCP Bucket Set: ${GCP_BUCKET}"
fi

if [ -z ${DATABASE_INSTANCE} ]; then
  echo "No database instance set."
  exit 1
else
  echo "Database Instance Set: ${DATABASE_INSTANCE}"
fi

echo "FINISHED parsing flags."

# Run Unit Tests
mvn clean test || exit 1

echo "FINISHED running unit tests."

# Run End-To-End (E2E) Tests

## Setup
mvn compile exec:java \
   -Dexec.mainClass=com.google.cloud.pontem.EndToEndHelper \
   -Dexec.args="--projectId=${GCP_PROJECT} \
                --gcsRootBackupFolderPath=gs://${GCP_BUCKET}/backup \
                --databaseInstanceId=${DATABASE_INSTANCE} \
                --databaseId=my-database \
                --operation=setup" || exit 1

echo "FINISHED setup phase."

## Backup
mvn clean compile exec:java  -Dexec.mainClass=com.google.cloud.pontem.CloudSpannerDatabaseBackup  -Dexec.args="--runner=DataflowRunner \
 --project=${GCP_PROJECT} \
 --gcpTempLocation=gs://${GCP_BUCKET}/tmp \
 --inputSpannerInstanceId=${DATABASE_INSTANCE} \
 --inputSpannerDatabaseId=my-database \
 --outputFolder=gs://${GCP_BUCKET}/backup \
 --projectId=${GCP_BUCKET}"  -Pdataflow-runner || exit 1

echo "FINISHED backup phase."

## Verify Backup
mvn compile exec:java \
   -Dexec.mainClass=com.google.cloud.pontem.EndToEndHelper \
   -Dexec.args="--projectId=${GCP_PROJECT} \
                --gcsRootBackupFolderPath=gs://${GCP_BUCKET}/backup \
                --databaseInstanceId=${DATABASE_INSTANCE} \
                --databaseId=my-database \
                --operation=verifyGcsBackup" || exit 1

echo "FINISHED verify backup phase."

## Tear Down Database
mvn compile exec:java \
   -Dexec.mainClass=com.google.cloud.pontem.EndToEndHelper \
   -Dexec.args="--projectId=${GCP_PROJECT} \
                --gcsRootBackupFolderPath=gs://${GCP_BUCKET}/multi-backup \
                --databaseInstanceId=${DATABASE_INSTANCE} \
                --databaseId=my-database \
                --operation=teardownDatabase" || exit 1

echo "FINISHED tear down database phase."

## Restore From Backup
mvn clean compile exec:java  -Dexec.mainClass=com.google.cloud.pontem.CloudSpannerDatabaseRestore  -Dexec.args="--runner=DataflowRunner \
 --project=${GCP_PROJECT} \
 --gcpTempLocation=gs://${GCP_BUCKET}/tmp \
 --outputSpannerInstanceId=${DATABASE_INSTANCE} \
 --outputSpannerDatabaseId=my-database \
 --inputFolder=gs://${GCP_BUCKET}/backup \
 --projectId=${GCP_PROJECT}"  -Pdataflow-runner || exit 1

echo "FINISHED restore from backup phase."

## Verify Database Restore
mvn compile exec:java \
   -Dexec.mainClass=com.google.cloud.pontem.EndToEndHelper \
   -Dexec.args="--projectId=${GCP_PROJECT} \
                --gcsRootBackupFolderPath=gs://${GCP_BUCKET}/multi-backup \
                --databaseInstanceId=${DATABASE_INSTANCE} \
                --databaseId=my-database \
                --operation=verifyDatabase" || exit 1

## Tear Down
mvn compile exec:java \
   -Dexec.mainClass=com.google.cloud.pontem.EndToEndHelper \
   -Dexec.args="--projectId=${GCP_PROJECT} \
                --gcsRootBackupFolderPath=gs://${GCP_BUCKET}/backup \
                --databaseInstanceId=${DATABASE_INSTANCE} \
                --databaseId=my-database \
                --operation=teardown" || exit 1

echo "FINISHED - All Tests Passed Successfully"
exit 0
