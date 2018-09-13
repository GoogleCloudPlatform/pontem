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

echo "FINISHED (Avro) parsing flags."

DATE_STR=`date +%s`
RAND_NUM=$((1 + RANDOM % 10000))
DATABASE_NAME="my-av-db-"${DATE_STR}"-"${RAND_NUM}
echo "Database Name Set: ${DATABASE_NAME}"

GCP_FOLDER="my-av-db-"${DATE_STR}"-"${RAND_NUM}
echo "GCP Folder Name Set: ${GCP_FOLDER}"

# Tear Down Method
full_tear_down_and_exit () {
echo "BEGIN Avro tear down function"
./gradlew clean run \
   -Dexec.mainClass=com.google.cloud.pontem.EndToEndHelper \
   -Dexec.args="--projectId=${GCP_PROJECT} \
                --gcsRootBackupFolderPath=gs://${GCP_BUCKET}/${GCP_FOLDER} \
                --databaseInstanceId=${DATABASE_INSTANCE} \
                --databaseId=${DATABASE_NAME} \
                --operation=teardown" || exit 1

echo "END Avro tear down function"
exit 1
}

# Run End-To-End (E2E) Tests

## Setup
echo "BEGIN Avro running E2E setup."
./gradlew clean run \
   -Dexec.mainClass=com.google.cloud.pontem.EndToEndHelper \
   -Dexec.args="--projectId=${GCP_PROJECT} \
                --gcsRootBackupFolderPath=gs://${GCP_BUCKET}/${GCP_FOLDER} \
                --databaseInstanceId=${DATABASE_INSTANCE} \
                --databaseId=${DATABASE_NAME} \
                --operation=setup" || full_tear_down_and_exit

echo "FINISHED Avro setup phase."

sleep 20s

## Backup
echo "BEGIN Avro backup."
./gradlew clean run \
   -Dexec.mainClass=com.google.cloud.pontem.AvroCloudSpannerDatabaseBackup \
   -Dexec.args="--runner=DataflowRunner \
                --project=${GCP_PROJECT} \
                --gcpTempLocation=gs://${GCP_BUCKET}/tmpavro \
                --inputSpannerInstanceId=${DATABASE_INSTANCE} \
                --inputSpannerDatabaseId=${DATABASE_NAME} \
                --outputFolder=gs://${GCP_BUCKET}/${GCP_FOLDER} \
                --projectId=${GCP_BUCKET}"  -Pdataflow-runner || full_tear_down_and_exit

echo "FINISHED Avro backup phase."

## Verify Backup
echo "BEGIN Avro verify backup."
./gradlew clean run \
   -Dexec.mainClass=com.google.cloud.pontem.EndToEndHelper \
   -Dexec.args="--projectId=${GCP_PROJECT} \
                --gcsRootBackupFolderPath=gs://${GCP_BUCKET}/${GCP_FOLDER} \
                --databaseInstanceId=${DATABASE_INSTANCE} \
                --databaseId=${DATABASE_NAME} \
                --operation=verifyGcsBackup" || full_tear_down_and_exit

echo "FINISHED Avro verify backup phase."

## Tear Down Database
echo "BEGIN Avro database teardown."
./gradlew clean run \
   -Dexec.mainClass=com.google.cloud.pontem.EndToEndHelper \
   -Dexec.args="--projectId=${GCP_PROJECT} \
                --gcsRootBackupFolderPath=gs://${GCP_BUCKET}/${GCP_FOLDER} \
                --databaseInstanceId=${DATABASE_INSTANCE} \
                --databaseId=${DATABASE_NAME} \
                --operation=teardownDatabase" || full_tear_down_and_exit

echo "FINISHED Avro tear down database phase."

sleep 20s

## Restore From Backup
echo "BEGIN Avro restore from backup."
./gradlew clean run \
 -Dexec.mainClass=com.google.cloud.pontem.AvroCloudSpannerDatabaseRestore \
 -Dexec.args="--runner=DataflowRunner \
 --project=${GCP_PROJECT} \
 --gcpTempLocation=gs://${GCP_BUCKET}/tmpavro \
 --outputSpannerInstanceId=${DATABASE_INSTANCE} \
 --outputSpannerDatabaseId=${DATABASE_NAME} \
 --inputFolder=gs://${GCP_BUCKET}/${GCP_FOLDER} \
 --projectId=${GCP_PROJECT}"  -Pdataflow-runner || full_tear_down_and_exit

echo "FINISHED Avro restore from backup phase."

## Verify Database Restore
echo "BEGIN Avro database restore verify."
./gradlew clean run \
   -Dexec.mainClass=com.google.cloud.pontem.EndToEndHelper \
   -Dexec.args="--projectId=${GCP_PROJECT} \
                --gcsRootBackupFolderPath=gs://${GCP_BUCKET}/${GCP_FOLDER} \
                --databaseInstanceId=${DATABASE_INSTANCE} \
                --databaseId=${DATABASE_NAME} \
                --operation=verifyDatabase" || full_tear_down_and_exit

echo "FINISHED Avro database restore verify."

## Tear Down
echo "BEGIN Avro final tear down."
./gradlew clean run \
   -Dexec.mainClass=com.google.cloud.pontem.EndToEndHelper \
   -Dexec.args="--projectId=${GCP_PROJECT} \
                --gcsRootBackupFolderPath=gs://${GCP_BUCKET}/${GCP_FOLDER} \
                --databaseInstanceId=${DATABASE_INSTANCE} \
                --databaseId=${DATABASE_NAME} \
                --operation=teardown" || exit 1
echo "FINISHED Avro All Tests Passed Successfully"
exit 0
