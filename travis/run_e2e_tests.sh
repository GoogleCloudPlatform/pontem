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

DATE_STR=`date +%s`
DATABASE_NAME="my-database-"${DATE_STR}
echo "Database Name Set: ${DATABASE_NAME}"

GCP_FOLDER="my-database-"${DATE_STR}
echo "GCP Folder Name Set: ${GCP_FOLDER}"

./travis/run_avro_e2e_tests.sh --project="${GCP_PROJECT}" --bucket="${GCP_BUCKET}" --database="${DATABASE_INSTANCE}" &&
./travis/run_serialized_e2e_tests.sh --project="${GCP_PROJECT}" --bucket="${GCP_BUCKET}" --database="${DATABASE_INSTANCE}" || exit 0

echo "FINISHED Avro and Serialized - All Tests Passed Successfully"
exit 0
