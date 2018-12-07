# Copyright 2018 The Pontem Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""GCP API utility functions."""
import logging

from google.auth import compute_engine
from google.cloud import storage

# Storage
def create_bucket(bucket_name,
                  project_id,
                  credentials=compute_engine.Credentials()):
  """Creates a new bucket.

  Creates a new Cloud Storage Bucket using credentials of the of the
  compute engine credentials by default.
  """

  storage_client = storage.Client(credentials=credentials, project=project_id)
  bucket = storage_client.create_bucket(bucket_name)
  logging.info('Created bucket {}'.format(bucket.name))

def delete_bucket(bucket_name,
    project_id,
    credentials=compute_engine.Credentials()):
  """Deletes a bucket.

  Deletes a Cloud Storage Bucket using credentials of the of the
  compute engine credentials by default.
  """

  storage_client = storage.Client(credentials=credentials, project=project_id)
  bucket = storage_client.get_bucket(bucket_name)
  bucket.delete()
  logging.info('Deleted bucket {}'.format(bucket.name))


def delete_blob(bucket_name,
                blob_name,
                project_id,
                credentials=compute_engine.Credentials()):
  """Deletes a blob from the bucket."""

  storage_client = storage.Client(credentials=credentials, project=project_id)
  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.blob(blob_name)
  blob.delete()
  logging.info('Deleted {}'.format(blob_name))


