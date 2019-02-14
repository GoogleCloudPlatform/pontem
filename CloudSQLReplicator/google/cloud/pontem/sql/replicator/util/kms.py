# Copyright 2019 The Pontem Authors. All rights reserved.
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

"""Cloud Key Management Service utility Module."""

import google.api_core.gapic_v1.client_info
from google.cloud import kms_v1
from google.cloud.kms_v1 import enums


from google.cloud.pontem.sql.replicator.util import gcp_api_util


def build_kms_client(credentials=None):
    """Builds Authorized Storage.Client with custom user agent.

      Args:
        credentials (google.auth.Credentials): credentials to authorize client.

      Returns:
        KeyManagementServiceClient: returns authorized KMS client with CloudSQL
        Replicator user agent.
    """
    default_credentials, _ = google.auth.default()

    client_info = google.api_core.gapic_v1.client_info.ClientInfo(
        client_library_version=gcp_api_util.get_user_agent())

    kms_client = kms_v1.KeyManagementServiceClient(
        credentials=credentials or default_credentials,
        client_info=client_info)

    return kms_client


def create_key_ring(key_ring_id, location='global',
                    project=None, credentials=None):
    """Create a key ring at the specified location

    Args:
        key_ring_id (str): Unique identifier for keyring.
        location (str): Location where the key will be created.
        project (str): Project ID where keyring will be created.
        credentials (google.auth.Credentials): credentials to authorize client.
    Returns:
        JSON: Keyring that was created.
    """
    _, default_project = google.auth.default()
    kms_client = build_kms_client(credentials)
    parent = kms_client.location_path(project or default_project, location)
    keyring_name = kms_client.key_ring_path(project or default_project,
                                            location, key_ring_id)
    keyring = {'name': keyring_name}

    response = kms_client.create_key_ring(parent, key_ring_id, keyring)
    return response


def create_key(key_id, key_ring_id, location='global',
               project=None, credentials=None):
    """Creates a crypto key at the specified location

    Args:
        key_id (str): Unique id for the key to be created.
        key_ring_id (str): Unique identifier for keyring.
        location (str): Location where the key will be created.
        project (str): Project ID where keyring will be created.
        credentials (google.auth.Credentials): credentials to authorize client.

    Returns:
        JSON: Key that was created.
    """
    _, default_project = google.auth.default()
    kms_client = build_kms_client(credentials)
    parent = kms_client.key_ring_path(project or default_project,
                                      location, key_ring_id)

    purpose = enums.CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT
    crypto_key = {'purpose': purpose}

    response = kms_client.create_crypto_key(parent, key_id, crypto_key)
    return response


def encrypt(plaintext, key_id, key_ring_id,
            location='global', project=None, credentials=None):
    """Encrypts plain text.

    Args:
        plaintext (str): Plain text to encrypt.
        key_id (str): Unique id for the key to be created.
        key_ring_id (str): Unique identifier for keyring.
        location (str): Location where the key will be created.
        project (str): Project ID where keyring will be created.
        credentials (google.auth.Credentials): credentials to authorize client.
    Returns:
        str: Encrypted cipher text.
    """
    _, default_project = google.auth.default()
    kms_client = build_kms_client(credentials)

    name = kms_client.crypto_key_path_path(project or default_project, location,
                                           key_ring_id, key_id)
    response = kms_client.encrypt(name, plaintext)
    return response.ciphertext


def decrypt(ciphertext, key_id, key_ring_id,
            location='global', project=None, credentials=None):
    """Decrypts cipher text.

    Args:
        ciphertext (str): Cipher text to decrypt.
        key_id (str): Unique id for the key to be created.
        key_ring_id (str): Unique identifier for keyring.
        location (str): Location where the key will be created.
        project (str): Project ID where key will be created.
        credentials (google.auth.Credentials): credentials to authorize client.
    Returns:
        str: Decrypted plain text.
    """
    _, default_project = google.auth.default()
    kms_client = build_kms_client(credentials)

    name = kms_client.crypto_key_path_path(project or default_project, location,
                                           key_ring_id, key_id)
    response = kms_client.decrypt(name, ciphertext)
    return response.plaintext


def add_member_to_crypto_key_policy(member, role, key_id, key_ring_id,
                                    location='global', project=None,
                                    credentials=None):
    """Adds a member of a role to crypto key policy.

    Args:
        member (str): Email address of member to add.
        role (str): Role member is in.
        key_id (str): Unique identifier for key.
        key_ring_id (str): Unique identifier for keyring.
        location (str): Where key exists.
        project (str): Project ID where keyring will be created.
        credentials (google.auth.Credentials): credentials to authorize client.

    Returns:
        JSON: Modified policy object
    """
    _, default_project = google.auth.default()
    kms_client = build_kms_client(credentials)
    resource = kms_client.crypto_key_path_path(project or default_project,
                                               location, key_ring_id, key_id)
    policy = kms_client.get_iam_policy(resource)
    policy.bindings.add(
        role=role,
        members=[member])

    kms_client.set_iam_policy(resource, policy)
    return policy
