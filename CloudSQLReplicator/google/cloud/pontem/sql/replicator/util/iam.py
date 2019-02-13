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

"""IAM utilility Module."""

import google.auth

from google.cloud.pontem.sql.replicator.util import gcp_api_util

IAM_SERVICE = 'iam'
IAM_SERVICE_VERSION = 'v1'


def build_iam_service(credentials=None):
    """Builds an authorized compute service proxy with a custom user agent.

    Args:
        credentials (google.auth.Credentials): Credentials to authorize client.

    Returns:
        Resource: Authorized IAM service proxy with custom user agent.
    """
    service = gcp_api_util.build_authorized_service(
        IAM_SERVICE,
        IAM_SERVICE_VERSION,
        credentials
    )

    return service


def create_service_account(name, display_name, project=None, credentials=None):
    """Creates a service account.

    Args:
        name (str): Name of the service account.
        display_name (str): Display name of the service account.
        project (str): Project ID where service account will be created.
        credentials (google.auth.Credentials): Credentials to authorize client.

    Returns:
        JSON: Service Account object returned from request.
    """
    default_credentials, default_project = google.auth.default()

    service = build_iam_service(credentials or default_credentials)

    service_account = service.projects().serviceAccounts().create(
        name='projects/' + project or default_project,
        body={
            'accountId': name,
            'serviceAccount': {
                'displayName': display_name
            }
        }).execute()

    return service_account


def create_key(service_account_email, credentials=None):
    """Creates a key for a service account.

    Args:
        service_account_email (str): Email of service account.
        credentials (google.auth.Credentials): Credentials to authorize client.

    Returns:
        JSON: Key object returned from request.
    """
    service = build_iam_service(credentials)
    key = service.projects().serviceAccounts().keys().create(
        name='projects/-/serviceAccounts/' + service_account_email, body={}
    ).execute()

    return key


def get_project_policy(service, project):
    """Gets IAM policy for a project.

    Args:
        service (resource): Service used to get project policy.
        project (str): Project ID for project of target policy.

    Returns:
        JSON: Policy object of project.
    """

    policy = service.projects().getIamPolicy(
        resource=project, body={}).execute()
    return policy


def modify_policy_add_members(policy, role, member_email_addresses):
    """Adds a new member to a role binding.

    Args:
        policy (JSON): Policy to modify.
        role (str): Role to add members to.
        member_email_addresses(List): List of email addresses of members to add.

    Returns:
        JSON: Modified policy object.
    """
    binding = next(b for b in policy['bindings'] if b['role'] == role)
    binding['members'].extend(member_email_addresses)

    return policy


def modify_policy_add_role_binding(policy, role, member_email_address):
    """Adds a new role binding to a policy.

    Args:
        policy (JSON): Policy to modify.
        role (str): Role to add member to.
        member_email_address(str): Email addresses of member to add.

    Returns:
        JSON: Modified policy object.
    """
    binding = {
        'role': role,
        'members': [member_email_address]
    }
    policy['bindings'].append(binding)
    return policy


def set_project_policy(service, project, policy):
    """Sets IAM policy for a project.

    Args:
        service (Resource): Service to set policy.
        project (str): Project id where policy will be set.
        policy (JSON): Policy that will be set.

    Returns:
        JSON: Policy that has been set.
    """
    policy = service.projects().setIamPolicy(
        resource=project, body={
            'policy': policy
        }).execute()
    return policy


def add_role_member_to_policy(role, member_email_addresses,
                              project=None, credentials=None):
    """Adds a member to a role for a project policy.

    Args:
        role (str): Role to add member to,
        member_email_addresses (List): Email addresses of role members.
        project (str): Project ID where service account will be created.
        credentials (google.auth.Credentials): Credentials to authorize client.

    Returns:
        JSON: Policy that was set in the project.
    """
    default_credentials, default_project = google.auth.default()

    service = build_iam_service(credentials or default_credentials)
    policy = get_project_policy(service, project or default_project)
    modified_policy = modify_policy_add_members(policy,
                                                role,
                                                member_email_addresses)
    set_policy = set_project_policy(service,
                                    project or default_project,
                                    modified_policy)
    return set_policy


def add_member_role_to_policy(role, member_email,
                              project=None, credentials=None):
    """Adds a binding for a role to a member to a project policy.

    Args:
        member_email (str): Email of role members.
        role (str): Role to add member to,
        project (str): Project ID where service account will be created.
        credentials (google.auth.Credentials): Credentials to authorize client.

    Returns:
        JSON: Policy that was set in the project.
    """
    default_credentials, default_project = google.auth.default()

    service = build_iam_service(credentials or default_credentials)
    policy = get_project_policy(service, project or default_project)
    modified_policy = modify_policy_add_role_binding(policy,
                                                     role,
                                                     member_email)
    set_policy = set_project_policy(service,
                                    project or default_project,
                                    modified_policy)
    return set_policy
