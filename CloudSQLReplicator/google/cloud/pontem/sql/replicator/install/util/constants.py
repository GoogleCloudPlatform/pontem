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
#
# NOTICE: THIS FILE HAS BEEN MODIFIED BY THE PONTEM AUTHORS UNDER COMPLIANCE
# WITH THE APACHE 2.0 LICENSE OF THE FORSETI SECURITY AUTHORS. THE FOLLOWING
# IS A COPYRIGHT OF THE ORIGINAL DOCUMENT:
#
# Copyright 2017 The Forseti Security Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Constants used for installing Cloud SQL Replicator."""

import os
from enum import Enum


class FirewallRuleAction(Enum):
    """Firewall rule action object."""
    ALLOW = 'ALLOW'
    DENY = 'DENY'


class FirewallRuleDirection(Enum):
    """Firewall rule direction object."""
    INGRESS = 'INGRESS'
    EGRESS = 'EGRESS'


class DeploymentStatus(Enum):
    """Deployment status."""
    RUNNING = 'RUNNING'
    DONE = 'DONE'


GCLOUD_MIN_VERSION = (180, 0, 0)
GCLOUD_VERSION_REGEX = r'Google Cloud SDK (.*)'
GCLOUD_ALPHA_REGEX = r'alpha.*'

SERVICE_ACCT_NAME_FMT = 'pontem-{}-{}-{}'
SERVICE_ACCT_EMAIL_FMT = '{}@{}.iam.gserviceaccount.com'

NOTIFICATION_SENDER_EMAIL = 'pontem-notify@localhost.domain'

# Roles
GCP_READ_IAM_ROLES = [
    'roles/appengine.appViewer',
    'roles/bigquery.dataViewer',
    'roles/browser',
    'roles/cloudasset.viewer',
    'roles/cloudsql.viewer',
    'roles/compute.networkViewer',
    'roles/iam.securityReviewer',
    'roles/orgpolicy.policyViewer',
    'roles/servicemanagement.quotaViewer',
    'roles/serviceusage.serviceUsageConsumer',
]

GCP_WRITE_IAM_ROLES = [
    'roles/compute.securityAdmin'
]

PROJECT_IAM_ROLES_SERVER = [
    'roles/storage.objectViewer',
    'roles/storage.objectCreator',
    'roles/cloudsql.client',
    'roles/logging.logWriter'
]

PROJECT_IAM_ROLES_CLIENT = [
    'roles/storage.objectViewer',
    'roles/logging.logWriter'
]

PONTEM_CAI_BUCKET_ROLES = [
    'objectAdmin'
]

SVC_ACCT_ROLES = [
    'roles/iam.serviceAccountTokenCreator'
]

# Required APIs
# Some of these may already be enabled when creating a project
# the Cloud Console. However, projects have been known to change default APIs,
# so these are explicitly enabled by the install script.  This also allows
# users to create their projects via GDM without specifying the same default
# APIs as the default project.
REQUIRED_APIS = [
    {'name': 'Admin SDK',
     'service': 'admin.googleapis.com'},
    {'name': 'AppEngine Admin',
     'service': 'appengine.googleapis.com'},
    {'name': 'BigQuery',
     'service': 'bigquery-json.googleapis.com'},
    {'name': 'Cloud Asset API',
     'service': 'cloudasset.googleapis.com'},
    {'name': 'Cloud Billing',
     'service': 'cloudbilling.googleapis.com'},
    {'name': 'Cloud Resource Manager',
     'service': 'cloudresourcemanager.googleapis.com'},
    {'name': 'Cloud SQL',
     'service': 'sql-component.googleapis.com'},
    {'name': 'Cloud SQL Admin',
     'service': 'sqladmin.googleapis.com'},
    {'name': 'Compute Engine',
     'service': 'compute.googleapis.com'},
    {'name': 'Deployment Manager',
     'service': 'deploymentmanager.googleapis.com'},
    {'name': 'Google Cloud Storage JSON API',
     'service': 'storage-api.googleapis.com'},
    {'name': 'IAM',
     'service': 'iam.googleapis.com'},
    {'name': 'Kubernetes Engine API',
     'service': 'container.googleapis.com'},
    {'name': 'Service Management API',
     'service': 'servicemanagement.googleapis.com'},
    {'name': 'Stackdriver Logging API',
     'service': 'logging.googleapis.com'}
]

# Org Resource Types
RESOURCE_TYPES = ['organization', 'folder', 'project']

# Paths
ROOT_DIR_PATH = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.dirname(
                os.path.dirname(__file__)))))

VERSIONFILE_REGEX = r'__version__ = \'(.*)\''

# Message templates
MESSAGE_WRONG_VERSION_OF_MYSQL = (
    'An unsupported version of MySQL was detected ({}).\n'
    'Please use on of the following versions {}'
)

MESSAGE_NO_CLOUD_SHELL = (
    'Pontem highly recommends running this script within '
    'Cloud Shell. If you would like to run the setup '
    'outside Cloud Shell, please be sure to do the '
    'following:\n\n'
    '1) Create a project.\n'
    '2) Enable billing for the project.\n'
    '3) Install gcloud and authenticate your account using '
    '"gcloud auth login".\n'
    '4) Set your project using '
    '"gcloud config set project <PROJECT_ID>".\n'
    '5) Run this setup again, with the --no-cloudshell flag, '
    'i.e.\n\n\tpython install/gcp_installer.py --no-cloudshell\n')

MESSAGE_HAS_ROLE_SCRIPT = (
    'Some roles could not be assigned during the installation. '
    'A script `grant_replication_roles.sh` has been generated in '
    'your cloud shell directory located at ~/cloudsql-replicator with '
    'the necessary commands to assign those roles. Please run this '
    'script to assign the roles so that Pontem Cloud SQL Replicator '
    'will work properly.\n\n')

MESSAGE_DEPLOYMENT_HAD_ISSUES = (
    'Your deployment had some issues. Please review the error '
    'messages. If you need help, please either file an issue '
    'on our Github Issues or email '
    'discuss@pontem.org.\n')

MESSAGE_GCLOUD_VERSION_MISMATCH = (
    'You need the following gcloud setup:\n\n'
    'gcloud version >= {}\n'
    'gcloud alpha components\n\n'
    'To install gcloud alpha components: '
    'gcloud components install alpha\n\n'
    'To update gcloud: gcloud components update\n')

MESSAGE_CREATE_ROLE_SCRIPT = (
    'One or more roles could not be assigned. Writing a '
    'script with the commands to assign those roles. Please '
    'give this script to someone (like an admin) who can '
    'assign these roles for you. If you do not assign these '
    'roles, Pontem Cloud SQL Replicator may not work properly!')

MESSAGE_BILLING_NOT_ENABLED = (
    '\nIt seems that billing is not enabled for your project. '
    'You can check whether billing has been enabled in the '
    'Cloud Platform Console:\n\n'
    '    https://console.cloud.google.com/billing/linkedaccount?'
    'project={}&organizationId={}\n\n'
    'Once you have enabled billing, re-run this setup.\n')

MESSAGE_NO_ORGANIZATION = (
    'You need to have an organization set up to use Pontem. '
    'Refer to the following documentation for more information.\n\n'
    'https://cloud.google.com/resource-manager/docs/'
    'creating-managing-organization')

MESSAGE_SSH_ERROR = (
    'Error occurred when sshing to the VM.  Unable to verify if the VM is '
    'fully initialized. This is a non-fatal error, but you should verify '
    'the VM is fully initialized before use. You can do so by SSHing to the '
    'VM, run command "tail -n1 /tmp/deployment.log" and make you get '
    '"Execution of startup script finished" in response. Will skip waiting '
    'for the instance to be initialized and proceed with the rest of the '
    'installation.')

# Questions templates
QUESTION_GSUITE_SUPERADMIN_EMAIL = (
    'Email: ')

QUESTION_SENDGRID_API_KEY = (
    'What is your SendGrid API key? '
    '(press [enter] to skip): ')

QUESTION_NOTIFICATION_RECIPIENT_EMAIL = (
    'At what email address do you want to receive '
    'notifications? (press [enter] to skip): ')

QUESTION_CONTINUE_IF_AUTHED_USER_IS_NOT_IN_DOMAIN = (
    '\n'
    'The currently authenticated user running the installer '
    'is not in the domain that Pontem is being installed to.\n'
    'If you wish to continue, you need to grant the '
    'compute.osLoginExternalUser role to your user on the org level, '
    'in order to have ssh access to the Pontem Cloud SQL Replicator '
    'worker VM.\n Would you like to continue? (y/n): '
)
