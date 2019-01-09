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

"""Cloud SQL Replicator setup."""

from setuptools import find_packages
from setuptools import setup

from google.cloud.pontem.sql import replicator

NAMESPACE_PACKAGES = [
    'google',
    'google.cloud'
]

REQUIRES = [
    'absl-py>=0.6.1',
    'future>=0.17.1',
    'futures>=3.0.5',
    'google-api-core>=1.6.0',
    'google-api-python-client>=1.7.6',
    'google-auth>=1.6.1',
    'google-auth-httplib2>=0.0.3',
    'google-cloud-core>=0.28.1',
    'google-cloud-storage>=1.13.0',
    'google-resumable-media>=0.3.1',
    'googleapis-common-protos>=1.5.5',
    'httplib2>=0.12.0',
    'mysql-connector>=2.1.6',
    'oauth2client>=4.1.3',
    'six>=1.11.0',
]

setup(
    name='cloudsql-replicator',
    version=replicator.__version__,
    install_requires=REQUIRES,
    packages=find_packages(exclude=['tests']),
    namespace_packages=NAMESPACE_PACKAGES,
    license='Apache 2.0',
    entry_points={
        'console_scripts': [
            'sr=google.cloud.pontem.sql.replicator.cli.main:run',
        ]
    }
)
