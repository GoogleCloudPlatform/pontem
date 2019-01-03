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

"""Constants used for replicating MySQL to Cloud SQL."""
SUPPORTED_MYSQL_VERSIONS = [(5, 6), (5, 7)]
MY_SQL_VERSION_QUERY = 'SELECT @@GLOBAL.version;'
MY_SQL_GTID_MODE_QUERY = ('SELECT VARIABLE_VALUE FROM '
                          'information_schema.global_variables '
                          'WHERE variable_name = \'gtid_mode\'')
MY_SQL_LIST_VIEWS_QUERY = ('SHOW FULL TABLES IN {} WHERE TABLE_TYPE '
                           'LIKE \'VIEW\'')
MY_SQL_SSL_CIPHER_QUERY = ('SELECT VARIABLE_VALUE FROM '
                           'information_schema.global_variables WHERE '
                           'variable_name=\'Ssl_cipher\'')
