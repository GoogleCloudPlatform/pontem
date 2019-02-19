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

"""MySQL utility functions."""

from __future__ import print_function
import subprocess

import mysql.connector
from mysql.connector import errorcode

from google.cloud.pontem.sql.replicator.util import mysql_constants
from google.cloud.pontem.sql.replicator.util import util_errors


def execute_mysqldump(command_args, bucket_url):
    """Automates the MySQLDump command.

    Args:
        command_args (List): a list of args for MySQLDump.
        bucket_url (str): GCS destination URL for SQL Dump file .

    Raises:
        MySQLDumpError - If something went wrong with the dump.
    """
    mysql_dump = subprocess.Popen(command_args, stdout=subprocess.PIPE)
    gzip = subprocess.Popen('gzip',
                            stdin=mysql_dump.stdout,
                            stdout=subprocess.PIPE)
    gsutil = subprocess.Popen(('gsutil',
                               'cp', '-', bucket_url),
                              stdin=gzip.stdout)
    _, error = gsutil.communicate()
    if error:
        raise util_errors.MySQLDumpError(error.strip())


class MySQL(object):
    """Helper to check MySQL requirements before replication to Cloud SQL."""

    def __init__(self, host, user, password, port='3306'):
        """Inits MySQL with a connection to target DB

        Args:
          host (str): MySQL server
          user (str): User to use when connecting
          password (str): Password to use when connecting
          port (str): Port to connect over
        """
        try:
            self._user = user
            self._password = password
            self._host = host
            self._port = port
            self._connection = mysql.connector.connect(
                user=user, password=password,
                host=host,
                port=port)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print('Something is wrong with your user name or password')
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print('Database does not exist')
            else:
                print(err)

    def __del__(self):
        """Closes connection object and allows for cleanup."""
        self._connection.close()

    def get_mysql_version(self):
        """ Gets MySQL version.

        Returns:
              tuple: MySQL version
        """
        version = self._connection.get_server_version()
        return version

    def is_supported_version(self):
        """ Checks if this version of MySQL.

        Returns:
            bool: True if this version is supported.
        """
        supported = (self.get_mysql_version() in
                     mysql_constants.SUPPORTED_MYSQL_VERSIONS)
        return supported

    def get_gtid_mode_on(self):
        """Gets GTID mode.

        Returns:
          bool: True if GTID mode is on, False otherwise.
        """
        cursor = self._connection.cursor()
        cursor.execute(mysql_constants.MY_SQL_GTID_MODE_QUERY)
        row = cursor.fetchone()
        return row[0] == 'ON'

    def get_ssl_used(self):
        """Gets SSL use.

        Returns:
            bool: True if SSL is being used, False otherwise.
        """
        cursor = self._connection.cursor()
        cursor.execute(mysql_constants.MY_SQL_SSL_CIPHER_QUERY)
        row = cursor.fetchone()
        return row[0] is not None

    def get_views(self, database):
        """ Returns a list of views on the database.

        Args:
            database (str): Database to find views for.
        Returns:
            list: list of views for the current database.
        """

        views = []
        cursor = self._connection.cursor()
        cursor.execute('USE {}'.format(database))
        cursor.execute(
            mysql_constants.MY_SQL_LIST_VIEWS_QUERY.format(database)
        )
        for (view_name, _) in cursor:
            views.append(view_name)

        return views

    def dump_external_sql(self, bucket_url, databases=None,
                          ssl_ca=None, ssl_cert=None, ssl_key=None):
        """Performs dump for external MySQL databases.

        Args:
            bucket_url (str): The URL of the Cloud Storage Bucket.
            databases (list): Databases to dump. None adds the
                --all-databases flag.
            ssl_ca (str): File path of the Certificate Authority Cert.
            ssl_cert (str): File path of the Client Certificate
                with public key.
            ssl_key (str): File path of the client private key file.
        """
        command_args = [
            'mysqldump',
            '-h', self._host,
            '-P', self._port,
            '-u', self._user,
            '--password=' + self._password
        ]

        if databases is not None:
            command_args.append('--databases')
            command_args.append(' '.join(databases))
        else:
            command_args.append('--all-databases')

        if ssl_ca:
            command_args.append('--ssl-ca')
            command_args.append(ssl_ca)

        if ssl_cert:
            command_args.append('--ssl-cert')
            command_args.append(ssl_cert)

        if ssl_key:
            command_args.append('--ssl-key')
            command_args.append(ssl_key)

        command_args.extend(
            [
                '--single-transaction',
                '--flush-privileges',
                '--hex-blob',
                '--skip-triggers',
                '--default-character-set=utf8'
            ]
        )

        execute_mysqldump(command_args, bucket_url)
        return

    def dump_sql(self, bucket_url, databases=None,
                 ssl_ca=None, ssl_cert=None, ssl_key=None):
        """Performs mysqldump

        Generates a gzipped SQL dump of the database that can be used to
        create a Cloud SQL replica. Because of the potentially large files
        created, mysqldump is not checked for errors.  The caller should
        validate a dump file was created in the Cloud Storage Bucket.

        Args:
            bucket_url (str): The URL of the Cloud Storage Bucket.
            databases (list): Databases to dump. None adds the
                --all-databases flag.
            ssl_ca (str): File path of the Certificate Authority Cert.
            ssl_cert (str): File path of the Client Certificate
                with public key.
            ssl_key (str): File path of the client private key file.
        """
        command_args = [
            'mysqldump',
            '-h', self._host,
            '-P', self._port,
            '-u', self._user,
            '--password=' + self._password,
        ]

        if databases is not None:
            command_args.append('--databases')
            command_args.append(' '.join(databases))
        else:
            command_args.append('--all-databases')

        if ssl_ca:
            command_args.append('--ssl-ca')
            command_args.append(ssl_ca)

        if ssl_cert:
            command_args.append('--ssl-cert')
            command_args.append(ssl_cert)

        if ssl_key:
            command_args.append('--ssl-key')
            command_args.append(ssl_key)

        command_args.extend(
            [
                '--skip-comments',
                '--hex-blob',
                '--skip-triggers',
                '--master-data=1',
                '--order-by-primary',
                '--no-autocommit',
                '--default-character-set=utf8',
                '--single-transaction',
                '--set-gtid-purged=on'
            ]
        )

        if databases is not None:
            for database in databases:
                for view in self.get_views(database):
                    command_args.append(
                        '--ignore-table=' + database + '.' + view)

        execute_mysqldump(command_args, bucket_url)
        return
