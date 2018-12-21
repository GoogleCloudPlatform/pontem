# Pontem - Cloud SQL Import Optimization

## Overview

Importing your MySQL backups into CloudSQL should be an automated and relatively
speedy process. The **CloudSQL Import Optimization Suite** aims to address this
need by incorporating a series of best practices from the Cloud SQL team to
significantly reduce the time required to import MySQL backups into a CloudSQL
instance.

These optimization include:

*   Primary Key ordered data dumps
*   Loading schemas and data separately
*   Dropping secondary indexes for speedier ingestion
*   Chunking table data into similarly sized chunks for speedier ingestion.
*   Loading data in parallel in multiple groups
*   Restoring secondary indexes after data load
*   Performing secondary indexes restores in parallel

We've also included quality of life features:

*   The MySQL dumper tool to dump your database in an importer compatible format
*   Checksum checks to verify the data integrity of the import
*   Checkpoints to resume failed import operations
*   Optionally dropping databases that failed to be imported
*   Low performance penalties from running on a local Linux machine vs Google
    Compute Engine

## Prerequisites

*   If you are running our tools on prem please ensure you have configured
    CloudSQL with
    [Public IP Connectivity](https://cloud.google.com/sql/docs/mysql/connect-admin-ip)
*   If you are running our tools from whithin GCP please ensure you have
    configured CloudSQL with
    [Private IP Connectivity](https://cloud.google.com/sql/docs/mysql/private-ip)
*   For exports ensure your MySQL user has regular admin privileges and
    **FILE**.
*   For imports ensure your CloudSQL user has admin privileges including the
    ability to create, populate and possibly, drop databases.
*   If you want use SSL/TLS connections please
    [configure SSL/TLS in CloudSQL.](https://cloud.google.com/sql/docs/mysql/configure-ssl-instance)

## Getting Started

### Exporting a copy of your MySQL database

Before being able to run the importer you'll need to have dumped copy of your
database. Unfortunately, a monolithic dump file won't work with the importer
therefore, we provide an exporter tool that can be used to generate dumps in
correct format. You can run the dumper by calling:

       $ ./mysql_exporter.py <database-name> <destination-folder>

To get a list of supported arguments run:

       $ ./mysql_exporter.py -h

TIP: Optional parameters have sensible defaults in place.

TIP: You can use the **--verbose** options for extra logging.

#### Limitations

WARNING: Please note that the exporter tool **must be run on the same computer
hosting your MySQL database**. This is because we use **mysqldump's --tab
option** to dump each table's data into separate files.

TIP: For more information on the limitations of using **--tab** see
[mysqldump's docs.](https://dev.mysql.com/doc/refman/8.0/en/mysqldump.html#option_mysqldump_tab)

In case runnning the exporter locally is a no-go in your environment here are
the steps necessary for you to produce a MySQL dump that will be compatible with
the importer:

*   Get your database's schema by calling `mysqldump --no-data`
*   Ensure the database schema follows the following naming convention:
    <database_name>_schema.sql
*   Dump your database's data by calling `mysqldump --order-by-primary`
*   For each table in your database create a new text file <table_name>.txt and
    copy it's data into said file.
*   The importer assumes that all .txt files in the dump directory are data
    files please ensure there are no extra text files in your dump directory.
*   If you wish to verify your imports via checksumming then get your checksum
    all your tables with
    [CHECKSUM TABLE](https://dev.mysql.com/doc/refman/5.7/en/checksum-table.html)
    and store the results in a test file with the following naming scheme:
    <database_name>_checksum.chk

### Importing a copy of your MySQL database into CloudSQL

Once you have a dumped copy of your database importing it into CloudSQL should
be a straightforward process. You can run the importer by calling:

        $ ./cloudsql_importer.py <database-name> <mysql-dump-folder> --host=<cloud_sql_ip>

To get a list of supported arguments run:

        $ ./cloudsql_importer.py -h

TIP: Optional parameters have sensible defaults in place.

TIP: You can use the **--verbose** options for extra logging.

### Verifying your imports with Checksum

If you wish to verify than your data has been correctly imported then you can
use the checksumming feature. While exporting your database you'll need to
provide the **--checksum** option to ensure the original checksum is computed.
For example:

       $ ./mysql_exporter.py <database-name> <destination-folder> --checksum

While importing the dump you'll need to provide the **--verify-checksum** option
so that the dump's checksum can be compared against the imported checksum. For
example:

        $ ./cloudsql_importer.py <database-name> <mysql-dump-folder> --host=<cloud-sql-ip> --verify-checksum

WARNING: Checksumming can be a slow operation and significantly increase your
import times

## Security

### Passwords

Providing passwords via command line can be insecure therefore, we provide
support for
[MySQL Option Files](https://dev.mysql.com/doc/refman/8.0/en/option-files.html).

TIP: You can use the **--login_path** option to select which Option File profile
to read. For example:

        $ ./cloudsql_importer.py <database-name> <mysql-dump-folder> --host=<cloud-sql-ip> --login-path=<my-profile>

WARNING: We don't read the Option File's connection settings so you'll still
need to pass these in via cmd arguments.

If your MySQL implementation does not support Option Files or you just prefer to
do so you can provide your credentials by using
**--do_not_use_credentials_file** alongside the **--user** and **--password**
options. For example:

        $ ./cloudsql_importer.py <database-name> <mysql-dump-folder> --host=<cloud-sql-ip> \
          --do-not-use-credentials-file \
          --user=<user> \
          --password=<password>

### SSL / TLS

We support using SSL/TLS connections to CloudSQL to ensure that your data is
secure in transit.

In order for the importer to establish an SSL connection you need to:

*   Use **--ssl-mode-required** or **--ssl** options alongside **--ssl-ca**,
    **--ssl-cert** and **--ssl-key**.
*   For more details please read the **Connect to your Cloud SQL instance using
    SSL** section of this
    [document.](https://cloud.google.com/sql/docs/mysql/connect-admin-ip#connect)

TIP: We provide **--ssl-mode-required** or **--ssl** options as different MySQL
client implementations have varying support for these. Be aware if both options
are set the former supersedes the latter.

TIP: **--ssl-ca**, **--ssl-cert** and **--ssl-key** are preconfigured to use
CloudSQL's required naming scheme. So you can skip these for CloudSQL
connections.

WARNING: **--ssl-ca**, **--ssl-cert** and **--ssl-key** will be ignored if
**--ssl-mode-required** or **--ssl** are not used.

Example call using **--ssl-mode-required**:

        $ ./cloudsql_importer.py <database-name> <mysql-dump-folder> --host=<cloud-sql-ip> --ssl-mode-required

Example call using **--ssl**:

        $ ./cloudsql_importer.py <database-name> <mysql-dump-folderr> --host=<cloud-sql-ip> --ssl

WARNING: We just forward your SSL/TLS preferences to your MySQL client. Please
make sure that you have correctly
[configured SSL/TLS](\(https://cloud.google.com/sql/docs/mysql/configure-ssl-instance\))
before using this option.

## Support

While this is not an officially supported Google product, we encourage you to
[file issues](https://github.com/GoogleCloudPlatform/pontem/issues/new) for
feature requests and bugs.

