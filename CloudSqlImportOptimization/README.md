# Pontem - Cloud SQL Import Optimization

## Overview

Importing your MySQL backups into Cloud SQL should be an automated and
relatively speedy process. The **Cloud SQL Import Optimization Suite** aims to
address this need by incorporating a series of best practices from the Cloud SQL
team to significantly reduce the time required to import MySQL backups into a
Cloud SQL instance.

These optimization include:

*   Primary Key ordered data dumps
*   Loading schemas and data separately
*   Dropping secondary indexes for speedier ingestion
*   Chunking table data into similarly sized chunks for speedier ingestion.
*   Loading data in parallel in multiple groups
*   Restoring secondary indexes after data load
*   Performing secondary indexes restores in parallel

We've also included quality of life features:

*   The MySQL dumper tool to dump your database in an **Importer** compatible
    format
*   Checksum checks to verify the data integrity of the import
*   Checkpoints to resume failed import operations
*   Optionally dropping databases that failed to be imported
*   Low performance penalties from running on a local Linux machine vs Google
    Compute Engine

## Prerequisites

*   Ensure the [mysqlclient](https://pypi.org/project/mysqlclient/) library is
    installed
*   If you are running our tools on prem please ensure you have configured Cloud
    SQL with
    [Public IP Connectivity](https://cloud.google.com/sql/docs/mysql/connect-admin-ip)
*   If you are running our tools from whithin GCP please ensure you have
    configured Cloud SQL with
    [Private IP Connectivity](https://cloud.google.com/sql/docs/mysql/private-ip)
*   For exports ensure your MySQL user has regular admin privileges and
    **FILE**.
*   For imports ensure your Cloud SQL user has admin privileges including the
    ability to create, populate and possibly, drop databases.
*   If you want use SSL/TLS connections please
    [configure SSL/TLS in Cloud SQL.](https://cloud.google.com/sql/docs/mysql/configure-ssl-instance)

## Getting Started

### Exporting a copy of your MySQL database

Before being able to run the **Importer** you'll need to have dumped copy of
your database. Unfortunately, a monolithic dump file won't work with the
**Importer** therefore, we provide an **Exporter** tool that can be used to
generate dumps in correct format. You can run the dumper by calling:

       $ ./mysql_exporter.py <database-name> <destination-folder>

To get a list of supported arguments run:

       $ ./mysql_exporter.py -h

TIP: Optional parameters have sensible defaults in place.

TIP: You can use the **--verbose** options for extra logging.

#### MySQL's Secure File Priv

If
[--secure-file-priv](https://dev.mysql.com/doc/refman/5.7/en/server-options.html#option_mysqld_secure-file-priv)
is enabled in your MySQL instance then you should run the **Exporter** with the
**--using-secure-file-priv** option to ensure that all files written in the
whitelisted directory and no temp folders are used.

       $ ./mysql_exporter.py <database-name> <destination-folder> \
         --using-secure-file-priv

TIP: If your user does not have write priveleges over the secure-file-prive but
you have admin rights then you could use **sudo** to force the writes.

#### Exporter Limitations

WARNING: Please note that the **Exporter** tool **must be run on the same
computer hosting your MySQL database**. This is because we use **mysqldump's
--tab option** to dump each table's data into separate files.

TIP: For more information on the limitations of using **--tab** see
[mysqldump's docs.](https://dev.mysql.com/doc/refman/8.0/en/mysqldump.html#option_mysqldump_tab)

In case runnning the **Exporter** locally is a no-go in your environment here
are the steps necessary for you to produce a MySQL dump that will be compatible
with the **Importer**:

*   Get your database's schema by calling `mysqldump --no-data`
*   Ensure the database schema follows the following naming convention:
    <database_name>_schema.sql
*   Dump your database's data by calling `mysqldump --order-by-primary`
*   For each table in your database create a new text file <table_name>.txt and
    copy it's data into said file.
*   The **Importer** assumes that all .txt files in the dump directory are data
    files please ensure there are no extra text files in your dump directory.
*   If you wish to verify your imports via checksumming then get your checksum
    all your tables with
    [CHECKSUM TABLE](https://dev.mysql.com/doc/refman/5.7/en/checksum-table.html)
    and store the results in a test file with the following naming scheme:
    <database_name>_checksum.chk

### Importing a copy of your MySQL database into Cloud SQL

Once you have a dumped copy of your database importing it into Cloud SQL should
be a straightforward process. You can run the **Importer** by calling:

        $ ./cloudsql_importer.py <database-name> <mysql-dump-folder> \
          --host=<cloud_sql_ip>

To get a list of supported arguments run:

        $ ./cloudsql_importer.py -h

TIP: Optional parameters have sensible defaults in place.

TIP: You can use the **--verbose** options for extra logging.

TIP: Even though the techniques used in the **Importer** help reduce import
times considerably you **can achieve even more performance gains by using a
large Cloud SQL instance** please check [Cloud SQL](#cloud-sql) and
[Performance Comparissons](#performance-comparissons) more info.

#### Database Encoding

If you are using a non UTF-8 encoding in your database **some Importer
operations may fail** unless you provide the correct encoding with the
**--database-encoding** option, please remember to use Python compatible values.

    $ ./cloudsql_importer.py <database-name> <mysql-dump-folder> \
      --database-encoding=<encoding>

### Verifying your imports with Checksum

If you wish to verify than your data has been correctly imported then you can
use the checksumming feature. While exporting your database you'll need to
provide the **--checksum** option to ensure the original checksum is computed.
For example:

       $ ./mysql_exporter.py <database-name> <destination-folder> --checksum

While importing the dump you'll need to provide the **--verify-checksum** option
so that the dump's checksum can be compared against the imported checksum. For
example:

        $ ./cloudsql_importer.py <database-name> <mysql-dump-folder> \
          --host=<cloud-sql-ip> \
          --verify-checksum

WARNING: Checksumming can be a slow operation and significantly increase your
import times

## Security

### Passwords

Providing passwords via command line can be insecure therefore, we provide
support for
[MySQL Option Files](https://dev.mysql.com/doc/refman/8.0/en/option-files.html).

TIP: You can use the **--login-path** option to select which Option File profile
to read. For example:

        $ ./cloudsql_importer.py <database-name> <mysql-dump-folder> --host=<cloud-sql-ip> --login-path=<my-profile>

TIP: The Option File default profile is **client** therefore, not specifying any
login path will be equal to using **--login-path=client**.

WARNING: We don't read the Option File's connection settings so you'll still
need to pass these in via cmd arguments.

WARNING: If you are running the **Importer** in a **Compute Engine** instance
**please use an Option File** as GCE prints warning messages when command line
passwords are used and these disrupt the **Importer's** execution.

If your MySQL implementation does not support Option Files or you just prefer to
do so you can provide your credentials by using
**--do_not_use_credentials_file** alongside the **--user** and **--password**
options. For example:

        $ ./cloudsql_importer.py <database-name> <mysql-dump-folder> \
          --host=<cloud-sql-ip> \
          --do-not-use-credentials-file \
          --user=<user> \
          --password=<password>

### SSL / TLS

We support using SSL/TLS connections to Cloud SQL to ensure that your data is
secure in transit.

In order for the **Importer** to establish an SSL connection you need to:

*   Use **--ssl-mode-required** or **--ssl** options alongside **--ssl-ca**,
    **--ssl-cert** and **--ssl-key**.
*   For more details please read the **Connect to your Cloud SQL instance using
    SSL** section of this
    [document.](https://cloud.google.com/sql/docs/mysql/connect-admin-ip#connect)

TIP: We provide **--ssl-mode-required** or **--ssl** options as different MySQL
client implementations have varying support for these. Be aware if both options
are set the former supersedes the latter.

TIP: **--ssl-ca**, **--ssl-cert** and **--ssl-key** are preconfigured to use
Cloud SQL's required naming scheme. So you can skip these for Cloud SQL
connections.

WARNING: **--ssl-ca**, **--ssl-cert** and **--ssl-key** will be ignored if
**--ssl-mode-required** or **--ssl** are not used.

Example call using **--ssl-mode-required**:

        $ ./cloudsql_importer.py <database-name> <mysql-dump-folder> \
          --host=<cloud-sql-ip> \
          --ssl-mode-required

Example call using **--ssl**:

        $ ./cloudsql_importer.py <database-name> <mysql-dump-folderr> \
          --host=<cloud-sql-ip> \
          --ssl

WARNING: We just forward your SSL/TLS preferences to your MySQL client. Please
make sure that you have correctly
[configured SSL/TLS](\(https://cloud.google.com/sql/docs/mysql/configure-ssl-instance\))
before using this option.

## Environments

As we've mentioned previously the **Exporter** can only be run on the same
machine hosting the MySQL instance you'd like to dump the **Importer**, on the
contrary, can be run on any **\*NIX** environment (sorry no Windows for now) as
long as it can connect to your Cloud SQL instance.

You could, for example, copy your database dump to any computer in your network
or even a **GCE** instance and run the **Importer** there.

Given that our code does not use any proprietary libraries the **Importer** can
also be used to import data into any MySQL compatible database. Some options
here would be: importing the dump into a new MySQL instance in your local
machine or, a MySQL instance hosted in GCE.

Having said that the main use cases this tool aims to cover are:

*   **Importing a database backup from your local \*NIX workstation to Cloud
    SQL.**
*   **Importing a database backup from a \*NIX GCE image to Cloud SQL.**

## Google Cloud Platform

In the following sections we provide some tips on **GCP** features we support
and tuning **GCE** and **Cloud SQL** for even better performance.

### Google Compute Engine

Running both the **Exporter** and **Importer** should be fully supported in a
**Compute Engine** instance.

[Private IP](https://cloud.google.com/sql/docs/mysql/private-ip) connections can
be used to connect your **GCE** instance to **Cloud SQL**.

TIP: You may obtain some performance benefits from using a more powerful GCE
instance. Nevertheless, **most performance improvements will come from using a
powerful Cloud SQL instance**. We've conducted most of our testing with a **8
vCPU and 30GB Memory** instance.

WARNING: If you are running the **Importer** in a **Compute Engine** instance
**please use an Option File** as GCE prints warning messages when command line
passwords are used and these disrupt the **Importer's** execution.

### Cloud SQL

As a rule of thumb please **prefer using large Cloud SQL instances** for imports
as these usually have much higher IO performance which **can considerably
speed-up imports**.

[Private IP](https://cloud.google.com/sql/docs/mysql/private-ip) connections can
be used to connect your **GCE** instance to **Cloud SQL**.

TIP: **Prefer using a Cloud SQL instance with enough memory as to hold any of
your tables fully in-memory.** This can significantly speed-up ALTER commands by
avoiding disk usage as these commands require creating a copy of a table
applying changes and then overwriting the original table.

## Performance Comparissons

Below are a set of tests and timing comparissons. All tests use the same source
database to ensure an equal workload for all methods under test.

*   Database: 4.7GB, 2 tables, table1: 10M rows and 4 indexes (PK and 3
    secondary indexes), table2: 100 rows 1 index (PK).
*   Workstation Specs: 6 Cores / 12 Threads, 64GB Memory, SSD partition
*   GCE Specs: 8 vCPU, 30GB Memory, 30GB HDD
*   Small Cloud SQL: 1 vCPU, 3.75GB Memory, 75GB SSD Storage
*   Large Cloud SQL: 16 vCPU, 60GB Memory, 1TB SSD Storage

Test                                                                    | Time
----------------------------------------------------------------------- | ----
**Regular import** from **Workstation** to **Small Cloud SQL** instance | 92m
**Regular import** from **Workstation** to **Large Cloud SQL** instance | 26m
**Importer** from **Workstation** to **Small Cloud SQL** instance       | 47m
**Importer** from **Workstation** to **Large Cloud SQL** instance       | 15m
**Importer** from **GCE** to **Small Cloud SQL** instance               | 47m
**Importer** from **GCE** to **Large Cloud SQL** instance               | 15m

## Support

While this is not an officially supported Google product, we encourage you to
[file issues](https://github.com/GoogleCloudPlatform/pontem/issues/new) for
feature requests and bugs.

