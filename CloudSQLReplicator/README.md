## Overview
CloudSQLReplicator allows users to set up replication from MySQL(versions 5.6 or 5.7) to CloudSQL. 
It automates the following operations:

1. Provisions a VM and a VPC in a Google Cloud Project (GCP).
2. Verifies connectivity between VM/VPC and MySQL master.
3. Provisions and permissions a Google Cloud Storage (GCS) bucket.
4. Performs a MySQLDump of a database into GCS bucket as a gzipped SQL file.
5. Creates a source representation of MySQL master in GCP.
6. Provisions and initializes a Cloud SQL replica from the gzipped SQL file.
7. Initializes replication.
8. Cleans up intermediary files.
