# pontem
## Overview
pontem is an open-source project that enable susers of Cloud Spanner
to perform four key operations:

1. Database Backup.
1. Database Backup Integrity Check which verifies that the backup completed
successfully.
1. Database Restore whereby a database backup that is stored on disk is
restored to a Cloud Spanner instance.
1. Database Restore Integrity Check which verifies that the restore
completed successfully.

## Getting started
pontem can be run in various modes. Consult the [usage guide](USAGE.md) for
relevant examples.

## A note on Security
- Since Cloud Spanner data will reside in Google Cloud Storage (GCS),
it is imperative that the ACLs for this bucket are at least as restrictive
as those for Cloud Spanner. Otherwise, there is the possibility for a privacy
or security incident. Admins should set restrictive ACLs for the GCS
buckets.

- The Google Cloud SDK, Cloud Dataflow SDK, Cloud Spanner SDK, and the
Container Engine SDK (used for Dataflow jobs that backup and restore
data) all have varying levels of access via Service Accounts. Admins should
set restrictive permissions for Service Accounts.
