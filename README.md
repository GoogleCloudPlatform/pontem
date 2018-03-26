# pontem
## Overview
pontem is an open-source project that enable susers of Cloud Spanner
to perform four key operations:

1. Database Backup.
1. Database Backup Integrity Check which verifies that the backup completed
successfully. In particular, this checks the list of tables backed-up
and the number of rows backed-up. It does not check the cell-level data
values.
1. Database Restore whereby a database backup that is stored on disk is
restored to a Cloud Spanner instance.
1. Database Restore Integrity Check which verifies that the restore
completed successfully. Integrity Check which verifies that the restore completed
successfully. In particular, this checks the list of tables restored
and the number of rows restored. It does not check the cell-level data
values.

## Prerequisites
Before using the pontem, you need to first setup a number of permissions
in your Google Cloud project. Make sure the service account running the jobs
has these permissions:

1. Project Viewer
1. Dataflow Developer
1. Cloud Spanner Database Administrator
1. Storage Object Admin

[Dataflow will use a service account.](https://cloud.google.com/dataflow/security-and-permissions#dataflow-service-account)
In order for Dataflow job to start, you will need to set the
[service account with these permissions](https://cloud.google.com/dataflow/access-control#creating_jobs).

You will want [to setup a Service Account key](https://support.google.com/googleapi/answer/6158857?hl=en)
, download the key as a `.json` file and export it into the path.

```bash
$ export GOOGLE_APPLICATION_CREDENTIALS=/path/to/my/downloaded/credentials/project-name-a1b2c3d4e.json
```

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
