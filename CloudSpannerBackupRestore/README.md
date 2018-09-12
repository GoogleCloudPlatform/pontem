# Pontem / Cloud Spanner Backup and Restore

**branch: master** | **branch: dev**
:------------ | :------------
[![Build Status](https://travis-ci.org/GoogleCloudPlatform/pontem.svg?branch=master)](https://travis-ci.org/GoogleCloudPlatform/pontem)|[![Build Status](https://travis-ci.org/GoogleCloudPlatform/pontem.svg?branch=dev)](https://travis-ci.org/GoogleCloudPlatform/pontem)
[![codecov](https://codecov.io/gh/GoogleCloudPlatform/pontem/branch/master/graph/badge.svg)](https://codecov.io/gh/GoogleCloudPlatform/pontem)|[![codecov](https://codecov.io/gh/GoogleCloudPlatform/pontem/branch/dev/graph/badge.svg)](https://codecov.io/gh/GoogleCloudPlatform/pontem)

## Overview
pontem is an open-source project that enable users of Cloud Spanner
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

## Support
While this is not an officially supported Google product, we encourage you
to [file issues](https://github.com/GoogleCloudPlatform/pontem/CloudSpannerBackupRestore/issues/new)
for feature requests and bugs.

## Getting Started
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
