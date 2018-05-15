# Prerequisites

The prerequisites required depend upon whether you use Google Cloud Shell
or the Local Command Line.

We strongly recommend you use Google Cloud Shell as there is no setup required.

## Google Cloud Shell

Simply open up a [Google Cloud Shell](https://cloud.google.com/shell/docs/quickstart)
and follow the [Usage](#usage) guide below.

## Local Command Line

### Java and Maven
Ensure that Java 8 or above is installed.

Ensure that [Maven 3+ is installed](https://maven.apache.org/install.html).

### Permissions
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


# Usage
**Examples**
- [Backup](#backup)
  - [Backup with an Integrity Check](#backup-with-an-integrity-check)
- [Restore](#restore)
  - [Restore with an Integrity Check](#restore-with-an-integrity-check)

## Backup
A simple run that backs up a database:

```bash
mvn compile exec:java \
  -Dexec.mainClass=com.google.cloud.pontem.SerializedCloudSpannerDatabaseBackup \
  -Dexec.args="--runner=DataflowRunner \
               --project=my-cloud-spanner-project \
               --region=us-central1 \
               --gcpTempLocation=gs://my-cloud-spanner-project/tmp \
               --inputSpannerInstanceId=my-cloud-spanner-project-db \
               --inputSpannerDatabaseId=words2 \
               --outputFolder=gs://my-cloud-spanner-project/backups/latest \
               --projectId=my-cloud-spanner-project" \
  -Pdataflow-runner
```

A sample run that queries and saves the table row counts while not saving table schemas:

```bash
mvn compile exec:java \
  -Dexec.mainClass=com.google.cloud.pontem.SerializedCloudSpannerDatabaseBackup \
  -Dexec.args="--runner=DataflowRunner \
               --project=my-cloud-spanner-project \
               --region=us-central1 \
               --gcpTempLocation=gs://my-cloud-spanner-project/tmp \
               --inputSpannerInstanceId=my-cloud-spanner-project-db \
               --inputSpannerDatabaseId=words2 \
               --outputFolder=gs://my-cloud-spanner-project/backup_today \
               --projectId=my-cloud-spanner-project \
               --shouldQueryTableRowCounts=true \
               --shouldQueryTableSchema=false" \
  -Pdataflow-runner
```

### Backup with an Integrity Check
A simple sample run:

```bash
mvn compile exec:java \
     -Dexec.mainClass=com.google.cloud.pontem.CloudSpannerDatabaseBackupIntegrityCheck \
     -Dexec.args="--project=my-cloud-spanner-project \
                  --region=us-central1 \
                  --databaseBackupLocation=gs://my-cloud-spanner-project/multi-backup \
                  --job=2017-10-25_11_18_28-6233650047978038157"
```

A sample run that requires checking row counts against the meta-data file:

```bash
mvn compile exec:java \
     -Dexec.mainClass=com.google.cloud.pontem.CloudSpannerDatabaseBackupIntegrityCheck \
     -Dexec.args="--project=my-cloud-spanner-project \
                  --region=us-central1 \
                  --databaseBackupLocation=gs://my-cloud-spanner-project/multi-backup \
                  --job=2017-10-25_11_18_28-6233650047978038157 \
                  --checkRowCountsAgainstGcsMetadataFile=true"
```

## Restore
A simple restore of a database:

```bash
mvn compile exec:java \
  -Dexec.mainClass=com.google.cloud.pontem.SerializedCloudSpannerDatabaseRestore \
  -Dexec.args="--runner=DataflowRunner \
               --project=my-cloud-spanner-project \
               --region=us-central1 \
               --gcpTempLocation=gs://my-cloud-spanner-project/tmp \
               --outputSpannerInstanceId=my-cloud-spanner-project-db \
               --outputSpannerDatabaseId=words2 \
               --inputFolder=gs://my-cloud-spanner-project/backups/latest \
               --projectId=my-cloud-spanner-project" \
  -Pdataflow-runner
```

A sample restore of a database with a smaller Spanner write batch size. Note
that you will need to get the number of mutations down to 20000 in order
to comply with [Spanner's limits](https://cloud.google.com/spanner/quotas):

```bash
mvn compile exec:java \
  -Dexec.mainClass=com.google.cloud.pontem.SerializedCloudSpannerDatabaseRestore \
  -Dexec.args="--runner=DataflowRunner \
               --project=my-cloud-spanner-project \
               --region=us-central1 \
               --gcpTempLocation=gs://my-cloud-spanner-project/tmp \
               --outputSpannerInstanceId=my-cloud-spanner-project-db \
               --outputSpannerDatabaseId=words2 \
               --inputFolder=gs://my-cloud-spanner-project/today_backup \
               --projectId=my-cloud-spanner-project \
               --spannerWriteBatchSizeBytes=524288"
  -Pdataflow-runner
```

### Restore with an Integrity Check
A sample run:

```bash
mvn compile exec:java \
  -Dexec.mainClass=com.google.cloud.pontem.CloudSpannerDatabaseRestoreIntegrityCheck \
  -Dexec.args="--project=my-cloud-spanner-project \
               --region=us-central1 \
               --backupJobId=2017-12-28_08_21_49-14004506096652894727 \
               --restoreJobId=2017-12-28_10_43_41-10818313728228686289 \
               --restoreJobId=2017-12-28_10_46_20-2492938473109299932 \
               --restoreJobId=2017-12-28_10_48_47-17657844663911609681 \
               --restoreJobId=2017-12-28_10_51_16-6862684906618456059 \
               --restoreJobId=2017-12-28_10_53_55-7680195084002915695 \
               --restoreJobId=2017-12-28_10_43_41-17045671774098975520 \
               --restoreJobId=2017-12-28_10_43_41-16236058809269230845 \
               --databaseBackupLocation=gs://my-cloud-spanner-project/words_db_apache2.2.0_b/ \
               --areAllTablesRestored=false"
```

A sample run that requires every table to have been restored:

```bash
mvn compile exec:java \
  -Dexec.mainClass=com.google.cloud.pontem.CloudSpannerDatabaseRestoreIntegrityCheck \
  -Dexec.args="--project=my-cloud-spanner-project \
               --region=us-central1 \
               --backupJobId=2017-12-28_08_21_49-14004506096652894727 \
               --restoreJobId=2017-12-28_10_43_41-10818313728228686289 \
               --restoreJobId=2017-12-28_10_46_20-2492938473109299932 \
               --restoreJobId=2017-12-28_10_48_47-17657844663911609681 \
               --restoreJobId=2017-12-28_10_51_16-6862684906618456059 \
               --restoreJobId=2017-12-28_10_53_55-7680195084002915695 \
               --restoreJobId=2017-12-28_10_43_41-17045671774098975520 \
               --restoreJobId=2017-12-28_10_43_47-170458491231849489454 \
               --restoreJobId=2017-12-28_10_43_41-16236058809269230845 \
               --databaseBackupLocation=gs://my-cloud-spanner-project/words_db_apache2.2.0_b/ \
               --areAllTablesRestored=true"
```

# Performance
## General Performance Tips
1. Examine the [Google Cloud IAM Quotas page](https://console.cloud.google.com/iam-admin/quotas).

    Look in particular for quotas that are maxed-out (i.e., showing in deep orange). If you have maxed out your Dataflow worker count, you will need more "CPUs (all regions)", "In-use IP addresses", and "CPUs".

Look also for "Persistent Disk Standard (GB)".

2. Examine your Cloud Spanner CPU.

    If the CPU usage is over 75%, it is likely worth increasing your [node count](https://cloud.google.com/spanner/docs/instances#node_count). Otherwise, the limiting factor is likely somewhere else.

3. Optimizing Dataflow Worker Count.

    In general, the more Dataflow workers you use for a backup, the better. In restoring a database, if you're Cloud Spanner CPU is above 75% and you cannot increase your Cloud Spanner node count, consider limiting the Dataflow worker count using the ``--maxNumWorkers`` flag.

4. Run everything in the same region.

    Ensure that the Cloud Spanner database instance, Dataflow workers and Cloud Storage bucket are all in the same region to prevent any cross-region traffic. 

## Benchmarks
The times to perform backup and restore will vary dramatically based upon on Cloud Spanner node count, Dataflow worker count, persistent disk available to Dataflow, and the number of parent-child tables. For example, if you have a parent-child table, the parent will need to be restored first before the child can even begin being restored.

The benchmarks below were performed on a 20 node Cloud Spanner instance using 1000 Dataflow workers with 250,000 Persistent Disk Standard (GB). In some instances, the flag ``--maxNumWorkers`` was used to limit the number of Dataflow workers to 150 as Cloud Spanner was running too hot during the insert period.

| Cloud Spanner Database Size  | Number of Rows  | Backup Time  | Backup Space   | Restore Time  |
|---|---|---|---|---|
| 26TB  | 10B  | ~70 min  |  ~17TB  | ~25 hours  |
| 8TB   | 2B | ~45 min | ~3.5TB | ~6.5 hours |
| 150GB | 1B | ~35 min | ~65GB | ~2.5 hours  |

# Cost
The costs involved in performing a backup and restore include:

  * [Costs associated with Dataflow jobs](https://cloud.google.com/dataflow/pricing) such as CPU, storage, and memory.
  * [Costs associated with Cloud Storage](https://cloud.google.com/storage/pricing) such as network usage and data storage.
  * [Costs associated with Cloud Spanner](https://cloud.google.com/spanner/pricing) such as network usage and any additional nodes.

For example, if you backup 2TB and use an additional 10 Cloud Spanner nodes with 750 Dataflow workers and 250TB of Dataflow storage and 2TB of Google Cloud Storage, the cost for a day will be about $300.

To compute a sample cost for you, use the [Google Cloud pricing tool](https://cloud.google.com/products/calculator/).

Please note especially that to minimize costs and backup time, you should seek to avoid cross-region network traffic. You can specify the backup [bucket region](https://cloud.google.com/storage/docs/bucket-locations) and [dataflow region](https://cloud.google.com/dataflow/docs/concepts/regional-endpoints).
