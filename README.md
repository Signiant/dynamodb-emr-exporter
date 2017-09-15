# DynamoDB EMR Exporter
Uses EMR clusters to export and import dynamoDB tables to/from S3.  This uses the same routines as dataPipeline BUT it runs everything though a single cluster for all tables rather than a cluster per table.

## Export Usage

The tool is packaged into a Docker container with all the prerequisites required.  To run this:

* Create a new IAM role
** Must be named _**dynamodb_emr_backup_restore**_
** Use the IAM policy contained in _**config-samples/dynamodb_emr_backup_restore.IAMPOLICY.json**_

* Create a new EMR Security Configuration in any region to backup or restore to
** Must be named _**dynamodb-backups**_

* Run the docker container as follows:

```bash
docker run \
  signiant/dynamodb-emr-exporter \
    app_name \
    emr_cluster_name \
    table_filter \
    read_throughput_percentage \
    s3_location \
    export_region \
    import_region \
    spiked_throughput
```

Where

* _**app_name**_ is a 'friendly name' for the DynamoDB table set you wish to export
* _**emr_cluster_name**_ is a name to give to the EMR cluster
* _**table_filter**_ is a filter for which table names to export (ie. MYAPP_PROD will export ALL tables starting with MYAPP_PROD)
* _**read_throughput_percentage**_ is the percent of provisioned read throughput to use (eg 0.45 will use 45% of the provisioned read throughput)
* _**S3_location**_ is a base S3 location to store the exports and all logs (ie. s3://mybucket/myfolder)
* _**export_region**_ is the AWS region where the tables to export exist
* _**import_region**_ is the AWS region where you expect to import the tables (the import steps are pre-generated at export time)
* _**spiked_throughput**_ is an optional provisioned read throughput value to spike the read throughtput to on the table being backed up

An optional environment variable _**DEBUG_OUTPUT**_ can also be specified to the container which will run the underlying script with debug enabled

## Import Usage

When the export runs, it also generates the configuration needed to execute an import. You can find the configuration file for imorting within the S3 location you specified (importSteps.json).

### Running the import

The import can be run from Docker but you'll need to exec into the container to run it.

```bash
docker run \
  --entrypoint bash \
  signiant/dynamodb-emr-exporter
```
Before running the import, you need to perform 2 tasks

1. The tables you are importing data into MUST already exist with the same key structure in the region you wish to import into
2. Copy the importSteps.json file from the S3 bucket which contains the exports into the Docker container into the /app/common-json folder

Once these are done, you can invoke the restore like so
```
./restoreEMR.sh app_name emr_cluster_name local_json_files_path s3_path_for_logs cluster_region
```

Where

* _**app_name**_ is a 'friendly name' for the DynamoDB table set you wish to import
* _**emr_cluster_name**_ is a name to give to the EMR cluster
* _**local_json_files_path**_ is a folder to containing the json files produced by the export (generally, this will be /app/common-json)
* _**s3_path_for_logs**_ is a base S3 location to store logs from EMR related to the import
* _**cluster_region**_ is the AWS region in which to start the EMR cluster.  This does not have to be the same region as the tables are being imported to

_**NOTE**_
The write throughput to use for the DynamoDB tables is actually defined in the script that runs at export time.  This is because it's then configured in the importSteps.json file.  If you wish to increase this, you can edit the generated importSteps.json file.

## Workings

The basic mechanics of the process are as follows

### Export

1. Check and see if there are any EMR clusters already running for 'this' app.  If so, exit.  Otherwise, carry on
2. Setup the common configuration for the cluster
3. Call the python script to generate the steps (tasks) for EMR for each table.  This essentially lists all the tables in the region, applies the provided filter and then generates the JSON that can be passed to EMR to export the tables
4. Once the steps JSON is present, create a new cluster with the AWS CLI. We have to handle cluster setup failure here so retries are used for failures.
5. Submit the tasks to the cluster and poll the cluster until it's complete.  Any errors of a step will result in a failure being logged
6. Once we know everyyhing was successful, write the export and import steps files to S3 in case this machine has issues.  We also write flag files to S3 indicating the progress of the export (in progress, complete, error, etc.) in case another process needs to ingest this data, it can poll on these status files.

### Import

1. Create a new EMR cluster with the import steps file as the tasks to perform
2. Poll the cluster to ensure success
