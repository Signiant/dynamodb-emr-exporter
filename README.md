# DynamoDB EMR Exporter
Uses EMR clusters to copy DynamoDB tables by prefix from one region to another.

## Setup

### 1. Local Setup
* Clone this repo locally
* Install python and dependencies
```shell
  apt-get install python
  pip install -r requirements.txt
  ```
* Configure at least one [boto profile](http://boto.cloudhackers.com/en/latest/boto_config_tut.html)
* Configure the size of your EMR cluster
>Edit the *common-json/instance-groups.json file* to set the number of masters and workers (typically, a single master and worker is fine)

### 2. AWS Setup
* Create a new IAM role called dynamodb_emr_backup_restore using the IAM policy contained in _**dynamodb_emr_backup_restore.IAMPOLICY.json**_

>The role name can be changed by editing common-json/ec2-attributes.json  

* Create a new s3 bucket to be used by the import-export process
* In this bucket, create a directory named _clone_
* Clone and build the [dynamodb-table-clone](https://github.com/Signiant/dynamodb-table-clone), then upload `clone.sh`, `reset.sh` and `table-clone.jar` to this s3 bucket under the _clone_ directory


## Usage
Run the importExport.sh script as follows
```shell
./importExport.sh app_name boto_profile_name table_filter read_throughput_percentage write_capacity json_output_directory s3_output_location import_region export_region
```
Where  

* _**app_name**_ is a 'friendly name' for the DynamoDB table set you wish to export
* _**boto_profile_name**_ is a valid boto profile name containing your keys and a region
* _**table_filter**_ is a filter for which table names to export (ie. MYAPP_PROD will export ALL tables starting with MYAPP_PROD)
* _**read_throughput_percentage**_ is the percent of provisioned read throughput to use (eg 0.45 will use 45% of the provisioned read throughput)
* _**write_capacity**_ is the spiked write capacity to use temporarily when importing table data
* _**json_output_directory**_ is a folder to output the json files for configuring the EMR cluster for export
* _**s3_output_location**_ is tje s3 bucket you created during the AWS setup (ie. s3://mybucket)
* _**export_region**_ is the region from which the dynamodb tables will be exported
* _**import_region**_ is the region to which the dynamodb tables will be imported


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
