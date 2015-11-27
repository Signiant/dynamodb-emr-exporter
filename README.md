# DynamoDB EMR Exporter
Uses EMR clusters to export and import dynamoDB tables to/from S3.  This uses the same routines as dataPipeline BUT it runs everything though a single cluster for all tables rather than a cluster per table.

## Export Usage

* Clone this repo to a folder called /usr/local/dynamodb-emr
* Install python
``` apt-get install python ```
 * Install the python dependancies
``` pip install -r requirements.txt ```
* Configure at least one [boto profile](http://boto.cloudhackers.com/en/latest/boto_config_tut.html)
* Create a new IAM role called dynamodb_emr_backup_restore using the IAM policy contained in _**dynamodb_emr_backup_restore.IAMPOLICY.json**_

>The role name can be changed by editing common-json/ec2-attributes.json

* Configure the size of your EMR cluster
 
>Edit the *common-json/instance-groups.json file* to set the number of masters and workers (typically, a single master and worker is fine)

* Run the invokeEMR.sh script as follows

```
./invokeEMR.sh app_name emr_cluster_name boto_profile_name table_filter read_throughput_percentage json_output_directory S3_location
```

Where

* _**app_name**_ is a 'friendly name' for the DynamoDB table set you wish to export
* _**emr_cluster_name**_ is a name to give to the EMR cluster
* _**boto_profile_name**_ is a valid boto profile name containing your keys and a region
* _**table_filter**_ is a filter for which table names to export (ie. MYAPP_PROD will export ALL tables starting with MYAPP_PROD)
* _**read_throughput_percentage**_ is the percent of provisioned read throughput to use (eg 0.45 will use 45% of the provisioned read throughput)
* _**json_output_directory**_ is a folder to output the json files for configuring the EMR cluster for export
* _**S3_location**_ is a base S3 location to store the exports and all logs (ie. s3://mybucket/myfolder)

## Import Usage

When the export runs, it also generates the configuration needed to execute an import. You can find the configuration file for imorting within the json output directory you used for the export (importSteps.json).  It is also copied to the S3 bucket at the completion of the export.

### Running the import 

Before running the import, you need to perform 2 tasks

1. The tables you are importing data into MUST already exist with the same key structure in the region you wish to import into
2. Edit the _**restoreEMR.sh**_ script to set the region to that in which you need to restore the data to (variable at the top of the script called CLUSTER_REGION)

Once these are done, you can invoke the restore like so

```
./restoreEMR.sh app_name emr_cluster_name boto_profile_name local_json_files_path s3_path_for_logs
```

Where

* _**app_name**_ is a 'friendly name' for the DynamoDB table set you wish to import
* _**emr_cluster_name**_ is a name to give to the EMR cluster
* _**boto_profile_name**_ is a valid boto profile name containing your keys and a region
* _**local_json_files_path**_ is a folder to containing the json files produced by the export
* _**s3_path_for_logs**_ is a base S3 location to store logs from EMR related to the import

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
