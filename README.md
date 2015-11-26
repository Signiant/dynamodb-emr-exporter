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

## Workings
