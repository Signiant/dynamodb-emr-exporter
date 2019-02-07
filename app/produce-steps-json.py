#!/usr/bin/env python

import datetime
import argparse
import syslog
import contextlib
import os
import os.path
import sys
import boto3
import json

parser = argparse.ArgumentParser(
  prog="produce-steps-json",
  description="""EMR JSON steps producer for DynamoDB table extraction"""
)

parser.add_argument(
  '-a',
  '--appname',
  type=str,
  default="MYAPP",
  help="Name of the application we are exporting tables for.  Used in the S3 path where the dumps finally end up."
)

parser.add_argument(
  '-r',
  '--region',
  type=str,
  default="us-east-1",
  help="The region to connect to for exporting."
)

parser.add_argument(
  '-i',
  '--impregion',
  type=str,
  default="us-west-2",
  help="The region to fill-in for the json import files."
)

parser.add_argument(
  '-e',
  '--readtput',
  type=str,
  default="0.25",
  help="The percentage of read throughput to utilize when exporting (def: 0.25)."
)

parser.add_argument(
  '-w',
  '--writetput',
  type=str,
  default="0.5",
  help="The percentage of write throughput to utilize when importing data (def: 0.5)."
)

parser.add_argument(
    '-s',
    '--spikedread',
    type=str,
    help="The value to spike read throughput to before table export"
)

parser.add_argument(
    '-x',
    '--excludes',
    type=str,
    help="A file containing a list of tables to exclude"
)

parser.add_argument(
  '-f',
  '--filter',
  type=str,
  default="",
  help="Only export tables with this filter criteria in the table name."
)

parser.add_argument(
  'destination',
  type=str,
  help="where to place the EMR export and import steps files"
)

parser.add_argument(
  's3location',
  type=str,
  help="The S3 FOLDER path to place export files in and read import files from."
)

def myLog(message):
  procName = __file__
  currDTS = datetime.datetime.now()
  dateTimeStr = currDTS.strftime('%Y/%B/%d/ %H:%M:%S')

  syslogMsg = procName + ": " + message
  syslog.syslog(syslogMsg)
  print ('%s %s' % (dateTimeStr,message))

def main(region,filter,destination,impregion,writetput,readtput, spikedread, s3location,appname,excludes):

  retCode = 0
  dateStr = datetime.datetime.now().strftime("%Y/%m/%d/%H_%M.%S")

  conn = boto3.client('dynamodb', region_name=region)

  # Have we been given an excludes file?  If so, read it.  Any tables in here
  # will not have export steps generated for them
  if excludes:
      myLog("excludes specified - reading " + excludes)

      if os.path.exists(excludes):
          exclude_table_list = [line.rstrip('\n') for line in open(excludes)]
      else:
          myLog("Unable to open " + excludes + " for reading")

  if conn:
    myLog("connected to dynamodb (region: %s)" % region)
    myLog("exporting all tables where table name contains %s " % filter)

    exportSteps = []
    importSteps = []

    # get a list of all tables in the region
    table_list = listTables(conn)
    table_desc_list = describeTables(conn, table_list)

    # Get the path we will use for 'this' backup
    s3ExportPath = generateS3Path(s3location,region,dateStr,appname)

    # Get the path to the update-throughput script
    s3ScriptPath = s3location.rstrip('/') + "/scripts/update-throughput.sh"

    S3PathFilename = destination + "/s3path.info"
    writeFile(s3ExportPath,S3PathFilename)

    # Now process them, ignoring any that don't match our filter
    for table in table_desc_list:
      if filter in table['name']:

        if table['name'] in exclude_table_list:
          myLog("Table " + table['name'] + " is in the exclude list - skipping")
          continue
        else:
          myLog("Generating EMR export JSON for table: [%s]" %table['name'])

        autoscale_min_spike_read_capacity = None # Assume no autoscaling
        autoscale_min_reset_read_capacity = None
        tableS3Path = s3ExportPath + "/" + table['name']

        # Check if table is set to On Demand Capacity
        if int(table['read']) > 0:
          myLog("Table uses provisioned capacity - need to add throughput spike and reset steps")
          # Does this table have autoscaling enabled?
          scalable_target_info = scalable_target_exists(region,"table/" + table['name'],"dynamodb:table:ReadCapacityUnits")
          if scalable_target_info is not None:
            myLog("Table " + table['name'] + " has autoscaling enabled")
            autoscale_min_spike_read_capacity = scalable_target_info[0]['MinCapacity'] + int(spikedread)
            autoscale_min_reset_read_capacity = scalable_target_info[0]['MinCapacity']
            myLog("Table " + table['name'] + " has a current AS min capacity of " + str(autoscale_min_reset_read_capacity))

          if spikedread is not None:
            tputSpikeStep = generateThroughputUpdateStep(table['name'], "Spike", s3ScriptPath, autoscale_min_spike_read_capacity, autoscale_min_spike_read_capacity, table['write'], region)
            exportSteps.append(tputSpikeStep)
        else:
          myLog("Table uses on-demand capacity - no need for spike and reset throughput steps")

        tableExportStep = generateTableExportStep(table['name'],tableS3Path,readtput)
        exportSteps.append(tableExportStep)

        if int(table['read']) > 0:
            if spikedread is not None:
                tputResetStep = generateThroughputUpdateStep(table['name'], "Reset", s3ScriptPath, table['read'], autoscale_min_reset_read_capacity, table['write'], region)
                exportSteps.append(tputResetStep)

        tableImportStep = generateTableImportStep(table['name'],tableS3Path,writetput)
        importSteps.append(tableImportStep)

    # Now we can write out the import and export steps files
    exportJSON = json.dumps(exportSteps,indent=4)
    exportJSONFilename = destination + "/exportSteps.json"
    writeFile(exportJSON,exportJSONFilename)

    importJSON = json.dumps(importSteps,indent=4)
    importJSONFilename = destination + "/importSteps.json"
    writeFile(importJSON,importJSONFilename)

###########
## Add a JSON entry for a single table throughput update step
###########
def generateThroughputUpdateStep(tableName, stepName, s3Path, readtput, autoscale_min_throughput,writetput, region):
    myLog("addThroughputUpdateStep (%s) %s" % (stepName, tableName))

    tputUpdateDict = {}

    if autoscale_min_throughput:
        tputUpdateDict = { "Name": stepName + " Throughput: " + tableName,
                            "ActionOnFailure": "CONTINUE",
                            "Type": "CUSTOM_JAR",
                            "Jar": "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
                            "Args": [s3Path,
                                region,
                                tableName,
                                str(readtput),
                                str(writetput),
                                str(autoscale_min_throughput)
                                ]
                        }
    else:
        tputUpdateDict = { "Name": stepName + " Throughput: " + tableName,
                            "ActionOnFailure": "CONTINUE",
                            "Type": "CUSTOM_JAR",
                            "Jar": "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
                            "Args": [s3Path,
                                region,
                                tableName,
                                str(readtput),
                                str(writetput)
                                ]
                        }

    return tputUpdateDict

###########
## Add a JSON entry for a single table export step
###########
def generateTableExportStep(tableName,s3Path,readtput,jarPath=None,classPath=None):
  myLog("addTableExportStep %s" % tableName)

  if not jarPath:
    # Default JAR
    jarPath = "s3://dynamodb-emr-us-east-1/emr-ddb-storage-handler/2.1.0/emr-ddb-2.1.0.jar"

  if not classPath:
    # Default ClassPath
    classPath = "org.apache.hadoop.dynamodb.tools.DynamoDbExport"

  tableExportDict = {}
  tableExportDict = {"Name": "Export Table:" + tableName,
                     "ActionOnFailure": "CONTINUE",
                     "Type": "CUSTOM_JAR",
                     "Jar": jarPath,
                     "Args":[classPath,
                             s3Path,
                             tableName,
                             readtput,
                            ]
                    }

  return tableExportDict


###########
## Add a JSON entry for a single table import step
###########
def generateTableImportStep(tableName,s3Path,writetput,jarPath=None,classPath=None):
  myLog("addTableImportStep %s" % tableName)

  if not jarPath:
    # Default JAR
    jarPath = "s3://dynamodb-emr-us-east-1/emr-ddb-storage-handler/2.1.0/emr-ddb-2.1.0.jar"

  if not classPath:
    # Default ClassPath
    classPath = "org.apache.hadoop.dynamodb.tools.DynamoDbImport"

  tableImportDict = {}
  tableImportDict = {"Name": "Import Table:" + tableName,
                     "ActionOnFailure": "CONTINUE",
                     "Type": "CUSTOM_JAR",
                     "Jar": jarPath,
                     "Args":[classPath,
                             s3Path,
                             tableName,
                             writetput
                            ]
                    }

  return tableImportDict

###########
## Generate a formatted S3 path which is used in the export and import steps file
###########
def generateS3Path(basePath,region,dateStr,appname):
  myLog("generateS3Path BASE:%s" % basePath)
  basePath = basePath.rstrip('/')

  s3Path = basePath + "/" + region + "/" + appname + "/" + dateStr
  myLog("S3 path generated is %s" % s3Path)

  return s3Path


def describeTables(conn, table_list):
    table_list_return = []

    for table in table_list:
        table_desc = conn.describe_table(TableName=table)
        table_return = dict()
        table_return['name'] = table
        table_return['read'] = str(table_desc['Table']['ProvisionedThroughput']['ReadCapacityUnits'])
        table_return['write'] = str(table_desc['Table']['ProvisionedThroughput']['WriteCapacityUnits'])
        table_list_return.append(table_return)

    return table_list_return


###########
## Obtain a list of dynamoDB tables from the current region
###########
def listTables(conn):

  table_list_return = []

  # Get the inital list of tables. boto only returns the first 100 tho....
  table_list = conn.list_tables()

  moreTables = True
  while moreTables:
    if 'LastEvaluatedTableName' in table_list:
      LastEvaluatedTableName = table_list['LastEvaluatedTableName']
      moreTables = True
    else:
      LastEvaluatedTableName = ''
      moreTables = False

    for table_name in table_list['TableNames']:
      table_list_return.append(table_name)

    if LastEvaluatedTableName != '':
      table_list = conn.list_tables(ExclusiveStartTableName=LastEvaluatedTableName,Limit=100)

  myLog("Read %d tables from dynamodb" % len(table_list_return))

  return table_list_return

# Checks if a dynamo table has a scalable target (ie. is autoscale enabled?)
def scalable_target_exists(region,resource_id,scalable_dimension):
    response=None
    retval=None

    myLog("Checking if scalable target exists for " + resource_id + " for dimension " + scalable_dimension)
    client = boto3.client('application-autoscaling', region_name=region)

    try:
        response = client.describe_scalable_targets(
            ServiceNamespace='dynamodb',
            ResourceIds=[
                resource_id,
            ],
            ScalableDimension=scalable_dimension
        )
    except Exception as e:
        myLog("Failed to describe scalable targets " + str(e))

    if response:
        if response['ScalableTargets']:
            retval = response['ScalableTargets']

    return retval

def writeFile(content,filename):
  myLog("writeFile %s" % filename)

  text_file = open(filename,"w")
  text_file.write(content)
  text_file.close()


@contextlib.contextmanager
def preserve_cwd():
    cwd = os.getcwd()
    try: yield
    finally: os.chdir(cwd)

if __name__ == '__main__':
  kwargs = dict(parser.parse_args(sys.argv[1:])._get_kwargs())
  main(**kwargs)
