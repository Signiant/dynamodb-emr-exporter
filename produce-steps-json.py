#!/usr/bin/env python

import shutil
import datetime
import argparse
import syslog
import contextlib
import os
import os.path
import sys
import subprocess
import boto
import boto.dynamodb2
import boto.exception
import glob
import tempfile
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
    default="1000",
    help="The value to spike read throughput to before table export"
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
  print '%s %s' % (dateTimeStr,message)

def main(region,filter,destination,impregion,writetput,readtput, spikedread, s3location,appname):

  retCode = 0
  dateStr = datetime.datetime.now().strftime("%Y/%m/%d/%H_%M.%S")

  conn = boto.dynamodb2.connect_to_region(region)

  if conn:
    myLog("connected to dynamodb (region: %s)" % region)
    myLog("exporting all tables where table name contains %s " % filter)

    exportSteps = []
    importSteps = []

    # Generate the taskRunner step - this is common
    taskRunnerStep = addTaskRunnerStep()

    exportSteps.append(taskRunnerStep)
    importSteps.append(taskRunnerStep)

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
        myLog("Generating EMR export JSON for table: [%s]" %table['name'])

        tableS3Path = s3ExportPath + "/" + table['name']

        tputSpikeStep = generateThroughputUpdateStep(table['name'], "Spike", s3ScriptPath, spikedread, table['write'], region)
        exportSteps.append(tputSpikeStep)

        tableExportStep = generateTableExportStep(table['name'],tableS3Path,readtput,region)
        exportSteps.append(tableExportStep)

        tputResetStep = generateThroughputUpdateStep(table['name'], "Reset", s3ScriptPath, table['read'], table['write'], region)
        exportSteps.append(tputResetStep)

        tableImportStep = generateTableImportStep(table['name'],tableS3Path,writetput,impregion)
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
def generateThroughputUpdateStep(tableName, stepName, s3Path, readtput, writetput, region):
    myLog("addThroughputUpdateStep %s" % tableName)

    tputUpdateDict = {}
    tputUpdateDict = { "Name": stepName + " Throughput: " + tableName,
                        "ActionOnFailure": "CONTINUE",
                        "Type": "CUSTOM_JAR",
                        "Jar": "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
                        "Args": [s3Path,
                            region,
                            tableName,
                            readtput,
                            writetput
                            ]
                    }

    return tputUpdateDict

###########
## Add a JSON entry for a single table export step
###########
def generateTableExportStep(tableName,s3Path,readtput,endpoint):
  myLog("addTableExportStep %s" % tableName)

  tableExportDict = {}
  tableExportDict = {"Name": "Export Table:" + tableName,
                     "ActionOnFailure": "CONTINUE",
                     "Type": "CUSTOM_JAR",
                     "Jar":"s3://dynamodb-emr-us-east-1/emr-ddb-storage-handler/2.1.0/emr-ddb-2.1.0.jar",
                     "Args":["org.apache.hadoop.dynamodb.tools.DynamoDbExport",
                             s3Path,
                             tableName,
                             readtput,
                            ]
                    }

  return tableExportDict


###########
## Add a JSON entry for a single table import step
###########
def generateTableImportStep(tableName,s3Path,writetput,impregion):
  myLog("addTableImportStep %s" % tableName)

  tableImportDict = {}
  tableImportDict = {"Name": "Import Table:" + tableName,
                     "ActionOnFailure": "CONTINUE",
                     "Type": "CUSTOM_JAR",
                     "Jar":"s3://dynamodb-emr-us-east-1/emr-ddb-storage-handler/2.1.0/emr-ddb-2.1.0.jar",
                     "Args":["org.apache.hadoop.dynamodb.tools.DynamoDbImport",
                             s3Path,
                             tableName,
                             writetput
                            ]
                    }

  return tableImportDict

###########
## Add a JSON entry for the taskRunner installation step.  Common to export and import
###########
def addTaskRunnerStep():
  myLog("addTaskRunnerStep")

  taskRunnerDict = {"Name":"Install TaskRunner",
                    "ActionOnFailure": "TERMINATE_CLUSTER",
                    "Type": "CUSTOM_JAR",
                    "Jar":"s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
                    "Args":["s3://datapipeline-us-east-1/us-east-1/bootstrap-actions/latest/TaskRunner/install-remote-runner-v2",
                            "--workerGroup=1234",
                            "--endpoint=https://datapipeline.us-east-1.amazonaws.com",
                            "--region=us-east-1",
                            "--logUri=none",
                            "--taskRunnerId=DynamoTaskRunner1",
                            "--zipFile=http://datapipeline-us-east-1.s3.amazonaws.com/us-east-1/software/latest/TaskRunner/TaskRunner-1.0.zip",
                            "--mysqlFile=http://datapipeline-us-east-1.s3.amazonaws.com/us-east-1/software/latest/TaskRunner/mysql-connector-java-bin.jar",
                            "--hiveCsvSerdeFile=http://datapipeline-us-east-1.s3.amazonaws.com/us-east-1/software/latest/TaskRunner/csv-serde.jar",
                            "--proxyHost=",
                            "--proxyPort=-1",
                            "--username=",
                            "--password=",
                            "--windowsDomain=",
                            "--windowsWorkgroup=",
                            "--releaseLabel="
                           ]
                   }
  return taskRunnerDict

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
        table_desc = conn.describe_table(table)
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
      table_list = conn.list_tables(LastEvaluatedTableName,100)

  myLog("Read %d tables from dynamodb" % len(table_list_return))

  return table_list_return

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
