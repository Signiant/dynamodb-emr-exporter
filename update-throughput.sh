#!/bin/bash
SOURCE_REGION=$1
TABLE_NAME=$2
READ_CAPACITY=$3
WRITE_CAPACITY=$4

USAGE="$0 source_region table_name read_capacity write_capacity"

#Update the table throughput
aws dynamodb update-table --region $SOURCE_REGION --table-name $TABLE_NAME --provisioned-throughput ReadCapacityUnits=${READ_CAPACITY},WriteCapacityUnits=${WRITE_CAPACITY}
if [ $? -ne 0 ]; then
  echo "Unable to spike throughput"
  exit 1
fi

# wait for the table to finish updating
aws dynamodb wait table-exists --region $SOURCE_REGION --table-name $TABLE_NAME
if [ $? -ne 0 ]; then
  echo "Table never transitioned to active"
  exit 1
fi

exit 0
