#!/bin/bash
SOURCE_REGION=$1
TABLE_NAME=$2
READ_CAPACITY=$3
WRITE_CAPACITY=$4

MAX_ATTEMPTS=50
ATTEMPTS=0
SLEEP_SECONDS=20

USAGE="$0 source_region table_name read_capacity write_capacity"

if [ $# != 4 ]; then
  echo $USAGE
  exit 1
fi


#Update the table throughput
TABLE_STATUS=$(aws dynamodb update-table --region $SOURCE_REGION --table-name $TABLE_NAME --provisioned-throughput ReadCapacityUnits=${READ_CAPACITY},WriteCapacityUnits=${WRITE_CAPACITY} --query 'Table.TableStatus' --output text)
if [ $? -ne 0 ]; then
  echo "Unable to spike throughput"
  exit 1
fi

# wait for the table to finish updating
while [ $ATTEMPTS -le $MAX_ATTEMPTS ]; do
  TABLE_STATUS=$(aws dynamodb describe-table --region $SOURCE_REGION --table-name $TABLE_NAME --query 'Table.TableStatus' --output text)
  if [ "$TABLE_STATUS" == "ACTIVE" ]; then
    exit 0
  fi
  (( ATTEMPTS++ ))
  sleep $SLEEP_SECONDS
done


echo "Table never transitioned to active"
exit 1
