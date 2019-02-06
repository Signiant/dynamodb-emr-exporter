#!/bin/bash
SOURCE_REGION=$1
TABLE_NAME=$2
READ_CAPACITY=$3
WRITE_CAPACITY=$4
AS_READ_MIN_TPUT=$5

MAX_ATTEMPTS=50
ATTEMPTS=0
SLEEP_SECONDS=20
RET_CODE=0

table_scaleable_read_dimension="dynamodb:table:ReadCapacityUnits"

USAGE="$0 source_region table_name read_capacity write_capacity"

if [ $# -lt 4 ]; then
  echo $USAGE
  exit 1
fi

# Does the table have an autoscaling scalable target for reads?
# If so, return the RoleARN and the max capacity
scalable_target_exists()
{
  resource_id=$1
  scalable_dimension=$2
  region=$3

  scalable_target=$(aws application-autoscaling describe-scalable-targets \
                        --service-namespace dynamodb \
                        --resource-id "${resource_id}" \
                        --query "ScalableTargets[?contains(ScalableDimension,\`${scalable_dimension}\`) == \`true\`].[RoleARN,MaxCapacity]" \
                        --region ${region} \
                        --output text)

  if [ -z "${scalable_target}" ]; then
    echo "false"
  else
    echo "${scalable_target}"
  fi
}

# Add or replace a scalable target on a table or index
register_scalable_target()
{
  resource_id=$1
  scalable_dimension=$2
  role_arn=$3
  min_tput=$4
  max_tput=$5
  region=$6

  aws application-autoscaling register-scalable-target \
    --service-namespace dynamodb \
    --resource-id "${resource_id}" \
    --scalable-dimension "${scalable_dimension}" \
    --min-capacity ${min_tput} \
    --max-capacity ${max_tput} \
    --role-arn ${role_arn} \
    --region ${region}

  status=$?

  if [ ${status} -eq 0 ]; then
    echo "true"
  else
    echo "false"
  fi
}

# Poll a table until it becomes ACTIVE
wait_for_active()
{
  table_name=$1
  region=$2

  # wait for the table to finish updating
  while [ $ATTEMPTS -le $MAX_ATTEMPTS ]; do
    TABLE_STATUS=$(aws dynamodb describe-table --region $region --table-name $TABLE_NAME --query 'Table.TableStatus' --output text)
    echo "Checking table status, attempt ${ATTEMPTS}" 1>&2
    if [ "$TABLE_STATUS" == "ACTIVE" ]; then
      echo "Table transition successful" 1>&2
      return 0
    fi
    echo "Table is $TABLE_STATUS, checking again in $SLEEP_SECONDS seconds" 1>&2
    (( ATTEMPTS++ ))
    sleep $SLEEP_SECONDS
  done

  # if we're here, the table did not become active in a reasonable time
  return 1
}

#
#  MAINLINE
#

table_resource_id="table/${TABLE_NAME}"
scalable_target_exists=$(scalable_target_exists ${table_resource_id} ${table_scaleable_read_dimension} ${SOURCE_REGION})

# Check if we have autoscaling enabled.  If so, we need to update
# the minimum tput so that we don't keep autoscaling down
if [ "${scalable_target_exists}" != "false" ]; then
  echo "Table ${TABLE_NAME} has an autoscaling policy - manipulating the min-tput"
  # get the role ARN and the max capacity currently set...the min capacity we are provided
  role_arn=$(echo ${scalable_target_exists}|cut -f1 -d" "); echo "role arn is ${role_arn}"
  max_tput=$(echo ${scalable_target_exists}|cut -f2 -d" "); echo "max_tput is ${max_tput}"

  if [[ "$(register_scalable_target ${table_resource_id} ${table_scaleable_read_dimension} ${role_arn} ${AS_READ_MIN_TPUT} ${max_tput} ${SOURCE_REGION})" == "true" ]]; then
    echo "Successfully registered new scalable target for ${table_resource_id} with minimum tput ${AS_READ_MIN_TPUT}"

   # Updating the min tput triggers autoscaling to update the table if there is read activity
   # so we need to wait for it to finish updating
   wait_for_active "${TABLE_NAME}" "${SOURCE_REGION}"
   table_status=$?

   if [ ${table_status} -eq 0 ]; then
     echo "Table has returned to ACTIVE state"
   else
     echo "FAILURE: Table never transitioned to active"
     RET_CODE=1
   fi
  else
    echo "ERROR registering new scalable target for ${table_resource_id}"
  fi
fi

echo "Updating the base table read throughput with update-table to ${READ_CAPACITY}"
if [ ${RET_CODE} -eq 0 ]; then
  # Update the table throughput directly
  # This is needed in case there is no autoscaling enabled OR
  # if autoscaling is enabled and there is no read activity on the table
  # Since autoscaling will never scale back down by itself
  TABLE_STATUS=$(aws dynamodb update-table \
                    --region $SOURCE_REGION \
                    --table-name $TABLE_NAME \
                    --provisioned-throughput ReadCapacityUnits=${READ_CAPACITY},WriteCapacityUnits=${WRITE_CAPACITY} \
                    --query 'Table.TableStatus' \
                    --output text 2>&1)

  if [ $? -ne 0 ]; then
    ERROR_TYPE=$(echo $TABLE_STATUS | cut -d \( -f2 | cut -d \) -f1)
    if [ "$ERROR_TYPE" == "ValidationException" ]; then
      echo "Provisioned throughput already set, no action taken"
      RET_CODE=0
    else
      echo "Unable to spike throughput"
      echo ${TABLE_STATUS}
      RET_CODE=1
    fi
  fi

  # Check the table staus again from the update-table call
  wait_for_active ${TABLE_NAME} ${SOURCE_REGION}
  table_status=$?

  if [ ${table_status} -eq 0 ]; then
    RET_CODE=0
  else
    echo "FAILURE: Table never transitioned to active"
    RET_CODE=1
  fi
else
  echo "FAILURE: Table never transitioned to active"
  RET_CODE=1
fi

exit $RET_CODE
