#!/bin/bash

# Inputs
APPNAME=$1
CLUSTER_NAME=$2
PROFILE=$3
JSON_INPUT_DIR=$4
S3LOCATION=$5

# Hard-codes (but can be changed here)
RETRY_DELAY=10
CLUSTER_REGION=us-west-2

# Just vars
INSTALL_DIR=/usr/local/dynamodb-emr
NEXTPHASE=0
RETCODE=0

logMsg()
{
        PROGNAME=restoreEMR
        PID=$$
        logger -t ${PROGNAME}[$PID] $1
        echo $1
}

usage()
{
        echo "Usage: restoreEMR app_name emr_cluster_name boto_profile_name json_input_directory S3_location_for_logs"
}

pollCluster()
{
        PROFILE=$1
        CLUSTERID=$2
        CLUSTERNAME=$3

        COMPLETE=0
        ERRORS=0

        logMsg "polling cluster NAME:${CLUSTERNAME} ID ${CLUSTERID} for status in profile ${PROFILE}"

        while [ $COMPLETE -ne 1 ]
        do
                CLUSTER_STATUS=$(aws emr describe-cluster --cluster-id $CLUSTERID --profile $PROFILE --region $CLUSTER_REGION |jq -r '.["Cluster"]["Status"]["State"]')
                #echo "STATUS IS $CLUSTER_STATUS"

                if [ "${CLUSTER_STATUS}" == "TERMINATED" ]; then
                        # We need to check if there were step errors
                        STEPS_STATUS=$(aws emr describe-cluster --cluster-id $CLUSTERID --profile $PROFILE --region $CLUSTER_REGION | jq -r '.["Cluster"]["Status"]["StateChangeReason"]["Message"]')

                        if [ "${STEPS_STATUS}" == "Steps completed with errors" ]; then
                                ERRORS=1
                        else
                                ERRORS=0
                        fi

                        COMPLETE=1
                elif [ "${CLUSTER_STATUS}" == "TERMINATED_WITH_ERRORS" ]; then
                        ERRORS=1
                        COMPLETE=1
                fi

                sleep 10
        done

        return $ERRORS
}

if [ $# != 5 ]; then
        usage
        exit 1
fi
logMsg "Starting up"

######
## PHASE 1 - See if there are any clusters already runing with our name.  If there are, exit
######
aws emr list-clusters --active --profile ${PROFILE} --region $CLUSTER_REGION | grep -q ${CLUSTER_NAME}
STATUS=$?

if [ $STATUS == 0 ]; then
        # We already have a cluster running - bail
        logMsg "Cluster ERROR: existing cluster ${CLUSTER_NAME} running"
        NEXTPHASE=0
        RETCODE=2
else
        logMsg "No existing EMR cluster with  name ${CLUSTER_NAME} running.  Creating"
        NEXTPHASE=1
fi

######
## PHASE 1 - Create the EMR cluster (with retries)
######
if [ $NEXTPHASE == 1 ]; then
        RETRIES=5
        CURR_ATTEMPT=1

        while [ $CURR_ATTEMPT -le $RETRIES ]
        do
                CLUSTERUP=0

                # Invoke the aws CLI to create the cluster
                logMsg "Creating new EMR Cluster NAME:${CLUSTER_NAME} Attempt ${CURR_ATTEMPT} of ${RETRIES}"

                CLUSTERID=$(aws emr create-cluster --name "${CLUSTER_NAME}"                                        \
                            --ami-version 3.8.0                                                                    \
                            --service-role "EMR_DefaultRole"                                                       \
                            --tags Name=${CLUSTER_NAME} signiant:product=devops signiant:email=devops@signiant.com \
                            --enable-debugging                                                                     \
                            --log-uri ${S3LOCATION}/emr-logs                                                       \
                            --applications file://${JSON_INPUT_DIR}/applications.json                              \
                            --instance-groups file://${JSON_INPUT_DIR}/instance-groups.json                        \
                            --ec2-attributes file://${JSON_INPUT_DIR}/ec2-attributes.json                          \
							--bootstrap-actions file://${JSON_INPUT_DIR}/bootstrap-actions-import.json             \
                            --steps file://${JSON_INPUT_DIR}/importSteps.json                                      \
                            --auto-terminate                                                                       \
                            --visible-to-all-users                                                                 \
                            --output text                                                                          \
                            --region ${CLUSTER_REGION}                                                             \
                            --profile ${PROFILE}) 

                logMsg "CLUSTERID for ${CLUSTER_NAME} is $CLUSTERID"
                # Now use the waiter to make sure the cluster is launched successfully
                if [ "$CLUSTERID" != "" ]; then
                        logMsg "Waiting for cluster NAME:${CLUSTER_NAME} ID:${CLUSTERID} to start...."
                        aws emr wait cluster-running --cluster-id ${CLUSTERID} --profile ${PROFILE} --region ${CLUSTER_REGION}
                        STATUS=$?

                        if [ $STATUS == 0 ]; then
                                logMsg "Cluster NAME:${CLUSTER_NAME} ID:${CLUSTERID} launched successfully"
                                CLUSTERUP=1
                                break
                        else
                                logMsg "Cluster ERROR: launch failure NAME:${CLUSTER_NAME} ID:${CLUSTERID} Attempt ${CURR_ATTEMPT} of ${RETRIES} "
                                CLUSTERUP=0
                                # Fall into the next iteration of the loop to try and create the cluster again
                        fi
                else
                        logMsg "Cluster ERROR: no cluster ID returned NAME:${CLUSTER_NAME}"
                        CLUSTERUP=0
                fi

                CURR_ATTEMPT=$[$CURR_ATTEMPT+1]
                logMsg "Delaying ${RETRY_DELAY} seconds before attempting to create cluster..."
                sleep ${RETRY_DELAY}
        done

        ####
        ## Phase 3.5 - poll the cluster for status so we know when it's done
        ####
        if [ $CLUSTERUP == 1 ]; then
                # We have a cluster provisioned...now we can poll it's tasks and make sure it completes ok

                pollCluster $PROFILE $CLUSTERID $CLUSTER_NAME
                STATUS=$?

                if [ $STATUS == 0 ]; then
                        logMsg "Cluster SUCCESS NAME:${CLUSTER_NAME} ID:${CLUSTERID}"
                        RETCODE=0
                else
                        logMsg "Cluster ERROR:task failure NAME:${CLUSTER_NAME} ID:${CLUSTERID}"
                        RETCODE=4
                fi
        else
                logMsg "Unable to provision a new cluster after ${RETRIES} attempts"
                RETCODE=6
        fi

fi

exit ${RETCODE}
