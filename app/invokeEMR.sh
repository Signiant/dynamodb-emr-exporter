#!/bin/bash

if [ "$DEBUG_OUTPUT" ]; then
    echo "DEBUG Output selected"
    set -x
fi

# Inputs
APPNAME=$1
CLUSTER_NAME=$2
TABLE_FILTER=$3
READ_TPUT=$4
S3LOCATION=$5
REGION=$6
IMPORT_REGION=$7
SPIKED_THROUGHPUT=$8

WRITE_TPUT=0.8		# Used when we generate the Import steps
RETRY_DELAY=60

# Just vars
INSTALL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
COMMON_JSON=${INSTALL_DIR}/common-json
STEP_PRODUCER=${INSTALL_DIR}/produce-steps-json.py
THROUGHPUT_SCRIPT=${INSTALL_DIR}/update-throughput.sh
JSON_OUTPUT_DIR=${INSTALL_DIR}/${TABLE_FILTER}
NEXTPHASE=0
RETCODE=0

# Lock files (delivered to S3 at different phases)
BACKUP_RUNNING_LOCK_NAME=BACKUP_RUNNING
BACKUP_COMPLETE_SUCCESS_LOCK_NAME=BACKUP_COMPLETE_SUCCESS
BACKUP_COMPLETE_FAILED_LOCK_NAME=BACKUP_COMPLETE_FAILED

BACKUP_RUNNING_LOCK_LOCAL_FILE=${INSTALL_DIR}/${BACKUP_RUNNING_LOCK_NAME}
BACKUP_COMPLETE_SUCCESS_LOCK_LOCAL_FILE=${INSTALL_DIR}/${BACKUP_COMPLETE_SUCCESS_LOCK_NAME}
BACKUP_COMPLETE_FAILED_LOCK_LOCAL_FILE=${INSTALL_DIR}/${BACKUP_COMPLETE_FAILED_LOCK_NAME}

logMsg()
{
        PROGNAME=invokeEMR
        PID=$$
        logger -t ${PROGNAME}[$PID] $1
        echo $1
}

usage()
{
        echo "Usage: invokeEMR app_name emr_cluster_name table_filter read_throughput_percentage json_output_directory S3_location export_region import_region [spiked_throughput]"
}

pollCluster()
{
        CLUSTERID=$1
        CLUSTERNAME=$2

        COMPLETE=0
        ERRORS=0

        logMsg "polling cluster NAME:${CLUSTERNAME} ID ${CLUSTERID} for status in region ${REGION}"

        while [ $COMPLETE -ne 1 ]
        do
                CLUSTER_STATUS=$(aws emr describe-cluster --cluster-id $CLUSTERID --region $REGION --output text --query 'Cluster.Status.State')
                #echo "STATUS IS $CLUSTER_STATUS"

                if [ "${CLUSTER_STATUS}" == "TERMINATED" ]; then
                        # We now need to check if there were step errors
                        STEPS_STATUS=$(aws emr describe-cluster --cluster-id $CLUSTERID --region $REGION --output text --query 'Cluster.Status.StateChangeReason.Message')

                        if [ "${STEPS_STATUS}" == "Steps completed with errors" ]; then
                                EXPORT_FAILS=$(aws emr list-steps --step-states FAILED --cluster-id $CLUSTERID --region $REGION --output text --query 'Steps[?starts_with(Name, `Export Table:`) == `true`]|[].Name')
                                if [ ! -z "${EXPORT_FAILS}" ]; then
                                  ERRORS=1
                                else
                                  ERRORS=0
                                fi
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

if [ $# != 7 ] && [ $# != 8 ]; then
        usage
        exit 1
fi
logMsg "Starting up"

######
## PHASE 1 - See if there are any clusters already runing with our name.  If there are, exit
######
aws emr list-clusters --active --region ${REGION} | grep -q ${CLUSTER_NAME}
STATUS=$?

if [ $STATUS -eq 0 ]; then
        # We already have a cluster running - bail
        logMsg "Cluster ERROR: existing cluster ${CLUSTER_NAME} running"
        NEXTPHASE=0
        RETCODE=2
else
        logMsg "No existing EMR cluster with  name ${CLUSTER_NAME} running.  Creating"
        NEXTPHASE=1
fi

######
## PHASE 2 - Copy in the common JSON files
######
if [ $NEXTPHASE -eq 1 ]; then
    if [ ! -d "${COMMON_JSON}" ]; then
            logMsg "The common-json folder is missing - unable to continue"
            NEXTPHASE=0
            RETCODE=2
    else
            mkdir -p ${JSON_OUTPUT_DIR}

            logMsg "Copying common json files to ${JSON_OUTPUT_DIR}"
            cp -f ${COMMON_JSON}/ec2-attributes.json ${JSON_OUTPUT_DIR}/ec2-attributes.json
            cp -f ${COMMON_JSON}/instance-groups.json ${JSON_OUTPUT_DIR}/instance-groups.json
            cp -f ${COMMON_JSON}/configurations.json ${JSON_OUTPUT_DIR}/configurations.json

            if [ ! -e "${JSON_OUTPUT_DIR}/ec2-attributes.json" ] ||
               [ ! -e "${JSON_OUTPUT_DIR}/configurations.json" ] ||
               [ ! -e "${JSON_OUTPUT_DIR}/instance-groups.json" ]; then
                    logMsg "Error copying common json files to ${JSON_OUTPUT_DIR}"
                    NEXTPHASE=0
                    RETCODE=2
            fi
    fi
fi

######
## PHASE 2.5 - Upload the jar file
######
if [ $NEXTPHASE -eq 1 ]; then
    BUNDLED_JAR=emr-dynamodb-tools-4.8.0-SNAPSHOT.jar
    BUNDLED_JAR_PATH=${INSTALL_DIR}/jar/${BUNDLED_JAR}
    if [ ! -e "${BUNDLED_JAR_PATH}" ]; then
        logMsg "The jar file is missing - unable to continue"
        NEXTPHASE=0
        RETCODE=2
    else
        aws s3 cp ${BUNDLED_JAR_PATH} ${S3LOCATION}/jar/${BUNDLED_JAR}
        if [ $? -ne 0 ]; then
            echo "ERROR: Unable to upload the jar file to s3, unable to continue"
            RETCODE=2
            NEXTPHASE=0
        fi
    fi
fi

######
## PHASE 3 - Upload the update-throughput script
######
if [ $NEXTPHASE -eq 1 ]; then
    if [ ! -e $THROUGHPUT_SCRIPT ]; then
      logMsg "The update-throughput.sh script is missing - unable to continue"
      NEXTPHASE=0
      RETCODE=2
    else
      aws s3 cp $THROUGHPUT_SCRIPT ${S3LOCATION}/scripts/update-throughput.sh
      if [ $? -ne 0 ]; then
        echo "ERROR: Unable to upload the update-throughput script to s3, unable to continue"
        RETCODE=2
        NEXTPHASE=0
      fi
    fi
fi


######
## PHASE 4 - See if we have an excludes file in the S3 bucket and download if so
######
if [ $NEXTPHASE -eq 1 ]; then
    aws s3 cp ${S3LOCATION}/excludes ./excludes

    if [ $? -eq 0 ]; then
      echo "Excludes file found in S3 - downloading ${S3LOCATION}/excludes"
      EXCLUDE_ARG="-x ./excludes"
    fi
fi

######
## PHASE 5 - Generate the steps files
######
if [ $NEXTPHASE -eq 1 ]; then
        # PHASE 2 - Get the EMR steps file for the tables to backup
        logMsg "Generating JSON files (R:${REGION} I: ${IMPORT_REGION} READ:${READ_TPUT} WRITE:${WRITE_TPUT} FILT:${TABLE_FILTER} JDIR:${JSON_OUTPUT_DIR} S3DIR:${S3LOCATION}"

        if [ -n "${SPIKED_THROUGHPUT}" ]; then
          SPIKE_ARG="-s ${SPIKED_THROUGHPUT}"
        fi
        ${STEP_PRODUCER} -a ${APPNAME} -r ${REGION} -i ${IMPORT_REGION} -e ${READ_TPUT} -w ${WRITE_TPUT} -f ${TABLE_FILTER} ${SPIKE_ARG} ${EXCLUDE_ARG} ${JSON_OUTPUT_DIR} ${S3LOCATION}
        RESULT=$?
        if [ $RESULT -eq 0 ]; then
                NEXTPHASE=1
        else
                logMsg "Cluster ERROR: Unable to generate the EMR steps files NAME:${CLUSTER_NAME}"
                RETCODE=3
                NEXTPHASE=0
        fi

        # Get the location of where 'this' backup will be placed in S3
        S3_BACKUP_BASE=$(cat ${JSON_OUTPUT_DIR}/s3path.info)
        logMsg "The S3 base path for this backup is ${S3_BACKUP_BASE}"

        if [ "${S3_BACKUP_BASE}" == "" ]; then
                logMsg "ERROR: No S3 base location for this backup - unable to continue"
                RETCODE=3
                NEXTPHASE=0
        fi
fi

######
## PHASE 6 - Create the EMR cluster (with retries)
######
if [ $NEXTPHASE -eq 1 ]; then
        RETRIES=5
        CURR_ATTEMPT=1

        # we need some status files which are delivered to S3 if the job is running or if it fails.
        # This just creates them - we deliver them to S3 at later steps

        if [ ! -e "${BACKUP_RUNNING_LOCK_LOCAL_FILE}" ]; then
                touch "${BACKUP_RUNNING_LOCK_LOCAL_FILE}"
        fi

        if [ ! -e "${BACKUP_COMPLETE_SUCCESS_LOCK_LOCAL_FILE}" ]; then
                touch "${BACKUP_COMPLETE_SUCCESS_LOCK_LOCAL_FILE}"
        fi

        if [ ! -e "${BACKUP_COMPLETE_FAILED_LOCK_LOCAL_FILE}" ]; then
                touch "${BACKUP_COMPLETE_FAILED_LOCK_LOCAL_FILE}"
        fi

        while [ $CURR_ATTEMPT -le $RETRIES ]
        do
                #double check that cluster isn't really running with one more check
                aws emr list-clusters --cluster-states STARTING BOOTSTRAPPING RUNNING WAITING --region ${REGION} | grep -q ${CLUSTER_NAME}
                STATUS=$?

                if [ $STATUS -eq 0 ]; then
                        # We already have a cluster running - bail
                        logMsg "Cluster ERROR: existing cluster ${CLUSTER_NAME} running"
                        CLUSTERUP=1
                        #set current attemps greater than while condition
                        CURR_ATTEMPT=$[$RETRIES+1]
                else
                        logMsg "No existing EMR cluster with  name ${CLUSTER_NAME} running.  Creating"
                        CLUSTERUP=0

                        # Invoke the aws CLI to create the cluster
                        logMsg "Creating new EMR Cluster NAME:${CLUSTER_NAME} Attempt ${CURR_ATTEMPT} of ${RETRIES}"

                        CLUSTERID=$(aws emr create-cluster --name "${CLUSTER_NAME}"                                        \
                                    --release-label "emr-5.20.0"                                                           \
                                    --service-role "EMR_DefaultRole"                                                       \
                                    --security-configuration "dynamodb-backups"                                            \
                                    --tags Name=${CLUSTER_NAME} signiant:product=devops signiant:email=devops@signiant.com \
                                    --enable-debugging                                                                     \
                                    --log-uri ${S3LOCATION}/emr-logs                                                       \
                                    --configurations file://${JSON_OUTPUT_DIR}/configurations.json                         \
                                    --instance-groups file://${JSON_OUTPUT_DIR}/instance-groups.json                       \
                                    --ec2-attributes file://${JSON_OUTPUT_DIR}/ec2-attributes.json                         \
                                    --steps file://${JSON_OUTPUT_DIR}/exportSteps.json                                     \
                                    --auto-terminate                                                                       \
                                    --visible-to-all-users                                                                 \
                                    --output text                                                                          \
                                    --region ${REGION})

                        logMsg "CLUSTERID for ${CLUSTER_NAME} is $CLUSTERID"
                        # Now use the waiter to make sure the cluster is launched successfully
                        if [ "$CLUSTERID" != "" ]; then
                                logMsg "Waiting for cluster NAME:${CLUSTER_NAME} ID:${CLUSTERID} to start...."
                                aws emr wait cluster-running --cluster-id ${CLUSTERID} --region ${REGION}
                                STATUS=$?

                                if [ $STATUS -eq 0 ]; then
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
                fi
        done

        ####
        ## Phase 6.5 - poll the cluster for status so we know when it's done
        ####
        if [ $CLUSTERUP -eq 1 ]; then
                # We have a cluster provisioned...now we can poll it's tasks and make sure it completes ok

                # First tag the backup as in progress so any downstream processes know not to copy this
                logMsg "Writing BACKUP_RUNNING_LOCK file for this backup"
                aws s3 cp ${BACKUP_RUNNING_LOCK_LOCAL_FILE} ${S3_BACKUP_BASE}/${BACKUP_RUNNING_LOCK_NAME}

                pollCluster $CLUSTERID $CLUSTER_NAME
                STATUS=$?

                if [ $STATUS -eq 0 ]; then
                        logMsg "Cluster SUCCESS NAME:${CLUSTER_NAME} ID:${CLUSTERID}"

                        # Copy the steps json files to S3 so we have a copy for 'this' job
                        if [ "${S3_BACKUP_BASE}" != "" ]; then
                                logMsg "Copying steps files to S3"
                                aws s3 cp ${JSON_OUTPUT_DIR}/exportSteps.json ${S3_BACKUP_BASE}/exportSteps.json
                                aws s3 cp ${JSON_OUTPUT_DIR}/importSteps.json ${S3_BACKUP_BASE}/importSteps.json

                                logMsg "Removing the BACKUP_RUNNING_LOCK file for this backup"
                                aws s3 rm ${S3_BACKUP_BASE}/${BACKUP_RUNNING_LOCK_NAME}

                                logMsg "Writing the BACKUP_COMPLETE_SUCCESS file for this backup"
                                aws s3 cp ${BACKUP_COMPLETE_SUCCESS_LOCK_LOCAL_FILE} ${S3_BACKUP_BASE}/${BACKUP_COMPLETE_SUCCESS_LOCK_NAME}
                        else
                                logMsg "No S3 base location for this backup specified - unable to copy steps files to S3"
                        fi

                        RETCODE=0
                else
                        logMsg "Cluster ERROR:task failure NAME:${CLUSTER_NAME} ID:${CLUSTERID}"

                        logMsg "Removing the BACKUP_RUNNING_LOCK file for this backup"
                        aws s3 rm ${S3_BACKUP_BASE}/${BACKUP_RUNNING_LOCK_NAME}

                        logMsg "Writing the BACKUP_COMPLETE_FAILED file for this backup"
                        aws s3 cp ${BACKUP_COMPLETE_FAILED_LOCK_LOCAL_FILE} ${S3_BACKUP_BASE}/${BACKUP_COMPLETE_FAILED_LOCK_NAME}

                        RETCODE=4
                fi
        else
                logMsg "Unable to provision a new cluster after ${RETRIES} attempts"
                RETCODE=6
        fi

fi

exit ${RETCODE}
