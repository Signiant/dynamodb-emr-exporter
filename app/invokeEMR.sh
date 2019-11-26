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
SPIKED_THROUGHPUT=$7

NUMBER_OF_CLUSTERS=1
if [ $# -gt 7 ]; then
    NUMBER_OF_CLUSTERS=$8
fi

WRITE_TPUT=0.8		# Used when we generate the Import steps
RETRY_DELAY=60
RUNNING_CHECK_DELAY=30

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
    echo "Usage: invokeEMR app_name emr_cluster_name table_filter read_throughput_percentage json_output_directory S3_location export_region [spiked_throughput] [number_of_clusters]"
}

pollClusters()
{
    CLUSTER_IDS=$1
    CLUSTERS=$2

    CLUSTERS_COMPLETE=()
    for cluster in "${CLUSTERS[@]}"
    do
        CLUSTERS_COMPLETE+=(0)
    done
    ALL_COMPLETE=0
    ERRORS=0

    while [ $ALL_COMPLETE -ne 1 ]
    do
        cluster_number=0
        for cluster in "${CLUSTERS[@]}"
        do
            if [ ${CLUSTERS_COMPLETE[$cluster_number]} -ne 1 ]; then
                # If the cluster is not yet complete
                logMsg "polling cluster NAME:${cluster} ID ${CLUSTER_IDS[$cluster_number]} for status in region ${REGION}"
                CLUSTER_STATUS=$(aws emr describe-cluster --cluster-id ${CLUSTER_IDS[$cluster_number]} --region $REGION --output text --query 'Cluster.Status.State')

                if [ "${CLUSTER_STATUS}" == "TERMINATED" ]; then
                    # We now need to check if there were step errors
                    STEPS_STATUS=$(aws emr describe-cluster --cluster-id ${CLUSTER_IDS[$cluster_number]} --region $REGION --output text --query 'Cluster.Status.StateChangeReason.Message')

                    if [ "${STEPS_STATUS}" == "Steps completed with errors" ]; then
                        EXPORT_FAILS=$(aws emr list-steps --step-states FAILED --cluster-id ${CLUSTER_IDS[$cluster_number]} --region $REGION --output text --query 'Steps[?starts_with(Name, `Export Table:`) == `true`]|[].Name')
                        if [ ! -z "${EXPORT_FAILS}" ]; then
                            ERRORS=1
                            logMsg "Cluster ERROR:task failure NAME:${cluster} ID:${CLUSTER_IDS[$cluster_number]}"
                        else
                            ERRORS=0
                        fi
                    else
                        ERRORS=0
                        logMsg "Cluster SUCCESS NAME:${cluster} ID:${CLUSTER_IDS[$cluster_number]}"
                    fi

                    CLUSTERS_COMPLETE[$cluster_number]=1
                elif [ "${CLUSTER_STATUS}" == "TERMINATED_WITH_ERRORS" ]; then
                    ERRORS=1
                    CLUSTERS_COMPLETE[$cluster_number]=1
                fi
            fi
            cluster_number=$((cluster_number+1))
        done

        # Parse the cluster complete values
        ALL_COMPLETE=1
        for complete in "${CLUSTERS_COMPLETE[@]}"
        do
            if [ $complete -eq 0 ]; then
                ALL_COMPLETE=0
                break
            fi
        done

        if [ $ALL_COMPLETE -eq 0 ]; then
            sleep 10
        fi
    done

    return $ERRORS
}

if [ $# != 6 ] && [ $# != 7 ] && [ $# != 8 ]; then
        usage
        exit 1
fi
logMsg "Starting up"

CLUSTERS=()
if [ $NUMBER_OF_CLUSTERS -gt 1 ]; then
    # CLUSTER_NAMES will be an arry of cluster names using CLUSTER_NAME_01, 02, etc...
    logMsg "Asked to create multiple clusters for this backup"
    #for num in `seq 1 $NUMBER_OF_CLUSTERS`;
    num=1
    while [ $num -le $NUMBER_OF_CLUSTERS ];
    do
        zero_pad_num=`printf "%02d\n" $num;`
        #logMsg "Adding ${CLUSTER_NAME}_${zero_pad_num}"
        CLUSTERS+=("${CLUSTER_NAME}_${zero_pad_num}")
        num=$(($num+1))
    done
else
    # single cluster
    CLUSTERS+=("${CLUSTER_NAME}")
fi

######
## PHASE 1 - See if there are any clusters already runing with our name.  If there are, exit
######
EXISTING_CLUSTERS=0
# Get a list of running clusters
running_clusters=$(aws emr list-clusters --active --region us-east-1 --query 'Clusters[].[Name]' --output text)
for cluster in "${CLUSTERS[@]}"
do
    echo $running_clusters | grep -q ${cluster}
    STATUS=$?
    if [ $STATUS -eq 0 ]; then
        # There is already a running cluster with this name - bail
        logMsg "Cluster ERROR: existing cluster ${cluster} running"
        EXISTING_CLUSTERS=1
        NEXTPHASE=0
        RETCODE=2
        break
    fi
done

if [ $EXISTING_CLUSTERS -eq 0 ]; then
    logMsg "No existing conflicting EMR clusters running.  Creating"
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
            logMsg "ERROR: Unable to upload the update-throughput script to s3, unable to continue"
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
        logMsg "Excludes file found in S3 - downloading ${S3LOCATION}/excludes"
        EXCLUDE_ARG="-x ./excludes"
    fi
fi

######
## PHASE 5 - Generate the steps files
######
if [ $NEXTPHASE -eq 1 ]; then
    # PHASE 2 - Get the EMR steps file for the tables to backup
    logMsg "Generating JSON files (R:${REGION} READ:${READ_TPUT} WRITE:${WRITE_TPUT} FILT:${TABLE_FILTER} CCOUNT:${NUMBER_OF_CLUSTERS} JDIR:${JSON_OUTPUT_DIR} S3DIR:${S3LOCATION}"

    if [ -n "${SPIKED_THROUGHPUT}" ]; then
        SPIKE_ARG="-s ${SPIKED_THROUGHPUT}"
    fi
    ${STEP_PRODUCER} -a ${APPNAME} -r ${REGION} -e ${READ_TPUT} -w ${WRITE_TPUT} -f ${TABLE_FILTER} -c ${NUMBER_OF_CLUSTERS} ${SPIKE_ARG} ${EXCLUDE_ARG} ${JSON_OUTPUT_DIR} ${S3LOCATION}
    RESULT=$?
    if [ $RESULT -eq 0 ]; then
        NEXTPHASE=1
    else
        logMsg "Cluster ERROR: Unable to generate the EMR steps files"
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
## PHASE 6 - Create the EMR cluster(s) (with retries)
######
if [ $NEXTPHASE -eq 1 ]; then
    RETRIES=5
    CHECK_RETRIES=60

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

    # Create and Wait for Clusters to start

    # Initialize statuses
    CLUSTER_ATTEMPT=()
    CLUSTER_CREATED=()
    CLUSTER_RUNNING=()
    CLUSTER_RUNNING_CHECK=()
    for cluster in "${CLUSTERS[@]}"
    do
        CLUSTER_ATTEMPT+=(1)
        CLUSTER_CREATED+=(0)
        CLUSTER_RUNNING+=(0)
        CLUSTER_RUNNING_CHECK+=(0)
    done

    CLUSTER_IDS=()
    TOO_MANY_RETRIES=0
    ALL_STARTED=0
    while [ $ALL_STARTED -ne 1 ] && [ $TOO_MANY_RETRIES -eq 0 ]
    do
        # Create all the clusters
        ALL_CREATED=0
        while [ $ALL_CREATED -ne 1 ]
        do
            cluster_number=0
            for cluster in "${CLUSTERS[@]}"
            do
                zero_pad_cluster_num=`printf "%02d\n" $((cluster_number+1));`
                if [ ${CLUSTER_ATTEMPT[$cluster_number]} -le $RETRIES ]; then
                    if [ ${CLUSTER_CREATED[$cluster_number]} -ne 1 ]; then
                        #double check that cluster isn't really running with one more check
                        running_clusters=$(aws emr list-clusters --cluster-states STARTING BOOTSTRAPPING RUNNING WAITING --region us-east-1 --query 'Clusters[].[Name]' --output text)
                        echo $running_clusters | grep -q ${cluster}
                        STATUS=$?
                        if [ $STATUS -eq 0 ]; then
                            # We already have a cluster running - bail
                            logMsg "Cluster ERROR: existing cluster ${cluster} running"
                            CLUSTER_CREATED[$cluster_number]=0
                            #set current attemps greater than while condition
                            CLUSTER_ATTEMPT[$cluster_number]=$[$RETRIES+1]
                            break
                        else
                            logMsg "No existing EMR cluster with  name ${cluster} running.  Creating"
                            # Invoke the aws CLI to create the cluster
                            logMsg "Creating new EMR Cluster NAME:${cluster} Attempt ${CLUSTER_ATTEMPT[$cluster_number]} of ${RETRIES}"

                            CLUSTERID=$(aws emr create-cluster --name "${cluster}"                                        \
                                        --release-label "emr-5.28.0"                                                           \
                                        --service-role "EMR_DefaultRole"                                                       \
                                        --security-configuration "dynamodb-backups"                                            \
                                        --tags Name=${CLUSTER_NAME} signiant:product=devops signiant:email=devops@signiant.com \
                                        --enable-debugging                                                                     \
                                        --log-uri ${S3LOCATION}/emr-logs                                                       \
                                        --configurations file://${JSON_OUTPUT_DIR}/configurations.json                         \
                                        --instance-groups file://${JSON_OUTPUT_DIR}/instance-groups.json                       \
                                        --ec2-attributes file://${JSON_OUTPUT_DIR}/ec2-attributes.json                         \
                                        --steps file://${JSON_OUTPUT_DIR}/exportSteps_${zero_pad_cluster_num}.json                                     \
                                        --auto-terminate                                                                       \
                                        --visible-to-all-users                                                                 \
                                        --output text                                                                          \
                                        --region ${REGION})

                            logMsg "CLUSTERID for ${cluster} is $CLUSTERID"
                            CLUSTER_IDS[$cluster_number]=$CLUSTERID
                            if [ "$CLUSTERID" != "" ]; then
                                CLUSTER_CREATED[$cluster_number]=1
                            else
                                logMsg "Cluster ERROR: no cluster ID returned NAME:${cluster}"
                                CLUSTER_CREATED[$cluster_number]=0
                                CLUSTER_ATTEMPT[$cluster_number]=$((CLUSTER_ATTEMPT[$cluster_number]+1))
                            fi
                        fi
                    fi
                fi
                cluster_number=$((cluster_number+1))
            done

            # Check the retry count for each cluster
            for attempts in "${CLUSTER_ATTEMPT[@]}"
            do
                if [ $attempts -gt $RETRIES ]; then
                    TOO_MANY_RETRIES=1
                    break
                fi
            done

            # Check to see if we've got a cluster ID for all clusters
            ALL_CREATED=1
            for cluster_id in "${CLUSTER_IDS[@]}"
            do
                if [ "$cluster_id" == "" ]; then
                    ALL_CREATED=0
                fi
            done

            if [ $ALL_CREATED -eq 0 ]; then
                # Wait before trying to create again
                sleep ${RETRY_DELAY}
            fi
        done # All Clusters Created

        if [ $TOO_MANY_RETRIES -eq 0 ]; then
            # Wait for all clusters to start
            cluster_number=0
            for cluster in "${CLUSTERS[@]}"
            do
                if [ ${CLUSTER_CREATED[$cluster_number]} -eq 1 ]; then
                    # Cluster created - check if it's running
                    if [ ${CLUSTER_RUNNING[$cluster_number]} -ne 1 ]; then
                        # Cluster isn't yet running
                        if [ ${CLUSTER_RUNNING_CHECK[$cluster_number]} -le $CHECK_RETRIES ]; then
                            # We haven't exceeded our check retries
                            logMsg "Waiting for cluster NAME:${cluster} ID:${CLUSTER_IDS[$cluster_number]} to start...."
                            CLUSTER_STATE=$(aws emr describe-cluster --cluster-id ${CLUSTER_IDS[$cluster_number]} --query 'Cluster.Status.State' --output text --region ${REGION})

                            if [ "$CLUSTER_STATE" == "RUNNING" ]; then
                                logMsg "Cluster NAME:${cluster} ID:${CLUSTER_IDS[$cluster_number]} launched successfully"
                                CLUSTER_RUNNING[$cluster_number]=1
                            else
                                # Not Running yet - increment CLUSTER_RUNNING_CHECK
                                CLUSTER_RUNNING_CHECK[$cluster_number]=$((CLUSTER_RUNNING_CHECK[$cluster_number]+1))
                                if [[ "$CLUSTER_STATE" == *"TERMINATED"* ]]; then
                                    logMsg "Cluster ERROR: launch failure NAME:${cluster} ID:${CLUSTER_IDS[$cluster_number]} Attempt ${CLUSTER_ATTEMPT[$cluster_number]} of ${RETRIES}"
                                    CLUSTER_CREATED[$cluster_number]=0
                                    CLUSTER_ATTEMPT[$cluster_number]=$((CLUSTER_ATTEMPT[$cluster_number]+1))
                                    CLUSTER_RUNNING[$cluster_number]=0
                                    CLUSTER_RUNNING_CHECK[$cluster_number]=0
                                fi
                            fi
                        fi
                    fi
                fi
                cluster_number=$((cluster_number+1))
            done

            # Check the retry count for each cluster
            for attempts in "${CLUSTER_ATTEMPT[@]}"
            do
                if [ $attempts -gt $RETRIES ]; then
                    TOO_MANY_RETRIES=1
                    break
                fi
            done

            # Check the running check retry count for each cluster
            for attempts in "${CLUSTER_RUNNING_CHECK[@]}"
            do
                if [ $attempts -gt $CHECK_RETRIES ]; then
                    TOO_MANY_RETRIES=1
                    break
                fi
            done

            # Check the status for each cluster
            ALL_STARTED=1
            for status in "${CLUSTER_RUNNING[@]}"
            do
                if [ $status -eq 0 ]; then
                    ALL_STARTED=0
                    break
                fi
            done

            if [ $ALL_STARTED -eq 0 ]; then
                logMsg "Delaying ${RUNNING_CHECK_DELAY} seconds before checking cluster(s) status..."
                sleep ${RUNNING_CHECK_DELAY}
            fi
        fi
    done # All Clusters Started

    if [ $ALL_STARTED -eq 1 ]; then
        # All cluster(s) provisioned...now we can poll their tasks
        # First tag the backup as in progress so any downstream processes know not to copy this
        logMsg "Writing BACKUP_RUNNING_LOCK file for this backup"
        aws s3 cp ${BACKUP_RUNNING_LOCK_LOCAL_FILE} ${S3_BACKUP_BASE}/${BACKUP_RUNNING_LOCK_NAME}

        pollClusters $CLUSTER_IDS $CLUSTERS
        STATUS=$?

        if [ $STATUS -eq 0 ]; then
            logMsg "All Clusters SUCCESS"

            # Copy the steps json files to S3 so we have a copy for 'this' job
            if [ "${S3_BACKUP_BASE}" != "" ]; then
                    logMsg "Copying steps files to S3"
                    for filename in ${JSON_OUTPUT_DIR}/exportSteps*; do
                        aws s3 cp $filename ${S3_BACKUP_BASE}/
                    done
                    for filename in ${JSON_OUTPUT_DIR}/importSteps*; do
                        aws s3 cp $filename ${S3_BACKUP_BASE}/
                    done

                    logMsg "Removing the BACKUP_RUNNING_LOCK file for this backup"
                    aws s3 rm ${S3_BACKUP_BASE}/${BACKUP_RUNNING_LOCK_NAME}

                    logMsg "Writing the BACKUP_COMPLETE_SUCCESS file for this backup"
                    aws s3 cp ${BACKUP_COMPLETE_SUCCESS_LOCK_LOCAL_FILE} ${S3_BACKUP_BASE}/${BACKUP_COMPLETE_SUCCESS_LOCK_NAME}
            else
                    logMsg "No S3 base location for this backup specified - unable to copy steps files to S3"
            fi

            logMsg "DynamoDB Export SUCCESSFUL for $APPNAME"

            RETCODE=0
        else
            logMsg "Cluster ERROR"

            logMsg "Removing the BACKUP_RUNNING_LOCK file for this backup"
            aws s3 rm ${S3_BACKUP_BASE}/${BACKUP_RUNNING_LOCK_NAME}

            logMsg "Writing the BACKUP_COMPLETE_FAILED file for this backup"
            aws s3 cp ${BACKUP_COMPLETE_FAILED_LOCK_LOCAL_FILE} ${S3_BACKUP_BASE}/${BACKUP_COMPLETE_FAILED_LOCK_NAME}

            logMsg "DynamoDB Export FAILED for $APPNAME"

            RETCODE=4
        fi
    else
        # TODO: Check status of clusters and terminate any that ARE running
        logMsg "Unable to provision new cluster(s) after ${RETRIES} attempts"
        RETCODE=6
    fi
fi

exit ${RETCODE}
