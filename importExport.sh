APPNAME=$1
PROFILE=$2
TABLE_FILTER=$3
READ_TPUT=$4
WRITE_CAPACITY=$5
JSON_OUTPUT_DIR=$6
S3LOCATION=$7
SOURCE_REGION=$8
DESTINATION_REGION=$9

EXPORT_CLUSTER_NAME="DynamoDB_Exporter_${TABLE_FILTER}"
IMPORT_CLUSTER_NAME="DynamoDB_Importer_${TABLE_FILTER}"

USAGE="$0 app_name profile table_prefix read_throughput_ratio write_capacity json_output_directory s3_output_location source_region destination_region"
RETCODE=0

if [ $# != 9 ]; then
  echo "Usage: $USAGE"
  RETCODE=1
fi

if [ $RETCODE -eq 0 ]; then
  echo "Running table export"
  ./invokeEMR.sh $APPNAME $EXPORT_CLUSTER_NAME $PROFILE $TABLE_FILTER $READ_TPUT $JSON_OUTPUT_DIR $S3LOCATION $SOURCE_REGION $DESTINATION_REGION $WRITE_CAPACITY
  if [ $? -ne 0 ]; then
    echo "Table export failed"
    RETCODE=1
  else
    echo "Table export complete"
  fi
fi

if [ $RETCODE -eq 0 ]; then
  echo "Running table import"
  ./restoreEMR.sh $APPNAME $IMPORT_CLUSTER_NAME $PROFILE $JSON_OUTPUT_DIR $S3LOCATION $DESTINATION_REGION
  if [ $? -ne 0 ]; then
    echo "Table import failed"
    RETCODE=1
  fi
fi

if [ $RETCODE -eq 0 ]; then
  echo "Deleting s3 backup"
  aws s3 rm --recursive $S3LOCATION/$SOURCE_REGION
fi

exit $RETCODE
