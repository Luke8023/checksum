#!/bin/sh
BASENAME="$( basename "${BASH_SOURCE[0]}" )"
#echo `date` "[INFO] all parameters:" $@

help(){

  echo ":: Usage ::"
  echo "$BASENAME <landing file> <ofz folder>"
  echo "-h or --help displays this message"
}

## parse parameters
## "/landing1/gtb/nh/CMS.ACCT_SUBTYPE.<Group.PrevDay_yyyy_mm_dd>.out*"
## /data/lz/int/ofz/nh/acct_subtype/

if [[ $@ = "" || $# -lt 2 ]]; then
  help
  exit 1
fi

## process input
LANDING_FILE=$1
OFZ_FOLDER=$2

LANDING_DIR=$(dirname "${LANDING_FILE}")
FILE_NAME="$( basename ${LANDING_FILE} )"

LOG_DIRECTORY="$LANDING_DIR/LOGS"
LOG_FILE="${LOG_DIRECTORY}/${FILE_NAME}.`date "+%Y%m%d-%H%M%S"`.log"
FILE_PATTERN=`ls $LANDING_FILE* | tail -n 1 | tail -c 2`
MARKER_FILE=`echo $(basename ${LANDING_FILE}) | sed "s/.out/_$FILE_PATTERN.mrk/" | cut -d '*' -f 1`
FILE_CNT=`ls $LANDING_FILE* | wc -l`
MISSING="FALSE"

#Creating the log directory if its not there
if [ ! -d "$LOG_DIRECTORY" ]; then
  mkdir ${LOG_DIRECTORY}
  chmod 775 ${LOG_DIRECTORY}
fi

#create the log file for this run
touch $LOG_FILE
echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] Starting Job Processing for $FILE_NAME"
echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] Starting Job Processing for $FILE_NAME" >> $LOG_FILE


echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] Checking for $MARKER_FILE"
echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] Checking for $MARKER_FILE" >> $LOG_FILE
if [ ! -f "$LANDING_DIR/$MARKER_FILE" ]; then
  echo "`date "+%Y%m%d-%H:%M:%S"`: [ERROR] MARKER File Not Found! $LANDING_DIR/$MARKER_FILE"
  echo "`date "+%Y%m%d-%H:%M:%S"`: [ERROR] MARKER File Not Found! $LANDING_DIR/$MARKER_FILE" >> $LOG_FILE
  exit 1
fi

echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] $MARKER_FILE found"
echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] $MARKER_FILE found" >> $LOG_FILE

# verify data files
while IFS== read -r DATA_FILE DATA_FILE_CHKSUM
do
  TRANSLATED_FILE=`echo "${DATA_FILE}.translated"`


  # check total file number
  if [ $DATA_FILE = "TOTAL" ]
  then
    echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] Checking for total file number"
    echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] Checking for total file number" >> $LOG_FILE

    totalFileNum=$DATA_FILE_CHKSUM



    if [ $FILE_CNT != $totalFileNum ]
    then
      echo "`date "+%Y%m%d-%H:%M:%S"`: [ERROR] Total file number did not match"
      echo "Actually: $FILE_CNT; Should be: $totalFileNum"
      echo "`date "+%Y%m%d-%H:%M:%S"`: [ERROR] Total file number did not match" >> $LOG_FILE
      echo "Actually: $FILE_CNT; Should be: $totalFileNum" >> $LOG_FILE
      exit 1
    else
      echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] Total file number match"
      echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] Total file number match" >> $LOG_FILE
      break
    fi
  fi

  echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] Check file: $DATA_FILE"
  echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] Check file: $DATA_FILE" >> $LOG_FILE
  # check file list
  if [ ! -f "$LANDING_DIR/$DATA_FILE" ]; then
    echo "`date "+%Y%m%d-%H:%M:%S"`: [ERROR] File Not Found! $LANDING_DIR/$DATA_FILE"
    echo "`date "+%Y%m%d-%H:%M:%S"`: [ERROR] File Not Found! $LANDING_DIR/$DATA_FILE" >> $LOG_FILE
    MISSING="TRUE"
    continue
  else
    echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] file found: $DATA_FILE"
    echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] file found: $DATA_FILE" >> $LOG_FILE

    echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] convert file encoding to UTF-8 and generate file: ${TRANSLATED_FILE}"
    echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] convert file encoding to UTF-8 and generate file: ${TRANSLATED_FILE}" >> $LOG_FILE

    convert=`iconv -f ISO-8859-1 -t UTF-8 $LANDING_DIR/${DATA_FILE} > ${LANDING_DIR}/${TRANSLATED_FILE}`
    chksum=`md5sum ${LANDING_DIR}/${TRANSLATED_FILE}| cut -d ' ' -f1`

    echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] check md5sum: $DATA_FILE"
    echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] check md5sum: $DATA_FILE" >> $LOG_FILE

    if [ "$chksum" != "$DATA_FILE_CHKSUM" ]; then
      echo "`date "+%Y%m%d-%H:%M:%S"`: [ERROR] Checksum Not Matching! $LANDING_DIR/$DATA_FILE"
      echo "`date "+%Y%m%d-%H:%M:%S"`: [ERROR] Checksum Not Matching! $LANDING_DIR/$DATA_FILE" >> $LOG_FILE
      exit 1
    else
      echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] checksum pass: $DATA_FILE"
      echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] checksum pass: $DATA_FILE" >> $LOG_FILE
    fi
  fi

  echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] Delete translated file ${DATA_FILE}.translated"
  echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] Delete translated file ${DATA_FILE}.translated" >> $LOG_FILE

  delete_trans=`find "${LANDING_DIR}/" -name "${DATA_FILE}.translated" -type f|xargs rm -f`

done <$LANDING_DIR/$MARKER_FILE


if [ $MISSING = "TRUE" ]
then
  exit 1
fi

while IFS== read -r DATA_FILE DATA_FILE_CHKSUM
do
  if [ $DATA_FILE = "TOTAL" ]
  then
    break
  else
    echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] Transfer file ${DATA_FILE} to ${OFZ_FOLDER}"
    echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] Transfer file ${DATA_FILE} to ${OFZ_FOLDER}" >> $LOG_FILE

    echo "`date "+%Y%m%d-%H:%M:%S"`: hdfs dfs -moveFromLocal -f $LANDING_DIR/$DATA_FILE $OFZ_FOLDER"
    `hdfs dfs -moveFromLocal -f $LANDING_DIR/$DATA_FILE $OFZ_FOLDER >> LOG_FILE` &
    echo "`date "+%Y%m%d-%H:%M:%S"`: hdfs dfs -moveFromLocal -f $LANDING_DIR/$MARKER_FILE $OFZ_FOLDER"

  fi
done <$LANDING_DIR/$MARKER_FILE

echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] Transfer marker file ${MARKER_FILE} to ${OFZ_FOLDER}"
echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] Transfer marker file ${MARKER_FILE} to ${OFZ_FOLDER}" >> $LOG_FILE

hdfs dfs -moveFromLocal -f $LANDING_DIR/$MARKER_FILE $OFZ_FOLDER

echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] done"
echo "`date "+%Y%m%d-%H:%M:%S"`: [LOG] done" >> $LOG_FILE
exit 0


#md5sum CMS.TNH_SCO_CPA_GROUP_LANG.2018-04-08.out_000_F | cut -d ' ' -f1
##openssl md5 <filename> CMS.TNH_TMPL_REC_REL.2018-04-06_F.mrk
