#!/bin/sh

SCO_CATALOG_FILENAME=$1
HDFS_MOVE="hdfs dfs -moveFromLocal -f"
EDL_LANDING_DIR="/landing1/gtb/nh" #make this $5
PREVIOUS_DAY=$2
MAX_ACTIVE_BACKGROUND_PROCESSES=10
COUNT=0
D_JOB_CMD="/opt/IBM/scripts/dsjob_tidal.sh"
Group_DS_PRJ=$4
FILE_DATE="file_date"
LOG_FILE=$3
CURRENT_COMPLETION_COUNT=0
TMP_CURRENT_COMPLETION_COUNT=0

touch $LOG_FILE
echo "Starting CMS-M jobs" >> $LOG_FILE

while read -r LINE
do
	TABLE_NAME=`echo $LINE | cut -d' ' -f1`
	IS_INCLUDED=`echo $LINE | cut -d' ' -f3`
	OFZ_FOLDER=`echo $LINE | cut -d' ' -f2`
	#echo "$TABLE_NAME is $IS_INCLUDED"
	FILENAME_PATTERN=`echo "CMS.$TABLE_NAME.$PREVIOUS_DAY.out"`
	LANDING_FILE=`echo "$EDL_LANDING_DIR/$FILENAME_PATTERN"`

	if [ "$IS_INCLUDED" = "PROD" ]
	then
		sh ./test.sh ${LANDING_FILE} ${OFZ_FOLDER} ${LOG_FILE}
		RETN_CODE=$?
		if [ "$RETN_CODE" = 1 ]; then
			exit 1
		fi
		#`$HDFS_MOVE $EDL_LANDING_DIR/$FILENAME_PATTERN $OFZ_FOLDER/ >> $LOG_FILE` &
		COUNT=`expr $COUNT + 1`
	fi

	ACTIVE_BACKGROUD_PROCESSES=`ps aux | grep ".*${EDL_LANDING_DIR}.*${PREVIOUS_DAY}.*" | wc -l`

	while [ $ACTIVE_BACKGROUD_PROCESSES -gt $MAX_ACTIVE_BACKGROUND_PROCESSES ]
	do
		ACTIVE_BACKGROUD_PROCESSES=`ps aux | grep ".*${EDL_LANDING_DIR}.*${PREVIOUS_DAY}.*" | wc -l`
	done


done < $SCO_CATALOG_FILENAME

ACTIVE_BACKGROUD_PROCESSES=`ps aux | grep ".*${EDL_LANDING_DIR}.*${PREVIOUS_DAY}.*" | wc -l`
while [ $ACTIVE_BACKGROUD_PROCESSES -gt 4 ]
        do
                ACTIVE_BACKGROUD_PROCESSES=`ps aux | grep ".*${EDL_LANDING_DIR}.*${PREVIOUS_DAY}.*" | wc -l`
        done


echo "Starting CMS-D jobs" >> $LOG_FILE

MAX_ACTIVE_BACKGROUND_PROCESSES=30

while read -r LINE
do
	TABLE_NAME=`echo $LINE | cut -d' ' -f1 | tr '[:upper:]' '[:lower:]'`
	IS_INCLUDED=`echo $LINE | cut -d' ' -f3`
	FILENAME_PATTERN=`echo "sqCms_${TABLE_NAME}_tsz"`


	if [ "$IS_INCLUDED" = "PROD" ]
	then
		`${D_JOB_CMD} ${Group_DS_PRJ} ${FILENAME_PATTERN} ${FILE_DATE} ${PREVIOUS_DAY} >> $LOG_FILE` &
	fi

	ACTIVE_BACKGROUD_PROCESSES=`ps aux | grep ".*${Group_DS_PRJ}.*${PREVIOUS_DAY}.*" | wc -l`
	while [ ${ACTIVE_BACKGROUD_PROCESSES} -gt ${MAX_ACTIVE_BACKGROUND_PROCESSES} ]
	do
		ACTIVE_BACKGROUD_PROCESSES=`ps aux | grep ".*${Group_DS_PRJ}.*${PREVIOUS_DAY}.*" | wc -l`
		TMP_COMPLETION_COUNT=`grep "Job completed normally" $LOG_FILE | wc -l`

		if [ ${TMP_COMPLETION_COUNT} -gt ${CURRENT_COMPLETION_COUNT} ]
		then
			CURRENT_COMPLETION_COUNT=$TMP_COMPLETION_COUNT
			echo "$CURRENT_COMPLETION_COUNT out of $COUNT completed" >> $LOG_FILE
		fi
	done


done < $SCO_CATALOG_FILENAME

ACTIVE_BACKGROUD_PROCESSES=`ps aux | grep ".*${Group_DS_PRJ}.*${PREVIOUS_DAY}.*" | wc -l`
while [ ${ACTIVE_BACKGROUD_PROCESSES} -gt 4 ]
do
       ACTIVE_BACKGROUD_PROCESSES=`ps aux | grep ".*${Group_DS_PRJ}.*${PREVIOUS_DAY}.*" | wc -l`

	TMP_COMPLETION_COUNT=`grep "Job completed normally" $LOG_FILE | wc -l`

                if [ ${TMP_COMPLETION_COUNT} -gt ${CURRENT_COMPLETION_COUNT} ]
		then
                        CURRENT_COMPLETION_COUNT=$TMP_COMPLETION_COUNT
                        echo "$CURRENT_COMPLETION_COUNT out of $COUNT completed" >> $LOG_FILE
                fi


done

TMP_COMPLETION_COUNT=`grep "Job completed normally" $LOG_FILE | wc -l`
echo "$CURRENT_COMPLETION_COUNT out of $COUNT completed" >> $LOG_FILE
echo "CMS-D jobs completed" >> $LOG_FILE
