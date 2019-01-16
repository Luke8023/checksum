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

if [[ $@ = "" || $# -lt 3 ]]; then
    help
    exit 1
fi

## process input
LANDING_FILE=$1
OFZ_FOLDER=$2
LOG_FILE=$3
FILE_PATTERN=`ls $LANDING_FILE* | tail -n 1 | tail -c 2`
MARKER_FILE=`echo $(basename ${LANDING_FILE}) | sed "s/.out/_$FILE_PATTERN.mrk/" | cut -d '*' -f 1`
FILE_CNT=`ls $LANDING_FILE* | wc -l`
LANDING_DIR=$(dirname "${LANDING_FILE}")

if [ ! -f "$LANDING_DIR/$MARKER_FILE" ]; then
        echo "$(date): [ERROR] MARKER File Not Found! $LANDING_DIR/$MARKER_FILE"
        exit 1
fi

# verify data files
while IFS== read -r DATA_FILE DATA_FILE_CHKSUM
do
    #echo "checking $DATA_FILE,$DATA_FILE_CHKSUM"

    # check total file number
    if [ $DATA_FILE = "TOTAL" ]
    	then
    	totalFileNum=$DATA_FILE_CHKSUM
    	if [ $FILE_CNT != $totalFileNum ]
    		then
    		echo "$(date): [ERROR] Total file number did not match"
            exit 1
    	else
    		break
    	fi
    fi

    # check file list
    # sleep timer
    if [ ! -f "$LANDING_DIR/$DATA_FILE" ]; then
    	echo "$(date): [ERROR] File Not Found! $LANDING_DIR/$DATA_FILE"
    	exit 1
    else

      #if the file is in ISO-8859-1, we need to translate it into UTF-8 then md5sum
      # delete all the trans files
    sort=`file -i ${DATA_FILE} | grep ".*iso-8859-1.*"`
    if [${sort}]
    then
      convert=`iconv -f ISO-8859-1 -t UTF-8 ${DATA_FILE} > ${DATA_FILE}.translated`
      TRANSLATED_FILE=`echo "${DATA_FILE}.translated"`
      chksum=`md5sum ${TRANSLATED_FILE}| cut -d ' ' -f1`
      echo "Translating ${DATA_FILE}" >> $LOG_FILE
    else
      chksum=`md5sum $DATA_FILE | cut -d ' ' -f1`
    fi
    if [ "$chksum" != "$DATA_FILE_CHKSUM" ]; then
      echo "$(date): [ERROR] Checksum Not Matching! $LANDING_DIR/$DATA_FILE"
      exit 1
    fi
  fi

done <$LANDING_DIR/$MARKER_FILE


delete_trans=`find . -name "*.translated" -type f|xargs rm -f`
echo "Deleting ${DATA_FILE}.translated" >> $LOG_FILE

while IFS== read -r DATA_FILE DATA_FILE_CHKSUM
do
	if [ $DATA_FILE = "TOTAL" ]
		then
		break
	else
	echo "$(date): hdfs dfs -moveFromLocal -f $LANDING_DIR/$DATA_FILE $OFZ_FOLDER"
	`hdfs dfs -moveFromLocal -f $LANDING_DIR/$DATA_FILE $OFZ_FOLDER >> LOG_FILE` &
    echo "$(date): hdfs dfs -moveFromLocal -f $LANDING_DIR/$MARKER_FILE $OFZ_FOLDER"
    ##hdfs dfs -moveFromLocal -f $LANDING_DIR/$MARKER_FILE $OFZ_FOLDER
fi
done <$MARKER_FILE


echo "$(date): done"
exit 0


#md5sum CMS.TNH_SCO_CPA_GROUP_LANG.2018-04-08.out_000_F | cut -d ' ' -f1
##openssl md5 <filename> CMS.TNH_TMPL_REC_REL.2018-04-06_F.mrk
