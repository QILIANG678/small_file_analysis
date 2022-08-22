#!/bin/bash
source ./config-env.sh
SF_VALUE=${SMALL_FILE_VALUE}*1048576
impala-shell -i $IMPALAD --var=DB_NAME=${DB_NAME} --var=DAY_NO=${DAY_NO} --var=SF_VALUE=${SF_VALUE} -f ./analyse_sql/all_hdfs.sql