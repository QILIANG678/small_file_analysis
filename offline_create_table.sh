#!/bin/bash
source ./config-env.sh
TMP_DIR=/tmp/${DB_NAME}/fsimage
impala-shell -i $IMPALAD -q "CREATE DATABASE IF NOT EXISTS ${DB_NAME}"
source ./sqoop_hive_metadata.sh
impala-shell -i $IMPALAD --var=DB_NAME=${DB_NAME} --var=TMP_DIR=${TMP_DIR} -f ./base_sql/create_table.sql
