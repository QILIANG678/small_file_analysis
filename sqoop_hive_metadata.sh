#!/bin/bash

#将Hive元数据库中的库及表等信息抽取到Hive仓库
sqoop import \
--connect "jdbc:mysql://${DB_IPADDR}:${DB_PORT}/${META_DB_NAME}" \
--username ${DB_USERNAME} \
--password ${DB_PASSWORD} \
--query 'select c.NAME,c.DB_LOCATION_URI,a.TBL_NAME,a.OWNER,a.TBL_TYPE,b.LOCATION from TBLS a,SDS b,DBS c where a.SD_ID=b.SD_ID and a.DB_ID=c.DB_ID and $CONDITIONS' \
--fields-terminated-by ',' \
--delete-target-dir \
--hive-database ${DB_NAME} \
--target-dir /tmp/${TARG_HIVE_TB} \
--hive-import \
--hive-overwrite \
--hive-table ${TARG_HIVE_TB} \
--hive-partition-key ${PARTITION_KEY} \
--hive-partition-value ${DAY_NO} \
--m ${MAP_COUNT} 