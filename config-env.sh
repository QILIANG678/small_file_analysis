#!/bin/bash

###定义一些全局的变量
DB_NAME=hdfs_metadata
IMPALAD=10.176.229.24

#sqoop抽数写入Hive表查询
DB_IPADDR=10.176.229.161
DB_PORT=3306
META_DB_NAME=hive
DB_USERNAME=hiveqry
DB_PASSWORD=pOAZR_R41lJ9K

TARG_HIVE_TB=hive_tables_temp
PARTITION_KEY=dayno
MAP_COUNT=1

#DAY_NO=`date "+%Y%m%d"`
DAY_NO=`date -d "1 day ago" +"%y%m%d"`

#设置小于64M的文件为小文件
SMALL_FILE_VALUE=64