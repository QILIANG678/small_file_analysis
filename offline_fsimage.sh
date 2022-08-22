#!/bin/bash

#加载基本配置信息
source ./config-env.sh

#将HDFS的FsImage导出
mkdir -p ./tmp_meta
rm -rf ./tmp_meta/*
hdfs dfsadmin -fetchImage ./tmp_meta

if [ $? -ne 0 ];then
  echo "获取FsImage失败....."
  exit
fi
echo "获取FsImage成功....."
#使用hdfs提供的oiv解析FsImage数据文件，将fsimage转换为csv格式数据
hdfs oiv -i ./tmp_meta/* -o ./tmp_meta/fsimage.csv -p Delimited

#将生成的csv文件头去掉，并上传至HDFS的/tmp目录
sed -i -e "1d" ./tmp_meta/fsimage.csv
TMP_DIR=/tmp/${DB_NAME}/fsimage
hdfs dfs -rmr  ${TMP_DIR}/dayno=${DAY_NO}
hdfs dfs -mkdir -p ${TMP_DIR}/dayno=${DAY_NO}

echo "创建HDFS分区目录[${TMP_DIR}/dayno=${DAY_NO}]"
hdfs dfs -copyFromLocal ./tmp_meta/fsimage.csv ${TMP_DIR}/dayno=${DAY_NO}

if [ $? -ne 0 ];then
  echo "上传fsimage.csv文件失败....."
  exit
fi
echo "上传[./tmp_meta/fsimage.csv]至[${TMP_DIR}/dayno=${DAY_NO}]成功"

#修改分区文件夹权限
hdfs dfs -chmod -R 755 ${TMP_DIR}/dayno=${DAY_NO}
if [ $? -ne 0 ];then
  echo "修改[${TMP_DIR}/dayno=${DAY_NO}]权限失败....."
  exit
fi
echo "修改[${TMP_DIR}/dayno=${DAY_NO}]权限成功....."

#添加分区
impala-shell -i $IMPALAD -q "ALTER TABLE hdfs_metadata.hdfs_meta_temp ADD PARTITION(dayno=\"${DAY_NO}\")"

hdfs dfs -ls ${TMP_DIR}/dayno=${DAY_NO}

#抽取MySQL中Hive元数据信息到Hive仓库
source ./sqoop_hive_metadata.sh
if [ $? -ne 0 ];then
  echo "Sqoop抽取hive库表元数据失败....."
  exit
fi
echo "Sqoop抽取hive库表元数据成功....."
#解析ods层数据
impala-shell -i $IMPALAD --var=DB_NAME=${DB_NAME}  --var=DAY_NO=${DAY_NO} -f ./base_sql/base.sql
#分析指标数据
source ./offline_analyse.sh