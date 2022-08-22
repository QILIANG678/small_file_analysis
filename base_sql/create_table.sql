invalidate metadata;

CREATE DATABASE IF NOT EXISTS ${VAR:DB_NAME};
use ${VAR:DB_NAME};

--创建FsImage临时表-ODS层
DROP TABLE IF EXISTS HDFS_META_TEMP;
CREATE EXTERNAL TABLE HDFS_META_TEMP (
 PATH STRING,
 REPL INT,
 MODIFICATION_TIME STRING,
 ACCESSTIME STRING,
 PREFERREDBLOCKSIZE INT,
 BLOCKCOUNT BIGINT,
 FILESIZE double,
 NSQUOTA INT,
 DSQUOTA INT,
 PERMISSION STRING,
 USERNAME STRING,
 GROUPNAME STRING)
partitioned by (dayno string)
row format delimited
fields terminated by '\t'
LOCATION '${VAR:TMP_DIR}';


--将临时表转换为Impala的Parquet表
DROP TABLE IF EXISTS HDFS_META;
CREATE TABLE HDFS_META LIKE HDFS_META_TEMP STORED AS PARQUET; 

--HDFS目录表
DROP TABLE IF EXISTS HDFS_META_DIRS;
CREATE TABLE HDFS_META_DIRS (
 ID BIGINT,
 PATH STRING,
 DIR_LEVEL BIGINT,
 REPL INT,
 MODIFICATION_TIME STRING ,
 ACCESSTIME STRING ,
 PREFERREDBLOCKSIZE INT ,
 BLOCKCOUNT BIGINT,
 FILESIZE DOUBLE ,
 NSQUOTA INT ,
 DSQUOTA INT ,
 PERMISSION STRING ,
 USERNAME STRING ,
 GROUPNAME STRING,
 PARENT_ID BIGINT,
 PPATH STRING)
 partitioned by (dayno string)
 STORED AS PARQUET; 
 
 --HDFS文件表
DROP TABLE IF EXISTS HDFS_META_FILES;
CREATE TABLE HDFS_META_FILES (
 FID BIGINT,
 PATH STRING,
 REPL INT,
 MODIFICATION_TIME STRING ,
 ACCESSTIME STRING ,
 PREFERREDBLOCKSIZE INT ,
 BLOCKCOUNT BIGINT,
 FILESIZE DOUBLE ,
 NSQUOTA INT ,
 DSQUOTA INT ,
 PERMISSION STRING ,
 USERNAME STRING ,
 GROUPNAME STRING,
 PARENT_ID BIGINT,
 PPATH STRING,
 FILENAME STRING)
 partitioned by (dayno string)
 STORED AS PARQUET; 
 
 
--将hive_tables_temp表转换为Impala Parquet格式表
DROP TABLE IF EXISTS hive_tables;
CREATE TABLE hive_tables LIKE hive_tables_temp STORED AS PARQUET;

--hive_table_details表
DROP TABLE IF EXISTS hive_table_details;
CREATE TABLE hive_table_details (
 dbname STRING,
 db_path STRING,
 tbl_name STRING,
 tb_path STRING,
 id BIGINT,
 fid BIGINT)
partitioned by (dayno string)
STORED AS PARQUET;

--指标表
--metrics_user_files 指标：用户的文件数、大小、平均文件大小
DROP TABLE IF EXISTS metrics_user_files;
CREATE TABLE metrics_user_files (
 username STRING,
 file_nums BIGINT,
 blockcounts BIGINT,
 filesizes_MB DOUBLE,
 avg_filesize_MB DOUBLE,
 sf_file_nums BIGINT,
 sf_blockcounts BIGINT,
 sf_filesizes_MB DOUBLE,
 sf_avg_filesize_MB DOUBLE
 )
partitioned by (dayno string)
STORED AS PARQUET;

--metrics_hdfs_dir 指标：HDFS一二三四五级目录的文件数、大小、平均文件大小
DROP TABLE IF EXISTS metrics_hdfs_dir;
CREATE TABLE metrics_hdfs_dir (
 dir_level BIGINT,
 PATH STRING,
 username STRING,
 MODIFICATION_TIME STRING,
 file_nums BIGINT,
 blockcounts BIGINT,
 filesizes_MB DOUBLE,
 avg_filesize_MB DOUBLE,
 sf_file_nums BIGINT,
 sf_blockcounts BIGINT,
 sf_filesizes_MB DOUBLE,
 sf_avg_filesize_MB DOUBLE)
partitioned by (dayno string)
STORED AS PARQUET;

--metrics_hive_db 指标：hive库的文件数、大小、平均文件大小
DROP TABLE IF EXISTS metrics_hive_db;
CREATE TABLE metrics_hive_db (
 dbname STRING ,
 file_nums BIGINT,
 blockcounts BIGINT,
 filesizes_MB DOUBLE,
 avg_filesize_MB DOUBLE,
 sf_file_nums BIGINT,
 sf_blockcounts BIGINT,
 sf_filesizes_MB DOUBLE,
 sf_avg_filesize_MB DOUBLE)
partitioned by (dayno string)
STORED AS PARQUET;

--metrics_hive_table 指标：hive表的文件数、大小、平均文件大小
DROP TABLE IF EXISTS metrics_hive_table;
CREATE TABLE metrics_hive_table (
 dbname STRING ,
 tbl_name STRING ,
 username STRING,
 file_nums BIGINT,
 blockcounts BIGINT,
 filesizes_MB DOUBLE,
 avg_filesize_MB DOUBLE,
 sf_file_nums BIGINT,
 sf_blockcounts BIGINT,
 sf_filesizes_MB DOUBLE,
 sf_avg_filesize_MB DOUBLE)
partitioned by (dayno string)
STORED AS PARQUET;