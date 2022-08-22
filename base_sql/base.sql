invalidate metadata;
use ${VAR:DB_NAME};

--插入当日分区数据
insert overwrite HDFS_META partition(dayno="${VAR:DAY_NO}")
SELECT PATH,
       REPL,
       concat(MODIFICATION_TIME,':00') MODIFICATION_TIME, 
       concat(ACCESSTIME,':00') ACCESSTIME, 
       PREFERREDBLOCKSIZE,
       BLOCKCOUNT,
       FILESIZE,
       NSQUOTA,
       DSQUOTA,
       PERMISSION,
       USERNAME,
       GROUPNAME
FROM HDFS_META_TEMP WHERE dayno="${VAR:DAY_NO}";

--将HDFS_META表中的数据目录存放在HDFS_META_DIRS表中并生成自增ID
insert overwrite HDFS_META_DIRS partition(dayno="${VAR:DAY_NO}")
SELECT row_number() over (ORDER BY path ASC) AS id,
        PATH ,
		CASE 
		    WHEN path = "/" THEN 0
			ELSE length(path)-length(replace(path,"/",""))
		    END AS DIR_LEVEL,
		REPL ,
		MODIFICATION_TIME,
		ACCESSTIME ,
		PREFERREDBLOCKSIZE  ,
		BLOCKCOUNT ,
		FILESIZE ,
		NSQUOTA,
		DSQUOTA ,
		PERMISSION  ,
		USERNAME ,
		GROUPNAME ,
        0 AS parent_id,
        CASE
            WHEN strleft(path, instr(path,'/',-1)-1)='' THEN '/' ELSE strleft(path, instr(path,'/',-1)-1) END AS ppath
FROM HDFS_META
WHERE dayno="${VAR:DAY_NO}" and repl=0 and permission LIKE 'd%';

--建立HDFS数据目录的子父级关系
insert overwrite HDFS_META_DIRS partition(dayno="${VAR:DAY_NO}")
select
    a.id,
    a.PATH,
	a.dir_level,
    a.REPL,
    a.MODIFICATION_TIME,
    a.ACCESSTIME,
    a.PREFERREDBLOCKSIZE,
    a.BLOCKCOUNT,
    a.FILESIZE,
    a.NSQUOTA,
    a.DSQUOTA,
    a.PERMISSION,
    a.USERNAME,
    a.GROUPNAME,
    b.id as parent_id,
    b.path ppath
from HDFS_META_DIRS a, HDFS_META_DIRS b where a.dayno="${VAR:DAY_NO}" and b.dayno="${VAR:DAY_NO}" and a.ppath=b.path;
--进行表统计分析, COMPUTE INCREMENTAL STATS为增量方式
COMPUTE STATS HDFS_META_DIRS;


--将HDFS_META表中的数据文件存放在HDFS_META_FILES表中并生成自增ID
insert overwrite HDFS_META_FILES partition(dayno="${VAR:DAY_NO}")
SELECT row_number() over (ORDER BY path ASC) AS fid,
        PATH ,
		REPL ,
		MODIFICATION_TIME ,
		ACCESSTIME ,
		PREFERREDBLOCKSIZE ,
		BLOCKCOUNT ,
		FILESIZE ,
		NSQUOTA ,
		DSQUOTA ,
		PERMISSION ,
		USERNAME ,
		GROUPNAME,
        0 AS parent_id,
        strleft(path, instr(path,'/',-1)-1) ppath,
        strright(path, length(path)-instr(path,'/',-1)) filename
FROM HDFS_META
WHERE dayno="${VAR:DAY_NO}" and repl>0
  AND permission LIKE '-%';

ALTER TABLE HDFS_META_FILES CHANGE parent_id parent_id bigint;

--建立数据文件与数据目录的子父级关系
insert overwrite HDFS_META_FILES partition(dayno="${VAR:DAY_NO}")
select
   b.fid,
   b.PATH,
   b.REPL,
   b.MODIFICATION_TIME,
   b.ACCESSTIME,
   b.PREFERREDBLOCKSIZE,
   b.BLOCKCOUNT,
   b.FILESIZE,
   b.NSQUOTA,
   b.DSQUOTA,
   b.PERMISSION,
   b.USERNAME,
   b.GROUPNAME,
   a.id as parent_id,
   b.ppath,
   b.filename
from HDFS_META_DIRS a,HDFS_META_FILES b where a.dayno="${VAR:DAY_NO}" and b.dayno="${VAR:DAY_NO}" and a.path=b.ppath;

--进行表统计分析, COMPUTE INCREMENTAL STATS为增量方式
COMPUTE STATS HDFS_META_FILES;

--将hive_tables_temp表转换为Impala Parquet格式表
insert overwrite hive_tables partition(dayno="${VAR:DAY_NO}") 
SELECT  name,       
		db_location_uri, 
		tbl_name,        
		owner,          
		tbl_type,       
		`location`        	
FROM hive_tables_temp where dayno="${VAR:DAY_NO}";
--进行表统计分析, COMPUTE INCREMENTAL STATS为增量方式
COMPUTE STATS hive_tables;

--插入数据文件对应的
insert overwrite hive_table_details partition(dayno="${VAR:DAY_NO}")
SELECT d.dbname,
       d.db_path,
       d.tbl_name,
       d.tb_path,
       d.id,
       c.fid
FROM
  (SELECT CASE
              WHEN instr(ppath,'=')>1 THEN strleft(ppath, instr(ppath,'/',-1)-1)
              ELSE ppath
          END AS tb_path,
          fid,
          filename,
          filesize,
          blockcount
   FROM  hdfs_meta_files where dayno="${VAR:DAY_NO}") c,

  (SELECT a.id,
          b.tbl_name,
          b.tb_path,
          b.name dbname,
          b.db_path
   FROM 
       (select * from hdfs_meta_dirs  where dayno="${VAR:DAY_NO}" )a
   JOIN
     (SELECT name,
             tbl_name,
             substr(`location`,instr(`location`,'/',1,3), length(`location`)) tb_path,
             substr(db_location_uri,instr(db_location_uri,'/',1,3), length(db_location_uri)) db_path
      FROM hive_tables where dayno="${VAR:DAY_NO}") b ON a.path=b.tb_path) d
WHERE c.tb_path=d.tb_path;

--进行表统计分析, COMPUTE INCREMENTAL STATS为增量方式
COMPUTE STATS hive_table_details;
