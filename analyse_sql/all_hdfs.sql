use ${VAR:DB_NAME};

--统计用户的所有文件数、文件大小、block数量及平均文件大小
insert overwrite metrics_user_files partition(dayno="${VAR:DAY_NO}")
select n.username,
		n.file_nums,
		n.blockcounts,
       round((n.filesizes/1048576),10) filesizes_MB,
       round((n.filesizes/n.file_nums/1048576),10) AS avg_filesize_MB,
	   m.sf_file_nums,
	   m.sf_blockcounts,
	   round((m.sf_filesizes/1048576),10) sf_filesizes_MB,
       round((m.sf_filesizes/m.sf_file_nums/1048576),10) AS sf_avg_filesize_MB
from(
	select a.username,
	count(1) file_nums,
	sum(a.blockcount) blockcounts,
	sum(a.filesize) filesizes 
	from (select * from HDFS_META_FILES where dayno="${VAR:DAY_NO}") a
	group by a.username) n left join 
	(select b.username,
	count(1) sf_file_nums,
	sum(b.blockcount) sf_blockcounts,
	sum(b.filesize) sf_filesizes 
	from (select * from HDFS_META_FILES where dayno="${VAR:DAY_NO}" and FILESIZE < ${VAR:SF_VALUE}) b
	group by b.username) m on n.username=m.username;
--统计一二三四五级HDFS目录下所有目录的文件数、文件大小、block数量及平均文件大小
insert overwrite metrics_hdfs_dir partition(dayno="${VAR:DAY_NO}")
SELECT a.*,
       b.file_nums,
       b.blockcounts,
       round((b.filesizes/1048576),10) filesizes_MB,
       round((b.filesizes/b.file_nums/1048576),10) AS avg_filesize_MB,
	   m.sf_file_nums,
	   m.sf_blockcounts,
	   round((m.sf_filesizes/1048576),10) sf_filesizes_MB,
       round((m.sf_filesizes/m.sf_file_nums/1048576),10) AS sf_avg_filesize_MB
FROM
  (SELECT c.dir_level,
          c.path,
          c.username,
          c.modification_time
   FROM (select * from hdfs_meta_dirs where dayno="${VAR:DAY_NO}") c
   WHERE dir_level=1) a
left JOIN
  (SELECT strleft(path, instr(path,'/',1,2)-1) basepath,
          sum(d.blockcount) blockcounts,
          sum(d.filesize) filesizes,
          count(*) file_nums
   FROM (select * from HDFS_META_FILES where dayno="${VAR:DAY_NO}") d
   GROUP BY basepath) b ON a.path=b.basepath
left JOIN 
   (SELECT strleft(path, instr(path,'/',1,2)-1) basepath,
          sum(n.blockcount) sf_blockcounts,
          sum(n.filesize) sf_filesizes,
          count(*) sf_file_nums
   FROM (select * from HDFS_META_FILES where dayno="${VAR:DAY_NO}" and filesize < ${VAR:SF_VALUE}) n
   GROUP BY basepath) m
ON  a.path=m.basepath
UNION ALL
SELECT a.*,
       b.file_nums,
       b.blockcounts,
       round((b.filesizes/1048576),10) filesizes_MB,
       round((b.filesizes/b.file_nums/1048576),10) AS avg_filesize_MB,
	   m.sf_file_nums,
	   m.sf_blockcounts,
	   round((m.sf_filesizes/1048576),10) sf_filesizes_MB,
       round((m.sf_filesizes/m.sf_file_nums/1048576),10) AS sf_avg_filesize_MB
FROM
  (SELECT c.dir_level,
          c.path,
          c.username,
          c.modification_time
   FROM (select * from hdfs_meta_dirs where dayno="${VAR:DAY_NO}") c
   WHERE dir_level=2) a
left JOIN
  (SELECT strleft(path, instr(path,'/',1,3)-1) basepath,
          sum(d.blockcount) blockcounts,
          sum(d.filesize) filesizes,
          count(*) file_nums
   FROM (select * from HDFS_META_FILES where dayno="${VAR:DAY_NO}") d
   GROUP BY basepath) b ON a.path=b.basepath
left JOIN 
   (SELECT strleft(path, instr(path,'/',1,3)-1) basepath,
          sum(n.blockcount) sf_blockcounts,
          sum(n.filesize) sf_filesizes,
          count(*) sf_file_nums
   FROM (select * from HDFS_META_FILES where dayno="${VAR:DAY_NO}" and filesize < ${VAR:SF_VALUE}) n
   GROUP BY basepath) m
ON  a.path=m.basepath
UNION ALL
SELECT a.*,
       b.file_nums,
       b.blockcounts,
       round((b.filesizes/1048576),10) filesizes_MB,
       round((b.filesizes/b.file_nums/1048576),10) AS avg_filesize_MB,
	   m.sf_file_nums,
	   m.sf_blockcounts,
	   round((m.sf_filesizes/1048576),10) sf_filesizes_MB,
       round((m.sf_filesizes/m.sf_file_nums/1048576),10) AS sf_avg_filesize_MB
FROM
  (SELECT c.dir_level,
          c.path,
          c.username,
          c.modification_time
   FROM (select * from hdfs_meta_dirs where dayno="${VAR:DAY_NO}") c
   WHERE dir_level=3) a
left JOIN
  (SELECT strleft(path, instr(path,'/',1,4)-1) basepath,
          sum(d.blockcount) blockcounts,
          sum(d.filesize) filesizes,
          count(*) file_nums
   FROM (select * from HDFS_META_FILES where dayno="${VAR:DAY_NO}") d
   GROUP BY basepath) b ON a.path=b.basepath
left JOIN 
   (SELECT strleft(path, instr(path,'/',1,4)-1) basepath,
          sum(n.blockcount) sf_blockcounts,
          sum(n.filesize) sf_filesizes,
          count(*) sf_file_nums
   FROM (select * from HDFS_META_FILES where dayno="${VAR:DAY_NO}" and filesize < ${VAR:SF_VALUE}) n
   GROUP BY basepath) m
ON  a.path=m.basepath
UNION ALL
SELECT a.*,
       b.file_nums,
       b.blockcounts,
       round((b.filesizes/1048576),10) filesizes_MB,
       round((b.filesizes/b.file_nums/1048576),10) AS avg_filesize_MB,
	   m.sf_file_nums,
	   m.sf_blockcounts,
	   round((m.sf_filesizes/1048576),10) sf_filesizes_MB,
       round((m.sf_filesizes/m.sf_file_nums/1048576),10) AS sf_avg_filesize_MB
FROM
  (SELECT c.dir_level,
          c.path,
          c.username,
          c.modification_time
   FROM (select * from hdfs_meta_dirs where dayno="${VAR:DAY_NO}") c
   WHERE dir_level=4) a
left JOIN
  (SELECT strleft(path, instr(path,'/',1,5)-1) basepath,
          sum(d.blockcount) blockcounts,
          sum(d.filesize) filesizes,
          count(*) file_nums
   FROM (select * from HDFS_META_FILES where dayno="${VAR:DAY_NO}") d
   GROUP BY basepath) b ON a.path=b.basepath
left JOIN 
   (SELECT strleft(path, instr(path,'/',1,5)-1) basepath,
          sum(n.blockcount) sf_blockcounts,
          sum(n.filesize) sf_filesizes,
          count(*) sf_file_nums
   FROM (select * from HDFS_META_FILES where dayno="${VAR:DAY_NO}" and filesize < ${VAR:SF_VALUE}) n
   GROUP BY basepath) m
ON  a.path=m.basepath
UNION ALL
SELECT a.*,
       b.file_nums,
       b.blockcounts,
       round((b.filesizes/1048576),10) filesizes_MB,
       round((b.filesizes/b.file_nums/1048576),10) AS avg_filesize_MB,
	   m.sf_file_nums,
	   m.sf_blockcounts,
	   round((m.sf_filesizes/1048576),10) sf_filesizes_MB,
       round((m.sf_filesizes/m.sf_file_nums/1048576),10) AS sf_avg_filesize_MB
FROM
  (SELECT c.dir_level,
          c.path,
          c.username,
          c.modification_time
   FROM (select * from hdfs_meta_dirs where dayno="${VAR:DAY_NO}") c
   WHERE dir_level=5) a
left JOIN
  (SELECT strleft(path, instr(path,'/',1,6)-1) basepath,
          sum(d.blockcount) blockcounts,
          sum(d.filesize) filesizes,
          count(*) file_nums
   FROM (select * from HDFS_META_FILES where dayno="${VAR:DAY_NO}") d
   GROUP BY basepath) b ON a.path=b.basepath
left JOIN 
   (SELECT strleft(path, instr(path,'/',1,6)-1) basepath,
          sum(n.blockcount) sf_blockcounts,
          sum(n.filesize) sf_filesizes,
          count(*) sf_file_nums
   FROM (select * from HDFS_META_FILES where dayno="${VAR:DAY_NO}" and filesize < ${VAR:SF_VALUE}) n
   GROUP BY basepath) m
ON  a.path=m.basepath;

	
--以数据库为单位查询每个库的文件数、文件大小、block数量及平均文件大小
insert overwrite metrics_hive_db partition(dayno="${VAR:DAY_NO}")
SELECT n.dbname,
		n.file_nums,
		n.blockcounts,
       round((n.filesizes/1048576),10) filesizes_MB,
       round((n.filesizes/n.file_nums/1048576),10) AS avg_filesize_MB,
	   m.sf_file_nums,
	   m.sf_blockcounts,
	   round((m.sf_filesizes/1048576),10) sf_filesizes_MB,
       round((m.sf_filesizes/m.sf_file_nums/1048576),10) AS sf_avg_filesize_MB
FROM 
  (SELECT a.dbname,
          count(1) file_nums,
          sum(b.blockcount) blockcounts,
          sum(b.filesize) filesizes
   FROM (select * from hive_table_details where dayno="${VAR:DAY_NO}") a,
        (select * from hdfs_meta_files where dayno="${VAR:DAY_NO}") b
   WHERE a.fid=b.fid
   GROUP BY a.dbname) n
LEFT JOIN 
   (SELECT a.dbname,
          count(1) sf_file_nums,
          sum(b.blockcount) sf_blockcounts,
          sum(b.filesize) sf_filesizes
   FROM (select * from hive_table_details where dayno="${VAR:DAY_NO}") a,
        (select * from hdfs_meta_files where dayno="${VAR:DAY_NO}" and filesize < ${VAR:SF_VALUE}) b
   WHERE a.fid=b.fid 
   GROUP BY a.dbname) m
   ON n.dbname=m.dbname;

--以表为单位统计每个表的文件数、文件大小、block数量及平均文件大小
insert overwrite metrics_hive_table partition(dayno="${VAR:DAY_NO}")
select n.dbname,
		n.tbl_name,
		p.username,
		n.file_nums,
		n.blockcounts,
       round((n.filesizes/1048576),10) filesizes_MB,
       round((n.filesizes/n.file_nums/1048576),10) AS avg_filesize_MB,
	   m.sf_file_nums,
	   m.sf_blockcounts,
	   round((m.sf_filesizes/1048576),10) sf_filesizes_MB,
       round((m.sf_filesizes/m.sf_file_nums/1048576),10) AS sf_avg_filesize_MB
from 
(select 
      a.id,b.dbname,b.tbl_name
   from (select * from hdfs_meta_dirs where dayno="${VAR:DAY_NO}") a 
	  join (select * from hive_table_details where dayno="${VAR:DAY_NO}") b 
	  on a.path=b.tb_path group by a.id,b.dbname,b.tbl_name) q
JOIN	  
(select id,username from hdfs_meta_dirs where dayno="${VAR:DAY_NO}") p
ON p.id=q.id
LEFT JOIN 	  
(SELECT
       a.dbname,a.tbl_name,
       count(1) file_nums,
       sum(b.blockcount) blockcounts,
       sum(b.filesize) filesizes
FROM (select * from hive_table_details where dayno="${VAR:DAY_NO}")  a,
     (select * from hdfs_meta_files where dayno="${VAR:DAY_NO}") b
WHERE a.fid=b.fid
GROUP BY a.dbname,a.tbl_name) n
ON q.dbname=n.dbname and q.tbl_name=n.tbl_name 
LEFT JOIN
(SELECT
       a.dbname,a.tbl_name,
       count(1) sf_file_nums,
       sum(b.blockcount) sf_blockcounts,
       sum(b.filesize) sf_filesizes
FROM (select * from hive_table_details where dayno="${VAR:DAY_NO}")  a,
     (select * from hdfs_meta_files where dayno="${VAR:DAY_NO}" and filesize < ${VAR:SF_VALUE}) b
WHERE a.fid=b.fid
GROUP BY a.dbname,a.tbl_name) m
ON q.dbname=m.dbname and q.tbl_name=m.tbl_name;