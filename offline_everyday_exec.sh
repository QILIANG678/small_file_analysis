export PATH=/app/hadoop/hive.2.2/apache-hive-2.2.0-bin/bin:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/usr/java/jdk1.8.0_161/bin:/app/scala-2.10.4/bin:/opt/vertica/bin:/app/group1//bin

#!/bin/bash
cd /app/group1/yx_analyse_fsimage
source ./config_env.sh
source ./offline_fsimage.sh > ./log/${DAY_NO}.log 2>&1 &