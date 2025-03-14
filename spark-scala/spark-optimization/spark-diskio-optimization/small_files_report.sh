#!/bin/bash
depth_level=$1
base_path=$2
rm -f count_list final_list.out path_list
touch count_list
export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Xmx5g"
hdfs dfs -ls -R $base_path | awk -F / '/^d/ && (NF=='"$depth_level"')' | awk '{print $8}' > path_list
while IFS='' read -r LINE || [ -n "${LINE}" ]; do
count=$(hdfs dfs -ls -R ${LINE} | awk '{if($5<67108864 && $1 !~ "^d") print $8}' | wc -l)
echo ${LINE},$count >> count_list
done < path_list
cat count_list | awk -v OFS="," '$1=$1' | sort -rt, -nk2 count_list | awk '{print $1}{print $2}' > final_list.csv