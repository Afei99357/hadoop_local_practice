#!/bin/bash

echo "HDFS Data Movement Practice Commands"
echo "====================================="

echo -e "\n1. Create HDFS directories:"
echo "hdfs dfs -mkdir -p /user/data"
echo "hdfs dfs -mkdir -p /user/output"
echo "hdfs dfs -mkdir -p /user/streaming/transactions"
echo "hdfs dfs -mkdir -p /user/hive/warehouse"
echo "hdfs dfs -mkdir -p /user/backup"

echo -e "\n2. Upload data to HDFS:"
echo "hdfs dfs -put /data/transactions.csv /user/data/"
echo "hdfs dfs -put /data/transactions_sample.json /user/data/"

echo -e "\n3. List files in HDFS:"
echo "hdfs dfs -ls /user/data"
echo "hdfs dfs -ls -R /user"

echo -e "\n4. View file contents:"
echo "hdfs dfs -cat /user/data/transactions.csv | head -20"
echo "hdfs dfs -tail /user/data/transactions.csv"

echo -e "\n5. Check file/directory size:"
echo "hdfs dfs -du -h /user/data"
echo "hdfs dfs -df -h /"

echo -e "\n6. Copy files within HDFS:"
echo "hdfs dfs -cp /user/data/transactions.csv /user/backup/"
echo "hdfs dfs -cp /user/data/*.json /user/backup/"

echo -e "\n7. Move files within HDFS:"
echo "hdfs dfs -mv /user/data/old_file.csv /user/archive/"

echo -e "\n8. Download files from HDFS:"
echo "hdfs dfs -get /user/output/results.txt /local/path/"
echo "hdfs dfs -getmerge /user/output/part-* /local/merged_output.txt"

echo -e "\n9. Delete files/directories:"
echo "hdfs dfs -rm /user/temp/file.txt"
echo "hdfs dfs -rm -r /user/temp"
echo "hdfs dfs -rm -skipTrash /user/data/old_file.csv"

echo -e "\n10. Change permissions:"
echo "hdfs dfs -chmod 755 /user/data/transactions.csv"
echo "hdfs dfs -chown hadoop:hadoop /user/data"

echo -e "\n11. Create empty file:"
echo "hdfs dfs -touchz /user/data/marker_file"

echo -e "\n12. Count files and directories:"
echo "hdfs dfs -count /user/data"

echo -e "\n13. Check HDFS health:"
echo "hdfs dfsadmin -report"
echo "hdfs fsck /user/data -files -blocks"

echo -e "\n14. DistCP for large data transfers:"
echo "hadoop distcp /user/source /user/destination"
echo "hadoop distcp hdfs://namenode1:9000/data hdfs://namenode2:9000/data"
echo "hadoop distcp -update -delete /source /destination"

echo -e "\n15. Set replication factor:"
echo "hdfs dfs -setrep -w 3 /user/data/important.csv"

echo -e "\n16. Get file checksum:"
echo "hdfs dfs -checksum /user/data/transactions.csv"

echo -e "\n17. Append to existing file:"
echo "hdfs dfs -appendToFile local_file.txt /user/data/hdfs_file.txt"

echo -e "\n18. Test file existence:"
echo "hdfs dfs -test -e /user/data/transactions.csv && echo 'File exists'"
echo "hdfs dfs -test -d /user/data && echo 'Directory exists'"
echo "hdfs dfs -test -z /user/data/empty_file && echo 'File is empty'"