#!/bin/bash

echo "=========================================="
echo "HADOOP PRACTICE PROJECT - COMPLETE WORKFLOW"
echo "=========================================="

echo -e "\n[1/8] Starting Hadoop cluster..."
docker-compose up -d

echo -e "\n[2/8] Waiting for services to be ready..."
sleep 30

echo -e "\n[3/8] Generating sample data..."
docker exec namenode python3 /scripts/generate_data.py

echo -e "\n[4/8] Creating HDFS directories..."
docker exec namenode hdfs dfs -mkdir -p /user/data
docker exec namenode hdfs dfs -mkdir -p /user/output
docker exec namenode hdfs dfs -mkdir -p /user/hive/warehouse
docker exec namenode hdfs dfs -mkdir -p /user/delta

echo -e "\n[5/8] Uploading data to HDFS..."
docker exec namenode hdfs dfs -put /data/transactions.csv /user/data/
docker exec namenode hdfs dfs -put /data/transactions_sample.json /user/data/

echo -e "\n[6/8] Running MapReduce job..."
docker exec namenode bash -c "hdfs dfs -cat /user/data/transactions.csv | python3 /scripts/mapper.py | sort | python3 /scripts/reducer.py > /data/mapreduce_output.txt"

echo -e "\n[7/8] Setting up Hive tables..."
docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /scripts/hive_setup.sql

echo -e "\n[8/8] Running Spark analytics..."
docker exec spark-master spark-submit \
    --master yarn \
    --deploy-mode client \
    --executor-memory 2g \
    --executor-cores 2 \
    --num-executors 2 \
    /jobs/spark_analytics.py

echo -e "\n=========================================="
echo "WORKFLOW COMPLETED SUCCESSFULLY!"
echo "=========================================="

echo -e "\nAccess points:"
echo "- HDFS NameNode: http://localhost:9870"
echo "- YARN ResourceManager: http://localhost:8088"
echo "- Spark Master: http://localhost:8080"
echo "- MapReduce History: http://localhost:8188"

echo -e "\nTo stop the cluster: docker-compose down"
echo "To remove all data: docker-compose down -v"