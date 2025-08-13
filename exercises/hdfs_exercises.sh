#!/bin/bash

echo "==============================================" 
echo "HDFS LEARNING EXERCISES - Hands-on Practice"
echo "=============================================="
echo ""

# Exercise 1: Basic HDFS Operations
echo "📁 Exercise 1: Basic HDFS File Operations"
echo "-----------------------------------------"
echo "Creating test files and directories..."

# Create local test files
sudo docker exec namenode bash -c "
echo 'Sample data for HDFS testing' > /tmp/test1.txt
echo 'Line 1' >> /tmp/test1.txt
echo 'Line 2' >> /tmp/test1.txt
echo 'Line 3' >> /tmp/test1.txt
"

# Upload to HDFS
echo "1. Uploading file to HDFS..."
sudo docker exec namenode hdfs dfs -put /tmp/test1.txt /user/test1.txt

echo "2. Creating HDFS directory structure..."
sudo docker exec namenode hdfs dfs -mkdir -p /user/practice/data
sudo docker exec namenode hdfs dfs -mkdir -p /user/practice/backup

echo "3. Listing HDFS contents..."
sudo docker exec namenode hdfs dfs -ls -R /user/practice/

echo ""
echo "📊 Exercise 2: File Replication"
echo "--------------------------------"
echo "4. Checking file replication factor..."
sudo docker exec namenode hdfs dfs -stat %r /user/test1.txt

echo "5. Changing replication factor to 2..."
sudo docker exec namenode hdfs dfs -setrep 2 /user/test1.txt

echo "6. Verifying replication change..."
sudo docker exec namenode hdfs dfs -ls /user/test1.txt

echo ""
echo "📋 Exercise 3: File Operations"
echo "------------------------------"
echo "7. Copying files within HDFS..."
sudo docker exec namenode hdfs dfs -cp /user/test1.txt /user/practice/data/

echo "8. Moving files in HDFS..."
sudo docker exec namenode hdfs dfs -mv /user/practice/data/test1.txt /user/practice/backup/test1_backup.txt

echo "9. Reading file content from HDFS..."
sudo docker exec namenode hdfs dfs -cat /user/practice/backup/test1_backup.txt

echo ""
echo "📈 Exercise 4: HDFS Statistics"
echo "------------------------------"
echo "10. Getting file statistics..."
sudo docker exec namenode hdfs dfs -du -h /user/

echo "11. Checking HDFS filesystem stats..."
sudo docker exec namenode hdfs dfsadmin -report | head -15

echo ""
echo "🔍 Exercise 5: Advanced Operations"
echo "-----------------------------------"
echo "12. Creating multiple files for testing..."
sudo docker exec namenode bash -c "
for i in {1..5}; do
    echo 'File number '\$i > /tmp/file\$i.txt
    hdfs dfs -put /tmp/file\$i.txt /user/practice/data/
done
"

echo "13. Using wildcards to list files..."
sudo docker exec namenode hdfs dfs -ls /user/practice/data/file*.txt

echo "14. Downloading files from HDFS to local..."
sudo docker exec namenode hdfs dfs -get /user/practice/backup/test1_backup.txt /tmp/downloaded.txt
sudo docker exec namenode cat /tmp/downloaded.txt

echo ""
echo "✅ HDFS Exercises Complete!"
echo ""
echo "What you learned:"
echo "- Upload/download files to/from HDFS"
echo "- Create and manage directories"
echo "- Set and verify replication factors"
echo "- Copy and move files within HDFS"
echo "- Use wildcards and check statistics"