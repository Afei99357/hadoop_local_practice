#!/bin/bash

echo "=================================================="
echo "HADOOP ECOSYSTEM COMPLETE LEARNING PATH"
echo "=================================================="
echo ""
echo "This script will guide you through all 5 key learning areas"
echo "Make sure your Hadoop cluster is running (./fresh_start.sh)"
echo ""
echo "Press Enter to continue or Ctrl+C to exit..."
read

# Make all scripts executable
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
chmod +x "$SCRIPT_DIR/hdfs_exercises.sh"
chmod +x "$SCRIPT_DIR/mapreduce_exercises.sh"
chmod +x "$SCRIPT_DIR/hive_exercises.sh"
chmod +x "$SCRIPT_DIR/spark_exercises.sh"

echo ""
echo "🚀 Starting Learning Journey..."
echo ""

# Exercise 1: HDFS
echo "=============================="
echo "MODULE 1: HDFS OPERATIONS"
echo "=============================="
echo "Press Enter to start HDFS exercises..."
read
"$SCRIPT_DIR/hdfs_exercises.sh"
echo ""
echo "Press Enter to continue to MapReduce..."
read

# Exercise 2: MapReduce
echo ""
echo "=============================="
echo "MODULE 2: MAPREDUCE PROGRAMMING"
echo "=============================="
echo "Press Enter to start MapReduce exercises..."
read
"$SCRIPT_DIR/mapreduce_exercises.sh"
echo ""
echo "Press Enter to continue to Hive..."
read

# Exercise 3: Hive
echo ""
echo "=============================="
echo "MODULE 3: HIVE OPTIMIZATION"
echo "=============================="
echo "Press Enter to start Hive exercises..."
read
"$SCRIPT_DIR/hive_exercises.sh"
echo ""
echo "Press Enter to continue to Spark..."
read

# Exercise 4: Spark
echo ""
echo "=============================="
echo "MODULE 4: SPARK ON YARN"
echo "=============================="
echo "Press Enter to start Spark exercises..."
read
"$SCRIPT_DIR/spark_exercises.sh"
echo ""

echo "=================================================="
echo "🎉 CONGRATULATIONS!"
echo "=================================================="
echo ""
echo "You've completed all core Hadoop learning exercises!"
echo ""
echo "Summary of what you learned:"
echo "✅ HDFS: File operations, replication, and management"
echo "✅ MapReduce: Data processing patterns and aggregations"
echo "✅ Hive: SQL on Hadoop with optimization techniques"
echo "✅ Spark: Advanced analytics and performance tuning"
echo ""
echo "Next steps:"
echo "1. Try modifying the scripts to create your own analyses"
echo "2. Load your own data and run custom queries"
echo "3. Experiment with different optimization parameters"
echo "4. Scale up with more data nodes"
echo ""
echo "For Delta Lake migration (Exercise 5), run:"
echo "  sudo docker exec spark-master /spark/bin/spark-submit /jobs/migrate_to_delta.py"
echo ""