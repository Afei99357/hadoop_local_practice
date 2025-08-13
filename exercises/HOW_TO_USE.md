# Hadoop Learning Exercises - User Guide

## 📚 Overview

This folder contains hands-on exercises for learning the Hadoop ecosystem. Each exercise is designed to teach specific concepts through practical examples using your running Hadoop cluster.

## 📋 Prerequisites

Before running these exercises, ensure:

1. **Hadoop cluster is running:**
   ```bash
   cd ..
   ./fresh_start.sh
   ```

2. **You have sudo access** (required for Docker commands)

3. **All containers are healthy:**
   ```bash
   sudo docker ps
   ```
   You should see: namenode, datanode, resourcemanager, nodemanager, spark-master, hive-server

## 🗂️ Exercise Structure

```
exercises/
├── hdfs_exercises.sh         # HDFS file operations & management
├── mapreduce_exercises.sh    # MapReduce programming patterns
├── hive_exercises.sh         # Hive SQL & optimization
├── spark_exercises.sh        # Spark analytics & tuning
├── run_all_exercises.sh     # Complete learning path
└── HOW_TO_USE.md            # This guide
```

## 🚀 Quick Start

### Option 1: Run Complete Learning Path (Recommended for First Time)
```bash
cd exercises/
chmod +x run_all_exercises.sh
sudo ./run_all_exercises.sh
```

This interactive script will:
- Guide you through all modules sequentially
- Pause between sections for review
- Provide summaries of what you learned

### Option 2: Run Individual Modules
```bash
cd exercises/
chmod +x *.sh

# Choose any module:
sudo ./hdfs_exercises.sh         # Learn HDFS
sudo ./mapreduce_exercises.sh    # Learn MapReduce
sudo ./hive_exercises.sh         # Learn Hive
sudo ./spark_exercises.sh        # Learn Spark
```

## 📖 Module Details

### 1️⃣ HDFS Operations (`hdfs_exercises.sh`)
**Duration:** ~10 minutes

**What you'll learn:**
- Upload/download files to/from HDFS
- Create and manage directory structures
- Set and verify replication factors
- Copy and move files within HDFS
- Use wildcards and check statistics
- Monitor HDFS health and capacity

**Key Commands Covered:**
```bash
hdfs dfs -put    # Upload files
hdfs dfs -get    # Download files
hdfs dfs -mkdir  # Create directories
hdfs dfs -ls     # List contents
hdfs dfs -setrep # Set replication
hdfs dfsadmin    # Admin operations
```

### 2️⃣ MapReduce Programming (`mapreduce_exercises.sh`)
**Duration:** ~15 minutes

**What you'll learn:**
- Understand mapper/reducer pattern
- Run Python-based MapReduce jobs
- Analyze category sales
- Create custom product analysis
- Perform customer spending analysis
- Time-based analytics (hourly patterns)

**Exercises Include:**
- Category sales aggregation
- Product quantity analysis
- Top spending customers
- Sales by hour of day

### 3️⃣ Hive Optimization (`hive_exercises.sh`)
**Duration:** ~15 minutes

**What you'll learn:**
- Create and query Hive tables
- Implement partitioning for performance
- Use bucketing for optimized joins
- Compare file formats (Text/ORC/Parquet)
- Apply Cost-Based Optimizer (CBO)
- Enable vectorization

**Key Concepts:**
- Partitioned tables by date
- Bucketed tables for joins
- Storage format comparison
- Query optimization techniques
- Performance tuning parameters

### 4️⃣ Spark Analytics (`spark_exercises.sh`)
**Duration:** ~20 minutes

**What you'll learn:**
- DataFrame operations
- Spark SQL analytics
- Customer segmentation
- Streaming simulation
- Performance optimization
- Caching strategies
- Broadcast joins

**Exercises Include:**
- Product revenue analysis
- Daily sales trends
- Customer segmentation (VIP/Regular/Occasional)
- Real-time processing simulation
- Performance comparison (cached vs uncached)

## 💡 Tips for Learning

### For Beginners:
1. Start with `run_all_exercises.sh` for guided learning
2. Read the output carefully - each command is explained
3. Take notes on commands that interest you
4. Re-run individual exercises to reinforce learning

### For Advanced Users:
1. Modify the scripts to analyze different aspects
2. Try different parameters for optimization
3. Create your own MapReduce jobs
4. Experiment with Spark configurations
5. Load your own datasets for analysis

## 🔍 Viewing Results

### Web Interfaces:
While exercises run in terminal, you can monitor progress via:
- **HDFS NameNode:** http://localhost:9870
- **YARN ResourceManager:** http://localhost:8088
- **Spark Master:** http://localhost:8080

### Checking Output:
```bash
# View HDFS contents after exercises
sudo docker exec namenode hdfs dfs -ls -R /user/

# Check Hive tables created
sudo docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "SHOW TABLES;"

# Monitor running jobs
sudo docker exec resourcemanager yarn application -list
```

## 🛠️ Troubleshooting

### Common Issues:

1. **"Permission denied" error:**
   ```bash
   # Make sure to use sudo
   sudo ./exercise_name.sh
   ```

2. **"Container not found" error:**
   ```bash
   # Restart the cluster
   cd ..
   ./fresh_start.sh
   ```

3. **Python syntax errors in MapReduce:**
   ```bash
   # Python 3.5 doesn't support f-strings
   # The fresh_start.sh script should handle this
   # Or manually upgrade Python in container (see main README)
   ```

4. **Hive connection refused:**
   ```bash
   # Wait for Hive to fully start
   sleep 30
   # Then retry the exercise
   ```

## 📊 Sample Data

The exercises use e-commerce transaction data with fields:
- transaction_id
- customer_id
- product_id
- product_name
- category
- price
- quantity
- total_amount
- transaction_date

Located at: `hdfs://namenode:9000/user/data/transactions.csv`

## 🎯 Learning Objectives

After completing all exercises, you will be able to:

✅ **HDFS:** Manage distributed file storage effectively  
✅ **MapReduce:** Write and run data processing jobs  
✅ **Hive:** Optimize SQL queries on big data  
✅ **Spark:** Perform advanced analytics and tuning  
✅ **Integration:** Understand how components work together  

## 📝 Next Steps

1. **Modify exercises** - Change queries to explore different insights
2. **Load custom data** - Try with your own CSV files
3. **Performance testing** - Compare execution times with larger datasets
4. **Build applications** - Create end-to-end data pipelines
5. **Scale up** - Add more data nodes to the cluster

## 🆘 Getting Help

1. **Check logs:**
   ```bash
   sudo docker logs namenode
   sudo docker logs spark-master
   ```

2. **Interactive debugging:**
   ```bash
   sudo docker exec -it namenode bash
   ```

3. **Review main documentation:**
   ```bash
   cd ..
   cat README.md
   ```

## 📚 Additional Resources

- [Apache Hadoop Documentation](https://hadoop.apache.org/docs/stable/)
- [Apache Hive Documentation](https://hive.apache.org/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [MapReduce Tutorial](https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)

---

**Happy Learning! 🚀**

Remember: The best way to learn is by doing. Don't hesitate to experiment and break things - you can always restart with `./fresh_start.sh`!