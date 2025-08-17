#!/bin/bash

# Start NiFi and integrate with existing Hadoop cluster

echo "Starting NiFi Data Pipeline Practice Environment..."
echo "=============================================="

# Check if main Hadoop cluster is running
if ! docker ps | grep -q namenode; then
    echo "⚠️  Hadoop cluster is not running. Starting it first..."
    cd ..
    docker-compose up -d
    echo "Waiting for Hadoop to be ready..."
    sleep 30
    cd nifi
fi

# Start NiFi services
echo "Starting NiFi services..."
docker-compose -f docker-compose.nifi.yml up -d

# Wait for NiFi to be ready
echo "Waiting for NiFi to initialize (this may take 1-2 minutes)..."
sleep 60

# Check NiFi status
if docker ps | grep -q nifi; then
    echo "✅ NiFi is running!"
    echo ""
    echo "Access Points:"
    echo "-------------"
    echo "NiFi UI:          http://localhost:8090/nifi"
    echo "  Username:       admin"
    echo "  Password:       nifiadminpassword123"
    echo ""
    echo "NiFi Registry:    http://localhost:18080/nifi-registry"
    echo ""
    echo "Hadoop Services:"
    echo "  HDFS NameNode:  http://localhost:9870"
    echo "  YARN Manager:   http://localhost:8088"
    echo ""
    
    # Create HDFS directories for NiFi
    echo "Setting up HDFS directories for NiFi..."
    docker exec namenode hdfs dfs -mkdir -p /user/nifi/data
    docker exec namenode hdfs dfs -mkdir -p /user/nifi/processed
    docker exec namenode hdfs dfs -mkdir -p /user/nifi/error
    docker exec namenode hdfs dfs -chmod -R 777 /user/nifi
    
    echo "✅ HDFS directories created"
    echo ""
    echo "Next Steps:"
    echo "----------"
    echo "1. Open NiFi UI at http://localhost:8090/nifi"
    echo "2. Follow exercises in nifi/README.md"
    echo "3. Check sample data in nifi/data/"
    echo ""
    echo "To generate streaming data:"
    echo "  docker exec -it nifi python3 /opt/nifi/scripts/generate_streaming_data.py"
    echo ""
else
    echo "❌ Failed to start NiFi. Check logs with:"
    echo "  docker logs nifi"
fi