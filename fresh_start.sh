#!/bin/bash

echo "=========================================="
echo "HADOOP PRACTICE - SIMPLE START"
echo "=========================================="

# Install docker-compose if needed
if ! command -v docker-compose &> /dev/null; then
    echo "Installing docker-compose..."
    sudo curl -L "https://github.com/docker/compose/releases/download/v2.24.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
fi

# Check Docker permissions and use sudo if needed
DOCKER_CMD="docker"
COMPOSE_CMD="docker-compose"

if ! docker info &> /dev/null; then
    if sudo docker info &> /dev/null; then
        echo "Using sudo for Docker commands..."
        DOCKER_CMD="sudo docker"
        COMPOSE_CMD="sudo docker-compose"
    else
        echo "Error: Docker is not running or accessible"
        exit 1
    fi
fi

# Clean up any existing containers
echo "Cleaning up..."
$COMPOSE_CMD down -v 2>/dev/null
sudo docker system prune -f 2>/dev/null

# Start the cluster
echo "Starting Hadoop cluster..."
$COMPOSE_CMD up -d

# Wait for services
echo "Waiting 30 seconds for services to start..."
sleep 30

# Check status
echo "Container status:"
$COMPOSE_CMD ps

# Function to install Python3 in a container
install_python3() {
    local container_name=$1
    echo "Installing Python3 in $container_name..."
    if $DOCKER_CMD exec "$container_name" bash -c "
        # Fix Debian Stretch apt sources (moved to archive)
        sed -i 's|deb.debian.org|archive.debian.org|g' /etc/apt/sources.list
        sed -i '/security.debian.org/d' /etc/apt/sources.list
        sed -i '/stretch-updates/d' /etc/apt/sources.list
        echo 'Acquire::Check-Valid-Until \"false\";' > /etc/apt/apt.conf.d/99no-check-valid

        # Now install Python3
        apt-get update && apt-get install -y python3 python3-pip
    "; then
        echo "✓ Python3 installed successfully in $container_name"
        return 0
    else
        echo "⚠ Python3 installation failed in $container_name"
        return 1
    fi
}

# Try installing Python in key containers
PYTHON_AVAILABLE=true
for container in namenode datanode spark-worker; do
    if ! install_python3 "$container"; then
        PYTHON_AVAILABLE=false
    fi
done

# Generate data and setup HDFS
echo "Setting up HDFS..."
$DOCKER_CMD exec namenode bash /scripts/generate_data.sh
$DOCKER_CMD exec namenode hdfs dfs -mkdir -p /user/data
$DOCKER_CMD exec namenode hdfs dfs -put /data/transactions.csv /user/data/

# Test MapReduce
echo "Testing MapReduce:"
if [ "$PYTHON_AVAILABLE" = true ]; then
    echo "Using Python MapReduce..."
    $DOCKER_CMD exec namenode bash -c "hdfs dfs -cat /user/data/transactions.csv | python3 /mapreduce/mapper.py | sort | python3 /mapreduce/reducer.py"
else
    echo "Using shell-based alternative..."
    $DOCKER_CMD exec namenode bash -c "hdfs dfs -cat /user/data/transactions.csv | tail -n +2 | cut -d',' -f5,8 | sort | awk -F',' '{sales[\$1] += \$2; count[\$1]++} END {for (cat in sales) print cat \": \$\" sales[cat] \" (\" count[cat] \" transactions)\"}'"
fi

echo ""
echo "=========================================="
echo "SUCCESS! Hadoop cluster is ready!"
echo "=========================================="
echo "Web interfaces:"
echo "- HDFS: http://localhost:9870"
echo "- YARN: http://localhost:8088"
echo "- Spark: http://localhost:8080"
echo ""
echo "Interactive shell: $DOCKER_CMD exec -it namenode bash"
echo "Stop cluster: $COMPOSE_CMD down"
