#!/usr/bin/env python3
"""
Main entry point for the Hadoop Practice Project.
This script provides a menu-driven interface to run various Hadoop operations.
"""

import subprocess
import sys
import time

def run_command(cmd, description="Running command"):
    """Execute a shell command and handle errors."""
    print(f"\n{description}...")
    print(f"Command: {cmd}")
    try:
        result = subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        if e.stderr:
            print(f"Error output: {e.stderr}")
        return False

def check_docker():
    """Check if Docker is running and accessible."""
    try:
        subprocess.run("docker ps", shell=True, check=True, capture_output=True)
        return True
    except subprocess.CalledProcessError:
        print("Error: Docker is not accessible. You may need to:")
        print("1. Start Docker: sudo systemctl start docker")
        print("2. Add your user to docker group: sudo usermod -aG docker $USER")
        print("3. Log out and back in")
        return False

def start_cluster():
    """Start the Hadoop cluster."""
    print("Starting Hadoop cluster...")
    
    if not run_command("docker-compose up -d", "Starting Docker containers"):
        return False
    
    print("Waiting 30 seconds for services to initialize...")
    time.sleep(30)
    
    if not run_command("docker-compose ps", "Checking container status"):
        return False
    
    return True

def generate_data():
    """Generate sample transaction data."""
    return run_command(
        "sudo docker exec namenode bash /scripts/generate_data.sh",
        "Generating sample data"
    )

def setup_hdfs():
    """Create HDFS directories and upload data."""
    commands = [
        ("sudo docker exec namenode hdfs dfs -mkdir -p /user/data", "Creating HDFS directories"),
        ("sudo docker exec namenode hdfs dfs -mkdir -p /user/output", "Creating output directory"),
        ("sudo docker exec namenode hdfs dfs -put /data/transactions.csv /user/data/", "Uploading data to HDFS"),
        ("sudo docker exec namenode hdfs dfs -ls /user/data", "Verifying data upload")
    ]
    
    for cmd, desc in commands:
        if not run_command(cmd, desc):
            return False
    return True

def run_mapreduce():
    """Run MapReduce analytics job."""
    cmd = """sudo docker exec namenode bash -c "hdfs dfs -cat /user/data/transactions.csv | tail -n +2 | cut -d',' -f5,8 | sort | awk -F',' '{sales[\\$1] += \\$2; count[\\$1]++} END {for (cat in sales) print cat \\\": \\$\\\" sales[cat] \\\" (\\\" count[cat] \\\" transactions)\\\"}'""""
    
    return run_command(cmd, "Running MapReduce analytics")

def run_spark():
    """Run Spark analytics job."""
    cmd = "sudo docker exec spark-master spark-submit --master local[2] --executor-memory 1g /jobs/spark_analytics.py"
    return run_command(cmd, "Running Spark analytics")

def setup_hive():
    """Setup Hive tables."""
    cmd = "sudo docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /scripts/hive_setup.sql"
    return run_command(cmd, "Setting up Hive tables")

def show_web_interfaces():
    """Display web interface URLs."""
    print("\n" + "="*50)
    print("WEB INTERFACES")
    print("="*50)
    print("- HDFS NameNode: http://localhost:9870")
    print("- YARN ResourceManager: http://localhost:8088")
    print("- Spark Master: http://localhost:8080")
    print("- MapReduce History: http://localhost:8188")
    print("="*50)

def stop_cluster():
    """Stop the Hadoop cluster."""
    return run_command("docker-compose down", "Stopping Hadoop cluster")

def full_workflow():
    """Run the complete workflow."""
    print("\n" + "="*60)
    print("HADOOP PRACTICE PROJECT - FULL WORKFLOW")
    print("="*60)
    
    steps = [
        (start_cluster, "Starting cluster"),
        (generate_data, "Generating data"),
        (setup_hdfs, "Setting up HDFS"),
        (run_mapreduce, "Running MapReduce"),
    ]
    
    for step_func, step_name in steps:
        print(f"\n>>> {step_name}")
        if not step_func():
            print(f"Failed at step: {step_name}")
            return False
    
    show_web_interfaces()
    print("\n✓ Full workflow completed successfully!")
    print("You can now explore the web interfaces and run additional commands.")
    return True

def main_menu():
    """Display the main menu and handle user choices."""
    while True:
        print("\n" + "="*50)
        print("HADOOP PRACTICE PROJECT MENU")
        print("="*50)
        print("1. Check Docker status")
        print("2. Start Hadoop cluster")
        print("3. Generate sample data")
        print("4. Setup HDFS directories and upload data")
        print("5. Run MapReduce analytics")
        print("6. Run Spark analytics")
        print("7. Setup Hive tables")
        print("8. Show web interfaces")
        print("9. Run full workflow (steps 2-5)")
        print("10. Stop cluster")
        print("0. Exit")
        print("="*50)
        
        try:
            choice = input("Enter your choice (0-10): ").strip()
            
            if choice == "0":
                print("Goodbye!")
                break
            elif choice == "1":
                if check_docker():
                    print("✓ Docker is accessible")
                else:
                    print("✗ Docker is not accessible")
            elif choice == "2":
                start_cluster()
            elif choice == "3":
                generate_data()
            elif choice == "4":
                setup_hdfs()
            elif choice == "5":
                run_mapreduce()
            elif choice == "6":
                run_spark()
            elif choice == "7":
                setup_hive()
            elif choice == "8":
                show_web_interfaces()
            elif choice == "9":
                full_workflow()
            elif choice == "10":
                stop_cluster()
            else:
                print("Invalid choice. Please enter 0-10.")
                
        except KeyboardInterrupt:
            print("\n\nExiting...")
            break
        except Exception as e:
            print(f"An error occurred: {e}")

def main():
    """Main function."""
    print("Hadoop Practice Project")
    print("Make sure Docker is running before proceeding.")
    main_menu()

if __name__ == "__main__":
    main()
