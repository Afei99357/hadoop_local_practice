#!/usr/bin/env python3
"""
Generate streaming data for NiFi ingestion practice.
This script simulates real-time data generation for IoT sensors.
"""

import json
import random
import time
from datetime import datetime
import sys
import argparse

def generate_sensor_reading():
    """Generate a single sensor reading."""
    sensors = [
        {"id": "SENS001", "location": "Building-A-Floor1"},
        {"id": "SENS002", "location": "Building-A-Floor2"},
        {"id": "SENS003", "location": "Building-B-Floor1"},
        {"id": "SENS004", "location": "Building-B-Floor2"},
        {"id": "SENS005", "location": "Building-C-Floor1"},
        {"id": "SENS006", "location": "Building-D-Floor1"},
        {"id": "SENS007", "location": "Building-D-Floor2"},
        {"id": "SENS008", "location": "Building-E-Floor1"},
        {"id": "SENS009", "location": "Building-E-Floor2"},
        {"id": "SENS010", "location": "Building-F-Floor1"},
    ]
    
    sensor = random.choice(sensors)
    
    # Generate realistic sensor values with some anomalies
    temperature = random.gauss(22.5, 2.5)  # Normal distribution around 22.5°C
    humidity = random.gauss(45, 5)  # Normal distribution around 45%
    pressure = random.gauss(1013.25, 2)  # Normal distribution around standard pressure
    battery_level = max(0, min(100, random.gauss(80, 15)))  # Battery level 0-100
    
    # Occasionally generate anomalies
    if random.random() < 0.05:  # 5% chance of anomaly
        temperature = random.choice([random.uniform(35, 40), random.uniform(5, 10)])
    
    # Determine status based on conditions
    status = "active"
    if battery_level < 20:
        status = "warning"
    if random.random() < 0.02:  # 2% chance of error
        status = "error"
    
    return {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "sensor_id": sensor["id"],
        "location": sensor["location"],
        "temperature": round(temperature, 2),
        "humidity": round(humidity, 2),
        "pressure": round(pressure, 2),
        "battery_level": round(battery_level),
        "status": status,
        "reading_quality": random.choice(["excellent", "good", "fair", "poor"]),
        "maintenance_required": random.random() < 0.01  # 1% chance
    }

def generate_application_log():
    """Generate a single application log entry."""
    services = [
        "auth-service", "payment-service", "inventory-service",
        "order-service", "cache-service", "email-service",
        "analytics-service", "api-gateway", "search-service",
        "database-service", "notification-service", "recommendation-service"
    ]
    
    levels = ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]
    level_weights = [0.1, 0.6, 0.2, 0.08, 0.02]  # Weighted probability
    
    log_templates = {
        "auth-service": [
            ("User login successful", "INFO"),
            ("Authentication failed", "WARN"),
            ("Session expired", "INFO"),
            ("Invalid token", "ERROR")
        ],
        "payment-service": [
            ("Payment processed successfully", "INFO"),
            ("Payment processing failed", "ERROR"),
            ("Refund initiated", "INFO"),
            ("Payment gateway timeout", "ERROR")
        ],
        "inventory-service": [
            ("Stock updated", "INFO"),
            ("Low stock alert", "WARN"),
            ("Out of stock", "ERROR"),
            ("Inventory sync completed", "INFO")
        ],
        "order-service": [
            ("Order placed successfully", "INFO"),
            ("Order cancelled", "INFO"),
            ("Order processing failed", "ERROR"),
            ("Order shipped", "INFO")
        ]
    }
    
    service = random.choice(services)
    
    if service in log_templates:
        message, level = random.choice(log_templates[service])
    else:
        level = random.choices(levels, weights=level_weights)[0]
        message = f"Service operation completed"
    
    log_entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "level": level,
        "service": service,
        "message": message,
        "trace_id": f"trace_{random.randint(100000, 999999)}",
        "span_id": f"span_{random.randint(1000, 9999)}",
        "response_time_ms": random.randint(10, 2000)
    }
    
    # Add additional fields based on service
    if service == "payment-service" and "failed" in message:
        log_entry["error_code"] = random.choice(["INSUFFICIENT_FUNDS", "CARD_DECLINED", "GATEWAY_ERROR"])
        log_entry["amount"] = round(random.uniform(10, 1000), 2)
    elif service == "order-service" and "placed" in message:
        log_entry["order_id"] = f"ORD-{datetime.now().strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
        log_entry["items_count"] = random.randint(1, 10)
    
    return log_entry

def main():
    parser = argparse.ArgumentParser(description='Generate streaming data for NiFi ingestion')
    parser.add_argument('--type', choices=['sensor', 'logs', 'both'], default='both',
                        help='Type of data to generate')
    parser.add_argument('--interval', type=float, default=1.0,
                        help='Interval between data generation in seconds')
    parser.add_argument('--output', choices=['console', 'file'], default='console',
                        help='Output destination')
    parser.add_argument('--file-prefix', default='stream_',
                        help='Prefix for output files')
    args = parser.parse_args()
    
    print(f"Starting data generation (type: {args.type}, interval: {args.interval}s)")
    print("Press Ctrl+C to stop")
    
    try:
        counter = 0
        while True:
            counter += 1
            
            if args.type in ['sensor', 'both']:
                sensor_data = generate_sensor_reading()
                if args.output == 'console':
                    print(f"SENSOR: {json.dumps(sensor_data)}")
                else:
                    with open(f"/opt/nifi/data/{args.file_prefix}sensor_{counter}.json", 'w') as f:
                        json.dump(sensor_data, f)
            
            if args.type in ['logs', 'both']:
                log_data = generate_application_log()
                if args.output == 'console':
                    print(f"LOG: {json.dumps(log_data)}")
                else:
                    with open(f"/opt/nifi/data/{args.file_prefix}log_{counter}.json", 'w') as f:
                        json.dump(log_data, f)
            
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print(f"\nData generation stopped. Generated {counter} records.")
        sys.exit(0)

if __name__ == "__main__":
    main()