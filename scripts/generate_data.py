#!/usr/bin/env python3

import csv
import random
from datetime import datetime, timedelta
import json

def generate_transactions(num_records=10000):
    """Generate sample e-commerce transaction data"""
    
    products = [
        {"id": "P001", "name": "Laptop", "category": "Electronics", "price": 999.99},
        {"id": "P002", "name": "Phone", "category": "Electronics", "price": 699.99},
        {"id": "P003", "name": "Headphones", "category": "Electronics", "price": 199.99},
        {"id": "P004", "name": "T-Shirt", "category": "Clothing", "price": 29.99},
        {"id": "P005", "name": "Jeans", "category": "Clothing", "price": 79.99},
        {"id": "P006", "name": "Sneakers", "category": "Footwear", "price": 119.99},
        {"id": "P007", "name": "Watch", "category": "Accessories", "price": 249.99},
        {"id": "P008", "name": "Backpack", "category": "Accessories", "price": 59.99},
        {"id": "P009", "name": "Book", "category": "Books", "price": 14.99},
        {"id": "P010", "name": "Coffee Maker", "category": "Appliances", "price": 89.99}
    ]
    
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
              "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
    
    payment_methods = ["Credit Card", "Debit Card", "PayPal", "Cash", "Bank Transfer"]
    
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 12, 31)
    
    transactions = []
    
    for i in range(num_records):
        transaction_date = start_date + timedelta(
            days=random.randint(0, (end_date - start_date).days),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        product = random.choice(products)
        quantity = random.randint(1, 5)
        
        transaction = {
            "transaction_id": f"TXN{i+1:06d}",
            "customer_id": f"CUST{random.randint(1, 2000):04d}",
            "product_id": product["id"],
            "product_name": product["name"],
            "category": product["category"],
            "quantity": quantity,
            "unit_price": product["price"],
            "total_amount": round(product["price"] * quantity, 2),
            "transaction_date": transaction_date.strftime("%Y-%m-%d"),
            "transaction_time": transaction_date.strftime("%H:%M:%S"),
            "city": random.choice(cities),
            "payment_method": random.choice(payment_methods),
            "discount": round(random.uniform(0, 0.3) * product["price"] * quantity, 2),
            "year": transaction_date.year,
            "month": transaction_date.month,
            "day": transaction_date.day
        }
        
        transactions.append(transaction)
    
    return transactions

def save_csv(transactions, filename):
    """Save transactions to CSV file"""
    if not transactions:
        return
    
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = transactions[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(transactions)
    print(f"Generated {len(transactions)} transactions in {filename}")

def save_json(transactions, filename):
    """Save transactions to JSON file"""
    with open(filename, 'w') as jsonfile:
        json.dump(transactions, jsonfile, indent=2)
    print(f"Generated {len(transactions)} transactions in {filename}")

if __name__ == "__main__":
    transactions = generate_transactions(10000)
    
    save_csv(transactions, "/data/transactions.csv")
    save_json(transactions[:1000], "/data/transactions_sample.json")
    
    print("\nSample transaction:")
    print(json.dumps(transactions[0], indent=2))