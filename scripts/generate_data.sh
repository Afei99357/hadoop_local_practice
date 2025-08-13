#!/bin/bash

echo "Generating sample e-commerce transaction data..."

# Create transactions.csv file directly
cat > /data/transactions.csv << 'EOF'
transaction_id,customer_id,product_id,product_name,category,quantity,unit_price,total_amount,transaction_date,transaction_time,city,payment_method,discount,year,month,day
TXN000001,CUST0001,P001,Laptop,Electronics,1,999.99,999.99,2024-01-15,09:30:00,New York,Credit Card,0.00,2024,1,15
TXN000002,CUST0002,P002,Phone,Electronics,2,699.99,1399.98,2024-01-15,10:45:00,Los Angeles,Debit Card,139.99,2024,1,15
TXN000003,CUST0003,P003,Headphones,Electronics,1,199.99,199.99,2024-01-16,14:20:00,Chicago,PayPal,19.99,2024,1,16
TXN000004,CUST0004,P004,T-Shirt,Clothing,3,29.99,89.97,2024-01-16,11:15:00,Houston,Credit Card,8.99,2024,1,16
TXN000005,CUST0005,P005,Jeans,Clothing,1,79.99,79.99,2024-01-17,16:00:00,Phoenix,Cash,7.99,2024,1,17
TXN000006,CUST0006,P006,Sneakers,Footwear,2,119.99,239.98,2024-01-17,12:30:00,Philadelphia,Credit Card,23.99,2024,1,17
TXN000007,CUST0007,P007,Watch,Accessories,1,249.99,249.99,2024-01-18,15:45:00,San Antonio,Bank Transfer,24.99,2024,1,18
TXN000008,CUST0008,P008,Backpack,Accessories,1,59.99,59.99,2024-01-18,13:20:00,San Diego,PayPal,5.99,2024,1,18
TXN000009,CUST0009,P009,Book,Books,5,14.99,74.95,2024-01-19,10:10:00,Dallas,Credit Card,7.49,2024,1,19
TXN000010,CUST0010,P010,Coffee Maker,Appliances,1,89.99,89.99,2024-01-19,17:25:00,San Jose,Debit Card,8.99,2024,1,19
TXN000011,CUST0001,P002,Phone,Electronics,1,699.99,699.99,2024-02-01,11:20:00,New York,Credit Card,69.99,2024,2,1
TXN000012,CUST0002,P004,T-Shirt,Clothing,2,29.99,59.98,2024-02-01,14:35:00,Los Angeles,PayPal,5.99,2024,2,1
TXN000013,CUST0003,P001,Laptop,Electronics,1,999.99,999.99,2024-02-02,09:50:00,Chicago,Credit Card,99.99,2024,2,2
TXN000014,CUST0004,P006,Sneakers,Footwear,1,119.99,119.99,2024-02-02,16:15:00,Houston,Debit Card,11.99,2024,2,2
TXN000015,CUST0005,P003,Headphones,Electronics,2,199.99,399.98,2024-02-03,13:40:00,Phoenix,Cash,39.99,2024,2,3
TXN000016,CUST0006,P007,Watch,Accessories,1,249.99,249.99,2024-02-03,12:00:00,Philadelphia,Credit Card,24.99,2024,2,3
TXN000017,CUST0007,P008,Backpack,Accessories,3,59.99,179.97,2024-02-04,15:30:00,San Antonio,Bank Transfer,17.99,2024,2,4
TXN000018,CUST0008,P009,Book,Books,2,14.99,29.98,2024-02-04,10:45:00,San Diego,PayPal,2.99,2024,2,4
TXN000019,CUST0009,P010,Coffee Maker,Appliances,1,89.99,89.99,2024-02-05,14:20:00,Dallas,Credit Card,8.99,2024,2,5
TXN000020,CUST0010,P005,Jeans,Clothing,2,79.99,159.98,2024-02-05,11:55:00,San Jose,Debit Card,15.99,2024,2,5
TXN000021,CUST0011,P001,Laptop,Electronics,1,999.99,999.99,2024-03-01,09:15:00,New York,Credit Card,0.00,2024,3,1
TXN000022,CUST0012,P002,Phone,Electronics,1,699.99,699.99,2024-03-01,13:25:00,Los Angeles,PayPal,69.99,2024,3,1
TXN000023,CUST0013,P003,Headphones,Electronics,1,199.99,199.99,2024-03-02,16:40:00,Chicago,Credit Card,19.99,2024,3,2
TXN000024,CUST0014,P004,T-Shirt,Clothing,4,29.99,119.96,2024-03-02,12:10:00,Houston,Debit Card,11.99,2024,3,2
TXN000025,CUST0015,P005,Jeans,Clothing,1,79.99,79.99,2024-03-03,15:20:00,Phoenix,Cash,7.99,2024,3,3
TXN000026,CUST0016,P006,Sneakers,Footwear,2,119.99,239.98,2024-03-03,11:30:00,Philadelphia,Credit Card,23.99,2024,3,3
TXN000027,CUST0017,P007,Watch,Accessories,1,249.99,249.99,2024-03-04,14:45:00,San Antonio,Bank Transfer,24.99,2024,3,4
TXN000028,CUST0018,P008,Backpack,Accessories,2,59.99,119.98,2024-03-04,10:00:00,San Diego,PayPal,11.99,2024,3,4
TXN000029,CUST0019,P009,Book,Books,3,14.99,44.97,2024-03-05,17:15:00,Dallas,Credit Card,4.49,2024,3,5
TXN000030,CUST0020,P010,Coffee Maker,Appliances,1,89.99,89.99,2024-03-05,13:50:00,San Jose,Debit Card,8.99,2024,3,5
EOF

# Create a smaller sample JSON file
cat > /data/transactions_sample.json << 'EOF'
[
  {
    "transaction_id": "TXN000001",
    "customer_id": "CUST0001",
    "product_id": "P001",
    "product_name": "Laptop",
    "category": "Electronics",
    "quantity": 1,
    "unit_price": 999.99,
    "total_amount": 999.99,
    "transaction_date": "2024-01-15",
    "transaction_time": "09:30:00",
    "city": "New York",
    "payment_method": "Credit Card",
    "discount": 0.00,
    "year": 2024,
    "month": 1,
    "day": 15
  },
  {
    "transaction_id": "TXN000002",
    "customer_id": "CUST0002",
    "product_id": "P002",
    "product_name": "Phone",
    "category": "Electronics",
    "quantity": 2,
    "unit_price": 699.99,
    "total_amount": 1399.98,
    "transaction_date": "2024-01-15",
    "transaction_time": "10:45:00",
    "city": "Los Angeles",
    "payment_method": "Debit Card",
    "discount": 139.99,
    "year": 2024,
    "month": 1,
    "day": 15
  }
]
EOF

echo "Generated sample transaction data:"
echo "- transactions.csv: $(wc -l < /data/transactions.csv) lines"
echo "- transactions_sample.json: 2 sample records"
echo "Sample data is ready for HDFS upload!"