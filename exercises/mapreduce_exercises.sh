#!/bin/bash

echo "================================================"
echo "MAPREDUCE LEARNING EXERCISES - Category Sales"
echo "================================================"
echo ""

echo "📊 Exercise 1: Understanding MapReduce Data Flow"
echo "------------------------------------------------"
echo "Viewing sample data structure..."
sudo docker exec namenode hdfs dfs -cat /user/data/transactions.csv | head -5

echo ""
echo "🔧 Exercise 2: Running Python MapReduce"
echo "---------------------------------------"
echo "Running category sales analysis with Python MapReduce..."
sudo docker exec namenode bash -c "
hdfs dfs -cat /user/data/transactions.csv | \
python3 /mapreduce/mapper.py | \
sort | \
python3 /mapreduce/reducer.py
"

echo ""
echo "🎯 Exercise 3: Custom MapReduce - Product Analysis"
echo "--------------------------------------------------"
echo "Creating custom mapper for product analysis..."
sudo docker exec namenode bash -c "cat > /tmp/product_mapper.py << 'EOF'
#!/usr/bin/env python3
import sys
import csv

# Skip header
next(sys.stdin)

for line in sys.stdin:
    fields = line.strip().split(',')
    if len(fields) >= 8:
        product_name = fields[3]
        quantity = int(fields[6])
        print(f'{product_name}\t{quantity}')
EOF"

echo "Creating custom reducer for product totals..."
sudo docker exec namenode bash -c "cat > /tmp/product_reducer.py << 'EOF'
#!/usr/bin/env python3
import sys

current_product = None
total_quantity = 0

for line in sys.stdin:
    product, quantity = line.strip().split('\t')
    quantity = int(quantity)
    
    if current_product == product:
        total_quantity += quantity
    else:
        if current_product:
            print(f'{current_product}: {total_quantity} units sold')
        current_product = product
        total_quantity = quantity

if current_product:
    print(f'{current_product}: {total_quantity} units sold')
EOF"

echo "Running custom product analysis..."
sudo docker exec namenode bash -c "
hdfs dfs -cat /user/data/transactions.csv | \
python3 /tmp/product_mapper.py | \
sort | \
python3 /tmp/product_reducer.py | \
head -10
"

echo ""
echo "💰 Exercise 4: Customer Spending Analysis"
echo "-----------------------------------------"
echo "Analyzing top spending customers..."
sudo docker exec namenode bash -c "
hdfs dfs -cat /user/data/transactions.csv | \
tail -n +2 | \
awk -F',' '{print \$2\",\"\$8}' | \
sort | \
awk -F',' '{
    spending[\$1] += \$2
} 
END {
    for (customer in spending) 
        print customer \": $\" spending[customer]
}' | \
sort -t':' -k2 -rn | \
head -5
"

echo ""
echo "📅 Exercise 5: Time-based Analysis"
echo "----------------------------------"
echo "Analyzing sales by hour of day..."
sudo docker exec namenode bash -c "
hdfs dfs -cat /user/data/transactions.csv | \
tail -n +2 | \
awk -F',' '{
    split(\$1, datetime, \" \")
    split(datetime[2], time, \":\")
    hour = time[1]
    print hour \",\" \$8
}' | \
sort | \
awk -F',' '{
    sales[sprintf(\"%02d\", \$1)] += \$2
    count[sprintf(\"%02d\", \$1)]++
} 
END {
    print \"Hour | Total Sales | Transactions\"
    print \"-----|-------------|-------------\"
    for (hour in sales) 
        printf \"%s:00 | $%.2f | %d\\n\", hour, sales[hour], count[hour]
}' | \
sort
"

echo ""
echo "✅ MapReduce Exercises Complete!"
echo ""
echo "What you learned:"
echo "- MapReduce data flow pattern"
echo "- Writing custom mappers and reducers"
echo "- Category and product sales aggregation"
echo "- Customer spending analysis"
echo "- Time-based analytics patterns"