#!/usr/bin/env python3

import sys
from collections import defaultdict

def reducer():
    """Reduce function for category sales analysis"""
    category_sales = defaultdict(lambda: {"total": 0, "count": 0})
    
    for line in sys.stdin:
        try:
            category, amount = line.strip().split('\t')
            amount = float(amount)
            category_sales[category]["total"] += amount
            category_sales[category]["count"] += 1
        except ValueError:
            continue
    
    print("Category\tTotal Sales\tTransaction Count\tAverage Sale")
    print("-" * 60)
    
    for category, stats in sorted(category_sales.items()):
        avg_sale = stats["total"] / stats["count"] if stats["count"] > 0 else 0
        print(f"{category}\t${stats['total']:.2f}\t{stats['count']}\t${avg_sale:.2f}")

if __name__ == "__main__":
    reducer()