#!/usr/bin/env python

import sys
import csv

def mapper():
    """Map function for category sales analysis"""
    reader = csv.DictReader(sys.stdin)
    
    for row in reader:
        try:
            category = row['category']
            total_amount = float(row['total_amount'])
            print("{}\t{}".format(category, total_amount))
        except (KeyError, ValueError):
            continue

if __name__ == "__main__":
    mapper()