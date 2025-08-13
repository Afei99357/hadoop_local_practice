#!/usr/bin/env python3

import sys
import re

def mapper():
    """Classic WordCount mapper for product names"""
    for line in sys.stdin:
        words = re.findall(r'\w+', line.lower())
        for word in words:
            print(f"{word}\t1")

if __name__ == "__main__":
    mapper()