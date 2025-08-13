#!/usr/bin/env python3

import sys
from collections import defaultdict

def reducer():
    """Classic WordCount reducer"""
    word_count = defaultdict(int)
    
    for line in sys.stdin:
        try:
            word, count = line.strip().split('\t')
            word_count[word] += int(count)
        except ValueError:
            continue
    
    for word, count in sorted(word_count.items(), key=lambda x: x[1], reverse=True):
        print(f"{word}\t{count}")

if __name__ == "__main__":
    reducer()