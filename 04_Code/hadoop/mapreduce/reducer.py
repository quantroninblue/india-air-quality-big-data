#!/usr/bin/env python3
"""Hadoop Streaming reducer: sums mapper values per year (input arrives sorted by key)."""
import sys


def emit(key, rows, valid, total):
    mean = round(total / valid, 2) if valid else ""
    print(f"{key}\t{rows}\t{valid}\t{mean}")


current, rows, valid, total = None, 0, 0, 0.0
for line in sys.stdin:
    key, value = line.rstrip("\n").split("\t")
    r, v, s = value.split(",")
    if key != current:
        if current is not None:
            emit(current, rows, valid, total)
        current, rows, valid, total = key, 0, 0, 0.0
    rows += int(r)
    valid += int(v)
    total += float(s)
if current is not None:
    emit(current, rows, valid, total)
