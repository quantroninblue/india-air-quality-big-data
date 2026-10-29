#!/usr/bin/env python3
"""Hadoop Streaming mapper: raw CPCB CSV lines -> "year<TAB>rows,pm25_valid,pm25_sum".

Each station file starts with its own header, so the mapper re-reads the PM2.5 column
position whenever it meets a header line (files are processed one after another, even
when CombineTextInputFormat packs several files into one split).
"""
import sys

pm_idx = None
for line in sys.stdin:
    line = line.rstrip("\n")
    if line.startswith("From Date"):
        header = line.split(",")
        pm_idx = header.index("PM2.5 (ug/m3)") if "PM2.5 (ug/m3)" in header else None
        continue
    fields = line.split(",")
    year = fields[0][:4]
    if not year.isdigit():
        continue
    pm = 0.0
    valid = 0
    if pm_idx is not None and pm_idx < len(fields) and fields[pm_idx]:
        try:
            value = float(fields[pm_idx])
            if value > 0:
                pm, valid = value, 1
        except ValueError:
            pass
    print(f"{year}\t1,{valid},{pm}")
