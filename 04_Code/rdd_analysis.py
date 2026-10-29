"""Step 5 - RDD processing: MapReduce-style aggregation on the raw HDFS files with Spark RDDs.

Reads every raw station CSV as (path, content) pairs, parses each file with its own
header, and computes hourly readings and raw mean PM2.5 per state and year using only
RDD transformations (flatMap, filter, map, reduceByKey, mapValues, join, sortBy).
The result is cross-checked against the DataFrame/Bronze route.
"""
import csv
import io
import time

import pandas as pd
from pyspark.sql import functions as F

from common import BRONZE_DIR, LOGS_DIR, RAW_URI, ROOT, STATIONS_DIR, get_spark, save_pdf


def parse_station_file(record, state_of):
    """flatMap function: one raw file -> ((state, year), (rows, pm25_n, pm25_sum)) per line."""
    path, content = record
    station = path.rsplit("/", 1)[-1].replace(".csv", "")
    state = state_of.value.get(station, "Unknown")
    lines = content.splitlines()
    header = lines[0].split(",")
    pm_idx = header.index("PM2.5 (ug/m3)") if "PM2.5 (ug/m3)" in header else None
    for line in lines[1:]:
        fields = line.split(",")
        year = fields[0][:4]
        pm = None
        if pm_idx is not None and pm_idx < len(fields) and fields[pm_idx]:
            try:
                pm = float(fields[pm_idx])
            except ValueError:
                pm = None
        valid = pm is not None and pm > 0
        yield (state, year), (1, 1 if valid else 0, pm if valid else 0.0)


def add(a, b):
    return a[0] + b[0], a[1] + b[1], a[2] + b[2]


def main():
    spark = get_spark("cpcb-rdd")
    sc = spark.sparkContext

    # Small lookup: station -> state, built from the raw metadata file and broadcast.
    meta = sc.textFile(f"{RAW_URI}/stations_info.csv")
    head = meta.first()
    state_map = (meta.filter(lambda l: l != head)
                 .map(lambda l: next(csv.reader(io.StringIO(l))))
                 .map(lambda r: (r[0].strip(), r[1].strip()))
                 .collectAsMap())
    state_of = sc.broadcast(state_map)

    t0 = time.perf_counter()
    files = sc.wholeTextFiles(f"{RAW_URI}/[A-Z]*.csv", minPartitions=48)
    pairs = files.flatMap(lambda rec: parse_station_file(rec, state_of))
    totals = pairs.filter(lambda kv: "2010" <= kv[0][1] <= "2023").reduceByKey(add)
    stats = totals.mapValues(lambda v: (v[0], v[1], round(v[2] / v[1], 2) if v[1] else None))

    # join with the number of stations per state (a second pair RDD)
    stations_per_state = (sc.parallelize(list(state_map.items()))
                          .map(lambda kv: (kv[1], 1)).reduceByKey(lambda a, b: a + b))
    joined = (stats.map(lambda kv: (kv[0][0], (kv[0][1],) + kv[1]))
              .join(stations_per_state)
              .map(lambda kv: (kv[0], kv[1][0][0], kv[1][1], *kv[1][0][1:]))
              .sortBy(lambda r: (r[0], r[1])))
    rows = joined.collect()
    rdd_seconds = time.perf_counter() - t0

    lineage = joined.toDebugString().decode().replace(f"{ROOT}/", "")
    (LOGS_DIR / "rdd_lineage.txt").write_text(lineage)
    rdd_df = pd.DataFrame(rows, columns=["state", "year", "stations_in_state", "hourly_rows",
                                         "pm25_valid", "mean_pm25_raw"])
    save_pdf(rdd_df, "rdd_state_year")

    # Same numbers through the DataFrame API on Bronze, to validate the RDD result.
    t0 = time.perf_counter()
    stations = spark.read.parquet(STATIONS_DIR).select("station_id", "state")
    df = (spark.read.parquet(BRONZE_DIR).join(F.broadcast(stations), "station_id")
          .groupBy("state", F.year("ts").cast("string").alias("year"))
          .agg(F.count("*").alias("hourly_rows_df"),
               F.count(F.when(F.col("pm25") > 0, 1)).alias("pm25_valid_df"))
          .toPandas())
    df_seconds = time.perf_counter() - t0
    check = rdd_df.merge(df, on=["state", "year"], how="outer")
    mismatches = int(((check.hourly_rows != check.hourly_rows_df) |
                      (check.pm25_valid != check.pm25_valid_df)).sum())
    save_pdf(pd.DataFrame([{
        "rdd_total_rows": int(rdd_df.hourly_rows.sum()),
        "dataframe_total_rows": int(df.hourly_rows_df.sum()),
        "state_year_groups": len(rdd_df), "mismatched_groups": mismatches,
        "rdd_seconds_raw_csv": round(rdd_seconds, 1), "dataframe_seconds_parquet": round(df_seconds, 1),
        "rdd_partitions": files.getNumPartitions(),
    }]), "rdd_validation")
    year_totals = (rdd_df.groupby("year")[["hourly_rows", "pm25_valid"]].sum().reset_index())
    save_pdf(year_totals, "rdd_year_totals")
    print(lineage)
    spark.stop()


if __name__ == "__main__":
    main()
