"""Step 1 - Ingest: read 453 heterogeneous station CSVs into one harmonised Parquet table.

The CPCB export has 198 distinct header layouts (different column sets, orders and
units). We group files by identical header, read each group with a positional string
schema, map every column to a canonical name + unit conversion factor, and union the
groups by name. Files are listed and headers read through the Hadoop FileSystem API, so
the same code runs against HDFS or the local disk. Output: bronze_hourly and
silver_stations Parquet in the data lake (see common.py).
"""
import csv
import io
import re
from collections import defaultdict
from functools import reduce

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from common import (BRONZE_DIR, MEASURES, RAW_URI, STATIONS_DIR, disk_usage, get_spark, list_files,
                    read_first_line, save_pdf, timed)

PARAM_MAP = {
    "PM2.5": "pm25", "PM10": "pm10", "NO": "no", "NO2": "no2", "NOx": "nox", "NH3": "nh3",
    "SO2": "so2", "CO": "co", "Ozone": "o3", "Benzene": "benzene", "Toluene": "toluene",
    "AT": "at", "Temp": "temp", "RH": "rh", "WS": "ws", "WD": "wd", "RF": "rf", "SR": "sr",
}
# Canonical units: gases in ug/m3, CO in mg/m3, NOx in ppb (CPCB convention).
CANONICAL_UNIT = {
    "pm25": "ug/m3", "pm10": "ug/m3", "no": "ug/m3", "no2": "ug/m3", "nox": "ppb",
    "nh3": "ug/m3", "so2": "ug/m3", "co": "mg/m3", "o3": "ug/m3", "benzene": "ug/m3",
    "toluene": "ug/m3",
}
# Conversion factors to the canonical unit (ppb -> ug/m3 at 25 C, 1 atm: MW / 24.45).
CONVERSIONS = {
    ("no", "ppb"): 1.227, ("no", "ppm"): 1227.0, ("no", "mg/m3"): 1000.0,
    ("nox", "ug/m3"): 1 / 1.88, ("nox", "ppm"): 1000.0,
    ("nh3", "ppb"): 0.696, ("o3", "ppb"): 1.963,
    ("co", "mg/Nm3"): 1.0, ("co", "ug/m3"): 1e-3, ("co", "ng/m3"): 1e-6,
    ("benzene", "mg/m3"): 1000.0,
}
HEADER_RE = re.compile(r"^(?P<param>.+?)\s*\((?P<unit>[^)]*)\)$")


def map_column(token):
    """Return (canonical_name, factor, note) for a raw header token, or None to drop it."""
    m = HEADER_RE.match(token.strip())
    if not m:  # pandas-style duplicate suffix such as "RF (mm).1", or From/To Date
        return None
    param, unit = m.group("param").strip(), m.group("unit").strip()
    canon = PARAM_MAP.get(param)
    if canon is None:
        return None
    if canon not in CANONICAL_UNIT:  # meteorology: accept label as-is, range checks later
        return canon, 1.0, "met"
    if unit == CANONICAL_UNIT[canon]:
        return canon, 1.0, "canonical"
    if unit == "":
        return canon, 1.0, "unit missing - assumed canonical"
    if (canon, unit) in CONVERSIONS:
        return canon, CONVERSIONS[(canon, unit)], f"converted from {unit}"
    return None


def scan_headers(spark):
    """{header tuple: [file uris]} for every station CSV under RAW_URI."""
    groups = defaultdict(list)
    for uri, _ in list_files(spark, RAW_URI):
        if uri.endswith("/stations_info.csv"):
            continue
        header = tuple(next(csv.reader(io.StringIO(read_first_line(spark, uri)))))
        groups[header].append(uri)
    return groups


def build_group_df(spark, header, files, mapping_rows):
    schema = StructType([StructField(f"c{i}", StringType()) for i in range(len(header))])
    # header=False + filter avoids Spark's per-file header-vs-schema warnings
    df = (spark.read.csv(files, schema=schema, header=False, mode="PERMISSIVE")
          .filter(F.col("c0") != "From Date"))
    exprs = {}
    for i, token in enumerate(header):
        mapped = map_column(token)
        mapping_rows.append({"raw_column": token.strip(), "files": len(files),
                             "canonical": mapped[0] if mapped else None,
                             "factor": mapped[1] if mapped else None,
                             "note": mapped[2] if mapped else "dropped"})
        if mapped and mapped[0] not in exprs:  # first occurrence wins
            exprs[mapped[0]] = F.col(f"c{i}").try_cast("double") * F.lit(mapped[1])
    at = exprs.pop("at", None)
    temp = exprs.pop("temp", None)
    if at is not None or temp is not None:
        exprs["at"] = F.coalesce(*[e for e in (at, temp) if e is not None])
    cols = [
        F.regexp_extract(F.col("_metadata.file_name"), r"^([A-Z]+\d+)\.csv$", 1).alias("station_id"),
        # wall-clock IST readings: keep as timestamp_ntz so no timezone shift is applied
        F.try_to_timestamp(F.col("c0"), F.lit("yyyy-MM-dd HH:mm:ss")).cast("timestamp_ntz").alias("ts"),
    ]
    cols += [(exprs[m] if m in exprs else F.lit(None).cast("double")).alias(m) for m in MEASURES]
    return df.select(*cols)


def load_stations(spark):
    df = spark.read.csv(f"{RAW_URI}/stations_info.csv", header=True, inferSchema=True)
    return df.select(F.trim("file_name").alias("station_id"),
                     *[F.trim(c).alias(c) for c in ("state", "city", "agency", "station_location")],
                     "start_year", "start_month_num")


def main():
    spark = get_spark("cpcb-ingest")
    timings = []
    groups = scan_headers(spark)
    n_files = sum(len(v) for v in groups.values())
    raw_bytes = sum(size for _, size in list_files(spark, RAW_URI))
    print(f"{n_files} station files, {len(groups)} distinct header layouts, "
          f"{raw_bytes / 1e9:.2f} GB")

    mapping_rows = []
    dfs = [build_group_df(spark, h, f, mapping_rows) for h, f in groups.items()]
    hourly = reduce(lambda a, b: a.unionByName(b), dfs)

    with timed("ingest: CSV -> bronze Parquet", timings):
        hourly.repartition(96).write.mode("overwrite").parquet(BRONZE_DIR)

    stations = load_stations(spark)
    stations.write.mode("overwrite").parquet(STATIONS_DIR)

    bronze = spark.read.parquet(BRONZE_DIR)
    n_rows = bronze.count()
    bronze_bytes = disk_usage(spark, BRONZE_DIR)

    mapping = (pd.DataFrame(mapping_rows)
               .groupby(["raw_column", "canonical", "factor", "note"], dropna=False)["files"]
               .sum().reset_index().sort_values("files", ascending=False))
    save_pdf(mapping, "column_mapping")
    save_pdf(pd.DataFrame([{
        "station_files": n_files, "header_layouts": len(groups),
        "raw_csv_gb": round(raw_bytes / 1e9, 3), "bronze_parquet_gb": round(bronze_bytes / 1e9, 3),
        "hourly_rows": n_rows, "stations_in_metadata": stations.count(),
        "states": stations.select("state").distinct().count(),
        "cities": stations.select("city").distinct().count(),
        "min_ts": str(bronze.agg(F.min("ts")).first()[0]),
        "max_ts": str(bronze.agg(F.max("ts")).first()[0]),
        "unparsed_ts": bronze.filter(F.col("ts").isNull()).count(),
    }]), "ingest_summary")
    save_pdf(pd.DataFrame(timings), "timings_ingest")
    print(bronze._jdf.schema().treeString())
    spark.stop()


if __name__ == "__main__":
    main()
