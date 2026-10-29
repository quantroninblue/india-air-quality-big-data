"""Shared paths, Spark session and helpers for the CPCB air quality pipeline.

Storage mode is chosen with the CPCB_STORAGE environment variable:
  hdfs  (default) raw CSVs and all Parquet layers live in HDFS (hdfs://localhost:9000/cpcb)
  local           raw CSVs from 02_Raw_Dataset/csv, Parquet layers in 03_Cleaned_Dataset
"""
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW_LOCAL = ROOT / "02_Raw_Dataset" / "csv"
CLEANED_LOCAL = ROOT / "03_Cleaned_Dataset"
TABLES_DIR = ROOT / "05_Outputs" / "tables"
LOGS_DIR = ROOT / "05_Outputs" / "logs"
FIGURES_DIR = ROOT / "06_Visualisations"
NOTEBOOK_DIR = ROOT / "04_Code" / "notebooks"

STORAGE = os.environ.get("CPCB_STORAGE", "hdfs")
HDFS_URI = os.environ.get("CPCB_HDFS_URI", "hdfs://localhost:9000")
if STORAGE == "hdfs":
    RAW_URI = f"{HDFS_URI}/cpcb/raw"
    LAKE_URI = f"{HDFS_URI}/cpcb/lake"
else:
    RAW_URI = RAW_LOCAL.as_uri()
    LAKE_URI = CLEANED_LOCAL.as_uri()
BRONZE_DIR = f"{LAKE_URI}/bronze_hourly"
SILVER_DIR = f"{LAKE_URI}/silver_hourly"
STATIONS_DIR = f"{LAKE_URI}/silver_stations"
GOLD_DAILY_DIR = f"{LAKE_URI}/gold_daily"
WAREHOUSE_URI = f"{LAKE_URI}/hive_warehouse"
METASTORE_DIR = Path(os.environ.get("CPCB_METASTORE", Path.home() / "opt" / "cpcb_metastore"))

for d in (TABLES_DIR, LOGS_DIR, FIGURES_DIR, NOTEBOOK_DIR):
    d.mkdir(parents=True, exist_ok=True)

# Canonical measurement columns kept after harmonisation.
POLLUTANTS = ["pm25", "pm10", "no", "no2", "nox", "nh3", "so2", "co", "o3", "benzene", "toluene"]
MET = ["at", "rh", "ws", "wd", "rf", "sr"]
MEASURES = POLLUTANTS + MET


def get_spark(app_name="cpcb-air-quality", hive=False):
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", "48")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.hadoop.dfs.replication", "1")  # single-DataNode cluster
        .config("spark.ui.showConsoleProgress", "false")
    )
    if hive:
        METASTORE_DIR.mkdir(parents=True, exist_ok=True)
        builder = (builder.enableHiveSupport()
                   .config("spark.sql.warehouse.dir", WAREHOUSE_URI)
                   .config("spark.hadoop.javax.jdo.option.ConnectionURL",
                           f"jdbc:derby:;databaseName={METASTORE_DIR / 'metastore_db'};create=true")
                   .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={METASTORE_DIR}"))
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---- Hadoop FileSystem API (works for hdfs:// and file:// URIs) -------------------
def _fs(spark, uri):
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(uri)
    return path.getFileSystem(spark._jsc.hadoopConfiguration()), path


def list_files(spark, uri, suffix=".csv"):
    """[(uri, size_bytes)] for files directly under uri, sorted by name."""
    fs, path = _fs(spark, uri)
    out = [(s.getPath().toString(), s.getLen()) for s in fs.listStatus(path)
           if s.isFile() and s.getPath().getName().endswith(suffix)]
    return sorted(out)


def read_first_line(spark, uri):
    fs, path = _fs(spark, uri)
    jvm = spark._jvm
    reader = jvm.java.io.BufferedReader(jvm.java.io.InputStreamReader(fs.open(path), "UTF-8"))
    try:
        return reader.readLine()
    finally:
        reader.close()


def disk_usage(spark, uri):
    fs, path = _fs(spark, uri)
    return fs.getContentSummary(path).getLength()


@contextmanager
def timed(label, sink=None):
    start = time.perf_counter()
    yield
    elapsed = time.perf_counter() - start
    print(f"[time] {label}: {elapsed:.1f}s")
    if sink is not None:
        sink.append({"step": label, "seconds": round(elapsed, 2)})


def save_pdf(pdf, name):
    """Write a small pandas result to 05_Outputs/tables/<name>.csv."""
    path = TABLES_DIR / f"{name}.csv"
    pdf.to_csv(path, index=False)
    print(f"[table] {path.relative_to(ROOT)} ({len(pdf)} rows)")
    return path
