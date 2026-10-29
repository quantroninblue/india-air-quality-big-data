"""Step 8 - Dataset profiling (project Hours 3-4): dataset profile, data dictionary, loading
checks, initial inspection and the data-quality assessment table.

Only identifies problems; the treatments listed are applied by clean.py.
"""
from functools import reduce

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

import ingest
from common import (BRONZE_DIR, LOGS_DIR, MEASURES, POLLUTANTS, RAW_URI, STATIONS_DIR, TABLES_DIR,
                    get_spark, list_files, save_pdf)

ACCESS_DATE = "2026-10-29"
SOURCE_URL = "https://www.kaggle.com/datasets/abhisheksjha/time-series-air-quality-data-of-india-2010-2023"

DICTIONARY = [
    # variable (raw header), canonical, description, type, unit/category, role
    ("file_name / station_id", "station_id", "CPCB station code, e.g. DL001 (= raw file name)", "Categorical (string)", "453 codes", "Key / grouping variable"),
    ("state", "state", "State or UT of the station (stations_info.csv)", "Categorical (string)", "31 states/UTs", "Grouping variable"),
    ("city", "city", "City of the station", "Categorical (string)", "241 cities", "Grouping variable"),
    ("agency", "agency", "Operating agency (CPCB, DPCC, MPCB, ...)", "Categorical (string)", "Agency name", "Descriptive"),
    ("station_location", "station_location", "Address / site name of the monitor", "Text (string)", "Free text", "Descriptive"),
    ("start_year", "start_year", "Year the station started reporting", "Integer", "Year", "Coverage check"),
    ("From Date", "ts", "Start of the 1-hour averaging window (IST)", "Timestamp", "yyyy-MM-dd HH:mm:ss", "Time variable"),
    ("To Date", "(dropped)", "End of the averaging window (= From Date + 1 h)", "Timestamp", "yyyy-MM-dd HH:mm:ss", "Redundant - dropped"),
    ("PM2.5 (ug/m3)", "pm25", "Fine particulate matter <= 2.5 microns", "Numerical (double)", "ug/m3", "Target / outcome variable"),
    ("PM10 (ug/m3)", "pm10", "Particulate matter <= 10 microns", "Numerical (double)", "ug/m3", "Analytical variable"),
    ("NO (ug/m3)", "no", "Nitric oxide", "Numerical (double)", "ug/m3", "Analytical variable"),
    ("NO2 (ug/m3)", "no2", "Nitrogen dioxide (traffic marker)", "Numerical (double)", "ug/m3", "Analytical variable"),
    ("NOx (ppb)", "nox", "Total nitrogen oxides", "Numerical (double)", "ppb", "Analytical variable"),
    ("NH3 (ug/m3)", "nh3", "Ammonia", "Numerical (double)", "ug/m3", "Analytical variable"),
    ("SO2 (ug/m3)", "so2", "Sulphur dioxide", "Numerical (double)", "ug/m3", "Analytical variable"),
    ("CO (mg/m3)", "co", "Carbon monoxide", "Numerical (double)", "mg/m3", "Analytical variable"),
    ("Ozone (ug/m3)", "o3", "Ground-level ozone", "Numerical (double)", "ug/m3", "Analytical variable"),
    ("Benzene (ug/m3)", "benzene", "Benzene (volatile organic compound)", "Numerical (double)", "ug/m3", "Analytical variable"),
    ("Toluene (ug/m3)", "toluene", "Toluene (volatile organic compound)", "Numerical (double)", "ug/m3", "Analytical variable"),
    ("AT / Temp (degree C)", "at", "Ambient air temperature", "Numerical (double)", "degree C", "Explanatory variable"),
    ("RH (%)", "rh", "Relative humidity", "Numerical (double)", "%", "Explanatory variable"),
    ("WS (m/s)", "ws", "Wind speed", "Numerical (double)", "m/s", "Explanatory variable"),
    ("WD (degree)", "wd", "Wind direction", "Numerical (double)", "degrees", "Explanatory variable"),
    ("RF (mm)", "rf", "Rainfall", "Numerical (double)", "mm", "Explanatory variable"),
    ("SR (W/mt2)", "sr", "Solar radiation", "Numerical (double)", "W/m2", "Explanatory variable"),
    ("(derived)", "aqi", "CPCB National AQI for the station-day", "Numerical (double)", "0-500", "Derived outcome variable"),
    ("(derived)", "aqi_category", "AQI band", "Categorical (string)", "Good ... Severe", "Derived outcome variable"),
    ("(derived)", "season", "Indian season from month", "Categorical (string)", "Winter/Summer/Monsoon/Post-monsoon", "Grouping variable"),
]


def raw_quality(spark):
    """One pass over the raw strings: rows, non-numeric tokens, bad To Date, unparseable dates."""
    parts = []
    for header, files in ingest.scan_headers(spark).items():
        schema = StructType([StructField(f"c{i}", StringType()) for i in range(len(header))])
        df = (spark.read.csv(files, schema=schema, header=False)
              .filter(F.col("c0") != "From Date"))
        measure_idx = [i for i, tok in enumerate(header) if ingest.map_column(tok)]
        bad_tokens = sum((F.col(f"c{i}").isNotNull() & F.col(f"c{i}").try_cast("double").isNull()
                          ).cast("int") for i in measure_idx) if measure_idx else F.lit(0)
        start = F.try_to_timestamp(F.col("c0"), F.lit("yyyy-MM-dd HH:mm:ss"))
        end = F.try_to_timestamp(F.col("c1"), F.lit("yyyy-MM-dd HH:mm:ss"))
        parts.append(df.select(
            F.lit(1).alias("rows"),
            bad_tokens.alias("non_numeric_tokens"),
            start.isNull().cast("int").alias("bad_from_date"),
            (end.isNull() | (F.unix_timestamp(end) - F.unix_timestamp(start) != 3600)).cast("int")
            .alias("bad_to_date"),
            ((F.year(start) < 2010) | (start > F.lit("2023-04-01").cast("timestamp"))).cast("int")
            .alias("out_of_range_date")))
    return reduce(lambda a, b: a.unionByName(b), parts).agg(
        *[F.sum(c).alias(c) for c in ["rows", "non_numeric_tokens", "bad_from_date",
                                      "bad_to_date", "out_of_range_date"]]).first().asDict()


def main():
    spark = get_spark("cpcb-profile")
    lines = []

    def log(title, text):
        lines.append(f"===== {title}\n{text}\n")

    raw_files = list_files(spark, RAW_URI)
    station_files = [f for f in raw_files if not f[0].endswith("stations_info.csv")]

    # ---- Task 7: loading checks on one file with automatic schema inference --------------
    sample = f"{RAW_URI}/DL001.csv"
    one = spark.read.csv(sample, header=True, inferSchema=True, sep=",")
    log("Single file load (DL001.csv, header=True, inferSchema=True)",
        one._jdf.schema().treeString() + f"\nrows: {one.count():,}  columns: {len(one.columns)}")
    # Loading every file at once with one header: Spark takes the header of one file and
    # applies it positionally to all files, so columns from other layouts are misaligned.
    naive = spark.read.csv(f"{RAW_URI}/[A-Z]*.csv", header=True, inferSchema=False)
    log("Naive multi-file load (single header applied to all files)",
        f"columns: {len(naive.columns)} -> {naive.columns[:6]} ...\n"
        f"distinct header layouts in the files: {len(ingest.scan_headers(spark))}")

    # ---- Task 8: initial inspection of the harmonised (bronze) DataFrame -----------------
    bronze = spark.read.parquet(BRONZE_DIR)
    stations = spark.read.parquet(STATIONS_DIR)
    n_rows = bronze.count()
    first10 = bronze.orderBy("station_id", "ts").limit(10).toPandas()
    last10 = bronze.orderBy(F.col("station_id").desc(), F.col("ts").desc()).limit(10).toPandas()
    save_pdf(first10, "inspect_first10")
    save_pdf(last10, "inspect_last10")
    log("Schema", bronze._jdf.schema().treeString())
    save_pdf(pd.DataFrame(bronze.dtypes, columns=["column", "data_type"]), "inspect_dtypes")
    desc = bronze.select(*POLLUTANTS[:9], "at", "rh", "ws").summary(
        "count", "mean", "stddev", "min", "25%", "50%", "75%", "max").toPandas()
    save_pdf(desc, "inspect_describe")
    uniques = pd.DataFrame([
        {"column": c, "distinct_values": stations.select(c).distinct().count(),
         "examples": ", ".join(r[0] for r in stations.select(c).distinct().orderBy(c).limit(6).collect())}
        for c in ["state", "city", "agency"]])
    save_pdf(uniques, "inspect_unique_categoricals")
    mm = bronze.agg(*[F.min(c).alias(f"{c}_min") for c in MEASURES],
                    *[F.max(c).alias(f"{c}_max") for c in MEASURES],
                    F.min("ts").alias("ts_min"), F.max("ts").alias("ts_max")).first().asDict()
    save_pdf(pd.DataFrame([{"column": c, "min": mm[f"{c}_min"], "max": mm[f"{c}_max"]}
                           for c in MEASURES]), "inspect_min_max")

    # ---- Task 9: data-quality problems (identify and count) -------------------------------
    rq = raw_quality(spark)
    empty = bronze.filter(F.coalesce(*[F.col(c) for c in MEASURES]).isNull()).count()
    nonempty = bronze.filter(F.coalesce(*[F.col(c) for c in MEASURES]).isNotNull())
    missing = nonempty.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in MEASURES]).first().asDict()
    dup_keys = n_rows - bronze.dropDuplicates(["station_id", "ts"]).count()
    exact_dups = n_rows - bronze.dropDuplicates().count()
    padded = spark.read.csv(f"{RAW_URI}/stations_info.csv", header=True).filter(
        (F.col("station_location") != F.trim("station_location")) |
        (F.col("city") != F.trim("city"))).count()
    nonpos = nonempty.agg(*[F.sum((F.col(c) <= 0).cast("int")).alias(c) for c in POLLUTANTS]).first().asDict()
    cm = pd.read_csv(TABLES_DIR / "column_mapping.csv")
    log_df = pd.read_csv(TABLES_DIR / "cleaning_log.csv")
    rule = lambda prefix, col: int(log_df.loc[log_df.rule.str.startswith(prefix), col].iloc[0])
    converted_files = int(cm[cm.note.fillna("").str.startswith(("converted", "unit missing"))].files.sum())
    mapped = cm[cm.canonical.notna()]
    label_variants = int(mapped.raw_column.nunique())
    standard = mapped.sort_values("files", ascending=False).drop_duplicates("canonical").raw_column
    variant_uses = int(mapped[~mapped.raw_column.isin(standard)].files.sum())
    dropped_cols = cm[(cm.note == "dropped") & ~cm.raw_column.isin(["From Date", "To Date"])]

    dq = [
        ("Missing values", "PM2.5", missing["pm25"], "Keep row; exclude from PM2.5 statistics; 24-h means need >= 16 valid hours"),
        ("Missing values", "Rainfall (RF)", missing["rf"], "Keep row; use only where present (meteorology analysis)"),
        ("Missing values", "NH3", missing["nh3"], "Keep row; AQI uses NH3 only when available"),
        ("Empty placeholder rows", "All measurements", empty, "Remove rows where every measurement is null"),
        ("Duplicate records", "(station_id, From Date)", dup_keys, "Remove duplicates (none found; check kept in pipeline)"),
        ("Exact duplicate rows", "Complete dataset", exact_dups, "Remove exact duplicates (none found)"),
        ("Incorrect data type", "From Date / To Date", n_rows, "Parse text to timestamp (yyyy-MM-dd HH:mm:ss)"),
        ("Incorrect data type", "All measurement columns", n_rows, "Read as text (layouts differ), cast to double after mapping"),
        ("Special symbols / non-numeric text", "Measurement columns", rq["non_numeric_tokens"], "Cast with try_cast; non-numeric -> null"),
        ("Blank / padded strings", "station_location, city (metadata)", padded, "Trim whitespace"),
        ("Inconsistent labels", "Column headers (files using a non-standard label)", variant_uses, f"Map all {label_variants} raw labels to 17 canonical names (e.g. 'WD (deg)' vs 'WD (degree)', 'Temp' vs 'AT')"),
        ("Different measurement units", "CO, NO, NOx, NH3, O3, Benzene", converted_files, "Convert to one unit per pollutant (e.g. CO ug/m3 x 0.001 -> mg/m3)"),
        ("Invalid numerical values", "Pollutants (<= 0)", sum(nonpos.values()), "Set to null (concentrations must be positive)"),
        ("Outliers (physically impossible)", "Pollutants and weather", rule("R4", "values_nulled"), "Set values outside plausible ranges to null"),
        ("Unreliable observations", "PM2.5 / PM10 stuck sensors", rule("R5", "values_nulled"), "Null runs of identical values lasting >= 24 h"),
        ("Inconsistent observations", "PM2.5 > PM10", rule("R6", "values_nulled") // 2, "Null both values for that hour"),
        ("Impossible dates", "From Date", rq["bad_from_date"] + rq["out_of_range_date"], "None found (all timestamps parse and fall in 2010-2023)"),
        ("Inconsistent time window", "To Date != From Date + 1 h", rq["bad_to_date"], "Drop To Date column (redundant)"),
        ("Unnecessary columns", ", ".join(sorted({c.split(" (")[0] for c in dropped_cols.raw_column}))[:80], int(dropped_cols.files.sum()), "Drop columns not used in the analysis"),
    ]
    dq_df = pd.DataFrame(dq, columns=["issue", "variable", "records", "proposed_treatment"])
    save_pdf(dq_df, "data_quality_assessment")
    save_pdf(pd.DataFrame(DICTIONARY, columns=["raw_variable", "canonical_name", "description",
                                               "data_type", "unit_or_category", "role"]), "data_dictionary")

    total_bytes = sum(s for _, s in raw_files)
    profile = [
        ("Dataset name", "Time Series Air Quality Data of India (2010-2023)"),
        ("Source organisation", "Central Pollution Control Board (CPCB), Govt. of India - compiled and published on Kaggle by Abhishek Jha"),
        ("Source link", SOURCE_URL + "  (original data: https://airquality.cpcb.gov.in/ccr)"),
        ("Access date", ACCESS_DATE),
        ("Number of files", f"{len(raw_files)} ({len(station_files)} station files + stations_info.csv)"),
        ("File format", "CSV (comma-delimited, UTF-8, header row); delivered as one ZIP archive"),
        ("File size", f"{total_bytes / 1e9:.2f} GB uncompressed ({(TABLES_DIR.parent.parent / '02_Raw_Dataset' / 'archive.zip').stat().st_size / 1e6:.0f} MB zipped)"),
        ("Number of rows", f"{n_rows:,} hourly records (+ 453 metadata rows)"),
        ("Number of columns", f"{int(cm.raw_column.nunique()) } distinct raw column names; 12-35 per file; 17 measurements kept"),
        ("Time period", f"{str(mm['ts_min'])[:10]} to {str(mm['ts_max'])[:10]}"),
        ("Geographic coverage", f"India: {stations.select('state').distinct().count()} states/UTs, {stations.select('city').distinct().count()} cities, {stations.count()} stations"),
        ("Unit of observation", "Station-hour (one monitoring station, one hour)"),
        ("Update frequency", "Source: continuous (15-min / hourly); this snapshot: static, last updated 9 June 2023"),
        ("Target variable", "PM2.5 concentration and the derived daily AQI"),
        ("Licence or usage condition", "CC BY-NC-SA 4.0 (non-commercial, academic use allowed); CPCB data is public"),
    ]
    save_pdf(pd.DataFrame(profile, columns=["parameter", "details"]), "dataset_profile")
    save_pdf(pd.DataFrame([rq | {"empty_rows": empty, "dup_keys": dup_keys}]), "raw_quality_counts")
    (LOGS_DIR / "profiling_log.txt").write_text("\n".join(lines).replace(RAW_URI, "<raw>"))
    spark.stop()


if __name__ == "__main__":
    main()
