"""Step 2 - Clean: bronze hourly -> silver hourly (Parquet partitioned by year).

Rules (each logged with the number of rows / values affected):
  R1 drop duplicate (station_id, ts) readings
  R2 drop placeholder rows where every measurement is null
  R3 null non-positive pollutant concentrations
  R4 null values outside physically plausible ranges
  R5 null PM2.5/PM10 "stuck sensor" runs (same value for >= 24 consecutive hours)
  R6 null PM2.5 and PM10 where PM2.5 > PM10 (physically inconsistent pair)
  R7 drop rows left with no pollutant value at all
"""
import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import (BRONZE_DIR, MEASURES, MET, POLLUTANTS, SILVER_DIR, STATIONS_DIR,
                    get_spark, save_pdf, timed)

# Plausible upper bounds (pollutants: lower bound is > 0, handled by R3).
UPPER = {"pm25": 1000, "pm10": 2000, "no": 1000, "no2": 1000, "nox": 1000, "nh3": 1000,
         "so2": 1000, "co": 50, "o3": 1000, "benzene": 500, "toluene": 1000}
MET_RANGE = {"at": (-10, 55), "rh": (1, 100), "ws": (0, 40), "wd": (0, 360),
             "rf": (0, 500), "sr": (0, 1500)}
STUCK_HOURS = 24


def nonnull_counts(df, cols):
    return df.agg(*[F.count(c).alias(c) for c in cols]).first().asDict()


def null_stuck_runs(df, col):
    w = Window.partitionBy("station_id").orderBy("ts")
    prev = F.lag(col).over(w)
    changed = F.when(F.col(col).isNull() | prev.isNull() | (F.col(col) != prev), 1).otherwise(0)
    df = df.withColumn("_chg", changed).withColumn("_run", F.sum("_chg").over(w))
    run_len = F.count(F.lit(1)).over(Window.partitionBy("station_id", "_run"))
    df = df.withColumn(col, F.when(F.col(col).isNotNull() & (run_len >= STUCK_HOURS), None)
                       .otherwise(F.col(col)))
    return df.drop("_chg", "_run")


def season_expr(month):
    return (F.when(month.isin(12, 1, 2), "Winter")
            .when(month.isin(3, 4, 5), "Summer")
            .when(month.isin(6, 7, 8, 9), "Monsoon")
            .otherwise("Post-monsoon"))


def main():
    spark = get_spark("cpcb-clean")
    timings, log = [], []
    bronze = spark.read.parquet(BRONZE_DIR)

    n0 = bronze.count()
    log.append({"rule": "R0 raw hourly rows", "rows_after": n0, "rows_removed": 0, "values_nulled": 0})

    df = bronze.filter(F.col("ts").isNotNull()).dropDuplicates(["station_id", "ts"])
    n1 = df.count()
    log.append({"rule": "R1 duplicate (station, hour) rows", "rows_after": n1,
                "rows_removed": n0 - n1, "values_nulled": 0})

    df = df.filter(F.coalesce(*[F.col(c) for c in MEASURES]).isNotNull()).cache()
    n2 = df.count()
    log.append({"rule": "R2 empty placeholder rows (all measures null)", "rows_after": n2,
                "rows_removed": n1 - n2, "values_nulled": 0})
    raw_nonnull = nonnull_counts(df, MEASURES)

    def value_rule(name, new_df, prev_counts):
        counts = nonnull_counts(new_df, MEASURES)
        nulled = {c: prev_counts[c] - counts[c] for c in MEASURES}
        log.append({"rule": name, "rows_after": n2, "rows_removed": 0,
                    "values_nulled": sum(nulled.values()),
                    **{f"nulled_{c}": v for c, v in nulled.items() if v}})
        return counts

    with timed("clean: value rules", timings):
        df3 = df.select("station_id", "ts", *[
            F.when(F.col(c) > 0, F.col(c)).alias(c) if c in POLLUTANTS else F.col(c)
            for c in MEASURES])
        c3 = value_rule("R3 non-positive pollutant values", df3, raw_nonnull)

        def in_range(c):
            lo, hi = (0, UPPER[c]) if c in UPPER else MET_RANGE[c]
            return F.when(F.col(c).between(lo, hi), F.col(c)).alias(c)
        df4 = df3.select("station_id", "ts", *[in_range(c) for c in MEASURES]).cache()
        c4 = value_rule("R4 out-of-range values", df4, c3)

        df5 = null_stuck_runs(null_stuck_runs(df4, "pm25"), "pm10").cache()
        c5 = value_rule(f"R5 stuck-sensor runs (>= {STUCK_HOURS} h constant PM)", df5, c4)
        df4.unpersist()

        bad_pair = F.col("pm25") > F.col("pm10")
        df6 = df5.withColumn("_bad", bad_pair) \
                 .withColumn("pm25", F.when(F.col("_bad"), None).otherwise(F.col("pm25"))) \
                 .withColumn("pm10", F.when(F.col("_bad"), None).otherwise(F.col("pm10"))) \
                 .drop("_bad")
        value_rule("R6 PM2.5 > PM10 inconsistent pairs", df6, c5)

    df7 = df6.filter(F.coalesce(*[F.col(c) for c in POLLUTANTS]).isNotNull())
    stations = spark.read.parquet(STATIONS_DIR).select("station_id", "state", "city")
    silver = (df7.join(F.broadcast(stations), "station_id", "left")
              .withColumn("date", F.to_date("ts"))
              .withColumn("year", F.year("ts"))
              .withColumn("month", F.month("ts"))
              .withColumn("hour", F.hour("ts"))
              .withColumn("dow", F.dayofweek("ts"))
              .withColumn("season", season_expr(F.month("ts"))))

    with timed("clean: write silver Parquet (partitioned by year)", timings):
        silver.repartition("year").write.mode("overwrite").partitionBy("year").parquet(SILVER_DIR)
    df.unpersist()
    df5.unpersist()

    out = spark.read.parquet(SILVER_DIR)
    n7 = out.count()
    log.append({"rule": "R7 rows with no valid pollutant left", "rows_after": n7,
                "rows_removed": n2 - n7, "values_nulled": 0})
    final_nonnull = nonnull_counts(out, MEASURES)

    save_pdf(pd.DataFrame(log).fillna(0), "cleaning_log")
    save_pdf(pd.DataFrame([{"measure": c,
                            "pct_available_raw": round(100 * raw_nonnull[c] / n2, 2),
                            "pct_available_clean": round(100 * final_nonnull[c] / n7, 2)}
                           for c in MEASURES]), "completeness")
    save_pdf(pd.DataFrame(timings), "timings_clean")
    spark.stop()


if __name__ == "__main__":
    main()
