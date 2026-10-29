"""Step 3 - Daily AQI: silver hourly -> gold daily station table with CPCB National AQI.

CPCB NAQI (2014) method:
  - 24-hour averages for PM2.5, PM10, NO2, SO2, NH3 (need >= 16 valid hours)
  - maximum 8-hour rolling average for CO and O3 (window needs >= 6 valid hours)
  - sub-index by linear interpolation between breakpoints; AQI = max sub-index,
    reported only if >= 3 pollutants are available and one of them is PM2.5 or PM10.
"""
import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import GOLD_DAILY_DIR, MEASURES, SILVER_DIR, get_spark, save_pdf, timed

AQI_BANDS = [(0, 50), (51, 100), (101, 200), (201, 300), (301, 400), (401, 500)]
# Concentration breakpoints (lower bound of each band); last band is open-ended with the
# extrapolation span commonly used for the CPCB calculator.
BREAKPOINTS = {
    "pm25": ([0, 31, 61, 91, 121, 251], 130),
    "pm10": ([0, 51, 101, 251, 351, 431], 80),
    "no2": ([0, 41, 81, 181, 281, 401], 120),
    "so2": ([0, 41, 81, 381, 801, 1601], 800),
    "nh3": ([0, 201, 401, 801, 1201, 1801], 600),
    "co8": ([0, 1.1, 2.1, 10.1, 17.1, 34.1], 17),
    "o38": ([0, 51, 101, 169, 209, 749], 120),
}
CATEGORIES = ["Good", "Satisfactory", "Moderate", "Poor", "Very Poor", "Severe"]
MIN_HOURS_24H, MIN_HOURS_8H = 16, 6


def sub_index(col, name):
    lows, last_span = BREAKPOINTS[name]
    c = F.col(col)
    expr = None
    for i in range(len(lows)):
        b_lo = lows[i]
        b_hi = lows[i + 1] - (0.1 if name == "co8" else 1) if i + 1 < len(lows) else b_lo + last_span
        i_lo, i_hi = AQI_BANDS[i]
        val = (i_hi - i_lo) / (b_hi - b_lo) * (c - b_lo) + i_lo
        cond = c < lows[i + 1] if i + 1 < len(lows) else F.lit(True)
        expr = F.when(cond, val) if expr is None else expr.when(cond, val)
    return F.when(c.isNull(), None).otherwise(F.least(expr, F.lit(500.0)))


def main():
    spark = get_spark("cpcb-aqi")
    timings = []
    hourly = spark.read.parquet(SILVER_DIR)

    # 8-hour rolling means over the preceding 8 clock hours (range window on epoch seconds).
    epoch = F.unix_seconds(F.col("ts").cast("timestamp"))
    w8 = Window.partitionBy("station_id").orderBy("_epoch").rangeBetween(-7 * 3600, 0)
    hourly = (hourly.withColumn("_epoch", epoch)
              .withColumn("co8", F.when(F.count("co").over(w8) >= MIN_HOURS_8H, F.avg("co").over(w8)))
              .withColumn("o38", F.when(F.count("o3").over(w8) >= MIN_HOURS_8H, F.avg("o3").over(w8))))

    def mean24(c):
        return F.when(F.count(c) >= MIN_HOURS_24H, F.avg(c)).alias(c)

    keys = ["station_id", "state", "city", "date", "year", "month", "season"]
    daily = hourly.groupBy(*keys).agg(
        *[mean24(c) for c in MEASURES],
        F.max("co8").alias("co8"), F.max("o38").alias("o38"),
        F.count("*").alias("hours_reported"))

    si = {n: sub_index(n, n) for n in BREAKPOINTS}
    daily = daily.select("*", *[e.alias(f"si_{n}") for n, e in si.items()])
    si_cols = [F.col(f"si_{n}") for n in BREAKPOINTS]
    n_avail = sum(F.when(c.isNotNull(), 1).otherwise(0) for c in si_cols)
    has_pm = F.col("si_pm25").isNotNull() | F.col("si_pm10").isNotNull()
    aqi = F.when((n_avail >= 3) & has_pm, F.round(F.greatest(*si_cols)))
    daily = daily.withColumn("aqi", aqi)

    cat = None
    for (lo, hi), name in zip(AQI_BANDS, CATEGORIES):
        cond = F.col("aqi") <= hi
        cat = F.when(cond, name) if cat is None else cat.when(cond, name)
    prominent = F.when(F.col("aqi").isNotNull(), F.coalesce(*[
        F.when(F.col(f"si_{n}") == F.greatest(*si_cols), F.lit(n)) for n in BREAKPOINTS]))
    daily = daily.withColumn("aqi_category", cat).withColumn("prominent_pollutant", prominent)

    with timed("aqi: hourly -> daily gold Parquet", timings):
        daily.write.mode("overwrite").partitionBy("year").parquet(GOLD_DAILY_DIR)

    gold = spark.read.parquet(GOLD_DAILY_DIR)
    summary = gold.agg(F.count("*").alias("station_days"),
                       F.count("aqi").alias("station_days_with_aqi"),
                       F.round(F.avg("aqi"), 1).alias("mean_aqi")).toPandas()
    save_pdf(summary, "aqi_summary")
    save_pdf(pd.DataFrame(timings), "timings_aqi")
    spark.stop()


if __name__ == "__main__":
    main()
