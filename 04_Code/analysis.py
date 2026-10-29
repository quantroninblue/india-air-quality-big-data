"""Step 4 - Analysis with Spark SQL: answers the five research questions.

All heavy lifting happens in Spark; only small aggregated results are collected to
pandas and written to 05_Outputs/tables/*.csv for charts and the report.

RQ1 Where is air quality worst (cities, states, Indo-Gangetic Plain vs rest)?
RQ2 How does pollution vary by season, hour of day and year?
RQ3 How large is the Diwali effect on PM2.5?
RQ4 How did the 2020 COVID-19 lockdown change pollutant levels?
RQ5 How do meteorological conditions relate to PM2.5?
"""
import time
from functools import reduce

import pandas as pd
from pyspark.sql import functions as F

import ingest
from common import (BRONZE_DIR, GOLD_DAILY_DIR, LOGS_DIR, SILVER_DIR, STATIONS_DIR, get_spark,
                    save_pdf)

METROS = ["Delhi", "Mumbai", "Kolkata", "Chennai", "Bengaluru", "Hyderabad"]
IGP_STATES = ["Punjab", "Haryana", "Delhi", "Chandigarh", "Uttar Pradesh", "Bihar",
              "West Bengal", "Jharkhand"]
# 2015+ only: Delhi had just 1-2 PM2.5 monitors before 2015, too few for a city mean.
DIWALI = ["2015-11-11", "2016-10-30", "2017-10-19", "2018-11-07", "2019-10-27",
          "2020-11-14", "2021-11-04", "2022-10-24"]


def sql(spark, name, query):
    pdf = spark.sql(query).toPandas()
    save_pdf(pdf, name)
    return pdf


def benchmark(spark):
    """Same aggregation on raw CSV vs bronze Parquet (cold read each time)."""
    query = lambda df: (df.filter(F.col("pm25").isNotNull())
                        .groupBy("station_id", F.year("ts").alias("y"))
                        .agg(F.avg("pm25")).count())
    rows = []
    groups = ingest.scan_headers(spark)
    raw = reduce(lambda a, b: a.unionByName(b),
                 [ingest.build_group_df(spark, h, f, []) for h, f in groups.items()])
    for label, df in [("Raw CSV - 198 layouts mapped on read", raw),
                      ("Bronze Parquet - columnar + snappy", spark.read.parquet(BRONZE_DIR))]:
        t = time.perf_counter()
        query(df)
        rows.append({"source": label, "seconds": round(time.perf_counter() - t, 2)})
    save_pdf(pd.DataFrame(rows), "benchmark_csv_vs_parquet")


def main():
    spark = get_spark("cpcb-analysis")
    spark.read.parquet(SILVER_DIR).createOrReplaceTempView("hourly")
    spark.read.parquet(GOLD_DAILY_DIR).cache().createOrReplaceTempView("daily")
    spark.read.parquet(STATIONS_DIR).createOrReplaceTempView("stations")
    igp = ", ".join(f"'{s}'" for s in IGP_STATES)
    metros = ", ".join(f"'{c}'" for c in METROS)
    spark.sql(f"""CREATE OR REPLACE TEMP VIEW daily_r AS
                  SELECT *, CASE WHEN state IN ({igp}) THEN 'Indo-Gangetic Plain'
                                 ELSE 'Rest of India' END AS region FROM daily""")

    # ---------- Data overview (Volume / Velocity) ----------
    sql(spark, "coverage_by_year", """
        SELECT year, COUNT(DISTINCT station_id) AS stations, COUNT(DISTINCT city) AS cities,
               COUNT(*) AS hourly_rows
        FROM hourly GROUP BY year ORDER BY year""")

    # ---------- RQ1: spatial ----------
    sql(spark, "rq1_city_ranking", """
        SELECT city, state, COUNT(DISTINCT station_id) AS stations,
               COUNT(pm25) AS pm25_days,
               ROUND(AVG(pm25), 1) AS mean_pm25, ROUND(AVG(aqi), 0) AS mean_aqi,
               ROUND(100 * AVG(CASE WHEN aqi > 200 THEN 1.0 ELSE 0 END), 1) AS pct_days_poor_or_worse
        FROM daily WHERE year BETWEEN 2019 AND 2022 AND aqi IS NOT NULL
        GROUP BY city, state HAVING COUNT(DISTINCT date) >= 600 AND COUNT(pm25) >= 300
        ORDER BY mean_pm25 DESC""")
    sql(spark, "rq1_state_aqi_categories", """
        SELECT state, aqi_category, COUNT(*) AS station_days
        FROM daily WHERE year = 2022 AND aqi_category IS NOT NULL
        GROUP BY state, aqi_category""")
    sql(spark, "rq1_region_monthly", """
        SELECT region, month, ROUND(AVG(pm25), 1) AS mean_pm25,
               ROUND(100 * AVG(CASE WHEN aqi > 200 THEN 1.0 ELSE 0 END), 1) AS pct_poor
        FROM daily_r WHERE year BETWEEN 2019 AND 2022 GROUP BY region, month ORDER BY region, month""")
    sql(spark, "rq1_prominent_pollutant", """
        SELECT region, prominent_pollutant, COUNT(*) AS days
        FROM daily_r WHERE year BETWEEN 2019 AND 2022 AND aqi > 100
        GROUP BY region, prominent_pollutant ORDER BY region, days DESC""")

    # ---------- RQ2: temporal ----------
    sql(spark, "rq2_metro_monthly", f"""
        SELECT city, month, ROUND(AVG(pm25), 1) AS mean_pm25
        FROM daily WHERE city IN ({metros}) AND year BETWEEN 2019 AND 2022
        GROUP BY city, month ORDER BY city, month""")
    sql(spark, "rq2_diurnal_season", """
        SELECT season, hour, ROUND(AVG(pm25), 1) AS mean_pm25, ROUND(AVG(no2), 1) AS mean_no2,
               ROUND(AVG(o3), 1) AS mean_o3
        FROM hourly WHERE year BETWEEN 2019 AND 2022 GROUP BY season, hour ORDER BY season, hour""")
    sql(spark, "rq2_delhi_month_hour", """
        SELECT month, hour, ROUND(AVG(pm25), 1) AS mean_pm25
        FROM hourly WHERE city = 'Delhi' AND year BETWEEN 2018 AND 2022
        GROUP BY month, hour ORDER BY month, hour""")
    sql(spark, "rq2_metro_annual", f"""
        SELECT city, year, ROUND(AVG(pm25), 1) AS mean_pm25, COUNT(pm25) AS pm25_days
        FROM daily WHERE city IN ({metros}) AND year BETWEEN 2015 AND 2022
        GROUP BY city, year HAVING COUNT(pm25) >= 200 ORDER BY city, year""")
    # Balanced panel: only stations with >= 200 valid PM2.5 days in every year 2018-2022,
    # so the national trend is not distorted by the network expanding.
    sql(spark, "rq2_panel_trend", """
        WITH per_year AS (
            SELECT station_id, year, AVG(pm25) AS pm25, COUNT(pm25) AS n
            FROM daily WHERE year BETWEEN 2018 AND 2022 GROUP BY station_id, year),
        panel AS (
            SELECT station_id FROM per_year WHERE n >= 200
            GROUP BY station_id HAVING COUNT(*) = 5)
        SELECT year, COUNT(*) AS stations, ROUND(AVG(pm25), 1) AS mean_pm25,
               ROUND(PERCENTILE_APPROX(pm25, 0.5), 1) AS median_pm25
        FROM per_year JOIN panel USING (station_id) GROUP BY year ORDER BY year""")
    sql(spark, "rq2_weekday_no2", """
        SELECT dow, ROUND(AVG(no2), 1) AS mean_no2, ROUND(AVG(co), 2) AS mean_co,
               ROUND(AVG(pm25), 1) AS mean_pm25
        FROM hourly WHERE year BETWEEN 2019 AND 2022 GROUP BY dow ORDER BY dow""")

    # ---------- RQ3: Diwali ----------
    spark.createDataFrame([(d,) for d in DIWALI], ["diwali"]) \
        .selectExpr("CAST(diwali AS DATE) AS diwali", "YEAR(CAST(diwali AS DATE)) AS dyear") \
        .createOrReplaceTempView("diwali")
    sql(spark, "rq3_diwali_offsets", """
        SELECT CASE WHEN d.city = 'Delhi' THEN 'Delhi' ELSE 'Rest of India' END AS area,
               DATEDIFF(d.date, w.diwali) AS day_offset,
               ROUND(AVG(d.pm25), 1) AS mean_pm25, COUNT(DISTINCT w.dyear) AS years
        FROM daily d JOIN diwali w
          ON d.date BETWEEN DATE_SUB(w.diwali, 10) AND DATE_ADD(w.diwali, 10)
        WHERE w.dyear >= 2015
        GROUP BY 1, 2 ORDER BY 1, 2""")
    sql(spark, "rq3_diwali_by_year", """
        WITH city_day AS (
            SELECT w.dyear, DATEDIFF(d.date, w.diwali) AS off, AVG(d.pm25) AS pm25
            FROM daily d JOIN diwali w
              ON d.date BETWEEN DATE_SUB(w.diwali, 10) AND DATE_ADD(w.diwali, 10)
            WHERE d.city = 'Delhi' AND w.dyear >= 2015 GROUP BY w.dyear, DATEDIFF(d.date, w.diwali))
        SELECT dyear AS year,
               ROUND(AVG(CASE WHEN off BETWEEN -10 AND -4 THEN pm25 END), 1) AS baseline_pm25,
               ROUND(MAX(CASE WHEN off = 1 THEN pm25 END), 1) AS day_after_pm25,
               ROUND(MAX(CASE WHEN off = 1 THEN pm25 END)
                     / AVG(CASE WHEN off BETWEEN -10 AND -4 THEN pm25 END), 2) AS ratio
        FROM city_day GROUP BY dyear ORDER BY dyear""")
    sql(spark, "rq3_delhi_diwali_hourly", """
        SELECT CAST((UNIX_SECONDS(CAST(h.ts AS TIMESTAMP))
                     - UNIX_SECONDS(CAST(w.diwali AS TIMESTAMP))) / 3600 AS INT) AS hour_offset,
               ROUND(AVG(h.pm25), 1) AS mean_pm25
        FROM hourly h JOIN diwali w
          ON h.ts >= CAST(DATE_SUB(w.diwali, 1) AS TIMESTAMP_NTZ)
         AND h.ts <  CAST(DATE_ADD(w.diwali, 3) AS TIMESTAMP_NTZ)
        WHERE h.city = 'Delhi' AND w.dyear >= 2017
        GROUP BY 1 ORDER BY 1""")

    # ---------- RQ4: COVID-19 lockdown (25 Mar - 31 May) vs same window 2019 ----------
    spark.sql("""CREATE OR REPLACE TEMP VIEW lockdown AS
        SELECT station_id, city, year,
               AVG(pm25) AS pm25, AVG(pm10) AS pm10, AVG(no2) AS no2, AVG(co) AS co,
               AVG(so2) AS so2, AVG(o3) AS o3, COUNT(*) AS days
        FROM daily
        WHERE year IN (2019, 2020)
          AND DATE_FORMAT(date, 'MM-dd') BETWEEN '03-25' AND '05-31'
        GROUP BY station_id, city, year""")
    pols = ["pm25", "pm10", "no2", "co", "so2", "o3"]
    pct = ",\n".join(
        f"ROUND(100 * PERCENTILE_APPROX((b.{p} - a.{p}) / a.{p}, 0.5), 1) AS {p}_median_pct_change"
        for p in pols)
    sql(spark, "rq4_lockdown_change", f"""
        SELECT COUNT(*) AS stations, {pct}
        FROM lockdown a JOIN lockdown b USING (station_id)
        WHERE a.year = 2019 AND b.year = 2020 AND a.days >= 50 AND b.days >= 50""")
    sql(spark, "rq4_lockdown_metros", f"""
        SELECT a.city, COUNT(*) AS stations,
               ROUND(AVG(a.pm25), 1) AS pm25_2019, ROUND(AVG(b.pm25), 1) AS pm25_2020,
               ROUND(AVG(a.no2), 1) AS no2_2019, ROUND(AVG(b.no2), 1) AS no2_2020
        FROM lockdown a JOIN lockdown b USING (station_id)
        WHERE a.year = 2019 AND b.year = 2020 AND a.days >= 50 AND b.days >= 50
          AND a.city IN ({metros})
        GROUP BY a.city ORDER BY a.city""")

    # ---------- RQ5: meteorology ----------
    sql(spark, "rq5_correlations", """
        SELECT ROUND(CORR(pm25, ws), 3) AS pm25_vs_wind_speed,
               ROUND(CORR(pm25, at), 3) AS pm25_vs_temperature,
               ROUND(CORR(pm25, rh), 3) AS pm25_vs_humidity,
               ROUND(CORR(pm25, rf), 3) AS pm25_vs_rainfall,
               ROUND(CORR(o3, sr), 3) AS ozone_vs_solar_radiation,
               COUNT(*) AS station_days
        FROM daily WHERE year BETWEEN 2019 AND 2022""")
    sql(spark, "rq5_wind_bins", """
        SELECT season, CAST(LEAST(FLOOR(ws * 2) / 2, 4.0) AS DOUBLE) AS ws_bin,
               ROUND(AVG(pm25), 1) AS mean_pm25, COUNT(*) AS station_days
        FROM daily WHERE year BETWEEN 2019 AND 2022 AND ws IS NOT NULL AND pm25 IS NOT NULL
        GROUP BY 1, 2 ORDER BY 1, 2""")

    # Physical plan of a representative query, for the report.
    plan = spark.sql("""
        SELECT d.state, AVG(d.pm25) FROM daily d JOIN stations s USING (station_id)
        WHERE d.year = 2022 GROUP BY d.state""")._jdf.queryExecution().explainString(
        spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString("formatted"))
    (LOGS_DIR / "explain_state_pm25_2022.txt").write_text(plan)

    benchmark(spark)
    spark.stop()


if __name__ == "__main__":
    main()
