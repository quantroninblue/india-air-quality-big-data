"""Step 7 - Hive layer: HiveQL tables over the HDFS data lake (Spark SQL with Hive support).

  - database `cpcb` with its warehouse on HDFS and a persistent (Derby) metastore
  - EXTERNAL tables: raw station metadata CSV (OpenCSVSerde, header skipped),
    partitioned daily AQI table (STORED AS PARQUET + MSCK REPAIR), hourly datasource table
  - MANAGED tables built with CTAS in two storage formats (Parquet and ORC)
  - a Python user-defined function used from HiveQL
  - HiveQL joins, subqueries, window ranking and DISTRIBUTE BY / SORT BY
"""
import pandas as pd

from common import (GOLD_DAILY_DIR, HDFS_URI, LOGS_DIR, SILVER_DIR, STORAGE, WAREHOUSE_URI,
                    disk_usage, get_spark, save_pdf)

REF_URI = f"{HDFS_URI}/cpcb/reference/stations_info" if STORAGE == "hdfs" else None

HEALTH_ADVICE = {  # CPCB AQI health-impact statements (abridged)
    "Good": "Minimal impact",
    "Satisfactory": "Minor breathing discomfort to sensitive people",
    "Moderate": "Breathing discomfort to people with lung or heart disease, children and older adults",
    "Poor": "Breathing discomfort to most people on prolonged exposure",
    "Very Poor": "Respiratory illness on prolonged exposure",
    "Severe": "Affects healthy people and seriously impacts those with existing diseases",
}


def health_advice(category):
    return HEALTH_ADVICE.get(category)


def hive_type(spark_type):
    return {"string": "STRING", "double": "DOUBLE", "date": "DATE", "int": "INT",
            "bigint": "BIGINT"}[spark_type]


def run(spark, log, title, statement, show=True, n=20):
    log.append(f"-- {title}\n{statement.strip()};\n")
    df = spark.sql(statement)
    if show:
        pdf = df.limit(n).toPandas()
        log.append(pdf.to_string(index=False, max_colwidth=60) + "\n")
        return pdf
    return None


def main():
    spark = get_spark("cpcb-hive", hive=True)
    spark.udf.register("health_advice", health_advice)
    log = []

    run(spark, log, "database", "DROP DATABASE IF EXISTS cpcb CASCADE", show=False)
    run(spark, log, "database", f"CREATE DATABASE cpcb LOCATION '{WAREHOUSE_URI}/cpcb.db'", show=False)
    spark.sql("USE cpcb")

    # --- EXTERNAL table over the raw metadata CSV (text file + CSV SerDe) ---------------
    ref = REF_URI or SILVER_DIR.rsplit("/", 1)[0] + "/../02_Raw_Dataset/csv"
    run(spark, log, "external table on raw CSV", f"""
        CREATE EXTERNAL TABLE stations_raw (
            file_name STRING, state STRING, city STRING, agency STRING,
            station_location STRING, start_month STRING, start_month_num STRING, start_year STRING)
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        STORED AS TEXTFILE
        LOCATION '{ref}'
        TBLPROPERTIES ('skip.header.line.count' = '1')""", show=False)

    # --- EXTERNAL partitioned Parquet table over the Gold layer --------------------------
    gold_schema = spark.read.parquet(GOLD_DAILY_DIR).schema
    cols = ",\n            ".join(f"{f.name} {hive_type(f.dataType.simpleString())}"
                                   for f in gold_schema.fields if f.name != "year")
    run(spark, log, "external partitioned table on gold Parquet", f"""
        CREATE EXTERNAL TABLE daily_aqi (
            {cols})
        PARTITIONED BY (year INT)
        STORED AS PARQUET
        LOCATION '{GOLD_DAILY_DIR}'""", show=False)
    run(spark, log, "register partitions", "MSCK REPAIR TABLE daily_aqi", show=False)
    run(spark, log, "partitions", "SHOW PARTITIONS daily_aqi")

    # --- Hourly Silver layer as a Spark datasource (unmanaged) table ---------------------
    run(spark, log, "unmanaged datasource table on silver Parquet",
        f"CREATE TABLE hourly USING PARQUET LOCATION '{SILVER_DIR}'", show=False)
    run(spark, log, "register partitions", "ALTER TABLE hourly RECOVER PARTITIONS", show=False)

    # --- MANAGED tables (CTAS) in two storage formats -----------------------------------
    summary_sql = """
        SELECT d.state, d.city, d.year, COUNT(*) AS days, ROUND(AVG(d.pm25), 1) AS mean_pm25,
               ROUND(AVG(d.aqi), 0) AS mean_aqi,
               SUM(CASE WHEN d.aqi > 200 THEN 1 ELSE 0 END) AS poor_days
        FROM daily_aqi d WHERE d.aqi IS NOT NULL
        GROUP BY d.state, d.city, d.year"""
    run(spark, log, "managed table, Parquet",
        f"CREATE TABLE city_year_parquet STORED AS PARQUET AS {summary_sql}", show=False)
    run(spark, log, "managed table, ORC",
        f"CREATE TABLE city_year_orc STORED AS ORC AS {summary_sql}", show=False)
    run(spark, log, "tables", "SHOW TABLES")

    catalog = []
    for t in ["stations_raw", "daily_aqi", "hourly", "city_year_parquet", "city_year_orc"]:
        info = spark.sql(f"DESCRIBE FORMATTED {t}").toPandas()
        get = lambda key: next((v.strip() for k, v in zip(info.col_name, info.data_type)
                                if k.strip() == key), "")
        location = get("Location")
        catalog.append({"table": t, "type": get("Type"), "provider": get("Provider"),
                        "serde": get("Serde Library").rsplit(".", 1)[-1],
                        "input_format": get("InputFormat").rsplit(".", 1)[-1],
                        "location": location.replace(HDFS_URI, "hdfs://"),
                        "bytes": disk_usage(spark, location)})
    save_pdf(pd.DataFrame(catalog), "hive_catalog")

    # --- HiveQL queries ------------------------------------------------------------------
    q1 = run(spark, log, "Q1 join + subquery: cities above the 2022 national mean", """
        SELECT c.city, c.state, c.mean_pm25, s.n_stations
        FROM city_year_parquet c
        JOIN (SELECT TRIM(city) AS city, COUNT(*) AS n_stations FROM stations_raw GROUP BY TRIM(city)) s
          ON c.city = s.city
        WHERE c.year = 2022 AND c.days >= 200
          AND c.mean_pm25 > (SELECT AVG(pm25) FROM daily_aqi WHERE year = 2022)
        ORDER BY c.mean_pm25 DESC""", n=15)
    save_pdf(q1, "hive_q1_cities_above_national_mean")
    q2 = run(spark, log, "Q2 window ranking: most polluted city in each state, 2022", """
        SELECT state, city, mean_pm25 FROM (
            SELECT state, city, mean_pm25,
                   RANK() OVER (PARTITION BY state ORDER BY mean_pm25 DESC) AS rnk
            FROM city_year_orc WHERE year = 2022 AND days >= 200) t
        WHERE rnk = 1 ORDER BY mean_pm25 DESC""", n=40)
    save_pdf(q2, "hive_q2_top_city_per_state")
    q3 = run(spark, log, "Q3 Python UDF: Delhi 2022 days by AQI category with health advice", """
        SELECT aqi_category, COUNT(*) AS station_days, health_advice(aqi_category) AS advice
        FROM daily_aqi WHERE year = 2022 AND city = 'Delhi' AND aqi_category IS NOT NULL
        GROUP BY aqi_category ORDER BY MIN(aqi)""")
    save_pdf(q3, "hive_q3_udf_health_advice")
    run(spark, log, "Q4 DISTRIBUTE BY / SORT BY (Hive-style per-reducer sort)", """
        SELECT state, city, year, mean_pm25 FROM city_year_parquet
        WHERE year = 2022 DISTRIBUTE BY state SORT BY state, mean_pm25 DESC""", n=12)
    q5 = run(spark, log, "Q5 hourly table: Delhi monthly NO2 2020 (lockdown dip)", """
        SELECT month, ROUND(AVG(no2), 1) AS mean_no2 FROM hourly
        WHERE year = 2020 AND city = 'Delhi' GROUP BY month ORDER BY month""")
    save_pdf(q5, "hive_q5_delhi_no2_2020")

    (LOGS_DIR / "hive_session_log.sql").write_text(
        "\n".join(log).replace(HDFS_URI, "hdfs://localhost:9000"))
    spark.stop()


if __name__ == "__main__":
    main()
