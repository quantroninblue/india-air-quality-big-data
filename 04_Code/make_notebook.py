"""Step 12 - Generate and execute the Jupyter notebooks in 04_Code/notebooks:

  01_Data_Loading_and_Inspection.ipynb  environment check, loading checks, initial inspection
  02_Pipeline_Walkthrough.ipynb         HDFS, Bronze/Silver/Gold, AQI, Spark SQL, RDD, Hive

Requires the Hadoop cluster and the pipeline outputs (run ./run.sh first).
"""
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

from common import NOTEBOOK_DIR as NB_DIR
from common import ROOT

SETUP = ("import sys, subprocess; sys.path.insert(0, '..')\n"
         "from common import *\nfrom pyspark.sql import functions as F\n")


def hdfs(cmd):
    return (f"print(subprocess.run(['bash', '-c', 'source ../hadoop/hadoop_env.sh && {cmd}'],\n"
            "                     capture_output=True, text=True).stdout)")


INSPECTION = [
    ("md", "# Dataset loading and initial inspection\n"
           "CPCB *Time Series Air Quality Data of India (2010-2023)*, stored in HDFS. "
           "Covers the loading checks (Task 7) and initial inspection (Task 8)."),
    ("md", "## 1. Environment check"),
    ("code", SETUP + "spark = get_spark('inspection')\n"
             "print('Spark', spark.version, '| Hadoop', spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion())\n"
             "print('Raw data URI:', RAW_URI)"),
    ("code", hdfs("jps | sort -k2 && hdfs dfs -count -h /cpcb/raw && hdfs dfs -ls /cpcb")),
    ("md", "## 2. Load one file with header and schema inference"),
    ("code", "one = spark.read.csv(f'{RAW_URI}/DL001.csv', header=True, inferSchema=True, sep=',')\n"
             "one.printSchema()\nprint('rows:', one.count(), '| columns:', len(one.columns))"),
    ("code", "one.show(5, truncate=False)"),
    ("md", "Header, delimiter, numeric types and both date columns (timestamp) are detected correctly for a single file."),
    ("md", "## 3. Why all files cannot be loaded with one header"),
    ("code", "import ingest\ngroups = ingest.scan_headers(spark)\n"
             "print(len(groups), 'distinct header layouts across', sum(map(len, groups.values())), 'files')\n"
             "for h in list(groups)[:3]:\n    print(len(h), 'columns:', ', '.join(h[:7]), '...')"),
    ("md", "Spark applies one header positionally to every file, so the other 197 layouts would be misaligned. "
           "`ingest.py` reads each layout group with its own schema and harmonises them into one DataFrame (Bronze)."),
    ("md", "## 4. Initial inspection of the harmonised DataFrame"),
    ("code", "df = spark.read.parquet(BRONZE_DIR)\nprint('rows:', f'{df.count():,}', '| columns:', len(df.columns))\n"
             "print(df.columns)"),
    ("code", "df.printSchema()"),
    ("code", "df.orderBy('station_id', 'ts').show(10)"),
    ("code", "df.orderBy(F.col('station_id').desc(), F.col('ts').desc()).show(5)"),
    ("code", "df.select('pm25', 'pm10', 'no2', 'so2', 'co', 'o3', 'at', 'rh', 'ws').summary().show()"),
    ("code", "stations = spark.read.parquet(STATIONS_DIR)\n"
             "for c in ['state', 'city', 'agency']:\n"
             "    print(c, stations.select(c).distinct().count())\n"
             "stations.groupBy('state').count().orderBy(F.desc('count')).show(10)"),
    ("code", "df.agg(F.min('ts'), F.max('ts'), F.min('pm25'), F.max('pm25'), F.min('co'), F.max('co')).show()"),
    ("md", "## 5. Data-quality checks"),
    ("code", "nonempty = df.filter(F.coalesce(*[F.col(c) for c in MEASURES]).isNotNull())\n"
             "print('empty placeholder rows:', df.count() - nonempty.count())\n"
             "print('duplicate (station, hour):', df.count() - df.dropDuplicates(['station_id', 'ts']).count())\n"
             "nonempty.select([F.sum(F.col(c).isNull().cast('int')).alias(c) for c in MEASURES[:9]]).show()"),
    ("code", "import pandas as pd\npd.read_csv(TABLES_DIR / 'data_quality_assessment.csv')"),
    ("code", "spark.stop()"),
]

WALKTHROUGH = [
    ("md", "# CPCB air quality - pipeline walkthrough\n"
           "HDFS storage, Bronze/Silver/Gold layers, CPCB AQI, Spark SQL, RDDs and Hive."),
    ("code", SETUP + "spark = get_spark('walkthrough', hive=True)\nspark.version"),
    ("md", "## 1. Data lake in HDFS"),
    ("code", hdfs("hdfs dfs -du -h /cpcb/lake")),
    ("md", "## 2. Silver layer: cleaned, enriched, partitioned by year"),
    ("code", "silver = spark.read.parquet(SILVER_DIR)\nprint(f'{silver.count():,} clean rows')\n"
             "silver.select('station_id', 'state', 'city', 'ts', 'season', 'hour', 'pm25').show(5)"),
    ("code", "import pandas as pd\npd.read_csv(TABLES_DIR / 'cleaning_log.csv')[['rule', 'rows_after', 'rows_removed', 'values_nulled']]"),
    ("md", "## 3. Gold layer: daily CPCB AQI"),
    ("code", "daily = spark.read.parquet(GOLD_DAILY_DIR)\ndaily.createOrReplaceTempView('daily')\n"
             "daily.filter(\"city = 'Delhi' AND date = '2021-11-05'\")"
             ".select('station_id', 'pm25', 'pm10', 'co8', 'o38', 'aqi', 'aqi_category', 'prominent_pollutant').show(5)"),
    ("md", "## 4. Spark SQL: most polluted cities (RQ1)"),
    ("code", "spark.sql('''\nSELECT city, state, ROUND(AVG(pm25), 1) AS mean_pm25, ROUND(AVG(aqi)) AS mean_aqi,\n"
             "       ROUND(100 * AVG(CASE WHEN aqi > 200 THEN 1.0 ELSE 0 END), 1) AS pct_poor\n"
             "FROM daily WHERE year BETWEEN 2019 AND 2022 AND aqi IS NOT NULL\n"
             "GROUP BY city, state HAVING COUNT(DISTINCT date) >= 600 AND COUNT(pm25) >= 300\n"
             "ORDER BY mean_pm25 DESC LIMIT 10''').show()"),
    ("md", "## 5. RDD transformations on the raw HDFS files"),
    ("code", "sc = spark.sparkContext\nlines = sc.textFile(f'{RAW_URI}/DL001.csv')\n"
             "header = lines.first()\npm_idx = header.split(',').index('PM2.5 (ug/m3)')\n"
             "by_year = (lines.filter(lambda l: l != header)\n"
             "                .map(lambda l: l.split(','))\n"
             "                .filter(lambda f: f[pm_idx] not in ('', None))\n"
             "                .map(lambda f: (f[0][:4], (float(f[pm_idx]), 1)))\n"
             "                .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\n"
             "                .mapValues(lambda v: round(v[0] / v[1], 1))\n"
             "                .sortByKey())\n"
             "print(by_year.collect())\nprint(by_year.toDebugString().decode()[:600])"),
    ("code", "pd.read_csv(TABLES_DIR / 'rdd_validation.csv')"),
    ("md", "## 6. Hive tables"),
    ("code", "spark.sql('SHOW TABLES IN cpcb').show()"),
    ("code", "spark.sql('DESCRIBE FORMATTED cpcb.daily_aqi').filter(\"col_name IN ('Type','Location','Serde Library','InputFormat')\").show(truncate=False)"),
    ("code", "spark.sql('''\nSELECT state, city, mean_pm25 FROM cpcb.city_year_orc\n"
             "WHERE year = 2022 AND days >= 200 ORDER BY mean_pm25 DESC LIMIT 8''').show()"),
    ("md", "## 7. Partition pruning in the physical plan"),
    ("code", "spark.sql(\"SELECT state, AVG(pm25) FROM daily WHERE year = 2022 GROUP BY state\").explain()"),
    ("code", "spark.stop()"),
]


def build(cells, name):
    nb = nbformat.v4.new_notebook()
    nb.cells = [nbformat.v4.new_markdown_cell(src) if kind == "md" else nbformat.v4.new_code_cell(src)
                for kind, src in cells]
    ExecutePreprocessor(timeout=900, kernel_name="python3").preprocess(nb, {"metadata": {"path": str(NB_DIR)}})
    out = NB_DIR / name
    nbformat.write(nb, out)
    print(f"[notebook] {out.relative_to(ROOT)}")


def main():
    (NB_DIR / "walkthrough.ipynb").unlink(missing_ok=True)
    build(INSPECTION, "01_Data_Loading_and_Inspection.ipynb")
    build(WALKTHROUGH, "02_Pipeline_Walkthrough.ipynb")


if __name__ == "__main__":
    main()
