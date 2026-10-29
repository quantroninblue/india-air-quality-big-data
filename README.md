# Air Quality Across India: Big Data Analysis of CPCB Data (2010-2023)

Course assignment for the **Big Data Analytics** module of the **MSc in Data Analytics** (team of two).

The project takes 13 years of hourly air-quality readings from India's Central Pollution Control
Board (CPCB), stores them in HDFS, cleans and combines them with Apache Spark, calculates the
official National Air Quality Index (AQI) and uses the result to answer five questions:

1. Which cities, states and regions have the worst air?
2. How does pollution change with season, time of day and year?
3. How much does Diwali raise PM2.5?
4. What happened to pollution during the 2020 COVID-19 lockdown?
5. How do wind, temperature, humidity and rain relate to PM2.5?

## Dataset

[Time Series Air Quality Data of India (2010-2023)](https://www.kaggle.com/datasets/abhisheksjha/time-series-air-quality-data-of-india-2010-2023)
on Kaggle (CC BY-NC-SA 4.0), compiled from the CPCB Central Control Room portal: 453 station CSV
files plus a station metadata file, 14.3 million hourly rows, 1.7 GB. The files use 198 different
column layouts and mixed units, which is most of the cleaning work.

The data is not included in this repository. Download `archive.zip` from Kaggle and extract it to
`02_Raw_Dataset/csv/`.

## What the code does

| Step | Code | What happens |
|---|---|---|
| Load to HDFS | `04_Code/hadoop/load_raw_to_hdfs.sh` | `hdfs dfs -put` the raw CSVs into `/cpcb/raw` |
| Ingest | `04_Code/ingest.py` | Groups files by header, maps 97 raw column names to 17 standard ones, converts units, writes Parquet (Bronze) |
| Clean | `04_Code/clean.py` | Removes empty and duplicate rows, nulls impossible values and frozen-sensor runs, logs every rule (Silver) |
| AQI | `04_Code/aqi.py` | CPCB AQI per station and day with Spark window functions (Gold) |
| Analysis | `04_Code/analysis.py` | Spark SQL for the five research questions, CSV vs Parquet benchmark |
| MapReduce | `04_Code/hadoop/run_streaming_job.sh` | Hadoop Streaming job on YARN with a Python mapper and reducer |
| RDDs | `04_Code/rdd_analysis.py` | Same counts using only RDD transformations, checked against the DataFrame results |
| Hive | `04_Code/hive_tables.py` | External and managed tables (CSV SerDe, Parquet, ORC), a Python UDF, HiveQL queries |
| Profiling | `04_Code/dataset_profile.py` | Dataset profile, data dictionary, inspection tables and a data-quality table |
| Charts | `04_Code/visualize.py` | Charts generated from the result tables |

## Tech stack

Hadoop 3.4.1 (HDFS, YARN, MapReduce streaming) on a single-node cluster, Apache Spark 4.0.1
(PySpark, Spark SQL, Hive support), Parquet and ORC, Python 3.12, pandas, Matplotlib.

## Running it

```bash
python3 -m venv .venv && .venv/bin/pip install pyspark==4.0.1 pandas pyarrow matplotlib jupyter nbconvert python-pptx python-docx

# Hadoop 3.4.1 in ~/opt/hadoop-3.4.1 and a JDK 11 in ~/opt/jdk11 (see 04_Code/hadoop/hadoop_env.sh)
04_Code/hadoop/setup_hdfs.sh        # build the config and format the NameNode (first time only)
04_Code/hadoop/start_cluster.sh     # NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager
04_Code/hadoop/load_raw_to_hdfs.sh  # copy the raw CSVs into HDFS

./run.sh                            # full pipeline, about 8 minutes
./run.sh clean aqi analysis         # or individual steps
CPCB_STORAGE=local ./run.sh         # run on local files without Hadoop

04_Code/hadoop/stop_cluster.sh
```

Spark needs Java 17 or newer; the Hadoop daemons run on Java 11. The scripts that build the written
report, slides and phase documents are not published, so `./run.sh` skips them when they are missing.

## Colab notebook

[`CPCB_Air_Quality_Colab.ipynb`](CPCB_Air_Quality_Colab.ipynb) runs the whole pipeline in one notebook
(ingest, cleaning, AQI, Spark SQL for all five questions, RDDs, the MapReduce scripts, Hive tables),
with a short explanation above every code cell. It downloads the dataset itself and needs no Hadoop
cluster: Spark reads from Colab's local disk instead of HDFS.
[Open it in Colab](https://colab.research.google.com/github/quantroninblue/india-air-quality-big-data/blob/main/CPCB_Air_Quality_Colab.ipynb)
and use `Runtime > Run all` (about 20-30 minutes; set `QUICK_RUN = True` for a few-minute demo on three states).

## Main results

- 91 of 133 well-monitored cities are above India's annual PM2.5 limit of 40 ug/m3, and the 15 worst are all in north India.
- In November the Indo-Gangetic Plain averages about 2.7 times the PM2.5 of the rest of the country.
- PM2.5 is highest on winter nights; in Delhi it rises from about 180 to about 296 ug/m3 the day after Diwali.
- During the 2020 lockdown NO2 fell by about 45% and PM2.5 by about 41% compared with 2019.
- At the same 97 stations, average PM2.5 fell about 16% between 2018 and 2022.
