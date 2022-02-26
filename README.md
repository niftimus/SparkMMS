# SparkMMS Reader
Custom Electricity Market Management System (MMS) CSV reader library for Apache Spark.

This library can be used to efficiently read MMS data model reports in bulk - e.g. from monthly DVDs:
(http://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2021/MMSDM_2021_08/MMSDM_Historical_Data_SQLLoader/DOCUMENTATION/Participant_Monthly_DVD.pdf)

It uses Spark's DataSource V2 API.

It reads files in AEMO's CSV format:
(https://aemo.com.au/-/media/files/market-it-systems/guide-to-csv-data-format-standard.pdf?la=en)

## Features
- Partitions large files to avoid out of memory (OOM) errors
- Supports multiple reports per file
- Supports zipped files
- Supports filter pushdown
- Supports column pruning
- Reads report schemas from input files
- Registers {report_type, report_subtype, report_version} as temporary tables

## Source data

Available from AEMO (Australian Energy Market Operator):
- [Current Reports](https://nemweb.com.au/Reports/Current)
- [Monthly archive DVDs (Zipped)](https://visualisations.aemo.com.au/aemo/nemweb/index.html#mms-data-model])

## Building

- Prerequisites:
  - Maven
  - JDK 1.8+
  - Spark 3.1.2, prebuilt for Apache Hadoop 3.2 and later (https://spark.apache.org/downloads.html)
- Clone this repository
```bash
git clone https://github.com/niftimus/SparkMMS.git
```
- Compile
```bash
cd SparkMMS
mvn install
```
- Confirm the JAR library is built:
```bash
ls -la ./target/SparkMMS-0.3-SNAPSHOT.jar
```

## Usage

- Start PySpark
```bash
# Ensure SPARK_HOME is set to the directory where Spark has been uncompressed
# export SPARK_HOME = <path_to_spark>
cd SparkMMS
$SPARK_HOME/bin/pyspark --jars ./target/SparkMMS-0.3-SNAPSHOT.jar --packages org.apache.hadoop:hadoop-azure:3.3.1,org.apache.hadoop:hadoop-aws:3.2.2
```

## Demo (within PySpark shell)
- Read in a sample file
```python
df = spark \
    .read \
    .format("com.analyticsanvil.SparkMMS") \
    .option("fileName", "./target/test-classes/com/analyticsanvil/test/PUBLIC_DVD_TRADINGLOAD_202010010000.CSV") \
    .option("maxRowsPerPartition","50000") \
    .option("minSplitFilesize","1000000") \
    .load()

```
- Show row chunks:
```python
df.show()
```
- Get a single report and show the results:
```python
# Get a new dataframe with the schema of a single report type
def getReport(df, report_type, report_subtype, report_version):
    from pyspark.sql.functions import explode
    df = df.where(f"report_type = '{report_type}' and report_subtype = '{report_subtype}' and report_version = {report_version}")
    tmpDF = df.select("column_headers", explode(df.data).alias("datarow"))
    
    colHeaders = df.select("column_headers").first().column_headers
    
    for idx, colName in enumerate(colHeaders):
        tmpDF = tmpDF.withColumn(colName, tmpDF.datarow[idx])
    
    tmpDF = tmpDF.drop("column_headers").drop("datarow")    
    
    return tmpDF

d=getReport(df, report_type = 'TRADING', report_subtype = 'UNIT_SOLUTION', report_version = 2)

d.show(20, False)
```
- Register the report as a temporary table and query using SQL:
```python
# Register all reports available in the dataframe as temporary view in the metastore
def registerAllReports(df):
    tmpDF = df.select("report_type","report_subtype","report_version")
    tmpDF = tmpDF.dropDuplicates()
    
    reports = tmpDF.collect()
    
    for r in reports:
        tmpReportDF = getReport(df,r.report_type,r.report_subtype,r.report_version)
        tmpReportDF.createOrReplaceTempView(f"{r.report_type}_{r.report_subtype}_{r.report_version}")

registerAllReports(df)

spark.sql("show tables;").show()

spark.sql("select * from TRADING_UNIT_SOLUTION_2").show()

```