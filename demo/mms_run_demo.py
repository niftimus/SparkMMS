from pyspark.sql.functions import explode

# Read data into data frame
df = spark.read.format("com.analyticsanvil.SparkMMS").option("fileName", "./target/test-classes/com/analyticsanvil/test/PUBLIC_DVD_TRADINGLOAD_202010010000.CSV").option("maxRowsPerPartition","50000").option("minSplitFilesize","1000000").load()

# Cache dataframe
df.cache()

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

