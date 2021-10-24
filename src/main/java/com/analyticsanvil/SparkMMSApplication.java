package com.analyticsanvil;

import static com.analyticsanvil.SparkMMSConstants.FILEPATH_TRADINGLOAD_CSV;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static com.analyticsanvil.SparkMMSData.registerAllReports;
import java.util.List;
import static org.apache.spark.sql.functions.spark_partition_id;

// Sample Spark Java application for testing
public class SparkMMSApplication {
    
    public static void main(String[] args) {
        SparkMMSApplication app = new SparkMMSApplication();
        app.start();
    }

    private boolean start() {
        SparkConf conf = new SparkConf();

        // Create Spark session
        SparkSession spark = SparkSession.builder()
                .config(conf)
                .appName("Spark MMS Example Application")
                .master("local[*]").getOrCreate();

        // Set log level to warning to suppress information messages
        spark.sparkContext().setLogLevel("WARN");
        
        // Create dataframe 
        Dataset<Row> df;        
    
        // Load file
        df = spark.read().format("com.analyticsanvil.SparkMMS").option("fileName", FILEPATH_TRADINGLOAD_CSV).option("maxRowsPerPartition", "50000").option("minSplitFilesize","1000000").load();
        
        // Validate raw dataframe output
        df.show();

        // Validate partition count
        System.out.println("Partition count is: " + df.rdd().getNumPartitions());
        System.out.println();
        
        // Register temporary view
        registerAllReports(df);

        // Validate that temporary view is created
        spark.sql("show tables;").show(false);

        // Validate rows are returned from sample query
        spark.sql("select * from trading_unit_solution_2 limit 10;").show();

        // Validate rowcount
        long rowcount = (long) spark.sql("select count(*) from trading_unit_solution_2 limit 10;").collectAsList().get(0).get(0);
        System.out.println("Row count is: " + Long.toString(rowcount));
        System.out.println();
        
        // Validate rowcount per partition
        List<Row> rowList = spark.sql("select * from trading_unit_solution_2").groupBy(spark_partition_id().alias("PartitionID")).count().orderBy("PartitionID").collectAsList();        
        rowList.forEach(r -> {
            System.out.println("Partition ID " + Integer.toString(r.getInt(0)) + " contains: " + Long.toString(r.getLong(1)));
        });
        System.out.println();
        
        return true;
    }

}
