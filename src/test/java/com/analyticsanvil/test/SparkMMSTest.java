/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.analyticsanvil.test;

import static com.analyticsanvil.SparkMMSConstants.FILEPATH_DUDETAILSUMMARY_CSV;
import static com.analyticsanvil.SparkMMSConstants.FILEPATH_DUDETAILSUMMARY_ZIP;
import static com.analyticsanvil.SparkMMSConstants.FILEPATH_FAIL_DIFFERENT_FILENAME_IN_ZIP;
import static com.analyticsanvil.SparkMMSConstants.FILEPATH_FIELD_SEPARATOR_QUOTED_CSV;
import static com.analyticsanvil.SparkMMSConstants.FILEPATH_TRADINGLOAD_CSV;
import static com.analyticsanvil.SparkMMSConstants.FILEPATH_TWO_REPORTS_SINGLE_FILE_ZIP;
import static com.analyticsanvil.SparkMMSConstants.FILEPATH_ALL_TEST_FILES;
import static com.analyticsanvil.SparkMMSConstants.FILEPATH_TWO_REPORTS_SINGLE_FILE_ZIP2;
import static com.analyticsanvil.SparkMMSConstants.STRING_REPORT_SUBTYPE;
import static com.analyticsanvil.SparkMMSConstants.STRING_REPORT_TYPE;
import static com.analyticsanvil.SparkMMSConstants.STRING_REPORT_VERSION;
import com.analyticsanvil.SparkMMSData;
import static com.analyticsanvil.SparkMMSData.registerAllReports;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.explode;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeAll;
import static com.analyticsanvil.SparkMMSData.getReport;

/**
 *
 * @author david
 */
public class SparkMMSTest {

    private static Dataset<Row> df;
    private static SparkSession spark;
    

    public SparkMMSTest() {

    }


    /**
     * Perform setup - starts a Spark session for use by the test cases.
     * 
     * @throws Exception 
     */
    @BeforeAll
    public static void setUpClass() throws Exception {

        SparkConf conf = new SparkConf();
        
        spark = SparkSession.builder()
                .config(conf)
                .appName("Spark MMS Unit Testing Application")
                .master("local[*]").getOrCreate();

    }

    /**
     * Validate that no rows are returned if an invalid report type is selected.
     * 
     */
    @Test
    public void testPushDownNoRowsReportType() {

        df = spark.read().format("com.analyticsanvil.SparkMMS").option("fileName", FILEPATH_TRADINGLOAD_CSV).load();

        Dataset<Row> df1 = df.filter("report_type = 'TRADINGX'");
        long x = df1.count();
        assertEquals(0, x);

    }

    /**
     * Validate that no rows are returned if an invalid report ID is selected.
     * 
     */
    @Test
    public void testPushDownNoRowsReportID() {

        df = spark.read().format("com.analyticsanvil.SparkMMS").option("fileName", FILEPATH_TRADINGLOAD_CSV).load();

        Dataset<Row> df1 = df.filter("report_id = 'XXX'");
        long x = df1.count();
        assertEquals(0, x);

    }

    /**
     * Validate that the row count is correct before data rows are exploded (ie - matches the number of partitions).
     * 
     */
    @Test
    public void testPushDownNormalRows() {

        df = spark.read().format("com.analyticsanvil.SparkMMS").option("fileName", FILEPATH_TRADINGLOAD_CSV).option("maxRowsPerPartition", "50000").load();
        Dataset<Row> df1 = df.filter("report_type = 'TRADING'");
        long x = df1.count();
        assertEquals(10, x);

    }

    /**
     * Validate that the row count is correct once data rows are exploded.
     */
    @Test
    public void testPushDownDataRows() {

        df = spark.read().format("com.analyticsanvil.SparkMMS").option("fileName", FILEPATH_TRADINGLOAD_CSV).load();

        Dataset<Row> df1 = df.select(explode(df.col("data")).as("Y"));
        long x = df1.count();
        assertEquals(488304, x);

    }

    /**
     * Validate that rows can be exploded and show a sample expected value.
     */
    @Test
    public void testZipFile() {

        df = spark.read().format("com.analyticsanvil.SparkMMS").option("fileName", FILEPATH_DUDETAILSUMMARY_ZIP).load();

        Dataset<Row> df1 = df.select(explode(df.col("data")).as("X"));
        String firstVal = (String) df1.select("X").collectAsList().get(0).getList(0).get(0).toString();

        assertEquals("AGLHAL", firstVal);

    }

    /**
     * Validate row count when using getReport to get a single report.
     */
    @Test
    public void testSingleTableConversion() {

        df = spark.read().format("com.analyticsanvil.SparkMMS").option("fileName", FILEPATH_DUDETAILSUMMARY_ZIP).option("maxRowsPerPartition", "25000").load();

        Dataset<Row> df1 = SparkMMSData.getReport(df, "PARTICIPANT_REGISTRATION", "DUDETAILSUMMARY", 4);
        long x = df1.count();

        assertEquals(10106, x);
    }

    /**
     * Validate that a sample row looks correct after registering the report as a temporary table in the metastore.
     * 
     */
    @Test
    public void testGetReport() {
        df = spark.read().format("com.analyticsanvil.SparkMMS").option("fileName", FILEPATH_DUDETAILSUMMARY_CSV).option("maxRowsPerPartition", "50000").load();
        Dataset<Row> df1 = SparkMMSData.getReport(df, "PARTICIPANT_REGISTRATION", "DUDETAILSUMMARY", 4);
        
        String x = (String) df1.collectAsList().get(0).mkString(",");
        assertEquals("AGLHAL,\"2001/12/18 00:00:00\",\"2002/04/01 00:00:00\",GENERATOR,SHPS1,SA1,AGLHAL,SOLARIS,\"2016/06/21 13:42:04\",0.9946,FAST,1,-994.60,4973,NON-SCHEDULED,0,0,null,null,null", x);

    }

    /**
     * Validate that rowcount is expected after registering a report as a temporary table in the metastore.
     * 
     */
    @Test
    public void testAllReports() {
        df = spark.read().format("com.analyticsanvil.SparkMMS").option("fileName", FILEPATH_TRADINGLOAD_CSV).option("maxRowsPerPartition", "50000").load();
        registerAllReports(df);

        spark.sql("show tables;").show(false);
        spark.sql("select * from trading_unit_solution_2 limit 10;").show();
        long rowcount = (long) spark.sql("select count(*) from trading_unit_solution_2 limit 10;").collectAsList().get(0).get(0);
        assertEquals(488304L,rowcount);
    }

    /**
     * Validate correct error handling if zip does not contain CSV with expected name (should match zip, excluding the extension)
     * 
     */
    @Test
    public void testFailDifferentFilenameInZip() {
        df = spark.read().format("com.analyticsanvil.SparkMMS").option("fileName", FILEPATH_FAIL_DIFFERENT_FILENAME_IN_ZIP).option("maxRowsPerPartition", "50000").load();

        // TODO: Update exception handling for zipfiles
        // Current test checks for failure if there is no CSV with the expected name in the zip file
        assertThrows(NullPointerException.class, () -> {
            df.show();
        });

    }
    
    /**
     * Validate multiple reports can be read from a zip file containing a single CSV.
     */
    @Test
    public void testRegisterFileContainingTwoReports()
    {
        
        df = spark.read().format("com.analyticsanvil.SparkMMS").option("fileName", FILEPATH_TWO_REPORTS_SINGLE_FILE_ZIP).option("maxRowsPerPartition", "50000").load();
        Dataset<Row> df1 = getReport(df,"TRADING","REGIONSUM",4);
        Dataset<Row> df2 = getReport(df,"TRADING","PRICE",2);
        
        long df1count = df1.count();
        long df2count = df2.count();
        
        assertEquals(2, df1count);
        assertEquals(5, df2count);        
        
    }
    
    /**
    * Validate line split with string containing commas (field separator in quoted field).
    */
    @Test
    public void testFieldSeparatorInQuotedField()
    {
        
        df = spark.read().format("com.analyticsanvil.SparkMMS").option("fileName", FILEPATH_FIELD_SEPARATOR_QUOTED_CSV).option("maxRowsPerPartition", "50000").load();
        Dataset<Row> df1 = getReport(df,"MARKET_NOTICE","MARKETNOTICETYPE",1);
        
        long df1count = df1.count();
        String field_check = (String) df1.filter("TYPEID = '\"NEM SYSTEMS\"'").select("DESCRIPTION").collectAsList().get(0).getString(0);
        
        assertEquals(32, df1count);
        assertEquals("\"MMS, EMS, SCADA, IT, BIDDING\"", field_check);
        
    }

    /**
    * Validate pushdown filter where there are multiple report types in the read path.
    */
    @Test
    public void testMultipleReportsInReadPath()
    {
        Row firstRow;
        df = spark.read().format("com.analyticsanvil.SparkMMS").option("fileName", FILEPATH_TWO_REPORTS_SINGLE_FILE_ZIP2).option("maxRowsPerPartition", "50000").load();
        Dataset<Row> df1 = df.where(STRING_REPORT_TYPE+"='OFFER' and " + STRING_REPORT_SUBTYPE+"='BIDDAYOFFER' and " + STRING_REPORT_VERSION+"=2");
        df1 = df1.select(STRING_REPORT_TYPE,STRING_REPORT_SUBTYPE,STRING_REPORT_VERSION).dropDuplicates();
        df1.show();
        
        // Get count of distinct reports
        long df1count = df1.count();
        
        firstRow = df1.collectAsList().get(0);
        String field_check1 = firstRow.getString(0);
        String field_check2 = firstRow.getString(1);
        int field_check3 = firstRow.getInt(2);
        
        assertEquals(1, df1count);
        assertEquals("OFFER", field_check1);
        assertEquals("BIDDAYOFFER", field_check2);
        assertEquals(2, field_check3);
    }
    

}
