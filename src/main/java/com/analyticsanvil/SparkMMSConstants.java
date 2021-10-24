/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.analyticsanvil;

import java.io.File;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Spark MMS constants (sample filenames, field names, schema and settings). 
 * 
 * @author david
 */
public final class SparkMMSConstants {
    // Column names
    public static final String STRING_SYSTEM = "system";
    public static final String STRING_REPORT_ID = "report_id";
    public static final String STRING_REPORT_FROM = "report_from";
    public static final String STRING_REPORT_TO = "report_to";
    public static final String STRING_PUBLISH_DATETIME = "publish_datetime";
    public static final String STRING_ID1 = "id1";
    public static final String STRING_ID2 = "id2";
    public static final String STRING_ID3 = "id3";
    public static final String STRING_REPORT_TYPE = "report_type";
    public static final String STRING_REPORT_SUBTYPE = "report_subtype";
    public static final String STRING_REPORT_VERSION = "report_version";
    public static final String STRING_COLUMN_HEADERS = "column_headers";
    public static final String STRING_DATA = "data";
    public static final String STRING_ORIGINAL_FILENAME = "original_filename";
    
    // Test files
    public static final String FILEPATH_TRADINGLOAD_CSV = new File("src/test/resources/com/analyticsanvil/test/PUBLIC_DVD_TRADINGLOAD_202010010000.CSV").getAbsolutePath();
    public static final String FILEPATH_DUDETAILSUMMARY_CSV = new File("src/test/resources/com/analyticsanvil/test/PUBLIC_DVD_DUDETAILSUMMARY_202010010000.CSV").getAbsolutePath();
    public static final String FILEPATH_DUDETAILSUMMARY_ZIP = new File("src/test/resources/com/analyticsanvil/test/PUBLIC_DVD_DUDETAILSUMMARY_202010010000.zip").getAbsolutePath();
    public static final String FILEPATH_FAIL_DIFFERENT_FILENAME_IN_ZIP = new File("src/test/resources/com/analyticsanvil/test/PUBLIC_DVD_AUCTION_CALENDAR_202010010000_DifferentFilenameInZip").getAbsolutePath();
    public static final String FILEPATH_TWO_REPORTS_SINGLE_FILE_ZIP = new File("src/test/resources/com/analyticsanvil/test/PUBLIC_DVD_TRADINGREGIONSUM_TRADINGREGIONPRICE_MultipleReportsInFile.zip").getAbsolutePath();
    public static final String WASBPATH_ALL_CSV = "wasb://data@storageemulator/*.CSV";
    
    // Default schema
    public static final StructType DEFAULT_SCHEMA = new StructType(new StructField[]{
            new StructField(STRING_ORIGINAL_FILENAME, DataTypes.StringType, false, Metadata.empty()),
            new StructField(STRING_SYSTEM, DataTypes.StringType, false, Metadata.empty()),
            new StructField(STRING_REPORT_ID, DataTypes.StringType, false, Metadata.empty()),
            new StructField(STRING_REPORT_FROM, DataTypes.StringType, false, Metadata.empty()),
            new StructField(STRING_REPORT_TO, DataTypes.StringType, false, Metadata.empty()),
            new StructField(STRING_PUBLISH_DATETIME, DataTypes.TimestampType, false, Metadata.empty()),
            new StructField(STRING_ID1, DataTypes.StringType, false, Metadata.empty()),
            new StructField(STRING_ID2, DataTypes.StringType, false, Metadata.empty()),
            new StructField(STRING_ID3, DataTypes.StringType, false, Metadata.empty()),
            new StructField(STRING_REPORT_TYPE, DataTypes.StringType, false, Metadata.empty()),
            new StructField(STRING_REPORT_SUBTYPE, DataTypes.StringType, false, Metadata.empty()),
            new StructField(STRING_REPORT_VERSION, DataTypes.IntegerType, false, Metadata.empty()),
            new StructField(STRING_COLUMN_HEADERS, DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty()),
            new StructField(STRING_DATA, DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.StringType)), false, Metadata.empty())
        }
        );
    
    // This is the maximum rowcount per partition
    static final int MMS_MAX_ROWS_DEFAULT = 50000;
    // This is the default minimum filesize to split (either zip or CSV)
    static final int MMS_MIN_SPLIT_FILESIZE_DEFAULT = 1048576;
    static final int MMS_START_ROW_DEFAULT = 0;
    static final String MMS_END_OF_LINE_STRING = "[\n\r]";
}
