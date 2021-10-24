/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.analyticsanvil;

import static com.analyticsanvil.SparkMMSConstants.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.element_at;
import static org.apache.spark.sql.functions.explode;

/**
 *
 * @author david
 */
public class SparkMMSData {

    /**
     * Get a dataframe containing a single MMS report only - i.e. single
     * combination of {report_type, report_subtype, report_version}. This
     * filters an input SparkMMS DataFrame and expands all columns for the
     * report with their appropriate headers.
     *
     * @param inputDF
     * @param report_type
     * @param report_subtype
     * @param report_version
     * @return
     */
    public static Dataset<Row> getReport(Dataset<Row> inputDF, String report_type, String report_subtype, int report_version) {

        Dataset<Row> outputDF;

        outputDF = inputDF.filter(STRING_REPORT_TYPE + " = '" + report_type + "' AND " + STRING_REPORT_SUBTYPE + " = '" + report_subtype + "' AND " + STRING_REPORT_VERSION + " = '" + report_version + "'");
        outputDF = outputDF.select(
                outputDF.col(STRING_COLUMN_HEADERS),
                explode(outputDF.col(STRING_DATA)).as("datarow")
        );

        String[] colHeadersArray;
        
        // Get the column headers for this report
        colHeadersArray = (String[]) outputDF.select(STRING_COLUMN_HEADERS).takeAsList(1).get(0).getList(0).toArray(new String[0]);
        ArrayList<String> colHeaders = new ArrayList<>(Arrays.asList(colHeadersArray));

        // Rename the data column names to the respective column names
        for (int i = 0; i < colHeaders.size(); i++) {
            outputDF = outputDF.withColumn(colHeaders.get(i), element_at(outputDF.col("datarow"), i + 1));
        }

        outputDF = outputDF.drop(STRING_COLUMN_HEADERS).drop("datarow");
        return outputDF;

    }

    /**
     * Register all MMS reports as new temporary tables in the metastore.
     *
     * @param df Input DataFrame. This input should be a DataFrame output by
     * SparkMMS.
     */
    public static void registerAllReports(Dataset<Row> df) {

        List<Row> distinctReports = df.select(STRING_REPORT_TYPE, STRING_REPORT_SUBTYPE, STRING_REPORT_VERSION).dropDuplicates().collectAsList();

        String tmpReportType;
        String tmpReportSubtype;
        int tmpReportVersion;
        Dataset<Row> tmpDF;

        for (Row r : distinctReports) {
            tmpReportType = r.getString(0);
            tmpReportSubtype = r.getString(1);
            tmpReportVersion = r.getInt(2);
            tmpDF = getReport(df, tmpReportType, tmpReportSubtype, tmpReportVersion);

            tmpDF.createOrReplaceTempView(tmpReportType + "_" + tmpReportSubtype + "_" + tmpReportVersion);

        }

    }

}
