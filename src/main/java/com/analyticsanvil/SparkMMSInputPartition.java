/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.analyticsanvil;

import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.sources.Filter;

public class SparkMMSInputPartition implements InputPartition {

    private final String filename;
    private final int startRow;
    private final int maxRowsPerPartition;
    private final Filter[] pushedFilters;
    private final boolean excludeReports;
    private final boolean excludeData;
    private final boolean splitFile;

    public SparkMMSInputPartition(String filename, int startRow, int maxRowsPerPartition, Filter[] pushedFilters, boolean excludeReports, boolean excludeData, boolean splitFile) {
        this.filename = filename;
        this.startRow = startRow;
        this.maxRowsPerPartition = maxRowsPerPartition;
        this.pushedFilters = pushedFilters;
        this.excludeReports = excludeReports;
        this.excludeData = excludeData;
        this.splitFile = splitFile;
    }

    @Override
    public String[] preferredLocations() {
        return new String[0];
    }

    public int getStartRow() {
        return startRow;
    }

    public int getMaxRowsPerPartition() {
        return maxRowsPerPartition;
    }

    public String getFilename() {
        return filename;
    }

    public Filter[] getPushedFilters() {
        return pushedFilters;
    }

}
