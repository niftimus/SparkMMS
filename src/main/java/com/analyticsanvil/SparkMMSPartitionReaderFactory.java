/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.analyticsanvil;

/**
 *
 * @author david
 */
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.sql.sources.Filter;

public class SparkMMSPartitionReaderFactory implements PartitionReaderFactory {

    private final StructType schema;
    private final String filePath;
    private final int startRow;
    private final int maxRowsPerPartition;
    private final Filter[] pushedFilters;
    private final boolean excludeReports;
    private final boolean excludeData;
    private final boolean splitFile;
    private static final Logger logger = Logger.getLogger(SparkMMSPartitionReaderFactory.class.getName());

    public SparkMMSPartitionReaderFactory(StructType schema, String fileName, int startRow, int maxRowsPerPartition, Filter[] pushedFilters, boolean excludeReports, boolean excludeData, boolean splitFile) {

        this.schema = schema;
        this.filePath = fileName;
        this.startRow = startRow;
        this.maxRowsPerPartition = maxRowsPerPartition;
        this.pushedFilters = pushedFilters;
        this.excludeReports = excludeReports;
        this.excludeData = excludeData;
        this.splitFile = splitFile;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
        try {
            return new SparkMMSPartitionReader((SparkMMSInputPartition) partition, schema, pushedFilters, excludeReports, excludeData, splitFile);
        } catch (FileNotFoundException | URISyntaxException e) {
            logger.log(Level.SEVERE, null, e);
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        return null;
    }
}
