/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.analyticsanvil;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.sources.Filter;

public class SparkMMSPartitionReader implements PartitionReader<InternalRow> {

    private final SparkMMSInputPartition mmsInputPartition;
    private final String fileName;
    private final int startRow;
    private final int maxRowsPerPartition;
    private Iterator<Object[]> iterator = null;
    private SparkMMSReader mmsReader;
    private final List<Function> valueConverters;
    private final Filter[] pushedFilters;
    Object[] currentObject;
    private final boolean excludeReports;
    private final boolean excludeData;
    private final boolean splitFile;
    private static final Logger logger = Logger.getLogger(SparkMMSPartitionReaderFactory.class.getName());

    public SparkMMSPartitionReader(
            SparkMMSInputPartition mmsInputPartition,
            StructType schema,
            Filter[] pushedFilters,
            boolean excludeReports,
            boolean excludeData,
            boolean splitFile
    ) throws FileNotFoundException, URISyntaxException, IOException {

        this.mmsInputPartition = mmsInputPartition;
        this.fileName = mmsInputPartition.getFilename();
        this.startRow = mmsInputPartition.getStartRow();
        this.maxRowsPerPartition = mmsInputPartition.getMaxRowsPerPartition();
        this.valueConverters = ValueConverters.getConverters(schema);
        this.pushedFilters = pushedFilters;
        this.excludeReports = excludeReports;
        this.excludeData = excludeData;
        this.splitFile = splitFile;
        this.createMMSReader();
    }

    private void createMMSReader() throws URISyntaxException, FileNotFoundException, IOException {

        try {

            // Set mmsReader to a new reader pointing to the input path (single file)
            Path path = new Path(this.fileName);
            mmsReader = new SparkMMSReader(path, this.startRow, this.maxRowsPerPartition, this.pushedFilters, this.excludeReports, this.excludeData, this.splitFile);

        } catch (MalformedURLException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        
    }

    @Override
    public boolean next() {
        // Lazily initialise iterator (this delays the first parsing pass over the file)
        if (iterator == null)
        {
            iterator = mmsReader.iterator();
        }
        
        // Ask the iterator whether it has a next record
        if (iterator.hasNext()) {
            this.currentObject = iterator.next();
            
            // Yes - there is at leaste one more record
            return true;
        } else {
            // No - there are no more records (e.g. end of file)
            return false;
        }
    }

    @Override
    public InternalRow get() {

        Object[] convertedValues = new Object[currentObject.length];
        for (int i = 0; i < currentObject.length; i++) {
            convertedValues[i] = valueConverters.get(i).apply(currentObject[i]);
        }
        return InternalRow.apply(JavaConverters.asScalaIteratorConverter(Arrays.asList(convertedValues).iterator()).asScala().toSeq());

    }

    @Override
    public void close() throws IOException {
        mmsReader.close();
    }
}
