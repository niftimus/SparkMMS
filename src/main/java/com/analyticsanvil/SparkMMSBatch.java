/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.analyticsanvil;

import java.io.File;
import java.util.zip.ZipInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import java.nio.charset.Charset;
import java.util.zip.ZipEntry;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.util.Utils;

/**
 *
 * @author david
 */
// Reference https://levelup.gitconnected.com/easy-guide-to-create-a-custom-read-data-source-in-apache-spark-3-194afdc9627a
// WASB testing: https://hub.docker.com/_/microsoft-azure-storage-azurite
// docker run -p 10000:10000 -p 10001:10001 -v /home/david/Desktop/spark/MMSDataSource/test/:/data mcr.microsoft.com/azure-storage/azurite
public class SparkMMSBatch implements Batch {

    private final StructType schema;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    private String filename;
    private int startRow;
    private final int maxRowsPerPartition;
    private final Charset UTF8_CHARSET = Charset.forName("UTF-8");
    private final Filter[] pushedFilters;
    private final boolean excludeReports;
    private final boolean excludeData;
    private boolean splitFile = false;
    private static final Logger logger = Logger.getLogger(SparkMMSBatch.class.getName());
    private final int BYTE_BUFFER_LENGTH = 100;
    private final Configuration hadoopConfig;
    private final int minSplitFilesize;

    public SparkMMSBatch(StructType schema,
            Map<String, String> properties,
            CaseInsensitiveStringMap options,
            Filter[] pushedFilters,
            boolean excludeReports,
            boolean excludeData) {

        this.schema = schema;
        this.properties = properties;
        this.options = options;
        this.filename = options.get("fileName");
        this.startRow = options.get("startRow") == null ? SparkMMSConstants.MMS_START_ROW_DEFAULT : Integer.parseInt(options.get("startRow"));
        this.maxRowsPerPartition = options.get("maxRowsPerPartition") == null ? SparkMMSConstants.MMS_MAX_ROWS_DEFAULT : Integer.parseInt(options.get("maxRowsPerPartition"));
        this.minSplitFilesize = options.get("minSplitFilesize") == null ? SparkMMSConstants.MMS_MIN_SPLIT_FILESIZE_DEFAULT : Integer.parseInt(options.get("minSplitFilesize"));
        this.pushedFilters = pushedFilters;
        this.excludeReports = excludeReports;
        this.excludeData = excludeData;        
        this.hadoopConfig = SparkContext.getOrCreate().hadoopConfiguration();
    }

    /**
     * Decode array of bytes and return a UTF-8 string.
     * 
     * @param bytes
     * @return 
     */
    String decodeUTF8(byte[] bytes) {
        return new String(bytes, UTF8_CHARSET);
    }

    /**
     * Change file extension.
     * 
     * @param f
     * @param newExtension
     * @return 
     */
    private static File changeExtension(String f, String newExtension) {
        int i = f.lastIndexOf('.');
        String name = f.substring(0, i);
        return new File(name + newExtension);
    }

    /**
     * Scan a byte string containing the end of the file (e.g. last 100 characters) and return the declared number of records in the file footer.
     * 
     * @param b
     * @return 
     */
    private Integer getNumRecordsInteger(byte[] b) {
        String s, numRecordsString;
        // Convert bytes to UTF-8 string
        s = decodeUTF8(b);
        int lastIndex;
        
        // Remove linebreak characters
        s = s.replaceAll(SparkMMSConstants.MMS_END_OF_LINE_STRING, "");
        
        // Get the final comma in the string
        // This is to locate where the record count indicator is - example:
        // C,"END OF REPORT",26750820 <--
        lastIndex = s.lastIndexOf(",");
        
        // Get a the integer on the last line of the file
        numRecordsString = s.substring(lastIndex + 1, s.length()).trim();
        
        // Return number of records as found in the file footer
        return Integer.parseInt(numRecordsString);
    }

    /**
     * Return a list of input partitions containing all input files.
     * 
     * @return 
     */
    private InputPartition[] createPartitions() {
        ArrayList<InputPartition> partitions = new ArrayList<>();
        int totalFileCount, thisFileNumber = 1;
        long currentFilesize;
        int endIndex;
        FileStatus[] statuses;
        Integer numRecords = null;
        FSDataInputStream in;
        byte[] b;
        long filesize;
        FileSystem fs;
        ZipInputStream zis;
        final byte[] buffer = new byte[BYTE_BUFFER_LENGTH];
        boolean skipFile;

        // Get a path representing the input file
        Path path = new Path(filename);
        Path filePath, tmpFilePath;

        try {

            // Set the filesystem to a hadoop compatible one (including WASB, S3, local)
            fs = Utils.getHadoopFileSystem(path.toString(), hadoopConfig);

            // Get files / folders from the glob input
            statuses = fs.globStatus(path);

            // Get the total filecount (for logging)
            totalFileCount = statuses.length;

            logger.log(Level.INFO, "Found {0} files in path {1}.", new Object[]{totalFileCount, path.toString()});

            for (FileStatus status : statuses) {
                skipFile = true;
                
                // Get the current filesize in bytes
                currentFilesize = status.getLen();
                
                // Set the splitFile flag to true if the raw file size is over the minimum
                splitFile = (currentFilesize >= this.minSplitFilesize);

                b = new byte[BYTE_BUFFER_LENGTH];

                logger.log(Level.INFO, "Processing file {0} / {1} to prepare partitions...", new Object[]{thisFileNumber, totalFileCount});

                if (status.isDirectory()) {

                    // Do nothing if the current status is a directory rather than a file
                    
                } else {

                    filePath = status.getPath();

                    // If the file is a zip:
                    if (".zip".equals(filePath.toString().substring(filePath.toString().length() - 4, filePath.toString().length()))) {
                        // .zip file
                        // Reference: https://github.com/eugenp/tutorials/blob/master/core-java-modules/core-java-io/src/main/java/com/baeldung/unzip/UnzipFile.java
                        
                        zis = new ZipInputStream((InputStream) fs.open(filePath));
                        ZipEntry zipEntry = zis.getNextEntry();
                        while (zipEntry != null) {

                            if (zipEntry.isDirectory() || !zipEntry.getName().equals(changeExtension(filePath.getName(), ".CSV").getName())) {
                                // Do nothing if this entry in the zip file is a folder or doesn't match the Zip file's name (exluding extension)
                                logger.log(Level.WARNING, "Read zip file {0}, skipping entry {1} inside (name doesn't match zip or is a directory).", new Object[]{filePath.getName(), zipEntry.getName()});
                            } else {
                                if (splitFile) {
                                    // If the file needs to be split, read through until the end to get the number of records
                                    logger.log(Level.INFO, "Reading zip file {0} to get rowcount...", filePath.toString());

                                    // Skip bytes to end of file - we only need the number of lines from the end of the file
                                    long skipBytes;
                                    if (zipEntry.getSize() < BYTE_BUFFER_LENGTH) {
                                        skipBytes = 0;
                                    } else {
                                        skipBytes = zipEntry.getSize() - BYTE_BUFFER_LENGTH;
                                    }

                                    // Byte skipping workaround: skip function seems not to cope with skipping more than the integer maximum
                                    // So we run the skip multiple times if necessary
                                    while (skipBytes > Integer.MAX_VALUE) {
                                        zis.skip(Integer.MAX_VALUE);
                                        skipBytes = skipBytes - Integer.MAX_VALUE;
                                    }
                                    zis.skip(skipBytes);

                                    zis.read(buffer);
                                    
                                    // Get number of records from file footer
                                    numRecords = getNumRecordsInteger(buffer);
                                    logger.log(Level.INFO, "Read zip file {0} and found rowcount: {1}.", new Object[]{filePath.toString(), numRecords.toString()});
                                }
                                skipFile = false;
                                
                            }
                            
                            // Advance to next file in the Zip
                            zipEntry = zis.getNextEntry();
                        }
                        
                        // Close the zip file, we have what we need
                        zis.closeEntry();
                        zis.close();

                    } else {
                        // .CSV file
                        

                        logger.log(Level.INFO, "Reading CSV file {0} to get rowcount...", filePath.toString());
                        
                        // Open the file
                        in = fs.open(filePath);

                        // Read the end of the file (e.g. 1000 bytes) to try to get the number of records string
                        in.read(currentFilesize - BYTE_BUFFER_LENGTH, b, 0, BYTE_BUFFER_LENGTH);
                        
                        // Get the number of records in the file footer, as per the Zip behaviour above
                        numRecords = getNumRecordsInteger(b);
                        logger.log(Level.INFO, "Read CSV file {0} and found rowcount: {1}.", new Object[]{filePath.toString(), numRecords.toString()});
                        
                        // Close the file
                        in.close();
                        skipFile = false;
                    }

                    this.filename = filePath.toString();

                    logger.log(Level.FINE, "Opening file: {0}", this.filename);

                    if (!skipFile) {
                        // Create one partition per file if data rows are not required or file size is below minimum to split
                        if (!splitFile || excludeData || excludeReports) {
                            logger.log(Level.FINE, "Adding 1 single partition for file: {0}.", this.filename);
                            // Create one partition per file
                            partitions.add(new SparkMMSInputPartition(filename, 0, 0, pushedFilters, excludeReports, excludeData, splitFile));
                        } else {
                            // Reading data rows, so create multiple partitions

                            endIndex = (int) Math.ceil((double) numRecords / maxRowsPerPartition);

                            logger.log(Level.FINE, "Adding {0} partitions for file: {1}.", new Object[]{endIndex, this.filename});

                            // Add muliple partitions with different ranges to avoid one executor processing too much data
                            int i;
                            for (i = 0; i < endIndex; i++) {
                                startRow = i * this.maxRowsPerPartition;
                                partitions.add(new SparkMMSInputPartition(filename, startRow, maxRowsPerPartition, pushedFilters, excludeReports, excludeData, splitFile));
                            }
                        }
                    }

                }

                logger.log(Level.INFO, "...processed file {0} / {1}.", new Object[]{thisFileNumber, totalFileCount});

                thisFileNumber++;
            }

        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }

        if (partitions != null) {
            InputPartition[] partitionsArray;
            partitionsArray = new InputPartition[partitions.size()];
            partitionsArray = partitions.toArray(partitionsArray);
            return partitionsArray;
        } else {
            InputPartition[] partitionsArray = {};
            return partitionsArray;
        }

    }

    @Override
    public InputPartition[] planInputPartitions() {
        return createPartitions();

    }

    @Override
    public PartitionReaderFactory createReaderFactory() {
        return new SparkMMSPartitionReaderFactory(schema, filename, startRow, maxRowsPerPartition, pushedFilters, excludeReports, excludeData, splitFile);
    }
}
