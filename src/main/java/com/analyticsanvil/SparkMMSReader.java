/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.analyticsanvil;

import static com.analyticsanvil.SparkMMSConstants.*;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.util.Utils;

/**
 * Iterate over MMS report data in chunks.
 * 
 * @author david
 */
public class SparkMMSReader implements Closeable, Iterable<Object[]> {

    protected BufferedReader br;
    protected Locale errorLocale;
    private int BUFFERED_READER_SIZE = 1048576;
    private String original_filename;
    private String[] currentLine = null;
    private String currentLineNoSplit = null;
    private String[] nextLine = null;
    private boolean hasnext = true;
    private boolean alreadyReadHeader = false;
    private String system = null;
    private String report_id = null;
    private String report_from = null;
    private String report_to = null;
    private String publish_datetime = null;
    private String id1 = null;
    private String id2 = null;
    private String id3 = null;
    private String report_type = null;
    private String report_subtype = null;
    private String report_version = null;
    private String[] column_headers = {new String()};
    private ArrayList<String[]> data = new ArrayList<String[]>();
    private int numRecords = 0;
    private final int startRow;
    private final int maxRowsPerPartition;
    private int rowsRead = 0;
    private FileSystem fs;
    private ArrayList pushedFilters = new ArrayList();
    private final boolean excludeReports;
    private final boolean excludeData;
    private final boolean splitFile;
    private static final Logger logger = Logger.getLogger(SparkMMSBatch.class.getName());
    private ZipInputStream zis=null;
    private ZipEntry zipEntry=null;
    private boolean isInitialised = false;
    final Pattern SPLIT_REGEXP_PATTERN = Pattern.compile(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
    //private final String SPLIT_REGEXP =  ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

    /***
     * Convert to Spark compatible timestamp string based on date and time.
     * 
     * @param date
     * @param time
     * @return 
     */
    private String convertToTimestampString(String date, String time) {
        String tmpPublishDatetime = date + " " + time;
        String tmpPublishDatetimeString = null;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date parseDate = sdf.parse(tmpPublishDatetime);
            tmpPublishDatetimeString = sdf1.format(parseDate);
        } catch (ParseException e) {

        }

        return tmpPublishDatetimeString;
    }

    /**
     * Change the extension of a file based on an input string and new extension.
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

    /***
     * Perform read of input files.
     * 
     * @param path
     * @param startRow
     * @param maxRowsPerPartition
     * @param pushedFilters
     * @param excludeReports
     * @param excludeData
     * @param splitFile
     * @throws IOException 
     */
    SparkMMSReader(Path path, int startRow, int maxRowsPerPartition, Filter[] pushedFilters, boolean excludeReports, boolean excludeData, boolean splitFile) throws IOException {

        this.original_filename = path.toString();       

        SparkConf conf = new SparkConf();
        Configuration hadoopConf = new Configuration();
        this.fs = Utils.getHadoopFileSystem(path.toUri(), hadoopConf);        

        if (".zip".equals(path.toString().substring(path.toString().length() - 4, path.toString().length()))) {            

            // Reference: https://github.com/eugenp/tutorials/blob/master/core-java-modules/core-java-io/src/main/java/com/baeldung/unzip/UnzipFile.java
            zis = new ZipInputStream((InputStream) fs.open(path));
            zipEntry = zis.getNextEntry();
            while (zipEntry != null) {

                if (zipEntry.isDirectory() || !zipEntry.getName().equals(changeExtension(path.getName(), ".CSV").getName())) {
                    logger.log(Level.WARNING, "Read zip file {0}, skipping entry {1} inside (name doesn't match zip or is a directory).", new Object[]{path.getName(), zipEntry.getName()});
                } else {

                    this.br = new BufferedReader(new InputStreamReader(zis),BUFFERED_READER_SIZE);
                    break;
        }
        zipEntry = zis.getNextEntry();
    }
            
            
}
else {        
    this.br= new BufferedReader(new InputStreamReader(fs.open(path)),BUFFERED_READER_SIZE);
}
        
        this.errorLocale = ObjectUtils.defaultIfNull(errorLocale, Locale.getDefault());
        this.startRow = startRow;
        this.maxRowsPerPartition = maxRowsPerPartition;
        
        this.pushedFilters.addAll(Arrays.asList(pushedFilters));       
      
        this.excludeReports = excludeReports;
        this.excludeData = excludeData;
        this.splitFile = splitFile;
        
    }
      
    
    private void getNextLineNoSplit()
    {
        
        try {
            this.currentLineNoSplit = br.readLine();

            if (this.currentLineNoSplit == null) {
                this.nextLine = null;
                this.hasnext = false;
            }

        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }

    }
    
    private void getNextLine()
    {
        
        try {
            this.currentLine = SPLIT_REGEXP_PATTERN.split(br.readLine());

            if (this.currentLine == null) {
                
                this.hasnext = false;
            }

        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }

    }

/***
 * Determine whether to exclude this input file entirely. Checks whether the file should be excluded based on pushed filters.
 * 
 * @param system
 * @param report_id
 * @param report_from
 * @param report_to
 * @param id1
 * @param id2
 * @param id3
 * @param publish_datetime 
 * @return Returns true if the file should be excluded, false otherwise.
 */
    
    private boolean excludeThisFile(String system,String report_id,String report_from,String report_to, String id1, String id2, String id3, String publish_datetime)
{
    // TODO
    boolean exclude = false;
    String s;
    List<String> sl;
    
    for(Object f : this.pushedFilters)
    {
        if (f instanceof org.apache.spark.sql.sources.EqualTo && ((EqualTo) f).value() instanceof String) {
            s=(String) ((EqualTo) f).value();
            switch(((EqualTo) f).attribute()) {
                
                case STRING_SYSTEM:
                    if (!s.equals(system)) { this.pushedFilters.remove(f); return true;}
                    break;
                case STRING_REPORT_ID:
                    if (!s.equals(report_id)) { this.pushedFilters.remove(f); return true;}
                    break;
                case STRING_REPORT_FROM:
                    if (!s.equals(report_from)) { this.pushedFilters.remove(f); return true;}
                    break;
                case STRING_REPORT_TO:
                    if (!s.equals(report_to)) { this.pushedFilters.remove(f); return true;}
                    break;
                case STRING_ID1:
                    if (!s.equals(id1)) { this.pushedFilters.remove(f); return true;}
                    break;
                case STRING_ID2:
                    if (!s.equals(id2)) { this.pushedFilters.remove(f); return true;}
                    break;
                case STRING_ID3:
                    if (!s.equals(id3)) { this.pushedFilters.remove(f); return true;}
                    break;
            }
        } else if (f instanceof org.apache.spark.sql.sources.In && ((In) f).values() instanceof String[])
        {
            sl=Arrays.asList((String[]) ((In) f).values());
            switch(((EqualTo) f).attribute()) {
                
                case STRING_SYSTEM:
                    if (!sl.contains(system)) {
                        this.pushedFilters.remove(f);
                        return true;
                    }
                    break;
                case STRING_REPORT_ID:
                    if (!sl.contains(report_id)) {
                        this.pushedFilters.remove(f);
                        return true;
                    }
                    break;
                case STRING_REPORT_FROM:
                    if (!sl.contains(report_from)) {
                        this.pushedFilters.remove(f);
                        return true;
                    }
                    break;
                case STRING_REPORT_TO:
                    if (!sl.contains(report_to)) {
                        this.pushedFilters.remove(f);
                        return true;
                    }
                    break;
                case STRING_ID1:
                    if (!sl.contains(id1)) {
                        this.pushedFilters.remove(f);
                        return true;
                    }
                    break;
                case STRING_ID2:
                    if (!sl.contains(id2)) {
                        this.pushedFilters.remove(f);
                        return true;
                    }
                    break;
                case STRING_ID3:
                    if (!sl.contains(id3)) {
                        this.pushedFilters.remove(f);
                        return true;
                    }
                    break;
            }
        } else if (f instanceof org.apache.spark.sql.sources.GreaterThanOrEqual && ((GreaterThanOrEqual) f).value() instanceof Timestamp) {            
            Timestamp ts1 = Timestamp.valueOf(publish_datetime);            
            Timestamp ts2 =(Timestamp) ((GreaterThanOrEqual) f).value();
            switch(((GreaterThanOrEqual) f).attribute()) {
                
                case STRING_PUBLISH_DATETIME:
                    if (ts1.compareTo(ts2) < 0) { this.pushedFilters.remove(f); return true;}
                    break;
                
            }
        } else if (f instanceof org.apache.spark.sql.sources.LessThanOrEqual && ((LessThanOrEqual) f).value() instanceof Timestamp) {            
            Timestamp ts1 = Timestamp.valueOf(publish_datetime);            
            Timestamp ts2 =(Timestamp) ((LessThanOrEqual) f).value();
            switch(((LessThanOrEqual) f).attribute()) {
                
                case STRING_PUBLISH_DATETIME:
                    if (ts1.compareTo(ts2) > 0) { this.pushedFilters.remove(f); return true;}
                    break;
                
            }
        } else if (f instanceof org.apache.spark.sql.sources.GreaterThan && ((GreaterThan) f).value() instanceof Timestamp) {            
            Timestamp ts1 = Timestamp.valueOf(publish_datetime);            
            Timestamp ts2 =(Timestamp) ((GreaterThan) f).value();
            switch(((GreaterThan) f).attribute()) {
                
                case STRING_PUBLISH_DATETIME:
                    if (ts1.compareTo(ts2) <= 0) { this.pushedFilters.remove(f); return true;}
                    break;
                
            }
        } else if (f instanceof org.apache.spark.sql.sources.LessThan && ((LessThan) f).value() instanceof Timestamp) {            
            Timestamp ts1 = Timestamp.valueOf(publish_datetime);            
            Timestamp ts2 =(Timestamp) ((LessThan) f).value();
            switch(((LessThan) f).attribute()) {
                
                case STRING_PUBLISH_DATETIME:
                    if (ts1.compareTo(ts2) >= 0) { this.pushedFilters.remove(f); return true;}
                    break;                
            }
        } else if (f instanceof org.apache.spark.sql.sources.EqualTo && ((EqualTo) f).value() instanceof Timestamp) {            
            Timestamp ts1 = Timestamp.valueOf(publish_datetime);            
            Timestamp ts2 =(Timestamp) ((EqualTo) f).value();
            switch(((EqualTo) f).attribute()) {
                
                case STRING_PUBLISH_DATETIME:
                    if (ts1.compareTo(ts2) == 0) { this.pushedFilters.remove(f); return true;}
                    break;                
            }
        }
    }
    
    return false;
}
    
private boolean excludeThisReport(String report_type, String report_subtype, String report_version)
{
    boolean exclude = false;
    String s;
    List<String> sl;
    
    for(Object f : this.pushedFilters)
    {
        if (f instanceof org.apache.spark.sql.sources.EqualTo && ((EqualTo) f).value() instanceof String)
        {
            s=(String) ((EqualTo) f).value();
            switch(((EqualTo) f).attribute()) {
                
                case STRING_REPORT_TYPE:
                    if (!s.equals(report_type)) { this.pushedFilters.remove(f); return true;}
                    break;
                case STRING_REPORT_SUBTYPE:
                    if (!s.equals(report_subtype)) { this.pushedFilters.remove(f); return true;}
                    break;
                case STRING_REPORT_VERSION:
                    if (!s.equals(report_version)) { this.pushedFilters.remove(f); return true;}
                    break;                           
            }               
        }
        else if (f instanceof org.apache.spark.sql.sources.In && ((In) f).values() instanceof String[])
        {
            sl=Arrays.asList((String[]) ((In) f).values());
            switch(((In) f).attribute()) {
                
                case STRING_REPORT_TYPE:
                    if (!sl.contains(report_type)) { this.pushedFilters.remove(f); return true;}
                    break;
                case STRING_REPORT_SUBTYPE:
                    if (!sl.contains(report_subtype)) { this.pushedFilters.remove(f); return true;}
                    break;
                case STRING_REPORT_VERSION:
                    if (!sl.contains(report_version)) { this.pushedFilters.remove(f); return true;}
                    break;                           
            }
        }
        
    }
    
    return false;
}
    
private void parse_report() {
    String line;
    String[] splitLine;
    boolean skipReport = false;

    //currentLine = nextLine;

    if (currentLine == null) {
        getNextLine();
    } 

        if (null != currentLine[0]) switch (currentLine[0]) {
            case "C":
                if (!alreadyReadHeader)
                {
                    this.system = currentLine[1];
                    this.report_id = currentLine[2];
                    this.report_from = currentLine[3];
                    this.report_to = currentLine[4];
                    this.publish_datetime = convertToTimestampString( currentLine[5], currentLine[6]);
                    this.id1 = currentLine[7];
                    this.id2 = currentLine[8];
                    this.id3 = currentLine[9];
                    if (excludeThisFile(system,report_id,report_from,report_to, id1, id2, id3, publish_datetime) || this.excludeReports)
                    {
                        this.hasnext = false;
                        break;
                    } else {
                    alreadyReadHeader = true;
                    getNextLine();
                    }
                } else
                {
                    this.numRecords = Integer.parseInt(currentLine[2]);
                    this.hasnext = false;
                    break;
                }

            case "I":
                this.report_type = currentLine[1];
                this.report_subtype = currentLine[2];
                this.report_version = currentLine[3];
//                this.column_headers = new String[currentLine.length-4];
//                System.arraycopy(currentLine, 4, this.column_headers, 0, (currentLine.length)-4);

                while(hasnext&&excludeThisReport(report_type, report_subtype, report_version))
                {
                    getNextLine();
                    currentLineNoSplit = String.join(",", currentLine);
                    
                    while (hasnext&&'I'!=(currentLineNoSplit.charAt(0)))
                    {
                        //getNextLine();
                        getNextLineNoSplit();
                        if ('C'==(currentLineNoSplit.charAt(0))) {
                            this.hasnext = false;
                        }
                    }
                    currentLine = SPLIT_REGEXP_PATTERN.split(this.currentLineNoSplit);
                    if(hasnext) 
                    {
                        this.report_type = currentLine[1];
                        this.report_subtype = currentLine[2];
                        this.report_version = currentLine[3];
                    } else
                    {
                        break;
                    }
                }                            

                if (this.hasnext == false) break;
                
                if(this.excludeData)
                {
                    getNextLine();
                    break;
                }
                
                this.column_headers = new String[currentLine.length-4];
                System.arraycopy(currentLine, 4, this.column_headers, 0, (currentLine.length)-4);

                getNextLine();

            case "D":
                currentLineNoSplit = String.join(",", currentLine);
                data = new ArrayList<String[]>();
                while(currentLineNoSplit.charAt(0)=='D')
                {
                    rowsRead++;
                    if ((!splitFile) || ((rowsRead>this.startRow) && (rowsRead<=this.startRow+this.maxRowsPerPartition)))
                    {
                        // Add data rows if the current row is within range (for split files) or all rows if this file doesn't need splitting
                        currentLine = SPLIT_REGEXP_PATTERN.split(this.currentLineNoSplit);
                        data.add(Arrays.copyOfRange(currentLine, 4, currentLine.length));
                    }
                    
                    // Read the next line, but don't use string split (for performance)
                    getNextLineNoSplit();

                }
                
                // Split the current unsplit line as the next pass will need elements
                this.nextLine = currentLine = SPLIT_REGEXP_PATTERN.split(this.currentLineNoSplit);
                this.currentLine = this.nextLine;
                
                
                break;
            default:
                break;
        }

}

    @Override
    public Iterator<Object[]> iterator() {

        // Perform initial parse when iterator is initialised
        parse_report();
        return new Iterator<Object[]>() {

     
            
    @Override
    public boolean hasNext() {
        return hasnext;
    }

    @Override
    public Object[] next() {
                if (!hasNext()) {
                    
                    // Throw exception if there are no more chunks
                    throw new NoSuchElementException();                    
                } else {

                    Object[] tmpOutput = {original_filename, system,report_id,report_from,report_to, publish_datetime, id1,id2,id3, report_type, report_subtype, report_version, column_headers,data.toArray(new String[0][0])};
                    parse_report();
                    return tmpOutput;
                }
            }
        };
    }

    /**
     * Closes the underlying reader.
     *
     * @throws IOException If the close fails
     */
    @Override
    public void close() throws IOException {
        
        // Close Zip input stream if it was opened
        if(zis!=null)
        {
            zis.closeEntry();
            zis.close();
        }
        
        // Close buffered reader stream and filesystem stream
        br.close();
        fs.close();
        
    }

}
