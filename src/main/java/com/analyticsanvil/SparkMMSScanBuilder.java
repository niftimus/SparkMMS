/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.analyticsanvil;

import static com.analyticsanvil.SparkMMSConstants.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.In;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 *
 * @author david
 */
// Reference: http://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/connector/read/SupportsPushDownFilters.html
// This class builds Scan objects
public class SparkMMSScanBuilder implements ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns {

    private Filter[] pushedFilters = {};
    private final StructType schema;
    private StructType requiredSchema = null;
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    private HashSet<String> requiredFields;
    private boolean excludeReports = false;
    private boolean excludeData = false;

    /**
     * Returns true if report metadata and report data should be excluded based
     * on required fields selected by the user. False otherwise.
     * 
     * @return 
     */
    private boolean areReportsExcluded()
    {
        return !(this.requiredFields.contains(STRING_REPORT_TYPE)||
                this.requiredFields.contains(STRING_REPORT_SUBTYPE)||
                this.requiredFields.contains(STRING_REPORT_VERSION)||
                this.requiredFields.contains(STRING_COLUMN_HEADERS)||
                this.requiredFields.contains(STRING_DATA)||
                this.requiredFields.isEmpty());
    }

    /**
     * Returns true if report data should be excluded based
     * on required fields selected by the user. False otherwise.
     * 
     * @return 
     */
    private boolean isDataExcluded()
    {
        return !(this.requiredFields.contains(STRING_COLUMN_HEADERS)||
                this.requiredFields.contains(STRING_DATA)||
                this.requiredFields.isEmpty());
    }
    
    /**
     * Set the schema, properties and options.
     * 
     * @param schema
     * @param properties
     * @param options
     */
    public SparkMMSScanBuilder(StructType schema,
            Map<String, String> properties,
            CaseInsensitiveStringMap options) {

        this.schema = schema;
        this.properties = properties;
        this.options = options;
    }

    /**
     * Return a new scan object.This passes information about whether
     * to exclude all report metadata and data, or whether to include report
     * metadata but not actual data rows.
     * 
     * @return 
     */
    @Override
    public Scan build() {
        return new SparkMMSScan(schema, properties, options, pushedFilters, excludeReports, excludeData);
    }

    /**
     * Prunes columns by checking whether data and / or metadata columns are
     * requested by the user (based on the requiredSchema passed from the query).
     * 
     * @param requiredSchema
     */
    @Override    
    public void pruneColumns(StructType requiredSchema)
    {
        this.requiredFields = new HashSet<>(Arrays.asList(requiredSchema.fieldNames()));
        this.excludeData = isDataExcluded();
        this.excludeReports = areReportsExcluded();
        this.requiredSchema = requiredSchema;
    }
    
    /**
     * Takes in a list of filters and subtracts filters which this data source
     * supports pushing down. The remainder is returned so Spark can apply
     * additional filtering of data which is returned by the scan.
     * 
     * @param filters
     * @return
     */
    @Override
    public Filter[] pushFilters(Filter[] filters) {
        this.pushedFilters = filters;

        ArrayList<Filter> tmpFilters = new ArrayList<>(Arrays.asList(filters));

        for (Object f : this.pushedFilters) {
            if (f instanceof org.apache.spark.sql.sources.EqualTo && ((EqualTo) f).value() instanceof String) {
                switch (((EqualTo) f).attribute()) {
                    case STRING_SYSTEM:
                    case STRING_REPORT_ID:
                    case STRING_REPORT_FROM:
                    case STRING_REPORT_TO:
                    case STRING_ID1:
                    case STRING_ID2:
                    case STRING_ID3:
                    case STRING_REPORT_TYPE:
                    case STRING_REPORT_SUBTYPE:
                    case STRING_REPORT_VERSION:
                        tmpFilters.remove((Filter) f);
                        break;
                }
            } else if (f instanceof org.apache.spark.sql.sources.In && ((In) f).values() instanceof String[]) {
                switch (((In) f).attribute()) {
                    case STRING_SYSTEM:
                    case STRING_REPORT_ID:
                    case STRING_REPORT_FROM:
                    case STRING_REPORT_TO:
                    case STRING_ID1:
                    case STRING_ID2:
                    case STRING_ID3:
                    case STRING_REPORT_TYPE:
                    case STRING_REPORT_SUBTYPE:
                    case STRING_REPORT_VERSION:
                        tmpFilters.remove((Filter) f);
                        break;
                }
            } else if (f instanceof org.apache.spark.sql.sources.In && ((GreaterThanOrEqual) f).value() instanceof Timestamp) {
                switch (((GreaterThanOrEqual) f).attribute()) {
                    case STRING_PUBLISH_DATETIME:
                        tmpFilters.remove((Filter) f);
                        break;
                }
            } else if (f instanceof org.apache.spark.sql.sources.In && ((LessThanOrEqual) f).value() instanceof Timestamp) {
                switch (((LessThanOrEqual) f).attribute()) {
                    case STRING_PUBLISH_DATETIME:
                        tmpFilters.remove((Filter) f);
                        break;
                }
            } else if (f instanceof org.apache.spark.sql.sources.In && ((GreaterThan) f).value() instanceof Timestamp) {
                switch (((GreaterThan) f).attribute()) {
                    case STRING_PUBLISH_DATETIME:
                        tmpFilters.remove((Filter) f);
                        break;
                }
            } else if (f instanceof org.apache.spark.sql.sources.In && ((LessThan) f).value() instanceof Timestamp) {
                switch (((LessThan) f).attribute()) {
                    case STRING_PUBLISH_DATETIME:
                        tmpFilters.remove((Filter) f);
                        break;
                }
            } else if (f instanceof org.apache.spark.sql.sources.In && ((EqualTo) f).value() instanceof Timestamp) {
                switch (((EqualTo) f).attribute()) {
                    case STRING_PUBLISH_DATETIME:
                        tmpFilters.remove((Filter) f);
                        break;
                }
            }
            
        }
        
        Filter[] tmpFiltersArray = new Filter[tmpFilters.size()];
        tmpFilters.toArray(tmpFiltersArray);
        return tmpFiltersArray;
        
    }

    /**
     * Returns the list of filters which can be pushed by this data source.
     * Currently supported:
     * - Equals / in - all C record fields plus report type,
     * subtype and version from the report header (I records)
     * - Less than, greater than, equal to - publish_datetime from C records
     * 
     * @return
     */
    @Override
    public Filter[] pushedFilters() {
        
        ArrayList<Filter> tmpFilters = new ArrayList<>();

        for (Object f : this.pushedFilters) {
            if (f instanceof org.apache.spark.sql.sources.EqualTo && ((EqualTo) f).value() instanceof String) {
                switch (((EqualTo) f).attribute()) {
                    case STRING_SYSTEM:
                    case STRING_REPORT_ID:
                    case STRING_REPORT_FROM:
                    case STRING_REPORT_TO:
                    case STRING_ID1:
                    case STRING_ID2:
                    case STRING_ID3:
                    case STRING_REPORT_TYPE:
                    case STRING_REPORT_SUBTYPE:
                    case STRING_REPORT_VERSION:
                        tmpFilters.add((Filter) f);
                        break;
                }
            } else if (f instanceof org.apache.spark.sql.sources.In && ((In) f).values() instanceof String[]) {
                switch (((In) f).attribute()) {
                    case STRING_SYSTEM:
                    case STRING_REPORT_ID:
                    case STRING_REPORT_FROM:
                    case STRING_REPORT_TO:
                    case STRING_ID1:
                    case STRING_ID2:
                    case STRING_ID3:                      
                    case STRING_REPORT_TYPE:
                    case STRING_REPORT_SUBTYPE:
                    case STRING_REPORT_VERSION:
                        tmpFilters.add((Filter) f);
                        break;
                }
            } else if (f instanceof org.apache.spark.sql.sources.In && ((GreaterThanOrEqual) f).value() instanceof Timestamp) {
                switch (((GreaterThanOrEqual) f).attribute()) {
                    case STRING_PUBLISH_DATETIME:
                        tmpFilters.add((Filter) f);
                        break;
                }
            } else if (f instanceof org.apache.spark.sql.sources.In && ((LessThanOrEqual) f).value() instanceof Timestamp) {
                switch (((LessThanOrEqual) f).attribute()) {
                    case STRING_PUBLISH_DATETIME:
                        tmpFilters.add((Filter) f);
                        break;
                }
            } else if (f instanceof org.apache.spark.sql.sources.In && ((GreaterThan) f).value() instanceof Timestamp) {
                switch (((GreaterThan) f).attribute()) {
                    case STRING_PUBLISH_DATETIME:
                        tmpFilters.add((Filter) f);
                        break;
                }
            } else if (f instanceof org.apache.spark.sql.sources.In && ((LessThan) f).value() instanceof Timestamp) {
                switch (((LessThan) f).attribute()) {
                    case STRING_PUBLISH_DATETIME:
                        tmpFilters.add((Filter) f);
                        break;
                }
            } else if (f instanceof org.apache.spark.sql.sources.In && ((EqualTo) f).value() instanceof Timestamp) {
                switch (((EqualTo) f).attribute()) {
                    case STRING_PUBLISH_DATETIME:
                        tmpFilters.add((Filter) f);
                        break;
                }
            }
        }

        Filter[] tmpFiltersArray = new Filter[tmpFilters.size()];
        tmpFilters.toArray(tmpFiltersArray);
        return tmpFiltersArray;

    }
}
