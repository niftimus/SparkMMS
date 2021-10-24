/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.analyticsanvil;

import java.util.Map;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 *
 * @author david
 */
public class SparkMMSScan implements Scan {
    private final StructType schema;    
    private final Map<String, String> properties;
    private final CaseInsensitiveStringMap options;
    private final Filter[] pushedFilters;
    private final boolean excludeReports;
    private final boolean excludeData;

    public SparkMMSScan(StructType schema,
                   Map<String, String> properties,
                   CaseInsensitiveStringMap options,
                   Filter[] pushedFilters,
                   boolean excludeReports,
                   boolean excludeData) {

        this.schema = schema;
        this.properties = properties;
        this.options = options;
        this.pushedFilters = pushedFilters;
        this.excludeReports = excludeReports;
        this.excludeData = excludeData;
    }

    @Override
    public StructType readSchema() {
        return schema;
    }

    @Override
    public String description() {
        return "MMS Scan";
    }

    @Override
    public Batch toBatch() {
        return new SparkMMSBatch(schema,properties,options, pushedFilters,excludeReports,excludeData);
    }
   
}
