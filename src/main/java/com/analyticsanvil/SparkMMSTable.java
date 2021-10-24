/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.analyticsanvil;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 *
 * @author david
 */
public class SparkMMSTable implements SupportsRead {

    private final StructType schema;
    private final Map<String, String> properties;
    private Set<TableCapability> capabilities;

    public SparkMMSTable(StructType schema, Map<String, String> properties) {
        this.schema = schema;
        this.properties = properties;
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new SparkMMSScanBuilder(schema, properties, options);
    }

    @Override
    public String name() {
        return "MMS Data";
    }

    @Override
    public StructType schema() {
        return schema;
    }

    @Override
    public Set<TableCapability> capabilities() {
        if (capabilities == null) {
            this.capabilities = new HashSet<>();
            // This table supports batch read only
            capabilities.add(TableCapability.BATCH_READ);
        }
        return capabilities;
    }

}
