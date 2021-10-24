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
import static com.analyticsanvil.SparkMMSConstants.*;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Map;

public class SparkMMS implements TableProvider {

    public SparkMMS() {

    }

    private static StructType getSchemaHeader() {
        
        // Return the default schema
        return DEFAULT_SCHEMA;
    }

    @Override
    public StructType inferSchema(CaseInsensitiveStringMap options) {
        return null;
    }

    @Override
    public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
       
        return new SparkMMSTable(getSchemaHeader(), properties);
    }

    @Override
    public boolean supportsExternalMetadata() {
        return true;
    }
}
