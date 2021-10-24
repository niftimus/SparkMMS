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
import java.sql.Timestamp;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
* Convert data types to Spark compatible data types.
*  
*/
public class ValueConverters {

    // Useful reference: https://github.com/aamargajbhiye/big-data-projects/tree/master/Datasource%20spark3 (https://aamargajbhiye.medium.com/)
    public static List<Function> getConverters(StructType schema) {
        StructField[] fields = schema.fields();
        List<Function> valueConverters = new ArrayList<>(fields.length);
        Arrays.stream(fields).forEach(field -> {
            if (field.dataType().equals(DataTypes.StringType)) {
                valueConverters.add(UTF8StringConverter);
            } else if (field.dataType().equals(DataTypes.IntegerType)) {
                valueConverters.add(IntConverter);
            } else if (field.dataType().equals(DataTypes.DoubleType)) {
                valueConverters.add(DoubleConverter);
            } else if (field.dataType().equals(DataTypes.TimestampType)) {
                valueConverters.add(TimestampConverter);
            } else if (field.dataType().equals(ArrayType.apply(DataTypes.StringType))) {
                valueConverters.add(ArrayStringConverter);
            } else if (field.dataType().equals(ArrayType.apply(ArrayType.apply(DataTypes.StringType)))) {
                valueConverters.add(ArrayOfArrayStringConverter);
            }
        });
        return valueConverters;
    }

    public static Function<String, UTF8String> UTF8StringConverter = UTF8String::fromString;
    public static Function<String, Long> TimestampConverter = (String value) -> value == null ? null : Timestamp.valueOf(value).getTime()*1000L;
    public static Function<String, Double> DoubleConverter = value -> value == null ? null : Double.parseDouble(value);
    public static Function<String, Integer> IntConverter = value -> value == null ? null : Integer.parseInt(value);
    public static Function<String[], ArrayData> ArrayStringConverter = value
            -> {
        UTF8String[] tmpArray;
        tmpArray = new UTF8String[value.length];

        for (int i = 0; i < value.length; i++) {
            tmpArray[i] = UTF8String.fromString(value[i]);
        }

        ArrayData tmpArrayData = ArrayData.toArrayData(tmpArray);
        return tmpArrayData;

    };
    
    // Create array of string arrays
    public static Function<String[][], ArrayData> ArrayOfArrayStringConverter = value
            -> {

        UTF8String[] tmpArrayString;
        UTF8String[][] tmpArray;
        ArrayData[] tmpArrayDataRows;
        ArrayData tmpArrayData;

        // If the outer array contains at least one entry iterate over it
        // and convert elements to String arrays
        if(value.length >0)
        {
            tmpArray = new UTF8String[value.length][value[0].length];
            tmpArrayDataRows = new ArrayData[value.length];

            for (int i = 0; i < value.length; i++) {
                tmpArrayString = new UTF8String[value[i].length];
                for (int j = 0; j < value[i].length; j++) {
                    tmpArrayString[j] = UTF8String.fromString(value[i][j]);
                }
                tmpArrayDataRows[i] = ArrayData.toArrayData(tmpArrayString);
            }

            tmpArrayData = ArrayData.toArrayData(tmpArrayDataRows);

        } else {
            // Otherwise create a single element array containing an array
            // with an empty String in it
            tmpArrayString = new UTF8String[1];
            tmpArrayString[0] = UTF8String.fromString("");
            tmpArrayDataRows = new ArrayData[1];
            tmpArrayDataRows[0] = ArrayData.toArrayData(tmpArrayString);
            tmpArrayData = ArrayData.toArrayData(tmpArrayDataRows);
        }

        return tmpArrayData;

    };
}
