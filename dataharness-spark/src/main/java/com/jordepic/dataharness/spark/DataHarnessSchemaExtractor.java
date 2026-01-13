/*
 * The MIT License
 * Copyright © 2026 Jordan Epstein
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.jordepic.dataharness.spark;

import com.jordepic.dataharness.proto.LoadTableResponse;
import org.apache.avro.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataHarnessSchemaExtractor {
    private static final Logger logger = LoggerFactory.getLogger(DataHarnessSchemaExtractor.class);

    public static org.apache.spark.sql.types.StructType extractSchema(String tableName, LoadTableResponse response) {
        if (response.hasAvroSchema() && !response.getAvroSchema().isEmpty()) {
            logger.debug("Extracting schema from Avro schema for table '{}'", tableName);
            return extractColumnsFromAvroSchema(response.getAvroSchema());
        }
        if (response.hasIcebergSchema() && !response.getIcebergSchema().isEmpty()) {
            logger.debug("Extracting schema from Iceberg schema for table '{}'", tableName);
            return extractColumnsFromIcebergSchema(response.getIcebergSchema());
        }
        if (response.hasProtobufSchema() && !response.getProtobufSchema().isEmpty()) {
            logger.debug("Extracting schema from Protobuf schema for table '{}'", tableName);
            return extractColumnsFromProtobufSchema(response.getProtobufSchema());
        }
        throw new IllegalArgumentException(
                "Table '" + tableName + "' does not have an avro, iceberg, or protobuf schema");
    }

    private static StructType extractColumnsFromAvroSchema(String avroSchemaStr) {
        Schema avroSchema = new Schema.Parser().parse(avroSchemaStr);
        SchemaConverters.SchemaType schemaType = SchemaConverters.toSqlType(avroSchema);
        return (StructType) schemaType.dataType();
    }

    private static StructType extractColumnsFromIcebergSchema(String icebergSchemaStr) {
        org.apache.iceberg.Schema icebergSchema = SchemaParser.fromJson(icebergSchemaStr);
        return SparkSchemaUtil.convert(icebergSchema);
    }

    private static StructType extractColumnsFromProtobufSchema(String protobufSchemaStr) {
        throw new UnsupportedOperationException("Protobuf schemas not yet supported in the spark connector!");
    }
}
