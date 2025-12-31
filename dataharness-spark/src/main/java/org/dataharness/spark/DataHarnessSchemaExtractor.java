package org.dataharness.spark;

import org.apache.avro.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;
import org.dataharness.proto.LoadTableResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataHarnessSchemaExtractor {
  private static final Logger logger = LoggerFactory.getLogger(DataHarnessSchemaExtractor.class);

  public static org.apache.spark.sql.types.StructType extractSchema(
      String tableName, LoadTableResponse response) {
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
    throw new UnsupportedOperationException(
        "Protobuf schemas not yet supported in the spark connector!");
  }
}
