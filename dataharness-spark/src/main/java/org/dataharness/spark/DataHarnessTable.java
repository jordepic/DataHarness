package org.dataharness.spark;

import java.util.Collections;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.types.StructType;
import org.dataharness.proto.LoadTableResponse;

public class DataHarnessTable implements Table {

  private final String tableName;
  private final LoadTableResponse response;

  public DataHarnessTable(String tableName, LoadTableResponse response) {
    this.tableName = tableName;
    this.response = response;
  }

  public LoadTableResponse getResponse() {
    return response;
  }

  @Override
  public String name() {
    return tableName;
  }

  @Override
  public StructType schema() {
    return DataHarnessSchemaExtractor.extractSchema(tableName, response);
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Collections.emptySet();
  }
}
