---
layout: page
title: Documentation
permalink: /docs/
---

# DataHarness Documentation

DataHarness is a unified abstraction layer that brings together multiple table sources into a single abstract table interface.

## Overview

DataHarness provides a gRPC-based service that manages:
- **Table Metadata**: Centralized tracking of table schemas and configurations
- **Source Management**: Unified interface for Kafka, Iceberg, PostgreSQL, and YugabyteDB sources
- **Time-Based Queries**: Support for reading data at specific timestamps/offsets
- **Schema Tracking**: Avro, Protobuf, and Iceberg schema management

## Architecture

The DataHarness system consists of:

1. **PostgreSQL Backend** - Stores table metadata, schemas, and source configurations
2. **gRPC Service** - Provides the primary client interface via the CatalogService
3. **Data Sources** - Pluggable connectors for various data systems
4. **Query Engine Plugins** - Integrations with Trino and Spark (in development)

## gRPC API Reference

The DataHarness gRPC API is defined in the `CatalogService` and provides the following operations:

### Table Management

#### CreateTable
Creates a new table in the DataHarness catalog.

**Request:**
- `name` (string): Name of the table to create

**Response:**
- `success` (bool): Whether the operation succeeded
- `message` (string): Status message
- `table_id` (int64): ID of the created table

#### DropTable
Removes a table from the DataHarness catalog.

**Request:**
- `table_name` (string): Name of the table to drop

**Response:**
- `success` (bool): Whether the operation succeeded
- `message` (string): Status message

#### TableExists
Checks if a table exists in the catalog.

**Request:**
- `table_name` (string): Name of the table to check

**Response:**
- `exists` (bool): Whether the table exists

#### ListTables
Lists all tables in the catalog.

**Request:** Empty

**Response:**
- `table_names` (repeated string): List of table names

### Source Management

#### UpsertSources
Creates or updates data sources for tables. This operation is atomic - all sources are updated together.

**Request:**
- `sources` (repeated SourceUpdate): List of source updates to apply

Each `SourceUpdate` contains:
- `table_name` (string): Name of the table
- One of:
  - `kafka_source`: Kafka topic configuration
  - `iceberg_source`: Iceberg table configuration
  - `yugabytedb_source`: YugabyteDB table configuration
  - `postgresdb_source`: PostgreSQL table configuration

**Response:**
- `success` (bool): Whether the operation succeeded
- `message` (string): Status message

#### ClaimSources
Claims ownership of specific sources by their modifier.

**Request:**
- `sources` (repeated SourceToClaim): List of sources to claim
  - `name` (string): Source name
  - `modifier` (string): Modifier identifier

**Response:**
- `success` (bool): Whether the operation succeeded
- `message` (string): Status message

### Schema Management

#### SetSchema
Sets the schema for a table (supports Avro, Protobuf, or Iceberg schemas).

**Request:**
- `table_name` (string): Name of the table
- `avro_schema` (optional string): Avro schema JSON
- `iceberg_schema` (optional string): Iceberg schema JSON
- `protobuf_schema` (optional string): Protobuf schema descriptor

**Response:**
- `success` (bool): Whether the operation succeeded
- `message` (string): Status message

#### LoadTable
Retrieves complete table metadata including schema and sources.

**Request:**
- `table_name` (string): Name of the table to load

**Response:**
- `avro_schema` (optional string): Avro schema if set
- `iceberg_schema` (optional string): Iceberg schema if set
- `protobuf_schema` (optional string): Protobuf schema if set
- `sources` (repeated TableSourceMessage): List of configured sources

## Data Source Types

### Kafka Source
Represents a Kafka topic as a data source.

**Configuration:**
- `name`: Source identifier
- `topic_name`: Kafka topic name
- `broker_urls`: Kafka broker connection string
- `start_offset` / `end_offset`: Offset range to read
- `partition_number`: Partition to read from
- `schema_type`: AVRO or PROTOBUF
- `schema`: Schema definition
- `trino_catalog_name` / `trino_schema_name`: Trino metadata

### Iceberg Source
Represents an Apache Iceberg table as a data source.

**Configuration:**
- `name`: Source identifier
- `table_name`: Iceberg table name
- `read_timestamp`: Timestamp for time-travel queries
- `spark_catalog_name` / `spark_schema_name`: Spark metadata
- `trino_catalog_name` / `trino_schema_name`: Trino metadata

### PostgreSQL Source
Represents a PostgreSQL table with temporal support.

**Configuration:**
- `name`: Source identifier
- `table_name`: Main table name
- `history_table_name`: History table for temporal queries
- `jdbc_url`: PostgreSQL connection string
- `username` / `password`: Database credentials
- `read_timestamp`: Timestamp for point-in-time queries

### YugabyteDB Source
Represents a YugabyteDB table.

**Configuration:**
- `name`: Source identifier
- `table_name`: YugabyteDB table name
- `jdbc_url`: YugabyteDB connection string
- `username` / `password`: Database credentials
- `read_timestamp`: Timestamp for consistent reads
- `trino_catalog_name` / `trino_schema_name`: Trino metadata

## Getting Started

### Prerequisites
- Java 17 or higher
- PostgreSQL database
- Maven 3.6+

### Running DataHarness

1. Start the required infrastructure:
   ```bash
   ./start_images.sh
   ```

2. Build the project:
   ```bash
   mvn clean install -DskipTests -T4 -U
   ```

3. Run the server:
   ```bash
   cd dataharness-server
   mvn exec:java
   ```

### Example: Creating a Table with Kafka Source

```java
// Create gRPC channel
ManagedChannel channel = ManagedChannelBuilder
    .forAddress("localhost", 9090)
    .usePlaintext()
    .build();

CatalogServiceGrpc.CatalogServiceBlockingStub stub = 
    CatalogServiceGrpc.newBlockingStub(channel);

// Create table
CreateTableResponse createResp = stub.createTable(
    CreateTableRequest.newBuilder()
        .setName("my_table")
        .build()
);

// Add Kafka source
UpsertSourcesResponse upsertResp = stub.upsertSources(
    UpsertSourcesRequest.newBuilder()
        .addSources(SourceUpdate.newBuilder()
            .setTableName("my_table")
            .setKafkaSource(KafkaSourceMessage.newBuilder()
                .setName("kafka_source_1")
                .setTopicName("my_topic")
                .setBrokerUrls("localhost:9092")
                .setStartOffset(0)
                .setEndOffset(1000)
                .setPartitionNumber(0)
                .setSchemaType(SchemaType.AVRO)
                .setSchema("{...avro schema...}")
                .build())
            .build())
        .build()
);
```

## Protobuf Schema Reference

The complete protobuf schema can be found at:
- [catalog.proto](/DataHarness/dataharness-rpc/src/main/proto/catalog.proto)

## Integration with Query Engines

### Trino Plugin (In Development)
The DataHarness Trino plugin allows querying unified tables via SQL:

```sql
SELECT * FROM dataharness.default.my_table;
```

This transparently queries all configured sources and unions the results.

### Spark Connector (Planned)
Spark integration will allow reading DataHarness tables as DataFrames.

## Best Practices

1. **Atomic Source Updates**: Always use `UpsertSources` to update all sources for a table atomically
2. **Schema Consistency**: Ensure all sources for a table have compatible schemas
3. **Timestamp Management**: Use consistent timestamp formats across sources for reliable time-based queries
4. **Connection Pooling**: Reuse gRPC channels for better performance
5. **Error Handling**: Always check the `success` flag in responses and handle errors appropriately
