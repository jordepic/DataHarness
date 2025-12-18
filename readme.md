# DataHarness Repository

## Project Overview

**DataHarness** is a unified data integration platform that consolidates diverse data sources—including message
brokers (Kafka), relational databases, and lakehouse/object store systems (Iceberg)—into a single queryable table. It
simplifies data insertion by providing exactly-once semantics, eliminating the need for complex streaming technologies
and infrastructure.

### Key Intentions

- **Unified Access**: Query data from multiple sources through a single interface
- **Simplified Insertion**: Built-in exactly-once semantics for reliable data ingestion without complex streaming
  frameworks
- **Multi-Source Support**: Seamlessly integrate Kafka topics, Iceberg tables, relational databases, and more
- **Extensible Architecture**: Pluggable design for adding new data sources

## Key Services & APIs

### CatalogService (gRPC)

The main service exposed via gRPC with the following RPC methods:

#### **CreateTable**

- **Request**: `CreateTableRequest` with table name
- **Response**: `CreateTableResponse` with table ID and status

#### **UpsertSource**

- **Request**: `UpsertSourceRequest` with table name and source configuration (Kafka or Iceberg)
- **Response**: `UpsertSourceResponse` with success status

#### **LoadTable**

- **Request**: `LoadTableRequest` with table name
- **Response**: `LoadTableResponse` with table schemas (Avro and/or Iceberg) and list of sources
- Retrieves a table along with its associated schemas and configured data sources

#### **ListTables**

- **Request**: `ListTablesRequest` (empty request)
- **Response**: `ListTablesResponse` with list of all table names in the catalog

#### **SetSchema**

- **Request**: `SetSchemaRequest` with table name and optional `avro_schema` and/or `iceberg_schema` fields
- **Response**: `SetSchemaResponse` with success status
- Allows associating Avro and/or Iceberg schemas with a DataHarness table
- Schemas are persisted and returned when loading tables

#### **DropTable**

- **Request**: `DropTableRequest` with table name
- **Response**: `DropTableResponse` with success status and message
- Drops a table and all of its associated sources (both Kafka and Iceberg)
- Performs cascading deletion of all sources linked to the table

### Data Source Support

#### **Kafka Source**

- Trino catalog and schema configuration
- Partition-level offset management (start/end offsets)
- Topic-based sourcing

#### **Iceberg Source**

- Trino integration
- Time-travel support via read timestamp
- Table-level management

## Database Models

### DataHarnessTable

Represents a managed table in the system with associated metadata, including:

- Table name (unique identifier)
- Avro schema (optional nullable string for schema definitions)
- Iceberg schema (optional nullable string for schema definitions)

### KafkaSourceEntity

Persists Kafka source configuration including:

- Trino catalog/schema mapping
- Partition and offset information
- Topic references

### IcebergSourceEntity

Persists Iceberg source configuration including:

- Trino catalog/schema mapping
- Table references
- Temporal query support

## Configuration

### Database Connection

Set via JVM flags (for container deployment):

- `hibernate.connection.url` - PostgreSQL JDBC URL
- `hibernate.connection.username` - Database user
- `hibernate.connection.password` - Database password
- `hibernate.hbm2ddl.auto` - Schema generation strategy

Default Docker configuration:

```
-Dhibernate.connection.url=jdbc:postgresql://postgres:5432/dataharness
-Dhibernate.connection.username=postgres
-Dhibernate.connection.password=postgres
-Dhibernate.hbm2ddl.auto=update
```

## Build & Deployment

### Maven Build

```bash
mvn clean compile
mvn test
mvn package
```

### Code Formatting

```bash
mvn spotless:apply
```

### Docker Image

```bash
mvn jib:build
```

Produces image: `data-harness:latest`  
Main class: `org.dataharness.Main`

## Dependencies Management

## Testing

### Integration Tests

- `CatalogServiceIntegrationTest` - Tests CatalogService functionality
- Uses TestContainers for PostgreSQL isolation
- Bootstrap utilities for test data population

## Accomplished Features

- ✅ **Table Creation**: Create and manage DataHarness tables with metadata persistence
- ✅ **Table Deletion**: Drop tables with cascading deletion of all associated sources
- ✅ **Kafka Source Integration**: Ingest data from Kafka topics with partition-level offset management
- ✅ **Iceberg Source Integration**: Query Iceberg tables with time-travel support
- ✅ **Source Management**: List and manage sources associated with a DataHarness table
- ✅ **Multi-Schema Support**: Associate both Avro and Iceberg schemas with DataHarness tables for flexible schema management
- ✅ **Schema Persistence**: Schemas are persisted in the database and returned when loading tables
- ✅ **gRPC API**: Complete service layer for catalog operations with CreateTable, DropTable, LoadTable, SetSchema, and source management

## Roadmap

### Planned Data Sources

- **Hudi**
- **Delta Lake**
- **CockroachDB**
- **TiDB**
- **YugabyteDB**
- **Google Spanner**

### Query Engine Support

- **In Progress**: Trino (production-ready for Kafka and Iceberg sources)
- **Planned**: Spark (Apache Spark integration for additional compute capabilities)

## License

Copyright (c) 2025
