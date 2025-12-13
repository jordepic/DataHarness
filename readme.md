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

#### **FetchSources**

- **Request**: `FetchSourcesRequest` with table name
- **Response**: `FetchSourcesResponse` with list of table sources

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

Represents a managed table in the system with associated metadata.

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
- ✅ **Kafka Source Integration**: Ingest data from Kafka topics with partition-level offset management
- ✅ **Iceberg Source Integration**: Query Iceberg tables with time-travel support
- ✅ **Source Management**: List and manage sources associated with a DataHarness table
- ✅ **gRPC API**: Complete service layer for catalog operations

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
