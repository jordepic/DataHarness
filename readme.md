# DataHarness Repository

## Project Overview

In the past few years, we've seen the proliferation of new table formats for analyzing big data. While these
work well enough for many analytical workloads, the role of the lake-house keeps expanding, and more development
work goes into making "hybrid" solutions.

Examples:

- Hybrid kafka/iceberg topics (RedPanda, Streambased, Confluent)
- Various delete semantics in lakehouses (positional/equality/deletion vectors)
- Mixing lakehouses with transactional databases (Mooncake/Crunchy Data)
- Proprietary "HTAP" solutions for hybrid processing (DataBricks, Snowflake)
- Streaming connectors between kafka and iceberg for exactly once processing (Kafka Connect, Flink)

**DataHarness** aims to remove the need for the proprietary technology to fuse data sources,
and let teams bring their own data sources. This should allow you to use the technology best
suited towards your pipeline.

## Key Intentions

- **Unified Access**: One DataHarness table can contain data from many different sources
- **Simplified Insertion**: Single metadata store ensures that you never see data across sources in an inconsistent
  state, eliminating the need to use Flink/Kafka Connect for simple pipelines
- **Multi-Source Support**: Seamlessly integrate Kafka topics, relational databases, data lakes, and more
- **Multi-Query Engine Support**: Currently developing for Spark and Trino, with plans to expand

The DataHarness is simply a powerful building-block for fusing many data systems together into a single table.
It allows

- Atomic schema updates
- Transactional data movement between sources

Developers can use the DataHarness to build out complete HTAP solutions and improve upon existing "object-store-only"
based table formats.

## What Can Be A Data Source?

For a data system to work as a "source" for a harness table, it must only satisfy one constraint:

- It must be able to provide an API to see state at a prior period of time

Examples:

- In Kafka, you can specify offsets to read between and get the same data back, regardless of if there are new messages
- In Iceberg/YugabyteDB, you can specify a read timestamp to see historic state of a table

What does this mean?

- In their vanilla state, we cannot support MySQL and PostgresSQL as first class citizens of DataHarness
- However, we can support MySQL and PostgresSQL wire-protocol compliant databases like TiDB and YugabyteDB or use
  extensions!

## Creating A DataHarness Table

This example displays how to create a data harness table which contains data from a topic partition
with avro-encoded data, an iceberg table, and a YugabyteDB table. The DataHarness is running on
localhost:50051.

```
      ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 50051).usePlaintext().build();
      CatalogServiceGrpc.CatalogServiceBlockingStub stub = CatalogServiceGrpc.newBlockingStub(channel);

      CreateTableRequest createTableRequest = CreateTableRequest.newBuilder()
        .setName(DATA_HARNESS_TABLE)
        .build();

      stub.createTable(createTableRequest);

      KafkaSourceMessage kafkaSource = KafkaSourceMessage.newBuilder()
        .setTrinoCatalogName("kafka")
        .setTrinoSchemaName("default")
        .setTopicName(TOPIC)
        .setStartOffset(0)
        .setEndOffset(kafkaResult.messageCount)
        .setPartitionNumber(0)
        .setBrokerUrls(BOOTSTRAP_SERVERS)
        .setSchemaType(SchemaType.AVRO)
        .setSchema(kafkaResult.avroSchema)
        .build();

      SourceUpdate kafkaSourceUpdate = SourceUpdate.newBuilder()
        .setTableName(DATA_HARNESS_TABLE)
        .setKafkaSource(kafkaSource)
        .build();

      UpsertSourcesRequest.Builder upsertSourcesBuilder = UpsertSourcesRequest.newBuilder()
        .addSources(kafkaSourceUpdate);

      IcebergSourceMessage icebergSource = IcebergSourceMessage.newBuilder()
        .setTrinoCatalogName("iceberg")
        .setTrinoSchemaName("default")
        .setTableName(ICEBERG_TABLE_NAME)
        .setReadTimestamp(icebergResult.snapshotId)
        .setSparkCatalogName("gravitino")
        .setSparkSchemaName("default")
        .build();

      SourceUpdate icebergSourceUpdate = SourceUpdate.newBuilder()
        .setTableName(DATA_HARNESS_TABLE)
        .setIcebergSource(icebergSource)
        .build();

      upsertSourcesBuilder.addSources(icebergSourceUpdate);

      YugabyteDBSourceMessage yugabyteSource = YugabyteDBSourceMessage.newBuilder()
        .setTrinoCatalogName(NOT_IMPLEMENTED)
        .setTrinoSchemaName(NOT_IMPLEMENTED)
        .setTableName(YUGABYTE_TABLE_NAME)
        .setJdbcUrl(YUGABYTE_JDBC_URL)
        .setUsername(YUGABYTE_USER)
        .setPassword(YUGABYTE_PASSWORD)
        .setReadTimestamp(yugabyteTimestamp)
        .build();

      SourceUpdate yugabyteSourceUpdate = SourceUpdate.newBuilder()
        .setTableName(DATA_HARNESS_TABLE)
        .setYugabytedbSource(yugabyteSource)
        .build();

      upsertSourcesBuilder.addSources(yugabyteSourceUpdate);

      stub.upsertSources(upsertSourcesBuilder.build());

      ProtobufSchema protobufSchemaObj = new ProtobufSchema(org.dataharness.test.TestMessage.getDescriptor());
      String protobufSchema = protobufSchemaObj.canonicalString();

      SetSchemaRequest.Builder schemaRequestBuilder = SetSchemaRequest.newBuilder()
        .setTableName(DATA_HARNESS_TABLE)
        .setAvroSchema(kafkaResult.avroSchema)
        .setProtobufSchema(protobufSchema);

      schemaRequestBuilder.setIcebergSchema(icebergResult.icebergSchema);

      SetSchemaRequest schemaRequest = schemaRequestBuilder.build();

      stub.setSchema(schemaRequest);
```

## Currently Supported

|                                                              | Apache Spark | Apache Trino |
|--------------------------------------------------------------|--------------|--------------|
| Kafka with Avro Encoded Data (Confluent Schema Registry)     | ✅            | ✅            |
| Kafka with Protobuf Encoded Data (Confluent Schema Registry) |              | ✅            |
| Pulsar with Avro Encoded Data                                |              |              |
| Pulsar with Protobuf Encoded Data                            |              |              |
| Amazon Kinesis with Avro Encoded Data                        |              |              |
| Amazon Kinesis with Protobuf Encoded Data                    |              |              |
| Azure Event Hubs with Avro Encoded Data                      |              |              |
| Azure Event Hubs with Protobuf Encoded Data                  |              |              |
| PostgreSQL (Temporal Tables Extension)                       | ✅            | ✅            |
| YugaByteDB (Postgres compatible)                             | ✅            |              |
| YugaByteDB (Cassandra compatible)                            |              |              |
| CockroachDB (Postgres compatible)                            |              |              |
| TiDB (MySQL compatible)                                      |              |              |
| CockroachDB (Postgres compatible)                            |              |              |
| Google Spanner                                               |              |              |
| Apache Iceberg                                               | ✅            | ✅            |
| Apache Hudi                                                  |              |              |
| Delta Lake                                                   |              |              |
| Duck Lake                                                    |              |              |
| Avro Files                                                   |              |              |
| Parquet Files                                                |              |              |
| Orc Files                                                    |              |              |

Main supported features:

- Transfer data between data sources and atomically update their state in the DataHarness
- Perform a rolling update of schemas and then atomically update the table schema in the DataHarness
- Specify the table's schema in terms of Avro schema, Iceberg schema, or Protocol Buffers schema

Two main features that are not currently supported but we hope to support soon:

- Primary key tables (right now the data from each table source is unioned together)
- Schema evolutions are currently atomic, though tables may temporarily break due to modifying inner fields of complex
  types

## Reading Data Harness Tables From Apache Spark

This repository exposes a spark catalog that you can set up very easily. To do so, create your spark program as follows:

```bash
./bin/spark-sql --packages org.example:dataharness-spark:1.0-SNAPSHOT
--conf spark.sql.catalog.harness=org.dataharness.spark.DataHarnessCatalog
--conf spark.sql.extensions=org.dataharness.spark.DataHarnessExtension
--conf spark.sql.catalog.harness.data-harness-host=data-harness
--conf spark.sql.catalog.harness.data-harness-port=50051
```

This creates a DataHarness catalog called `harness`. To query it, you can run:
`SELECT * FROM harness.data_harness.<table_name>;` -> Note that `data_harness` is currently the only supported namespace

While the above configuration allows using the DataHarness catalog, it does not import any dependencies that might
be needed to read:

- Avro data
- Protobuf data
- Iceberg data
- And so on...

You can
see [our integration test docker compose file](./dataharness-server/src/test/java/org/dataharness/bootstrap/docker-compose.yaml)
for an example of us runnning with an iceberg catalog, a YugabyteDB table, and a kafka topic with avro-encoded data.

NOTE:

- For iceberg tables, the "spark catalog" and "spark namespace" should be equal to the name of the catalog in spark and
  its namespace
- For kafka topic partitions, spark needs to know the broker URLs for each source, there is no "kafka catalog"
    - It needs to know the schema as well, there is no schema registry support for the time being
- For JDBC tables, the jdbc url, username, password, etc... will all need to be specified in the source

## Reading Data Harness Tables From Apache Trino

For Trino, setting up the Data Harness is fairly simple!

1. Set up all data sources like you normally would (e.g. configure kafka catalog, postgres catalog, iceberg catalog
   individually)
2. Add a "data harness" catalog
    - Since this is a custom plugin, you should follow the directions to install them from
      the [trino docs](https://trino.io/docs/current/installation/plugins.html#installation)
    - We have an example of extracting the zip file and putting it in the trino docker
      image [here](/Users/jordanepstein/data/DataHarness/dataharness-server/src/test/java/org/dataharness/bootstrap/Dockerfile.trino)
3. In each of your table "sources", be sure to set the appropriate Trino catalog name and schema name that you used for
   your catalogs in step 1

## Setting Up PostgreSQL Sources

By default, PostgreSQL does not support time travel reads. However, there are extensions that enable this functionality.

The DataHarness project currently supports Postgres databases that use one of the two following extensions:

- [Temporal Tables](https://github.com/arkhipov/temporal_tables)
- [Temporal Tables (PgSQL Based)](https://github.com/nearform/temporal_tables)

These extensions allow creating a second table for data auditing that allows us to perform time travel reads.

1) The audit table should have all of the same data columns as the main table
    - It can be indexed in any way you see fit (all of our queries to this table are inherently timestamp column based)
2) These plugins use the tstzrange column to establish a timestamp bound for the validity of each row
    - The Trino Postgres plugin does not support these
    - To work in Trino, you must create a view for the "current" table and "audit" (or history) table which extracts the
      range column to two simple timestamp columns called `tsstart` and `tsend`
        - You can see an example of how to do this
          in [DataPopulatorIntegrationTest](dataharness-server/src/test/java/org/dataharness/bootstrap/DataPopulatorIntegrationTest.java)

## Contributing Guide

TODO
